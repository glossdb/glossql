//! The metadata backend: where the store's relations live.
//!
//! The glossary, the declarations (functions, aspects, witnesses,
//! sources, relationships) and the measurements are each one Iceberg v3
//! table; [`MetadataBackend`] is the two-method seam they cross the
//! lake through — scan history, append rows. Named for what it holds:
//! the workspace's metadata, as opposed to the data plane the recipes
//! land into. (It was called `Relations` until 2026-08-18, which
//! collided with the `relationships` relation and read as a collection
//! rather than a backend.)
//!
//! Stage 3 of `reports/2026-08-17-the-foundation.md` §6. Three decisions
//! make this smaller than it looks:
//!
//! - **Supersession is not ours to implement.** Iceberg v3 row lineage
//!   supplies `_last_updated_sequence_number` (the commit that last
//!   touched the row) and `_pos` (its position in the file); together
//!   they are a total order over writes, assigned by the catalog with no
//!   coordination between writers and nothing for us to mint (spike 7,
//!   2026-08-17, apache/iceberg-rust#2966).
//! - **The dataset is a key column, and the format partitions by it.**
//!   A workspace holds many datasets, so a relation about a dataset's
//!   subjects carries a `dataset` column and declares it as its
//!   partition key — separate files per dataset and pruning on a
//!   dataset filter are the format's own feature, not a namespace
//!   convention of ours. iceberg-datafusion's insert path projects
//!   partition values and fans out per partition
//!   (`integrations/datafusion/src/table/mod.rs:172-190`). One table,
//!   one scan, one append; `(seq, pos)` totally ordered with no caveat.
//! - **Every column is a string.** The relations already read back as
//!   text through `relation_rows`, and the rules parse what they need.
//!   A typed schema would be a second place for the shape to live.
//!
//! Metadata columns are readable through **iceberg-rust's own scan**, not
//! through iceberg-datafusion's SQL surface, which is why this reads with
//! `table.scan()` and writes through DataFusion's insert path.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};

use crate::Lake;

/// One stored row: its cells in the relation's declared column order,
/// and what ordered the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub cells: Vec<Option<String>>,
    /// The write's position in the store's total order — the commit's
    /// data sequence number and the row's position in its file, which
    /// together order writes inside one commit as well as across them
    /// (spike 7, 2026-08-17).
    pub seq: (i64, i64),
}

impl Row {
    pub fn new(cells: Vec<Option<String>>, seq: (i64, i64)) -> Self {
        Row { cells, seq }
    }

    /// A cell by position in the relation's column order.
    pub fn get(&self, i: usize) -> Option<&str> {
        self.cells.get(i).and_then(|c| c.as_deref())
    }
}

/// The shape of one relation the seam carries — declared by the store,
/// which owns the column order.
#[derive(Debug, Clone)]
pub struct RelationSpec {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    /// Columns the table is identity-partitioned by, each naming an
    /// entry of `columns`. The store declares `["dataset"]` on its
    /// dataset-keyed relations, so each dataset's rows land in their own
    /// files — the format's split, no classification of ours on top.
    pub partition: &'static [&'static str],
}

/// Everything a store backend must provide. Deliberately two methods:
/// anything more is a rule, and rules live in `glossql-glossary`.
///
/// `scan` hands back **history**, not the current view. Supersession is
/// `rules::latest_by` applied on top; a backend that filtered would be
/// reimplementing the rule, which is what the SQL
/// `NOT EXISTS ... n.id > g.id` was. `append` adds rows — replacement is
/// a later row, never an update, so nothing here mutates. A scan of a
/// relation nothing has written is empty, never an act: reads never
/// write, so tables are created by the first append alone.
#[async_trait::async_trait]
pub trait MetadataBackend: Send + Sync + std::fmt::Debug {
    /// Every row ever written to the relation, in no guaranteed order —
    /// callers order by [`Row::seq`] because that is the rule.
    async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>>;

    /// The rows whose `column` equals `value` — the predicate pushed into
    /// the format's own scan, so a big relation's history is not read to
    /// serve one key.
    async fn scan_where(&self, relation: &str, column: &str, value: &str)
    -> crate::Result<Vec<Row>>;

    /// Append rows as one write. Ordering inside one append is by
    /// position, so a caller that appends two rows sharing a supersession
    /// key gets the later one — see the batching ruling of 2026-08-17.
    async fn append(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> crate::Result<()>;
}

/// `_last_updated_sequence_number` and `_pos`, the two halves of the
/// write order. Named here rather than spelled at each use.
const SEQ: &str = "_last_updated_sequence_number";
const POS: &str = "_pos";

/// The relations seam over a workspace's lake: every crossed relation is
/// one table in the store's namespace.
pub struct IcebergMetadata {
    lake: Lake,
    namespace: String,
    specs: HashMap<String, RelationSpec>,
    ctx: SessionContext,
}

impl std::fmt::Debug for IcebergMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergMetadata")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

fn arrow_schema(columns: &[&str]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(*c, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

impl IcebergMetadata {
    pub async fn open(
        lake: Lake,
        namespace: &str,
        relations: &[RelationSpec],
    ) -> crate::Result<Self> {
        Ok(IcebergMetadata {
            lake,
            namespace: namespace.to_string(),
            specs: relations
                .iter()
                .map(|s| (s.name.to_string(), s.clone()))
                .collect(),
            ctx: SessionContext::new(),
        })
    }

    fn spec(&self, relation: &str) -> crate::Result<&RelationSpec> {
        self.specs
            .get(relation)
            .ok_or_else(|| crate::Error::Workspace(format!("no relation `{relation}`")))
    }

    fn ident(&self, relation: &str) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(self.namespace.clone()),
            relation.to_string(),
        )
    }

    /// Mount the store namespace into this instance's context so the
    /// insert path can address its tables.
    async fn mount(&self) -> crate::Result<()> {
        let provider = self.lake.provider().await?;
        let schema = provider.schema(&self.namespace).ok_or_else(|| {
            crate::Error::Workspace(format!("namespace {} did not mount", self.namespace))
        })?;
        self.ctx
            .catalog("datafusion")
            .expect("default catalog")
            .register_schema(&self.namespace, schema)
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        Ok(())
    }

    async fn scan_filtered(
        &self,
        relation: &str,
        filter: Option<iceberg::expr::Predicate>,
    ) -> crate::Result<Vec<Row>> {
        let spec = self.spec(relation)?.clone();
        let catalog = self.lake.catalog();
        let ident = self.ident(relation);
        if !catalog.table_exists(&ident).await? {
            return Ok(Vec::new());
        }
        let table = catalog.load_table(&ident).await?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        // The ordering columns ride the projection: the rule reads them,
        // the caller never sees them.
        let mut select: Vec<String> = spec.columns.iter().map(|c| c.to_string()).collect();
        select.push(SEQ.into());
        select.push(POS.into());
        let mut scan = table.scan().select(select);
        if let Some(filter) = filter {
            scan = scan.with_filter(filter);
        }
        let scan = scan.build()?;
        let mut stream = scan.to_arrow().await?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let text = |i: usize, r: usize| -> Option<String> {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .filter(|a| !a.is_null(r))
                    .map(|a| a.value(r).to_string())
            };
            let num = |name: &str, r: usize| -> i64 {
                batch
                    .schema()
                    .index_of(name)
                    .ok()
                    .and_then(|i| {
                        datafusion::arrow::compute::cast(
                            batch.column(i),
                            &datafusion::arrow::datatypes::DataType::Int64,
                        )
                        .ok()
                    })
                    .and_then(|a| {
                        a.as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .filter(|a| !a.is_null(r))
                            .map(|a| a.value(r))
                    })
                    .unwrap_or(0)
            };
            for r in 0..batch.num_rows() {
                out.push(Row::new(
                    (0..spec.columns.len()).map(|i| text(i, r)).collect(),
                    (num(SEQ, r), num(POS, r)),
                ));
            }
        }
        Ok(out)
    }

    /// Create the table at v3 if it is not there yet. `format-version` is
    /// a reserved property and cannot be set at create (spike 7), so the
    /// upgrade is a transaction of its own. Called from the append path
    /// only — a read finding no table reads an empty relation.
    async fn ensure_table(&self, spec: &RelationSpec) -> crate::Result<()> {
        let ident = self.ident(spec.name);
        let catalog = self.lake.catalog();
        if catalog.table_exists(&ident).await? {
            return Ok(());
        }
        self.lake.ensure_namespace(&self.namespace, HashMap::new()).await?;
        let fields = spec
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Arc::new(NestedField::optional(
                    i as i32 + 1,
                    *c,
                    Type::Primitive(PrimitiveType::String),
                ))
            })
            .collect::<Vec<_>>();
        let builder = TableCreation::builder()
            .name(spec.name.to_string())
            .schema(IcebergSchema::builder().with_fields(fields).build()?)
            .properties(HashMap::new());
        let creation = if spec.partition.is_empty() {
            builder.build()
        } else {
            let mut partition = UnboundPartitionSpec::builder();
            for name in spec.partition {
                // Field ids are 1-based positions in the declared order.
                let source = spec
                    .columns
                    .iter()
                    .position(|c| c == name)
                    .ok_or_else(|| {
                        crate::Error::Workspace(format!(
                            "`{}` partitions by `{name}`, which is not one of its columns",
                            spec.name
                        ))
                    })?;
                partition =
                    partition.add_partition_field(source as i32 + 1, *name, Transform::Identity)?;
            }
            builder.partition_spec(partition.build()).build()
        };
        let table = catalog
            .create_table(&NamespaceIdent::new(self.namespace.clone()), creation)
            .await?;
        Transaction::new(&table)
            .upgrade_table_version()
            .set_format_version(FormatVersion::V3)
            .apply(Transaction::new(&table))?
            .commit(catalog.as_ref())
            .await?;
        self.lake.invalidate_provider();
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetadataBackend for IcebergMetadata {
    async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>> {
        self.scan_filtered(relation, None).await
    }

    async fn scan_where(
        &self,
        relation: &str,
        column: &str,
        value: &str,
    ) -> crate::Result<Vec<Row>> {
        self.scan_filtered(
            relation,
            Some(
                iceberg::expr::Reference::new(column)
                    .equal_to(iceberg::spec::Datum::string(value)),
            ),
        )
        .await
    }

    async fn append(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let spec = self.spec(relation)?.clone();
        self.ensure_table(&spec).await?;
        self.mount().await?;
        let schema = arrow_schema(spec.columns);
        let arrays = (0..spec.columns.len())
            .map(|i| {
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.get(i).cloned().flatten())
                        .collect::<Vec<_>>(),
                )) as datafusion::arrow::array::ArrayRef
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        let staged = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        let stage = format!("__append_{relation}");
        let _ = self.ctx.deregister_table(stage.as_str());
        self.ctx
            .register_table(stage.as_str(), Arc::new(staged))
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        let sql = format!(
            "INSERT INTO {ns}.{relation} SELECT * FROM {stage}",
            ns = self.namespace
        );
        let out = self.ctx.sql(&sql).await;
        let _ = self.ctx.deregister_table(stage.as_str());
        out.map_err(|e| crate::Error::Workspace(e.to_string()))?
            .collect()
            .await
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        Ok(())
    }
}
