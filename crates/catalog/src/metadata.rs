//! The metadata backend: where the store's relations live.
//!
//! The glossary, the declarations (functions, aspects, witnesses,
//! sources, relationships) and the measurements are each one Iceberg v3
//! table; [`IcebergMetadata`] is the seam they cross the
//! lake through — scan history, append rows. Named for what it holds:
//! the workspace's metadata, as opposed to the data plane the recipes
//! land into. Three decisions make this smaller than it looks:
//!
//! - **Supersession is not ours to implement.** Iceberg v3 row lineage
//!   supplies `_last_updated_sequence_number` (the commit that last
//!   touched the row) and `_pos` (its position in the file); together
//!   they are a total order over writes, assigned by the catalog with no
//!   coordination between writers and nothing for us to mint
//!   (apache/iceberg-rust#2966).
//! - **The dataset is a key column, and the format partitions by it.**
//!   A workspace holds many datasets, so a relation about a dataset's
//!   subjects carries a `dataset` column and declares it as its
//!   partition key — separate files per dataset and pruning on a
//!   dataset filter are the format's own feature, not a namespace
//!   convention of ours. [`Lake::append_batches`] splits an append by
//!   partition value and writes one file per value. One table, one scan,
//!   one append; `(seq, pos)` totally ordered with no caveat.
//! - **Every column is a string.** The relations already read back as
//!   text through `relation_rows`, and the rules parse what they need.
//!   A typed schema would be a second place for the shape to live.
//!
//! Metadata columns are readable through **iceberg-rust's own scan**, not
//! through iceberg-datafusion's SQL surface, which is why this reads with
//! `table.scan()`. It writes with [`Lake::append_batches`], the one write
//! path into an Iceberg table — a store relation is an Iceberg table like
//! any other, and nothing about a row of metadata needs the engine to put
//! it there.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};

use crate::Lake;

/// One stored row: its cells in the relation's declared column order,
/// and what ordered the write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub cells: Vec<Option<String>>,
    /// The write's position in the store's total order — the commit's
    /// data sequence number and the row's position in its file, which
    /// together order writes inside one commit as well as across them.
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
            // The lineage columns ARE the supersession order — a
            // missing or unreadable value silently kept an arbitrary
            // row, so absence is an error, never a zero.
            let num = |name: &str, r: usize| -> crate::Result<i64> {
                let i = batch.schema().index_of(name).map_err(|e| {
                    crate::Error::Workspace(format!("{relation}: no `{name}` in the scan: {e}"))
                })?;
                datafusion::arrow::compute::cast(
                    batch.column(i),
                    &datafusion::arrow::datatypes::DataType::Int64,
                )
                .ok()
                .and_then(|a| {
                    a.as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        .filter(|a| !a.is_null(r))
                        .map(|a| a.value(r))
                })
                .ok_or_else(|| {
                    crate::Error::Workspace(format!(
                        "{relation}: `{name}` does not read as a row-lineage number"
                    ))
                })
            };
            for r in 0..batch.num_rows() {
                out.push(Row::new(
                    (0..spec.columns.len()).map(|i| text(i, r)).collect(),
                    (num(SEQ, r)?, num(POS, r)?),
                ));
            }
        }
        Ok(out)
    }

    /// Create the table at v3 if it is not there yet. Called from the
    /// append path only — a read finding no table reads an empty
    /// relation.
    ///
    /// The version rides the create rather than a second transaction
    /// upgrading it, and that is a correctness requirement, not tidiness:
    /// between a v2 create and its upgrade the table exists at v2, and a
    /// concurrent writer that loads it there writes a snapshot with no
    /// row range — which the catalog then refuses, because by the time it
    /// applies, the table is v3 and v3 requires one.
    async fn ensure_table(&self, spec: &RelationSpec) -> crate::Result<()> {
        let ident = self.ident(spec.name);
        let catalog = self.lake.catalog();
        if catalog.table_exists(&ident).await? {
            return Ok(());
        }
        self.lake
            .ensure_namespace(&self.namespace, HashMap::new())
            .await?;
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
            .format_version(FormatVersion::V3)
            .properties(HashMap::new());
        let creation = if spec.partition.is_empty() {
            builder.build()
        } else {
            let mut partition = UnboundPartitionSpec::builder();
            for name in spec.partition {
                // Field ids are 1-based positions in the declared order.
                let source = spec.columns.iter().position(|c| c == name).ok_or_else(|| {
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
        if let Err(e) = catalog
            .create_table(&NamespaceIdent::new(self.namespace.clone()), creation)
            .await
        {
            // Another writer may have created it between the check above
            // and this call. What this function promises is that the
            // relation exists when it returns, not that we are the one
            // who made it so — so the question a failure raises is
            // whether it exists now, which is also the only question
            // whose answer does not depend on how a backend spells its
            // refusal.
            if !catalog.table_exists(&ident).await? {
                return Err(e.into());
            }
        }
        self.lake.invalidate_provider();
        Ok(())
    }
}

/// Deliberately three methods: anything more is a rule, and rules live
/// in `glossql-glossary`. `scan` hands back **history**, not the
/// current view — supersession is `rules::latest_by` applied on top.
/// `append` adds rows — replacement is a later row, never an update, so
/// nothing here mutates. A scan of a relation nothing has written is
/// empty, never an act: reads never write, so tables are created by the
/// first append alone.
impl IcebergMetadata {
    /// Every row ever written to the relation, in no guaranteed order —
    /// callers order by [`Row::seq`] because that is the rule.
    pub async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>> {
        self.scan_filtered(relation, None).await
    }

    /// The rows whose `column` equals `value` — the predicate pushed into
    /// the format's own scan, so a big relation's history is not read to
    /// serve one key.
    pub async fn scan_where(
        &self,
        relation: &str,
        column: &str,
        value: &str,
    ) -> crate::Result<Vec<Row>> {
        self.scan_filtered(
            relation,
            Some(
                iceberg::expr::Reference::new(column).equal_to(iceberg::spec::Datum::string(value)),
            ),
        )
        .await
    }

    /// Append rows as one write. Ordering inside one append is by
    /// position, so a caller that appends two rows sharing a supersession
    /// key gets the later one.
    pub async fn append(
        &self,
        relation: &str,
        rows: Vec<Vec<Option<String>>>,
    ) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let spec = self.spec(relation)?.clone();
        self.ensure_table(&spec).await?;
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
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        // Column order is the contract: the batch is built from
        // `spec.columns` and so was the Iceberg schema in `ensure_table`,
        // so the two line up field for field, which is what the
        // partition split reads its source column through.
        self.lake
            .append_batches(&self.namespace, relation, &[batch], HashMap::new())
            .await
    }
}
