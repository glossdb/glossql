//! The store's relations, on Iceberg v3.
//!
//! Stage 3 of `reports/2026-08-17-the-foundation.md` §6. Two things make
//! this smaller than it looks:
//!
//! - **Supersession is not ours to implement.** Iceberg v3 row lineage
//!   supplies `_last_updated_sequence_number` (the commit that last
//!   touched the row) and `_pos` (its position in the file); together
//!   they are a total order over writes, assigned by the catalog with no
//!   coordination between writers and nothing for us to mint (spike 7,
//!   2026-08-17, apache/iceberg-rust#2966).
//! - **Every column is a string.** The relations already read back as
//!   text through `relation_rows`, and the rules parse what they need.
//!   A typed schema would be a second place for the shape to live.
//!
//! Where a relation lives follows 2026-08-16 §5: workspace-wide
//! relations (`sources`, `aspects`, `functions`, `witnesses`) in the
//! `glossql` namespace; dataset-scoped ones (`relationships`, later
//! `glossary`) in that dataset's `<dataset>_meta` namespace, so a REST
//! catalog can grant a dataset and its record as a unit. The namespace
//! carries the dataset — the stored table has no `dataset` column,
//! because a column restating the namespace is a place for the two to
//! disagree.
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
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
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
    /// (spike 7, 2026-08-17). Sequence numbers are per table, so two
    /// rows compare only when they live in one table — which every
    /// supersession key guarantees, because the dataset is part of the
    /// key and the dataset picks the table.
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
    /// Dataset-scoped relations lead with a `dataset` column and store
    /// under `<dataset>_meta`; the namespace carries that column, the
    /// table does not.
    pub dataset_scoped: bool,
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
pub trait Relations: Send + Sync + std::fmt::Debug {
    /// Every row ever written to the relation, in no guaranteed order —
    /// callers order by [`Row::seq`] because that is the rule.
    async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>>;

    /// Append rows as one write. Ordering inside one append is by
    /// position, so a caller that appends two rows sharing a supersession
    /// key gets the later one — see the batching ruling of 2026-08-17.
    async fn append(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> crate::Result<()>;
}

/// `_last_updated_sequence_number` and `_pos`, the two halves of the
/// write order. Named here rather than spelled at each use.
const SEQ: &str = "_last_updated_sequence_number";
const POS: &str = "_pos";

/// The suffix pairing a dataset with its record namespace (ruled
/// 2026-08-11, S3-metadata precedent; layout 2026-08-16 §5).
const META: &str = "_meta";

fn meta_namespace(dataset: &str) -> String {
    format!("{dataset}{META}")
}

/// The dataset a `<dataset>_meta` namespace records, `None` for any
/// other namespace.
fn dataset_of(namespace: &str) -> Option<&str> {
    namespace.strip_suffix(META).filter(|d| !d.is_empty())
}

/// The relations seam over a workspace's lake. Workspace-wide relations
/// live in one namespace; dataset-scoped ones fan out to `<dataset>_meta`
/// and fan back in at scan with the dataset injected from the namespace.
pub struct IcebergRelations {
    lake: Lake,
    workspace: String,
    specs: HashMap<String, RelationSpec>,
    ctx: SessionContext,
}

impl std::fmt::Debug for IcebergRelations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRelations")
            .field("workspace", &self.workspace)
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

impl IcebergRelations {
    pub async fn open(
        lake: Lake,
        workspace: &str,
        relations: &[RelationSpec],
    ) -> crate::Result<Self> {
        Ok(IcebergRelations {
            lake,
            workspace: workspace.to_string(),
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

    /// The columns the lake table itself holds: a dataset-scoped
    /// relation's leading `dataset` column rides the namespace instead.
    fn stored_columns<'a>(spec: &'a RelationSpec) -> crate::Result<&'a [&'static str]> {
        if !spec.dataset_scoped {
            return Ok(spec.columns);
        }
        match spec.columns.first() {
            Some(&"dataset") => Ok(&spec.columns[1..]),
            _ => Err(crate::Error::Workspace(format!(
                "`{}` is dataset-scoped but does not lead with a dataset column",
                spec.name
            ))),
        }
    }

    /// Mount one namespace into this instance's context so the insert
    /// path can address its tables.
    async fn mount(&self, namespace: &str) -> crate::Result<()> {
        let provider = self.lake.provider().await?;
        let schema = provider.schema(namespace).ok_or_else(|| {
            crate::Error::Workspace(format!("namespace {namespace} did not mount"))
        })?;
        self.ctx
            .catalog("datafusion")
            .expect("default catalog")
            .register_schema(namespace, schema)
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        Ok(())
    }

    /// Create the table at v3 if it is not there yet. `format-version` is
    /// a reserved property and cannot be set at create (spike 7), so the
    /// upgrade is a transaction of its own. Called from the append path
    /// only — a read finding no table reads an empty relation.
    async fn ensure_table(
        &self,
        namespace: &str,
        relation: &str,
        columns: &[&str],
    ) -> crate::Result<()> {
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            relation.to_string(),
        );
        let catalog = self.lake.catalog();
        if catalog.table_exists(&ident).await? {
            return Ok(());
        }
        self.lake.ensure_namespace(namespace).await?;
        let fields = columns
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
        let creation = TableCreation::builder()
            .name(relation.to_string())
            .schema(IcebergSchema::builder().with_fields(fields).build()?)
            .properties(HashMap::new())
            .build();
        let table = catalog
            .create_table(&NamespaceIdent::new(namespace.to_string()), creation)
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

    /// Every row of one lake table, cells in `columns` order plus the
    /// write order from the metadata columns.
    async fn scan_table(
        &self,
        namespace: &str,
        relation: &str,
        columns: &[&str],
    ) -> crate::Result<Vec<Row>> {
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            relation.to_string(),
        );
        let catalog = self.lake.catalog();
        if !catalog.table_exists(&ident).await? {
            return Ok(Vec::new());
        }
        let table = catalog.load_table(&ident).await?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        // The ordering columns ride the projection: the rule reads them,
        // the caller never sees them.
        let mut select: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        select.push(SEQ.into());
        select.push(POS.into());
        let scan = table.scan().select(select).build()?;
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
                    (0..columns.len()).map(|i| text(i, r)).collect(),
                    (num(SEQ, r), num(POS, r)),
                ));
            }
        }
        Ok(out)
    }

    /// Append one batch of already-stripped rows to one lake table.
    async fn append_to(
        &self,
        namespace: &str,
        relation: &str,
        columns: &[&str],
        rows: &[&Vec<Option<String>>],
        skip: usize,
    ) -> crate::Result<()> {
        self.ensure_table(namespace, relation, columns).await?;
        self.mount(namespace).await?;
        let schema = arrow_schema(columns);
        let arrays = (0..columns.len())
            .map(|i| {
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.get(i + skip).cloned().flatten())
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
        let sql = format!("INSERT INTO {namespace}.{relation} SELECT * FROM {stage}");
        let out = self.ctx.sql(&sql).await;
        let _ = self.ctx.deregister_table(stage.as_str());
        out.map_err(|e| crate::Error::Workspace(e.to_string()))?
            .collect()
            .await
            .map_err(|e| crate::Error::Workspace(e.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Relations for IcebergRelations {
    async fn scan(&self, relation: &str) -> crate::Result<Vec<Row>> {
        let spec = self.spec(relation)?.clone();
        let stored = Self::stored_columns(&spec)?;
        if !spec.dataset_scoped {
            return self.scan_table(&self.workspace, relation, stored).await;
        }
        // Fan back in: one table per dataset, the dataset read off the
        // namespace name rather than a stored column.
        let mut out = Vec::new();
        for ns in self.lake.catalog().list_namespaces(None).await? {
            let parts: &Vec<String> = ns.as_ref();
            let [name] = parts.as_slice() else { continue };
            let Some(dataset) = dataset_of(name) else {
                continue;
            };
            for mut row in self.scan_table(name, relation, stored).await? {
                row.cells.insert(0, Some(dataset.to_string()));
                out.push(row);
            }
        }
        Ok(out)
    }

    async fn append(&self, relation: &str, rows: Vec<Vec<Option<String>>>) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let spec = self.spec(relation)?.clone();
        let stored = Self::stored_columns(&spec)?;
        if !spec.dataset_scoped {
            let all: Vec<&Vec<Option<String>>> = rows.iter().collect();
            return self
                .append_to(&self.workspace, relation, stored, &all, 0)
                .await;
        }
        // Fan out by the leading dataset cell; each dataset's rows are
        // one write to its own `<dataset>_meta` table.
        let mut by_dataset: HashMap<String, Vec<&Vec<Option<String>>>> = HashMap::new();
        for row in &rows {
            let Some(Some(dataset)) = row.first() else {
                return Err(crate::Error::Workspace(format!(
                    "a `{relation}` row arrived without its dataset"
                )));
            };
            by_dataset.entry(dataset.clone()).or_default().push(row);
        }
        for (dataset, group) in by_dataset {
            self.append_to(&meta_namespace(&dataset), relation, stored, &group, 1)
                .await?;
        }
        Ok(())
    }
}
