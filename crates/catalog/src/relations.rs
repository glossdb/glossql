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
use glossql_glossary::{Relations, Row};
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{NamespaceIdent, TableCreation, TableIdent};

use crate::Lake;

/// `_last_updated_sequence_number` and `_pos`, the two halves of the
/// write order. Named here rather than spelled at each use.
const SEQ: &str = "_last_updated_sequence_number";
const POS: &str = "_pos";

/// One namespace's relations. `glossql` holds what reads workspace-wide;
/// `<dataset>_meta` holds a dataset's own record (2026-08-16 §5 — paired
/// namespaces so a REST catalog can grant a dataset and its record as a
/// unit).
pub struct IcebergRelations {
    lake: Lake,
    namespace: String,
    /// Column order per relation, from the store's own `RELATIONS`.
    columns: HashMap<String, Vec<String>>,
    ctx: SessionContext,
}

impl std::fmt::Debug for IcebergRelations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRelations")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

fn arrow_schema(columns: &[String]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(c, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

impl IcebergRelations {
    /// Open (creating on first use) the namespace and mount it.
    pub async fn open(
        lake: Lake,
        namespace: &str,
        relations: &[(&str, &[&str])],
    ) -> crate::Result<Self> {
        lake.ensure_namespace(namespace).await?;
        let this = IcebergRelations {
            lake,
            namespace: namespace.to_string(),
            columns: relations
                .iter()
                .map(|(n, c)| (n.to_string(), c.iter().map(|s| s.to_string()).collect()))
                .collect(),
            ctx: SessionContext::new(),
        };
        this.mount().await?;
        Ok(this)
    }

    /// (Re)mount the namespace into this instance's context.
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

    fn ident(&self, relation: &str) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(self.namespace.clone()),
            relation.to_string(),
        )
    }

    fn columns_of(&self, relation: &str) -> crate::Result<&[String]> {
        self.columns
            .get(relation)
            .map(Vec::as_slice)
            .ok_or_else(|| crate::Error::Workspace(format!("no relation `{relation}`")))
    }

    /// Create the table at v3 if it is not there yet. `format-version` is
    /// a reserved property and cannot be set at create (spike 7), so the
    /// upgrade is a transaction of its own.
    async fn ensure_table(&self, relation: &str) -> crate::Result<iceberg::table::Table> {
        let ident = self.ident(relation);
        let catalog = self.lake.catalog();
        if !catalog.table_exists(&ident).await? {
            let columns = self.columns_of(relation)?;
            let fields = columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    Arc::new(NestedField::optional(
                        i as i32 + 1,
                        c,
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
                .create_table(&NamespaceIdent::new(self.namespace.clone()), creation)
                .await?;
            Transaction::new(&table)
                .upgrade_table_version()
                .set_format_version(FormatVersion::V3)
                .apply(Transaction::new(&table))?
                .commit(catalog.as_ref())
                .await?;
            self.lake.invalidate_provider();
            // The context mounted this namespace before the table existed;
            // remount so the insert path can see it.
            self.mount().await?;
        }
        Ok(catalog.load_table(&ident).await?)
    }
}

#[async_trait::async_trait]
impl Relations for IcebergRelations {
    async fn scan(&self, relation: &str) -> glossql_glossary::Result<Vec<Row>> {
        let columns = self
            .columns_of(relation)
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?
            .to_vec();
        let table = self
            .ensure_table(relation)
            .await
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        if table.metadata().current_snapshot().is_none() {
            return Ok(Vec::new());
        }
        // The ordering columns ride the projection: the rule reads them,
        // the caller never sees them.
        let mut select: Vec<String> = columns.clone();
        select.push(SEQ.into());
        select.push(POS.into());
        let scan = table
            .scan()
            .select(select)
            .build()
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let mut stream = scan
            .to_arrow()
            .await
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
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

    async fn append(
        &self,
        relation: &str,
        rows: Vec<Vec<Option<String>>>,
    ) -> glossql_glossary::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let columns = self
            .columns_of(relation)
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?
            .to_vec();
        self.ensure_table(relation)
            .await
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let schema = arrow_schema(&columns);
        let arrays = (0..columns.len())
            .map(|i| {
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.get(i).cloned().flatten())
                        .collect::<Vec<_>>(),
                )) as datafusion::arrow::array::ArrayRef
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let staged = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let stage = format!("__append_{relation}");
        let _ = self.ctx.deregister_table(stage.as_str());
        self.ctx
            .register_table(stage.as_str(), Arc::new(staged))
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        let sql = format!(
            "INSERT INTO {ns}.{relation} SELECT * FROM {stage}",
            ns = self.namespace
        );
        let out = self.ctx.sql(&sql).await;
        let _ = self.ctx.deregister_table(stage.as_str());
        out.map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?
            .collect()
            .await
            .map_err(|e| glossql_glossary::Error::Backend(e.to_string()))?;
        Ok(())
    }
}
