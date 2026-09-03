//! Filter pushdown declined for nested columns, at the one seam the
//! engine consults before pushing.
//!
//! iceberg-rust evaluates a pushed predicate by binding each column
//! reference to a field accessor, and accessors exist for primitive
//! fields alone (iceberg-0.10.1 `spec/schema/mod.rs:189`,
//! `build_accessors`): `WHERE tags IS NULL` on a list column binds to
//! nothing and the scan fails at its first poll (`expr/term.rs:329`,
//! "Accessor for Field … not found"). iceberg-datafusion pushes every
//! filter as `Inexact` and leaves the dropping to the scanner
//! (`table/mod.rs:334`), which is too late for this one. A filter
//! declined here stays in the engine's own `FilterExec` over the
//! scanned rows, where a predicate on a list belongs.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, Result, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

/// A table provider that answers as the one it wraps, except that a
/// filter naming a nested column is never pushed down.
#[derive(Debug)]
pub struct PrimitivePushdown {
    inner: Arc<dyn TableProvider>,
}

impl PrimitivePushdown {
    pub fn wrap(inner: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        Arc::new(PrimitivePushdown { inner })
    }
}

#[async_trait]
impl TableProvider for PrimitivePushdown {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let schema = self.inner.schema();
        let names_nested = |filter: &Expr| {
            filter.column_refs().iter().any(|c| {
                schema
                    .field_with_name(&c.name)
                    .is_ok_and(|f| f.data_type().is_nested())
            })
        };
        let mut answers = self.inner.supports_filters_pushdown(filters)?;
        for (answer, filter) in answers.iter_mut().zip(filters) {
            if names_nested(filter) {
                *answer = TableProviderFilterPushDown::Unsupported;
            }
        }
        Ok(answers)
    }
}
