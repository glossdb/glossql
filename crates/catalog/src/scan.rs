//! The read side: a landed table at one version is its file list, and
//! the scan over it is the engine's own parquet scan — a
//! `DataSourceExec` over a `FileScanConfig`, the one leaf the physical
//! optimizer pushes filters into, dynamic ones included. Nothing is
//! listed or fetched at plan time: the paths and sizes come from the
//! catalog rows, and the footers are read by the scan as it runs.
//!
//! Beside it the mounted catalog: a `CatalogProvider` over every
//! dataset, each a `SchemaProvider` over its tables at the version the
//! mount was built at — what `information_schema` and a
//! dataset-qualified name resolve through.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::Result as DFResult;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;

/// One table at one version: its schema and its files, scanned by the
/// engine. Filters are offered as inexact: the parquet source prunes
/// row groups and pages by them and the engine keeps the filter above.
#[derive(Debug)]
pub struct FilesTable {
    schema: SchemaRef,
    store: ObjectStoreUrl,
    files: Vec<PartitionedFile>,
    format: Arc<ParquetFormat>,
}

impl FilesTable {
    /// `files` are `(object path, size)` under the store `store` keys.
    pub(crate) fn new(schema: SchemaRef, store: ObjectStoreUrl, files: Vec<(String, u64)>) -> Self {
        FilesTable {
            schema,
            store,
            files: files
                .into_iter()
                .map(|(path, size)| PartitionedFile::new(path, size))
                .collect(),
            format: Arc::new(ParquetFormat::default()),
        }
    }
}

#[async_trait]
impl TableProvider for FilesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.files.is_empty() {
            let projected = datafusion::physical_plan::project_schema(&self.schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(projected)));
        }
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(self.store.clone(), source)
            .with_file_group(FileGroup::new(self.files.clone()))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        self.format.create_physical_plan(state, config).await
    }
}

/// A grounding's view as the mount lists it: its definition and the
/// schema its tag carries, of type View, so `information_schema` and
/// `SHOW TABLES` see it beside the tables. The mount has no session
/// to plan the definition with — it may name a door — so a statement
/// bound to the dataset never scans this: its pre-pass plans the
/// definition at the statement's pins and serves the plan as a
/// `ViewTable` under the name. A reader outside the binding is told
/// that road.
#[derive(Debug)]
pub struct ViewStub {
    dataset: String,
    name: String,
    definition: String,
    schema: SchemaRef,
}

impl ViewStub {
    pub(crate) fn new(dataset: &str, name: &str, definition: &str, schema: SchemaRef) -> Self {
        ViewStub {
            dataset: dataset.to_string(),
            name: name.to_string(),
            definition: definition.to_string(),
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for ViewStub {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.definition)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::Plan(format!(
            "`{}` is a grounding of `{}`: it serves under `USE {}`, as `{}` or read.{}()",
            self.name, self.dataset, self.dataset, self.name, self.name
        )))
    }
}

/// One dataset's tables and views at the mount's version.
#[derive(Debug)]
pub struct DatasetSchema {
    tables: HashMap<String, Arc<dyn TableProvider>>,
    views: HashMap<String, Arc<dyn TableProvider>>,
}

impl DatasetSchema {
    pub(crate) fn new(
        tables: HashMap<String, Arc<dyn TableProvider>>,
        views: HashMap<String, Arc<dyn TableProvider>>,
    ) -> Self {
        DatasetSchema { tables, views }
    }

    /// The dataset's views: each name to its definition.
    pub fn views(&self) -> HashMap<String, String> {
        self.views
            .iter()
            .map(|(name, view)| {
                (
                    name.clone(),
                    view.get_table_definition().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }
}

#[async_trait]
impl SchemaProvider for DatasetSchema {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .keys()
            .chain(self.views.keys())
            .cloned()
            .collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .tables
            .get(name)
            .or_else(|| self.views.get(name))
            .cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name) || self.views.contains_key(name)
    }
}

/// The mounted catalog: every dataset as a schema, built from the
/// catalog rows at one moment and shared by every session until a
/// commit moves the generation.
#[derive(Debug)]
pub struct Mount {
    schemas: HashMap<String, Arc<DatasetSchema>>,
}

impl Mount {
    pub(crate) fn new(schemas: HashMap<String, Arc<DatasetSchema>>) -> Self {
        Mount { schemas }
    }

    /// A dataset's views at the mount's version, each name to its
    /// definition; empty for a dataset the mount does not hold.
    pub fn views(&self, dataset: &str) -> HashMap<String, String> {
        self.schemas
            .get(dataset)
            .map(|s| s.views())
            .unwrap_or_default()
    }
}

impl CatalogProvider for Mount {
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}
