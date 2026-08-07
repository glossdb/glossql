//! The workspace data plane: iceberg-rust behind the `Catalog` trait
//! (SPEC.md §3, reports/2026-08-03-poc-substrate.md).
//!
//! One `Lake` per workspace: a SQL catalog on a SQLite file plus a local
//! warehouse directory. Datasets are namespaces. Tables are created and
//! written through iceberg-datafusion's own front door — the session mounts
//! [`IcebergCatalogProvider`] schemas and materializes recipes with
//! `SchemaProvider::register_table` + `INSERT INTO`; this crate only opens
//! the catalog and answers the questions DataFusion cannot (namespaces,
//! snapshot ids).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
pub use iceberg_datafusion::IcebergCatalogProvider;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("workspace data plane: {0}")]
    Workspace(String),
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
}

/// The workspace's Iceberg side: catalog + warehouse.
#[derive(Debug, Clone)]
pub struct Lake {
    catalog: Arc<dyn Catalog>,
    warehouse: PathBuf,
    /// The one mounted representation of the lake, shared by every
    /// session — `provider()` hands out Arc clones of it. A namespace
    /// create invalidates it (the namespace list is the one thing
    /// [`IcebergCatalogProvider`] freezes); table lookups inside a
    /// namespace go to the catalog live and need no rebuild.
    provider: Arc<std::sync::RwLock<Option<Arc<IcebergCatalogProvider>>>>,
}

impl Lake {
    /// Open (creating on first use) the workspace data plane. Must be called
    /// inside a multi-thread tokio runtime — the catalog and the providers
    /// built on it block in place for their async work.
    pub async fn open(catalog_db: &Path, warehouse: &Path) -> Result<Self> {
        std::fs::create_dir_all(warehouse)
            .map_err(|e| Error::Workspace(format!("warehouse dir {}: {e}", warehouse.display())))?;
        let warehouse = warehouse
            .canonicalize()
            .map_err(|e| Error::Workspace(format!("warehouse dir {}: {e}", warehouse.display())))?;
        if let Some(parent) = catalog_db.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Workspace(format!("catalog dir {}: {e}", parent.display())))?;
        }
        if !catalog_db.exists() {
            // sqlx's sqlite URL opens read-write but does not create; an
            // empty file is a valid empty database.
            std::fs::File::create(catalog_db).map_err(|e| {
                Error::Workspace(format!("catalog db {}: {e}", catalog_db.display()))
            })?;
        }
        let catalog = SqlCatalogBuilder::default()
            .uri(format!("sqlite:{}", catalog_db.display()))
            .warehouse_location(warehouse.display().to_string())
            .sql_bind_style(SqlBindStyle::QMark)
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("glossql", HashMap::new())
            .await?;
        Ok(Lake {
            catalog: Arc::new(catalog),
            warehouse,
            provider: Arc::new(std::sync::RwLock::new(None)),
        })
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    pub fn warehouse(&self) -> &Path {
        &self.warehouse
    }

    /// Create the dataset's namespace if it is missing; `true` = created.
    /// A create invalidates the shared provider — the next `provider()`
    /// rebuilds over the current namespace list.
    pub async fn ensure_namespace(&self, dataset: &str) -> Result<bool> {
        let ns = NamespaceIdent::new(dataset.to_string());
        if self.catalog.namespace_exists(&ns).await? {
            return Ok(false);
        }
        self.catalog.create_namespace(&ns, HashMap::new()).await?;
        self.invalidate_provider();
        Ok(true)
    }

    /// The shared catalog provider — built over the current namespace
    /// list on first touch, then an Arc clone for every caller. Two
    /// concurrent first touches may both build; either result is valid
    /// and one wins the slot.
    pub async fn provider(&self) -> Result<Arc<IcebergCatalogProvider>> {
        if let Some(shared) = self.provider.read().expect("provider lock").as_ref() {
            return Ok(Arc::clone(shared));
        }
        let built = Arc::new(IcebergCatalogProvider::try_new(self.catalog()).await?);
        *self.provider.write().expect("provider lock") = Some(Arc::clone(&built));
        Ok(built)
    }

    /// Forget the cached provider. Callers that miss a namespace another
    /// writer just created invalidate and touch again.
    pub fn invalidate_provider(&self) {
        *self.provider.write().expect("provider lock") = None;
    }

    /// Current snapshot id of `dataset.table`; `None` when the table does
    /// not exist (a subject may be glossed before its recipe lands) or has
    /// no snapshot yet.
    pub async fn snapshot_id(&self, dataset: &str, table: &str) -> Result<Option<i64>> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        if !self.catalog.table_exists(&ident).await? {
            return Ok(None);
        }
        let table = self.catalog.load_table(&ident).await?;
        Ok(table.metadata().current_snapshot_id())
    }

    /// Data tables in the dataset's namespace.
    pub async fn table_names(&self, dataset: &str) -> Result<Vec<String>> {
        let ns = NamespaceIdent::new(dataset.to_string());
        Ok(self
            .catalog
            .list_tables(&ns)
            .await?
            .into_iter()
            .map(|t| t.name)
            .collect())
    }

    pub async fn table_exists(&self, dataset: &str, table: &str) -> Result<bool> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        Ok(self.catalog.table_exists(&ident).await?)
    }


    /// Column names of `dataset.table`, empty when the table is missing —
    /// the session's disclosure grid enumerates them as subjects.
    pub async fn table_columns(&self, dataset: &str, table: &str) -> Result<Vec<String>> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        if !self.catalog.table_exists(&ident).await? {
            return Ok(Vec::new());
        }
        let table = self.catalog.load_table(&ident).await?;
        Ok(table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect())
    }
}
