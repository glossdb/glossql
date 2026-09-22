//! The workspace data plane: landed tables as parquet files under a
//! warehouse, and the catalog of them — every table, its versions,
//! each version's files — as rows of the record's own database in the
//! shapes the DuckLake 1.0 specification names, so any DuckLake reader
//! attaches to the same database and reads the same files. The
//! engine's own parquet scan serves every read; the data keeps no
//! history: a file no version references is deleted.
//!
//! What lives here: the catalog tables (`tables`), the scan over a
//! version's files and the mounted catalog (`scan`), the record's
//! relations on the same database (`record`), the object store behind
//! the warehouse (`storage`). Facts about a landing — what it read,
//! what it dropped, the casts — are the record's; this crate keeps
//! files and versions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod record;
mod scan;
pub mod storage;
mod tables;

pub use record::{Db, Number, Record, RelationSpec, Row};
pub use scan::{FilesTable, Mount};
pub use tables::{LandedFile, Landing};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt as _};
use url::Url;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("workspace data plane: {0}")]
    Workspace(String),
    #[error("record: {0}")]
    Record(#[from] sqlx::Error),
    #[error("warehouse: {0}")]
    Store(#[from] object_store::Error),
    #[error(transparent)]
    Engine(#[from] datafusion::error::DataFusionError),
    #[error("parquet: {0}")]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
}

/// A landed table as a statement holds it: its name, its version, its
/// columns, and the provider that scans its files at that version.
pub struct PinnedTable {
    pub name: String,
    /// The version — the snapshot that last changed the table. `None`
    /// never happens for a landed table and stays an `Option` for the
    /// callers that key on absence.
    pub snapshot_id: Option<i64>,
    /// The table's columns in schema order — what can be glossed, and
    /// the same schema the provider beside them advertises.
    pub columns: Vec<String>,
    pub provider: Arc<dyn datafusion::catalog::TableProvider>,
}

/// What a write produced, before its commit.
#[derive(Debug)]
pub struct Written {
    pub rows: u64,
    files: Vec<LandedFile>,
}

/// The warehouse: where the files are, as the engine keys it.
#[derive(Debug)]
struct Warehouse {
    /// The root every path hangs under, with its trailing slash —
    /// `file:///…/warehouse/` or `gs://bucket/prefix/`.
    root: Url,
    /// The object path of the root within its store: `` or `prefix/`.
    prefix: String,
    /// The engine's key for the store: `scheme://authority/`.
    key: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
}

impl Warehouse {
    fn open(warehouse: &str) -> Result<Self> {
        match storage::Warehouse::parse(warehouse)? {
            storage::Warehouse::Local(dir) => {
                std::fs::create_dir_all(&dir).map_err(|e| {
                    Error::Workspace(format!("warehouse dir {}: {e}", dir.display()))
                })?;
                let dir = dir.canonicalize().map_err(|e| {
                    Error::Workspace(format!("warehouse dir {}: {e}", dir.display()))
                })?;
                let root = Url::from_directory_path(&dir).map_err(|()| {
                    Error::Workspace(format!("warehouse dir {} is not a URL", dir.display()))
                })?;
                Ok(Warehouse {
                    prefix: root.path().trim_start_matches('/').to_string(),
                    root,
                    key: ObjectStoreUrl::local_filesystem(),
                    store: Arc::new(LocalFileSystem::new()),
                })
            }
            storage::Warehouse::Remote(location) => {
                let (store, key) = storage::environment_store(&location)?;
                let root = Url::parse(&format!("{location}/")).map_err(|e| {
                    Error::Workspace(format!("warehouse `{location}` is not a URL: {e}"))
                })?;
                let mut prefix = root.path().trim_start_matches('/').to_string();
                if !prefix.is_empty() && !prefix.ends_with('/') {
                    prefix.push('/');
                }
                Ok(Warehouse {
                    prefix,
                    root,
                    key: ObjectStoreUrl::parse(key.as_str())?,
                    store,
                })
            }
        }
    }

    /// The object path of a table's directory.
    fn table_dir(&self, dataset: &str, table: &str) -> String {
        format!("{}{dataset}/{table}/", self.prefix)
    }
}

/// The data plane of one workspace: the catalog tables in the record's
/// database and the files under the warehouse. Cloned per access;
/// everything shared sits behind an `Arc`.
#[derive(Debug, Clone)]
pub struct Lake {
    db: Db,
    warehouse: Arc<Warehouse>,
    /// The one mounted representation of the catalog, shared by every
    /// session — `provider()` hands out Arc clones of it. Every commit
    /// invalidates it: a mount holds each table's file list at one
    /// version.
    mount: Arc<std::sync::RwLock<Option<Arc<Mount>>>>,
    /// Moved by every invalidation, so a build can tell whether the
    /// catalog changed while it ran.
    generation: Arc<AtomicU64>,
    /// How many times [`Lake::pin_dataset`] has walked a dataset — the
    /// tests hold the walk to one per statement. A mount build is not
    /// one: it is the catalog read whole, once per generation.
    walks: Arc<AtomicU64>,
    /// The URI the catalog was opened on. Carries credentials, which is
    /// why it is never logged whole.
    database: String,
    /// How long an ended file stays before a commit deletes it: a
    /// reader that pinned the file before the commit is still scanning
    /// it, and a scan has no lease. The data keeps no history — the
    /// grace is for the statement in flight, not for time travel.
    grace: std::time::Duration,
}

/// The default grace: longer than any statement runs.
const GRACE: std::time::Duration = std::time::Duration::from_secs(3600);

const MOUNT_BUILD_ATTEMPTS: usize = 5;

impl Lake {
    /// The laptop's shape: a SQLite file and a warehouse directory.
    pub async fn open(catalog_db: &Path, warehouse: &Path) -> Result<Self> {
        if let Some(parent) = catalog_db.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Workspace(format!("catalog dir {}: {e}", parent.display())))?;
        }
        Self::open_sql(
            &format!("sqlite:{}?mode=rwc", catalog_db.display()),
            &warehouse.display().to_string(),
        )
        .await
    }

    /// A deployment's shape: the catalog on a SQL server named by its
    /// URI (`sqlite:<file>` or `postgres://…`), the warehouse a
    /// directory or an object-store location.
    pub async fn open_sql(catalog_uri: &str, warehouse: &str) -> Result<Self> {
        let db = Db::connect(catalog_uri).await?;
        let warehouse = Warehouse::open(warehouse)?;
        tables::create(&db, warehouse.root.as_str()).await?;
        Ok(Lake {
            db,
            warehouse: Arc::new(warehouse),
            mount: Arc::new(std::sync::RwLock::new(None)),
            generation: Arc::new(AtomicU64::new(0)),
            walks: Arc::new(AtomicU64::new(0)),
            database: catalog_uri.to_string(),
            grace: GRACE,
        })
    }

    /// The same lake with another grace before ended files are deleted
    /// — the tests run with none.
    pub fn with_grace(mut self, grace: std::time::Duration) -> Self {
        self.grace = grace;
        self
    }

    /// The database the catalog tables and the record share.
    pub fn db(&self) -> Db {
        self.db.clone()
    }

    /// The catalog URI, credentials and all — never logged whole.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The warehouse's object store, registered under its key in the
    /// engine runtime a session scans with.
    pub fn register(&self, env: &RuntimeEnv) {
        env.register_object_store(
            self.warehouse.key.as_ref(),
            Arc::clone(&self.warehouse.store),
        );
    }

    // -- datasets ----------------------------------------------------------

    /// The dataset's schema row, created if absent; whether it was.
    pub async fn ensure_dataset(&self, name: &str) -> Result<bool> {
        let created = tables::ensure_schema(&self.db, name).await?;
        if created {
            self.invalidate_provider();
        }
        Ok(created)
    }

    pub async fn dataset_exists(&self, name: &str) -> Result<bool> {
        tables::schema_exists(&self.db, name).await
    }

    /// Every dataset, by name, sorted.
    pub async fn datasets(&self) -> Result<Vec<String>> {
        tables::schema_names(&self.db).await
    }

    pub async fn table_names(&self, dataset: &str) -> Result<Vec<String>> {
        tables::table_names(&self.db, dataset).await
    }

    pub async fn table_exists(&self, dataset: &str, table: &str) -> Result<bool> {
        Ok(self.version(dataset, table).await?.is_some())
    }

    /// The table's version — the snapshot that last changed it — or
    /// `None` when the dataset holds no such table.
    pub async fn version(&self, dataset: &str, table: &str) -> Result<Option<i64>> {
        let pinned = tables::pin(&self.db, dataset, Some(&[table.to_string()])).await?;
        Ok(pinned
            .and_then(|rows| rows.into_iter().next())
            .map(|t| t.version))
    }

    // -- writes ------------------------------------------------------------

    /// The rows written as one parquet file under the table's
    /// directory, nothing committed: the file joins the table through
    /// [`Lake::commit`], and a write whose commit never comes is a file
    /// the next commit on the table finds nothing referencing.
    pub async fn write(
        &self,
        dataset: &str,
        table: &str,
        schema: SchemaRef,
        mut rows: SendableRecordBatchStream,
    ) -> Result<Written> {
        let name = format!("{}.parquet", uuid::Uuid::now_v7());
        let path = object_store::path::Path::from(format!(
            "{}{name}",
            self.warehouse.table_dir(dataset, table)
        ));
        let span = tracing::info_span!("write", dataset, table, rows = tracing::field::Empty);
        let _enter = span.enter();
        drop(_enter);
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build();
        let sink = BufWriter::new(Arc::clone(&self.warehouse.store), path.clone());
        let mut writer = AsyncArrowWriter::try_new(sink, Arc::clone(&schema), Some(properties))?;
        let mut count: u64 = 0;
        while let Some(batch) = rows.next().await {
            let batch = batch?;
            count += batch.num_rows() as u64;
            writer.write(&batch).await?;
        }
        let metadata = writer.close().await?;
        let size = self.warehouse.store.head(&path).await?.size;
        // The footer's length sits in the four bytes before the magic,
        // and that number is the specification's `footer_size` — what
        // a reader checks against the file when it opens it.
        let trailer = self
            .warehouse
            .store
            .get_range(&path, size.saturating_sub(8)..size.saturating_sub(4))
            .await?;
        let footer = trailer
            .as_ref()
            .try_into()
            .map(u32::from_le_bytes)
            .map(i64::from)
            .unwrap_or(0);
        span.record("rows", count);
        Ok(Written {
            rows: count,
            files: vec![LandedFile {
                path: name,
                size,
                rows: metadata.file_metadata().num_rows(),
                footer,
            }],
        })
    }

    /// The written files joined to the table as one commit — created,
    /// replaced or appended — and the version that commit made. Files a
    /// replace ended are scheduled for deletion and deleted by a later
    /// commit's sweep, once the grace has passed.
    pub async fn commit(
        &self,
        dataset: &str,
        table: &str,
        schema: &Schema,
        written: Written,
        landing: Landing,
    ) -> Result<i64> {
        let span = tracing::info_span!(
            "commit",
            dataset,
            table,
            landing = ?landing,
            files = written.files.len(),
            rows = written.rows
        );
        let (version, _ended) = tracing::Instrument::instrument(
            tables::commit_landing(&self.db, dataset, table, schema, &written.files, landing),
            span,
        )
        .await?;
        self.invalidate_provider();
        self.sweep(self.grace).await;
        Ok(version)
    }

    /// The table ended, its files scheduled for deletion.
    pub async fn drop_table(&self, dataset: &str, table: &str) -> Result<()> {
        tables::drop_table(&self.db, dataset, table).await?;
        self.invalidate_provider();
        self.sweep(self.grace).await;
        Ok(())
    }

    /// Every file scheduled for deletion longer than `older_than` ago,
    /// deleted from the store and unscheduled; one the store keeps
    /// stays scheduled for the next sweep.
    pub async fn sweep(&self, older_than: std::time::Duration) {
        let before = chrono::DateTime::<chrono::Utc>::from(
            std::time::SystemTime::now()
                .checked_sub(older_than)
                .unwrap_or(std::time::UNIX_EPOCH),
        )
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let scheduled = match tables::scheduled(&self.db, &before).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!("the deletion schedule could not be read: {e}");
                return;
            }
        };
        for (file_id, dataset, table, relative) in scheduled {
            let path = object_store::path::Path::from(format!(
                "{}{relative}",
                self.warehouse.table_dir(&dataset, &table)
            ));
            match self.warehouse.store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    if let Err(e) = tables::unschedule(&self.db, file_id).await {
                        tracing::warn!(file = %path, "the deleted file stays scheduled: {e}");
                    }
                }
                Err(e) => tracing::warn!(file = %path, "not deleted, stays scheduled: {e}"),
            }
        }
    }

    // -- reads -------------------------------------------------------------

    /// Every table of the dataset, pinned at its current version — one
    /// walk per statement, and the walk is three queries on the
    /// catalog, whatever the table count. Nothing is fetched from the
    /// warehouse: the files' paths and sizes are rows.
    pub async fn pin_dataset(&self, dataset: &str) -> Result<Vec<PinnedTable>> {
        self.walks.fetch_add(1, Ordering::Relaxed);
        let span = tracing::info_span!("pin", dataset, tables = tracing::field::Empty);
        tracing::Instrument::instrument(self.walk(dataset, None), span)
            .await
            .map(|pinned| pinned.unwrap_or_default())
    }

    /// The named tables of `dataset`, each pinned at its current
    /// version; `None` when a name is not a table there, which is where
    /// a misspelling gets its hint from the whole walk.
    pub async fn pin_tables(
        &self,
        dataset: &str,
        names: &[String],
    ) -> Result<Option<Vec<PinnedTable>>> {
        self.walk(dataset, Some(names)).await
    }

    async fn walk(
        &self,
        dataset: &str,
        names: Option<&[String]>,
    ) -> Result<Option<Vec<PinnedTable>>> {
        let Some(rows) = tables::pin(&self.db, dataset, names).await? else {
            return Ok(None);
        };
        tracing::Span::current().record("tables", rows.len());
        Ok(Some(
            rows.into_iter().map(|t| self.pinned(dataset, t)).collect(),
        ))
    }

    fn pinned(&self, dataset: &str, rows: tables::TableRows) -> PinnedTable {
        let dir = self.warehouse.table_dir(dataset, &rows.name);
        let files = rows
            .files
            .iter()
            .map(|f| (format!("{dir}{}", f.path), f.size))
            .collect();
        let provider = Arc::new(FilesTable::new(
            Arc::clone(&rows.schema),
            self.warehouse.key.clone(),
            files,
        ));
        PinnedTable {
            name: rows.name,
            snapshot_id: Some(rows.version),
            columns: rows
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            provider,
        }
    }

    /// The mounted catalog — built over every dataset on first touch,
    /// then an Arc clone for every caller. Two concurrent first touches
    /// may both build; either result is valid and one wins the slot.
    pub async fn provider(&self) -> Result<Arc<Mount>> {
        for _ in 0..MOUNT_BUILD_ATTEMPTS {
            if let Some(shared) = self.mount.read().expect("mount lock").as_ref() {
                return Ok(Arc::clone(shared));
            }
            let began = self.generation.load(Ordering::Acquire);
            let built = Arc::new(
                tracing::Instrument::instrument(self.build_mount(), tracing::info_span!("mount"))
                    .await?,
            );
            let mut slot = self.mount.write().expect("mount lock");
            if self.generation.load(Ordering::Acquire) == began {
                *slot = Some(Arc::clone(&built));
                return Ok(built);
            }
            // A commit landed while this was building, so the file
            // lists it froze are already behind. Neither cache it nor
            // hand it out — build again against what the writer left.
        }
        Err(Error::Workspace(
            "the catalog kept changing while its mounted view was being built".into(),
        ))
    }

    async fn build_mount(&self) -> Result<Mount> {
        let mut schemas = HashMap::new();
        for dataset in self.datasets().await? {
            let tables = self
                .walk(&dataset, None)
                .await?
                .unwrap_or_default()
                .into_iter()
                .map(|p| (p.name, p.provider))
                .collect();
            schemas.insert(dataset, Arc::new(scan::DatasetSchema::new(tables)));
        }
        Ok(Mount::new(schemas))
    }

    /// Forget the mounted catalog and mark every build now in flight as
    /// behind. Every commit calls it.
    pub fn invalidate_provider(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        *self.mount.write().expect("mount lock") = None;
    }

    /// Dataset walks so far — one per [`Lake::pin_dataset`].
    pub fn walk_count(&self) -> u64 {
        self.walks.load(Ordering::Relaxed)
    }
}
