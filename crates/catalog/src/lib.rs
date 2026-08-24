//! The workspace data plane: iceberg-rust behind the `Catalog` trait
//! (SPEC.md §3).
//!
//! One `Lake` per workspace: a SQL catalog on a SQLite file plus a local
//! warehouse directory. Datasets are namespaces. Tables are **created**
//! through iceberg-datafusion's own front door — the session mounts
//! [`IcebergCatalogProvider`] schemas and declares a recipe's table with
//! `SchemaProvider::register_table`. They are **written** through
//! [`Lake::append_batches`], one path for every table the workspace has:
//! a landing that materializes a recipe and a store append that records a
//! gloss differ in what they carry, not in how they commit. Writing here
//! rather than through the engine is what lets facts ride the snapshot
//! they describe, and it keeps the write path clear of the engine's
//! process-wide session state.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub mod metadata;
pub use metadata::{IcebergMetadata, RelationSpec, Row};

use datafusion::arrow::array::RecordBatch;
use iceberg::arrow::FieldMatchMode;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
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

/// How many times a writer re-reads and re-stages before a conflict is
/// the caller's news.
///
/// Iceberg commits optimistically: a writer stages against the metadata
/// it read, and the catalog's conditional update refuses whichever of
/// two writers read the same version second. That refusal is the format
/// working — the loser has lost nothing but its turn, so the answer is
/// to read again and re-stage, never to force. Bounded, because a
/// conflict surviving this many honest attempts is contention someone
/// should be told about rather than a race to ride out.
pub const COMMIT_ATTEMPTS: usize = 5;

/// Whether a failed commit was refused for conflicting, told by the
/// error's own kind rather than by its text.
pub fn is_commit_conflict(error: &iceberg::Error) -> bool {
    error.kind() == iceberg::ErrorKind::CatalogCommitConflicts
}

/// The data files one append writes, before any of them is committed.
///
/// A data file belongs to exactly one partition, so a partitioned table
/// needs the rows split by their partition value before anything is
/// written — the split the format defines, computed from the columns the
/// spec names. Fanout keeps one file open per value, which is what
/// unsorted input requires and what iceberg-datafusion defaults to
/// (`write.datafusion.fanout.enabled`, default true). Without the split a
/// writer emits files carrying an empty partition struct and the commit
/// refuses them: `SnapshotProducer::validate_added_data_files` checks the
/// struct's arity against the table's partition type.
///
/// Free-standing rather than a method, so the generic writer chain it
/// builds is one state machine a caller can box away rather than carry.
async fn write_files(
    table: &iceberg::table::Table,
    batches: &[RecordBatch],
) -> Result<Vec<DataFile>> {
    let table_props = table.metadata().table_properties()?;
    let schema = table.metadata().current_schema().clone();
    // Landed batches carry no field-id metadata; match by name, as
    // iceberg-datafusion's own write path does.
    let parquet = ParquetWriterBuilder::from_table_properties(&table_props, schema.clone())?
        .with_match_mode(FieldMatchMode::Name);
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        table_props.write_target_file_size_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(
            uuid::Uuid::now_v7().to_string(),
            None,
            DataFileFormat::Parquet,
        ),
    );
    let builder = DataFileWriterBuilder::new(rolling);
    let spec = Arc::clone(table.metadata().default_partition_spec());
    if spec.is_unpartitioned() {
        let mut writer = UnpartitionedWriter::new(builder);
        for batch in batches {
            writer.write(batch.clone()).await?;
        }
        Ok(writer.close().await?)
    } else {
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec)?;
        let mut writer = FanoutWriter::new(builder);
        for batch in batches {
            for (key, part) in splitter.split(batch)? {
                writer.write(key, part).await?;
            }
        }
        Ok(writer.close().await?)
    }
}

/// One table pinned at its current snapshot: every scan reads that
/// snapshot whatever lands after — the statement's consistent view, and
/// a durable key, since a snapshot stays addressable after later
/// commits.
pub struct PinnedTable {
    pub name: String,
    pub snapshot_id: Option<i64>,
    /// The table's columns as the catalog holds them *now*, in schema
    /// order — the current schema, not the pinned snapshot's.
    ///
    /// The two diverge after a commit that changes the schema, and the
    /// caller that wants these wants the current one: they name what can
    /// be glossed, and a column that exists is glossable the moment it
    /// does. The provider beside them answers the other question — what
    /// this statement's scans may read — which is why both are here and
    /// neither is derived from the other.
    pub columns: Vec<String>,
    pub provider: Arc<dyn datafusion::catalog::TableProvider>,
}

/// One append snapshot on a data table, with the facts that rode it.
#[derive(Debug, Clone)]
pub struct Landing {
    pub dataset: String,
    pub table: String,
    pub committed_at: String,
    pub added_records: Option<i64>,
    pub properties: HashMap<String, String>,
}

/// The workspace's Iceberg side: catalog + warehouse.
#[derive(Debug, Clone)]
pub struct Lake {
    catalog: Arc<dyn Catalog>,
    /// The one mounted representation of the lake, shared by every
    /// session — `provider()` hands out Arc clones of it. A namespace
    /// create invalidates it, and so does a table create:
    /// [`IcebergCatalogProvider`] freezes the table map per namespace
    /// at build (iceberg-datafusion schema.rs, `try_new`), so only a
    /// rebuild or an explicit `register_table` sees a new table.
    provider: Arc<std::sync::RwLock<Option<Arc<IcebergCatalogProvider>>>>,
    /// Moved by every invalidation, so a build can tell whether the
    /// catalog changed while it ran. Without it an invalidation that
    /// lands mid-build is lost: the build stores a map assembled before
    /// the create, and the table stays invisible until something else
    /// invalidates. Shared, because [`Lake`] is cloned per access and a
    /// counter copied per clone counts nothing.
    generation: Arc<std::sync::atomic::AtomicU64>,
    /// How many times [`Lake::pin_dataset`] has walked a catalog, for
    /// the tests that hold the walk to one per statement. A walk is a
    /// load and a metadata parse per table, so how often it happens is a
    /// property worth being able to assert rather than reason about.
    /// Shared for the reason the generation is: a counter copied per
    /// clone counts nothing.
    walks: Arc<std::sync::atomic::AtomicU64>,
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
            provider: Arc::new(std::sync::RwLock::new(None)),
            generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            walks: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Create the dataset's namespace if it is missing; `true` = created.
    /// Properties apply at create only — an existing namespace keeps its
    /// own (set-at-create). A create invalidates the
    /// shared provider — the next `provider()` rebuilds over the current
    /// namespace list.
    pub async fn ensure_namespace(
        &self,
        dataset: &str,
        properties: HashMap<String, String>,
    ) -> Result<bool> {
        let ns = NamespaceIdent::new(dataset.to_string());
        if self.catalog.namespace_exists(&ns).await? {
            return Ok(false);
        }
        self.catalog.create_namespace(&ns, properties).await?;
        self.invalidate_provider();
        Ok(true)
    }

    /// Append Arrow batches to `dataset.table` as one commit, with the
    /// given facts riding it as snapshot properties. **The one write
    /// path into an Iceberg table**: the landing that materializes a
    /// recipe and the store append that records a gloss both come
    /// through here, so there is one commit, one conflict rule and one
    /// place partitioning is handled. Facts about a write ride the
    /// write, which DataFusion's own INSERT cannot do — its `fast_append`
    /// sets no snapshot properties.
    pub async fn append_batches(
        &self,
        dataset: &str,
        table: &str,
        batches: &[RecordBatch],
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        let table = self.catalog.load_table(&ident).await?;
        // Boxed: the writer stack is a deep chain of generic futures, and
        // this sits several awaits below a door. Inlined it grows every
        // caller's frame for a state machine that lives for one call.
        let files = Box::pin(write_files(&table, batches)).await?;
        // The data files are written once and stay valid whichever
        // metadata they land against; only the metadata swap is
        // contended, so only it repeats.
        let mut table = table;
        for attempt in 1..=COMMIT_ATTEMPTS {
            let commit = Transaction::new(&table)
                .fast_append()
                .add_data_files(files.clone())
                .set_snapshot_properties(properties.clone())
                .apply(Transaction::new(&table))?
                .commit(self.catalog.as_ref())
                .await;
            match commit {
                Ok(_) => return Ok(()),
                Err(e) if is_commit_conflict(&e) && attempt < COMMIT_ATTEMPTS => {
                    table = self.catalog.load_table(&ident).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// The shared catalog provider — built over the current namespace
    /// list on first touch, then an Arc clone for every caller. Two
    /// concurrent first touches may both build; either result is valid
    /// and one wins the slot.
    pub async fn provider(&self) -> Result<Arc<IcebergCatalogProvider>> {
        use std::sync::atomic::Ordering;
        for _ in 0..COMMIT_ATTEMPTS {
            if let Some(shared) = self.provider.read().expect("provider lock").as_ref() {
                return Ok(Arc::clone(shared));
            }
            let began = self.generation.load(Ordering::Acquire);
            let built = Arc::new(IcebergCatalogProvider::try_new(self.catalog()).await?);
            let mut slot = self.provider.write().expect("provider lock");
            if self.generation.load(Ordering::Acquire) == began {
                *slot = Some(Arc::clone(&built));
                return Ok(built);
            }
            // A create landed while this was building, so the map it
            // froze is already behind. Neither cache it nor hand it
            // out — build again against what the writer left.
        }
        Err(Error::Workspace(
            "the catalog kept changing while its mounted view was being built".into(),
        ))
    }

    /// Forget the cached provider and mark every build now in flight as
    /// behind. Callers that miss a namespace or a table another writer
    /// just created invalidate and touch again.
    pub fn invalidate_provider(&self) {
        use std::sync::atomic::Ordering;
        self.generation.fetch_add(1, Ordering::AcqRel);
        *self.provider.write().expect("provider lock") = None;
    }

    /// Every table of the dataset, pinned at its current snapshot — one
    /// catalog resolution per statement, everything derived computed
    /// against that set. The catalog-backed provider always reads
    /// current, so two scans in one query could otherwise straddle a
    /// landing.
    ///
    /// **The one walk over a dataset's catalog.** Loading a table is two
    /// SQLite queries and a full parse of its metadata file, and
    /// everything a caller asks about a table — its snapshot, its
    /// columns, a provider to scan it — is one field or another of the
    /// metadata this already holds. Asking separately meant loading each
    /// table three times to read three fields of one document.
    ///
    /// A table listed here that will not load is a concurrent drop, and
    /// it fails the read. Nothing in the language rules on that race, so
    /// the catalog's own answer is the answer.
    pub async fn pin_dataset(&self, dataset: &str) -> Result<Vec<PinnedTable>> {
        use std::sync::atomic::Ordering;
        self.walks.fetch_add(1, Ordering::Relaxed);
        let ns = NamespaceIdent::new(dataset.to_string());
        let mut out = Vec::new();
        for ident in self.catalog.list_tables(&ns).await? {
            let table = self.catalog.load_table(&ident).await?;
            let snapshot_id = table.metadata().current_snapshot_id();
            // Before the table moves into the provider, and from the
            // current schema rather than the provider's: the provider
            // resolves the pinned snapshot's.
            let columns = table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect();
            let provider: Arc<dyn datafusion::catalog::TableProvider> = match snapshot_id {
                Some(id) => Arc::new(
                    iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table_snapshot(
                        table, id,
                    )
                    .await?,
                ),
                None => Arc::new(
                    iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table(table)
                        .await?,
                ),
            };
            out.push(PinnedTable {
                name: ident.name,
                snapshot_id,
                columns,
                provider,
            });
        }
        Ok(out)
    }

    /// Catalog walks so far — see [`Lake::walks`] on the field.
    pub fn walk_count(&self) -> u64 {
        self.walks.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Single-part namespaces with their properties.
    pub async fn namespaces(&self) -> Result<Vec<(String, HashMap<String, String>)>> {
        let mut out = Vec::new();
        for ns in self.catalog.list_namespaces(None).await? {
            let parts: &Vec<String> = ns.as_ref();
            let [name] = parts.as_slice() else { continue };
            let got = self.catalog.get_namespace(&ns).await?;
            out.push((name.clone(), got.properties().clone()));
        }
        Ok(out)
    }

    /// Every landing on the dataset's tables: one entry per append
    /// snapshot, its facts read back from the snapshot it rode.
    pub async fn landings(&self, dataset: &str) -> Result<Vec<Landing>> {
        let ns = NamespaceIdent::new(dataset.to_string());
        let mut out = Vec::new();
        for ident in self.catalog.list_tables(&ns).await? {
            let table = self.catalog.load_table(&ident).await?;
            for snapshot in table.metadata().snapshots() {
                let summary = snapshot.summary();
                if summary.operation != iceberg::spec::Operation::Append {
                    continue;
                }
                out.push(Landing {
                    dataset: dataset.to_string(),
                    table: ident.name.clone(),
                    committed_at: snapshot
                        .timestamp()?
                        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        .to_string(),
                    added_records: summary
                        .additional_properties
                        .get("added-records")
                        .and_then(|v| v.parse().ok()),
                    properties: summary.additional_properties.clone(),
                });
            }
        }
        Ok(out)
    }

    /// A table's properties; `None` when the table does not exist.
    pub async fn table_properties(
        &self,
        dataset: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        if !self.catalog.table_exists(&ident).await? {
            return Ok(None);
        }
        let table = self.catalog.load_table(&ident).await?;
        Ok(Some(table.metadata().properties().clone()))
    }

    /// Set properties on a table, one commit.
    pub async fn set_table_properties(
        &self,
        dataset: &str,
        table: &str,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        let table = self.catalog.load_table(&ident).await?;
        let mut action = Transaction::new(&table).update_table_properties();
        for (k, v) in properties {
            action = action.set(k, v);
        }
        action
            .apply(Transaction::new(&table))?
            .commit(self.catalog.as_ref())
            .await?;
        Ok(())
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

    pub async fn namespace_exists(&self, name: &str) -> Result<bool> {
        Ok(self
            .catalog
            .namespace_exists(&NamespaceIdent::new(name.to_string()))
            .await?)
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
}
