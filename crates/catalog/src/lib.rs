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

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

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

/// How many times a provider build may be overtaken before the caller is
/// told the catalog will not hold still.
///
/// Nothing to do with commits: a build that loses here has conflicted
/// with nothing, it has frozen a table map a concurrent create already
/// made stale. Bounded because creates arriving faster than the map can
/// be assembled is worth reporting rather than looping on.
const PROVIDER_BUILD_ATTEMPTS: usize = 5;

/// The commit-retry arrangement the store relations are created with.
///
/// A store relation is one Iceberg table that every gloss in the
/// workspace appends to, so writers contend for its metadata pointer far
/// harder than they ever do for a data table's. Iceberg retries a
/// conflict itself — reload, re-base, exponential backoff — and the
/// whole arrangement is read from the table's own properties, which is
/// why it is set here and not wrapped in a loop of ours.
///
/// The count and the curve are one setting, not two. The format's
/// defaults are four retries backing off towards a minute, tuned for a
/// remote object store; both halves are wrong here. Four was measured
/// insufficient — seventeen of twenty-four concurrent writers were
/// refused — while a minute of sleeping is absurd for a commit that is
/// a local SQLite update. So: more attempts, over a curve that stays in
/// the milliseconds, and a total bound so a pathological burst fails
/// instead of hanging.
const COMMIT_PROPERTIES: [(&str, &str); 4] = [
    ("commit.retry.num-retries", "20"),
    ("commit.retry.min-wait-ms", "20"),
    ("commit.retry.max-wait-ms", "200"),
    ("commit.retry.total-timeout-ms", "30000"),
];

/// Whether a failed commit was refused for conflicting, told by the
/// error's own kind rather than by its text.
fn is_commit_conflict(error: &iceberg::Error) -> bool {
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
    /// The table's columns in schema order — what can be glossed, and
    /// the same schema the provider beside them advertises.
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
    /// How many commits reached a caller as a conflict — after iceberg
    /// had already retried them to the end of its own budget.
    ///
    /// Not the raw contention: a lost race that the format's backoff
    /// recovers never appears here, which is the point. One of these is
    /// a writer that re-based `commit.retry.num-retries` times and still
    /// lost, and that is the number worth knowing, because it is the one
    /// that says the table's retry property is set too low. Shared for
    /// the reason the others are: a counter copied per clone counts
    /// nothing.
    conflicts: Arc<std::sync::atomic::AtomicU64>,
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
        let catalog = SqlCatalogBuilder::default()
            // `mode=rwc` is how sqlx is told to create the file — it
            // parses the mode off the URL and sets `create_if_missing`.
            // Touching an empty file first said the same thing in a
            // second place, and only sqlite knows what an empty database
            // is.
            .uri(format!("sqlite:{}?mode=rwc", catalog_db.display()))
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
            conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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
        let span = tracing::debug_span!(
            "commit",
            dataset,
            table,
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );
        // Boxed: this sits several awaits below a door, and a wrapper
        // holding the writer chain by value would copy it onto the stack
        // once more at construction in a debug build.
        tracing::Instrument::instrument(
            Box::pin(self.commit_append(dataset, table, batches, properties)),
            span,
        )
        .await
    }

    /// The append, under its span.
    async fn commit_append(
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
        // One commit, with no retry of ours around it. `Transaction::commit`
        // already retries a conflict at iceberg's own seam: `do_commit`
        // reloads the table, re-bases on the refreshed metadata and
        // re-applies the actions, under an exponential backoff bounded by
        // the table's `commit.retry.num-retries`; the SQL catalog marks
        // its conflict retryable so that backoff fires. A loop out here
        // only doubled the attempts and re-entered the backoff with no
        // delay of its own.
        // One transaction: the action is built from it and applied to it.
        // `Transaction::new` clones the table (iceberg transaction/mod.rs),
        // so a second one is a second copy of the metadata for nothing.
        let tx = Transaction::new(&table);
        let append = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(properties);
        match append.apply(tx)?.commit(self.catalog.as_ref()).await {
            Ok(_) => Ok(()),
            // Counted where it surfaces, which is after iceberg has spent
            // its whole retry budget: one here is an exhausted backoff,
            // never a single lost race.
            Err(e) => {
                if is_commit_conflict(&e) {
                    tracing::warn!("commit conflict after iceberg's own retries");
                    self.conflicts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e.into())
            }
        }
    }

    /// The shared catalog provider — built over the current namespace
    /// list on first touch, then an Arc clone for every caller. Two
    /// concurrent first touches may both build; either result is valid
    /// and one wins the slot.
    pub async fn provider(&self) -> Result<Arc<IcebergCatalogProvider>> {
        use std::sync::atomic::Ordering;
        for _ in 0..PROVIDER_BUILD_ATTEMPTS {
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
        let idents = self.catalog.list_tables(&ns).await?;
        // Every table's load in flight at once, as the substrate's own
        // schema provider drives them (iceberg-datafusion schema.rs,
        // `try_join_all`): a load is catalog round trips plus a metadata
        // parse, and over a remote catalog the trips are the cost.
        let tables = futures::future::try_join_all(
            idents.iter().map(|ident| self.catalog.load_table(ident)),
        )
        .await?;
        let mut out = Vec::with_capacity(tables.len());
        for (ident, table) in idents.into_iter().zip(tables) {
            let snapshot_id = table.metadata().current_snapshot_id();
            let columns = table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect();
            // One constructor for both: the provider holds the table it is
            // given and never refreshes it, so a scan with no snapshot named
            // resolves the current snapshot of *this* clone — the same one
            // `snapshot_id` above records (iceberg-rust table/mod.rs:245-261,
            // scan/mod.rs:216-231). A table with no snapshot yet scans empty
            // through the same call (scan/mod.rs:218-229).
            let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
                iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table(table).await?,
            );
            out.push(PinnedTable {
                name: ident.name,
                snapshot_id,
                columns,
                provider,
            });
        }
        Ok(out)
    }

    /// Catalog walks so far — one per `pin_dataset`, which loads and
    /// parses every table of the dataset.
    pub fn walk_count(&self) -> u64 {
        self.walks.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Commits that reached a caller as a conflict, after iceberg had
    /// already retried them to the end of its own budget. A lost race
    /// the format's backoff recovered is not one of these.
    pub fn conflict_count(&self) -> u64 {
        self.conflicts.load(std::sync::atomic::Ordering::Relaxed)
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
        Ok(self
            .load_if_present(&ident)
            .await?
            .map(|t| t.metadata().properties().clone()))
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
        let tx = Transaction::new(&table);
        let mut action = tx.update_table_properties();
        for (k, v) in properties {
            action = action.set(k, v);
        }
        action.apply(tx)?.commit(self.catalog.as_ref()).await?;
        Ok(())
    }

    /// A table the catalog holds, or `None` when it holds no such table.
    ///
    /// `load_table` opens with the same existence query a caller's own
    /// pre-check would run and refuses with a typed kind
    /// (iceberg-catalog-sql `catalog.rs`, `load_table`), so asking first
    /// only ran it twice. Absence is read off the refusal instead.
    pub(crate) async fn load_if_present(
        &self,
        ident: &TableIdent,
    ) -> Result<Option<iceberg::table::Table>> {
        match self.catalog.load_table(ident).await {
            Ok(table) => Ok(Some(table)),
            Err(e) if e.kind() == iceberg::ErrorKind::TableNotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Current snapshot id of `dataset.table`; `None` when the table does
    /// not exist (a subject may be glossed before its recipe lands) or has
    /// no snapshot yet.
    pub async fn snapshot_id(&self, dataset: &str, table: &str) -> Result<Option<i64>> {
        let ident = TableIdent::new(NamespaceIdent::new(dataset.to_string()), table.to_string());
        Ok(self
            .load_if_present(&ident)
            .await?
            .and_then(|t| t.metadata().current_snapshot_id()))
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
