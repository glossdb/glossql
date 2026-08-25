//! Recipe execution: source files in, Arrow batches out (SPEC.md §3).
//!
//! A recipe at a file source runs on the server: the recipe SQL executes in
//! a scratch DataFusion context where `read_parquet` / `read_csv` /
//! `read_json` resolve under the source's `location` root, and
//! `try_to_date`/`try_to_timestamp` are registered — the recipe carries
//! the casts. A probe is the same SQL surface
//! without a landing: paths' first segment names the source. A recipe at a
//! relational source runs its SQL **at the source** over ADBC (`adbc`
//! module): the driver returns Arrow batches, so what the source computed
//! is what lands.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

pub mod accounting;
mod adbc;
pub mod casts;
mod normalize;

pub use accounting::{CastAccounting, CastCheck};

use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::StreamExt as _;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("source `{name}`: {detail}")]
    BadSource { name: String, detail: String },
    #[error("source `{name}` (relational): {detail}")]
    Relational { name: String, detail: String },
    #[error("recipe failed: {0}")]
    Recipe(#[from] DataFusionError),
    /// The same engine failure, named for the statement that caused it.
    /// A refused PROBE used to answer "recipe failed", which sends the
    /// author looking at a recipe they have not written yet (run 4).
    #[error("probe failed: {0}")]
    Probe(DataFusionError),
    #[error("recipe result: {0}")]
    Batches(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Parquet,
    Csv,
    Json,
    RelationalDb,
}

/// A declared source, decoded from its stored `SET (…)` settings.
#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub name: String,
    pub kind: SourceKind,
    /// Where the source lives: file sources, the root directory recipe
    /// paths resolve under; relational sources, the connection URI.
    pub location: PathBuf,
    /// Relational sources: the ADBC driver, a searched name or a library
    /// path. Meaningless (and ignored) for file sources.
    pub driver: Option<String>,
}

impl SourceSpec {
    pub fn from_settings(name: &str, settings: &serde_json::Value) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::BadSource {
                    name: name.into(),
                    detail: format!("missing `{key}` in settings"),
                })
        };
        let kind = match get("type")? {
            "parquet" => SourceKind::Parquet,
            "csv" => SourceKind::Csv,
            "json" => SourceKind::Json,
            "relational_db" => SourceKind::RelationalDb,
            other => {
                return Err(Error::BadSource {
                    name: name.into(),
                    detail: format!("unknown type `{other}`"),
                });
            }
        };
        let driver = settings
            .get("driver")
            .and_then(|v| v.as_str())
            .map(String::from);
        if kind == SourceKind::RelationalDb && driver.is_none() {
            return Err(Error::BadSource {
                name: name.into(),
                detail: "missing `driver` in settings — the ADBC driver name or library path"
                    .into(),
            });
        }
        Ok(SourceSpec {
            name: name.into(),
            kind,
            location: PathBuf::from(get("location")?),
            driver,
        })
    }
}

/// What a recipe run landed, plus what it read — which rows were
/// dropped is the author's question, answered on the
/// files; the engine keeps the count where it is honest.
#[derive(Debug)]
pub struct Landed {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// Each scan the recipe made, in scan order: the `read_*` path it
    /// named and the rows that relation held. Relational sources scan
    /// nothing here — the source computed the SQL itself. There is
    /// deliberately no sum: adding the scans of a join produces a
    /// number that looks like "what was read" and is not.
    pub source_scans: Vec<(String, u64)>,
    /// Whether the recipe's shape maps source rows to landed rows one
    /// for one — a flat SELECT does, an aggregate, DISTINCT, set
    /// operation or CTE does not, and neither does a recipe whose SQL
    /// did not re-parse. Read from the same shape analysis the cast
    /// accounting plans against, but kept apart from it: a companion
    /// query that fails leaves the casts unchecked without changing
    /// what the row counts mean.
    pub row_preserving: bool,
    /// What the landing knows about its casts (`accounting` module): a
    /// failed `try_*` is a kept row with a NULL cell, invisible in the
    /// row counts above.
    pub casts: CastAccounting,
}

impl Landed {
    /// How many source rows the recipe dropped, or `None` where no
    /// single number is true.
    /// Three cases, and only the first has an answer:
    ///
    ///   * one relation scanned by a row-preserving recipe — the
    ///     difference against the landed count is the dropped count;
    ///   * more than one scan — a join reads more source rows than any
    ///     one relation holds, so a difference against their sum is
    ///     arithmetic on unrelated numbers;
    ///   * a recipe that aggregates or de-duplicates — `vendors`
    ///     collapsing 16,817 invoices to 120 distinct ids dropped
    ///     nothing.
    ///
    /// A relational source is the fourth case and it is zero: the
    /// source computed its own SQL, so what came back is both what was
    /// read and what landed. Which rows its WHERE excluded is a
    /// question for the source, not for this count.
    pub fn dropped_rows(&self) -> Option<u64> {
        let [(_, scanned)] = self.source_scans[..] else {
            return self.source_scans.is_empty().then_some(0);
        };
        if !self.row_preserving {
            return None;
        }
        scanned.checked_sub(self.landed_rows() as u64)
    }

    /// What this landing holds — the one count every summary derives
    /// from.
    pub fn landed_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// The outcome's row accounting, sized to what the counts can
    /// honestly say.
    pub fn row_summary(&self) -> String {
        let landed_rows = self.landed_rows();
        if let Some(dropped) = self.dropped_rows() {
            return format!("{landed_rows} rows landed, {dropped} dropped");
        }
        let scans = self
            .source_scans
            .iter()
            .map(|(name, rows)| format!("{name} {rows} rows"))
            .collect::<Vec<_>>()
            .join(", ");
        if scans.is_empty() {
            return format!("{landed_rows} rows landed");
        }
        format!("{landed_rows} rows landed; sources scanned: {scans}")
    }
}

/// Run a recipe against its source and return the batches that will land
/// as the table — exactly the schema the recipe's SQL produced (the
/// probe's rehearsed identity), folded only where Iceberg v2 cannot hold
/// a type. Typing is authored: an uncast csv/json
/// column is Utf8 because the read side is, never because the import
/// refolds it.
pub async fn run_recipe(spec: &SourceSpec, sql: &str) -> Result<Landed> {
    if spec.kind == SourceKind::RelationalDb {
        // The source computed the SQL itself, so its result set is both
        // what was read and what lands — dropped is structurally zero
        // here; which rows a WHERE excluded is the source's own answer.
        let read = tokio::task::block_in_place(|| adbc::run_at_source(spec, sql, usize::MAX))?;
        let (schema, batches) = normalize::compat(read.schema, read.batches)?;
        return Ok(Landed {
            schema,
            batches,
            source_scans: Vec::new(),
            row_preserving: true,
            casts: CastAccounting::Unchecked(
                "the recipe ran at the source — its dialect owns the casts".into(),
            ),
        });
    }
    let seen: Scanned = Arc::default();
    let ctx = reader_ctx(spec, Some(Arc::clone(&seen)))?;

    let df = ctx.sql_with_options(sql, read_only()).await?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    let mut source_scans = Vec::new();
    let scanned = std::mem::take(&mut *seen.lock().expect("seen"));
    for (name, provider) in scanned {
        let rows = ctx.read_table(provider)?.count().await? as u64;
        source_scans.push((name, rows));
    }

    // One shape analysis, two readers. A `Checked` plan means the recipe
    // is a flat SELECT — the fact the row counts need — and that fact
    // must survive a companion query failing below, which only makes the
    // casts unchecked.
    let plan = accounting::plan(sql);
    let row_preserving = matches!(plan, accounting::Plan::Checked { .. });

    // The landing succeeded; the accounting is best effort on top of it —
    // a companion that errors becomes a disclosed note, never a failure.
    let casts = match plan {
        accounting::Plan::Unchecked(note) => CastAccounting::Unchecked(note),
        accounting::Plan::Checked { targets, .. } if targets.is_empty() => {
            CastAccounting::Checked(Vec::new())
        }
        accounting::Plan::Checked {
            counts_sql,
            targets,
            select,
        } => match account_casts(&ctx, &counts_sql, &targets, &select).await {
            Ok(checks) => CastAccounting::Checked(checks),
            Err(e) => CastAccounting::Unchecked(format!("companion query failed: {e}")),
        },
    };

    let (schema, batches) = normalize::compat(schema, batches)?;
    Ok(Landed {
        schema,
        batches,
        source_scans,
        row_preserving,
        casts,
    })
}

/// Run the companion queries: one aggregate for every cast column's
/// failure count, then one grouped read per failing column for its top
/// tokens. Costs one extra scan, plus one per column that actually
/// failed.
/// The file-source reader context: the try-cast functions plus the
/// three read functions, path resolution rooted at the source. One
/// builder serves the recipe (which counts scans) and the probe (which
/// does not).
fn reader_ctx(spec: &SourceSpec, seen: Option<Scanned>) -> Result<SessionContext> {
    let root = canonical_root(spec)?;
    let ctx = SessionContext::new();
    casts::register_try_functions(&ctx);
    for (fn_name, kind) in [
        ("read_parquet", SourceKind::Parquet),
        ("read_csv", SourceKind::Csv),
        ("read_json", SourceKind::Json),
    ] {
        ctx.register_udtf(
            fn_name,
            Arc::new(ReadFiles {
                root: root.clone(),
                kind,
                seen: seen.clone(),
            }),
        );
    }
    Ok(ctx)
}

async fn account_casts(
    ctx: &SessionContext,
    counts_sql: &str,
    targets: &[accounting::Target],
    select: &datafusion::sql::sqlparser::ast::Select,
) -> Result<Vec<CastCheck>> {
    use datafusion::arrow::array::{Array, Int64Array};
    let one_row = ctx
        .sql_with_options(counts_sql, read_only())
        .await?
        .collect()
        .await?;
    // The companion is this module's own ungrouped SUM list: exactly
    // one row of Int64s. A shape that fails to read is an error the
    // caller reports as Unchecked — never a silent "0 cast failures".
    let batch = one_row
        .first()
        .ok_or_else(|| Error::Batches("the cast companion returned no rows".into()))?;
    let counts: Vec<u64> = (0..targets.len())
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .filter(|a| !a.is_empty() && !a.is_null(0))
                .map(|a| a.value(0) as u64)
                .ok_or_else(|| {
                    Error::Batches(format!(
                        "the cast companion's column {i} does not read as a count"
                    ))
                })
        })
        .collect::<Result<Vec<u64>>>()?;

    let mut checks = Vec::with_capacity(targets.len());
    for (target, failed) in targets.iter().zip(counts) {
        // One token read per call site, merged by frequency: a
        // composite expression samples each input's own failures, so
        // the tokens under a column are the values that nulled it —
        // never another input's.
        let mut merged: Vec<(String, u64)> = Vec::new();
        if failed > 0 {
            for site in &target.sites {
                let rows = ctx
                    .sql_with_options(&accounting::tokens_sql(select, target, site), read_only())
                    .await?
                    .collect()
                    .await?;
                for batch in &rows {
                    // The token column's concrete type follows the engine's
                    // string preferences (Utf8View today) — render generically
                    // rather than downcasting to one spelling of "string".
                    let t = batch.column(0);
                    let n = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| Error::Batches("token count is not Int64".into()))?;
                    for i in 0..batch.num_rows() {
                        if !t.is_null(i) {
                            let token =
                                datafusion::arrow::util::display::array_value_to_string(t, i)
                                    .map_err(|e| Error::Batches(e.to_string()))?;
                            match merged.iter_mut().find(|(k, _)| *k == token) {
                                Some((_, c)) => *c += n.value(i) as u64,
                                None => merged.push((token, n.value(i) as u64)),
                            }
                        }
                    }
                }
            }
        }
        merged.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        merged.truncate(8);
        checks.push(CastCheck {
            column: target.column.clone(),
            failed,
            tokens: merged,
        });
    }
    Ok(checks)
}

/// Run a probe: a recipe rehearsal (`PROBE source AS $$sql$$`) — the same
/// SQL surface, the same path resolution, landing nothing. The result
/// carries the schema the recipe would land, so `LIMIT 0` rehearses the
/// identity a `DECLARE RECIPE` would stamp.
pub async fn run_probe(spec: &SourceSpec, sql: &str, row_cap: usize) -> Result<Vec<RecordBatch>> {
    if spec.kind == SourceKind::RelationalDb {
        let read = tokio::task::block_in_place(|| adbc::run_at_source(spec, sql, row_cap))?;
        let mut batches = read.batches;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(read.schema));
        }
        return Ok(batches);
    }
    let ctx = reader_ctx(spec, None)?;
    let df = ctx
        .sql_with_options(sql, read_only())
        .await
        .map_err(Error::Probe)?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    // A rehearsal is read at the door like any other answer, so it stops at
    // the door's cap — a probe without a LIMIT used to pull the whole
    // source into memory to show 200 rows of it.
    let mut stream = df.execute_stream().await.map_err(Error::Probe)?;
    let mut batches = Vec::new();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(Error::Probe)?;
        rows += batch.num_rows();
        batches.push(batch);
        if rows > row_cap {
            break;
        }
    }
    if batches.is_empty() {
        // An empty result still carries the shape — the whole point of a
        // `LIMIT 0` rehearsal.
        batches.push(RecordBatch::new_empty(schema));
    }
    Ok(batches)
}

/// Recipe and probe SQL is a read at its source: it selects from the
/// `read_*` table functions and nothing else. Without this, DataFusion's
/// default options let a body `COPY` to any path the process can write
/// — the statement allowlist never sees this SQL.
fn read_only() -> datafusion::prelude::SQLOptions {
    datafusion::prelude::SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn canonical_root(spec: &SourceSpec) -> Result<PathBuf> {
    spec.location.canonicalize().map_err(|e| Error::BadSource {
        name: spec.name.clone(),
        detail: format!("location {}: {e}", spec.location.display()),
    })
}

/// Providers a recipe run scanned — the `read_*` path each was called
/// with and the provider — recorded so source rows can be counted per
/// scan.
type Scanned = Arc<Mutex<Vec<(String, Arc<dyn TableProvider>)>>>;

/// `read_parquet('…') | read_csv('…') | read_json('…')` — one file format,
/// rooted at the source's location. CSV reads with an all-Utf8 schema so
/// raw text survives byte-exact (no inferred typing to undo); parquet and
/// json read as the files are typed. When `seen` is set, every provider
/// built is recorded so the caller can count source rows.
#[derive(Debug)]
struct ReadFiles {
    root: PathBuf,
    kind: SourceKind,
    seen: Option<Scanned>,
}

impl TableFunctionImpl for ReadFiles {
    /// `call_with_args` rather than `call`: the older one is deprecated
    /// (datafusion-catalog table.rs, since 53.0.0) and its default body
    /// is an internal error, so an implementation that only had `call`
    /// would still be reached through this. The arguments carry the
    /// calling session as well as the expressions; this reader needs
    /// only the expressions, and takes them by name.
    fn call_with_args(
        &self,
        args: datafusion::catalog::TableFunctionArgs,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let plan_err = |m: String| DataFusionError::Plan(m);
        let rel = match args.exprs() {
            [Expr::Literal(ScalarValue::Utf8(Some(s)), _)] => s.clone(),
            _ => {
                return Err(plan_err(
                    "read_* takes exactly one string: a path or glob under the source's location"
                        .into(),
                ));
            }
        };
        let rel_path = Path::new(&rel);
        if rel_path.is_absolute()
            || rel_path
                .components()
                .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(plan_err(format!(
                "`{rel}` must stay under the source's location — relative, no `..`"
            )));
        }
        let target = self.root.join(rel_path);
        // `..` is not the only way out: a symlink under the root resolves
        // wherever it points. Check the deepest real directory the path
        // names — everything before the first glob segment.
        let mut real = self.root.clone();
        for component in rel_path.components() {
            if component
                .as_os_str()
                .to_string_lossy()
                .contains(['*', '?', '['])
            {
                break;
            }
            real.push(component);
        }
        if let Ok(resolved) = real.canonicalize()
            && !resolved.starts_with(&self.root)
        {
            return Err(plan_err(format!(
                "`{rel}` resolves outside the source's location"
            )));
        }

        let format: Arc<dyn FileFormat> = match self.kind {
            SourceKind::Parquet => Arc::new(ParquetFormat::default()),
            // Raw text survives byte-exact because inference is switched
            // off, not because its result is thrown away afterwards: a
            // record cap of zero is how the format is told to call every
            // field Utf8 whatever the content
            // (datafusion-datasource-csv file_format.rs, the
            // `schema_infer_max_rec` doc). Rebuilding the fields by hand
            // ran full type inference first to discard it.
            SourceKind::Csv => Arc::new(
                CsvFormat::default()
                    .with_has_header(true)
                    .with_schema_infer_max_rec(0),
            ),
            SourceKind::Json => Arc::new(JsonFormat::default()),
            SourceKind::RelationalDb => unreachable!("never registered"),
        };
        // The session's own listing options, not the constructor's
        // defaults. `ListingOptions::new` starts at `target_partitions: 1`
        // and `collect_stat: false`, so a recipe scan built from it read
        // every file on one thread and carried no statistics — silently
        // overriding the session that is about to run it.
        let mut options =
            ListingOptions::new(format).with_session_config_options(args.session().config());
        if rel.contains(['*', '?', '[']) {
            // the glob names the files; the extension filter would fight it
            options = options.with_file_extension("");
        }
        let url = ListingTableUrl::parse(target.display().to_string())?;

        // Blocking, because `call_with_args` is synchronous and schema
        // inference is not — the one place in this crate where that is
        // forced by a trait rather than by a blocking driver. Against the
        // calling session, so the object store, the runtime and the
        // format options inference reads are the ones the scan will use.
        // The config infers its own schema rather than being handed one:
        // a schema *specified* makes `ListingTable` decline the session's
        // statistics cache, a schema *inferred* keeps it
        // (datafusion-catalog-listing table.rs, `SchemaSource`).
        let session = args.session();
        let config = ListingTableConfig::new(url).with_listing_options(options);
        let config = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(config.infer_schema(session))
        })?;
        let provider: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
        if let Some(seen) = &self.seen {
            seen.lock()
                .expect("seen")
                .push((rel.clone(), Arc::clone(&provider)));
        }
        Ok(provider)
    }
}
