//! Recipe execution: source files in, Arrow batches out (SPEC.md §3).
//!
//! A recipe at a file source runs on the server: the recipe SQL executes in
//! a scratch DataFusion context where `read_parquet` / `read_csv` /
//! `read_json` resolve under the source's `location` root, and
//! `try_to_date`/`try_to_timestamp` are registered — the recipe carries the
//! casts (project lead, 2026-08-04). A probe is the same SQL surface
//! without a landing: paths' first segment names the source. A recipe at a
//! relational source runs its SQL **at the source** over ADBC (`adbc`
//! module): the driver returns Arrow batches, so what the source computed
//! is what lands.

pub mod accounting;
mod adbc;
pub mod casts;
mod normalize;

pub use accounting::{CastAccounting, CastCheck};

use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
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

/// What a recipe run landed, plus what it read: `source_rows` is the row
/// count of every source relation the recipe scanned, so the caller can
/// record `source_rows - landed` as the dropped-row count (project lead,
/// 2026-08-04 — which rows were dropped is the author's question, answered
/// on the files).
#[derive(Debug)]
pub struct Landed {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub source_rows: u64,
    /// Each scan the recipe made, in scan order: the `read_*` path it
    /// named and the rows that relation held. `source_rows` is their
    /// sum — one number that only means "what was read" when a single
    /// relation was scanned. Relational sources scan nothing here: the
    /// source computed the SQL itself.
    pub source_scans: Vec<(String, u64)>,
    /// What the landing knows about its casts (`accounting` module): a
    /// failed `try_*` is a kept row with a NULL cell, invisible in the
    /// row counts above.
    pub casts: CastAccounting,
}

impl Landed {
    /// The outcome's row accounting, sized to what the counts can
    /// honestly say. One relation scanned: the difference against the
    /// landed count is the dropped-row count. More than one (a join or
    /// union): a single difference misleads — a join reads more source
    /// rows than any one relation holds — so each scan reports its own
    /// count (found 2026-08-12).
    pub fn row_summary(&self, landed_rows: usize) -> String {
        if self.source_scans.len() > 1 {
            let scans = self
                .source_scans
                .iter()
                .map(|(name, rows)| format!("{name} {rows} rows"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{landed_rows} rows landed; sources scanned: {scans}")
        } else {
            format!(
                "{landed_rows} rows landed, {} dropped",
                self.source_rows.saturating_sub(landed_rows as u64)
            )
        }
    }
}

/// Run a recipe against its source and return the batches that will land
/// as the table — exactly the schema the recipe's SQL produced (the
/// probe's rehearsed identity), folded only where Iceberg v2 cannot hold
/// a type. Typing is authored (ruled 2026-08-04): an uncast csv/json
/// column is Utf8 because the read side is, never because the import
/// refolds it.
pub async fn run_recipe(spec: &SourceSpec, sql: &str) -> Result<Landed> {
    if spec.kind == SourceKind::RelationalDb {
        // The source computed the SQL itself, so its result set is both
        // what was read and what lands — dropped is structurally zero
        // here; which rows a WHERE excluded is the source's own answer.
        let read = tokio::task::block_in_place(|| adbc::run_at_source(spec, sql, usize::MAX))?;
        let rows: u64 = read.batches.iter().map(|b| b.num_rows() as u64).sum();
        let (schema, batches) = normalize::compat(read.schema, read.batches)?;
        return Ok(Landed {
            schema,
            batches,
            source_rows: rows,
            source_scans: Vec::new(),
            casts: CastAccounting::Unchecked(
                "the recipe ran at the source — its dialect owns the casts".into(),
            ),
        });
    }
    let root = canonical_root(spec)?;

    let ctx = SessionContext::new();
    casts::register_try_functions(&ctx);
    let seen: Scanned = Arc::default();
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
                seen: Some(Arc::clone(&seen)),
            }),
        );
    }

    let df = ctx.sql_with_options(sql, read_only()).await?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    let mut source_rows = 0u64;
    let mut source_scans = Vec::new();
    let scanned = std::mem::take(&mut *seen.lock().expect("seen"));
    for (name, provider) in scanned {
        let rows = ctx.read_table(provider)?.count().await? as u64;
        source_rows += rows;
        source_scans.push((name, rows));
    }

    // The landing succeeded; the accounting is best effort on top of it —
    // a companion that errors becomes a disclosed note, never a failure.
    let casts = match accounting::plan(sql) {
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
        source_rows,
        source_scans,
        casts,
    })
}

/// Run the companion queries: one aggregate for every cast column's
/// failure count, then one grouped read per failing column for its top
/// tokens. Costs one extra scan, plus one per column that actually
/// failed.
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
    let counts: Vec<u64> = (0..targets.len())
        .map(|i| {
            one_row
                .first()
                .and_then(|b| b.column(i).as_any().downcast_ref::<Int64Array>())
                .map_or(0, |a| {
                    if a.is_empty() || a.is_null(0) {
                        0
                    } else {
                        a.value(0) as u64
                    }
                })
        })
        .collect();

    let mut checks = Vec::with_capacity(targets.len());
    for (target, failed) in targets.iter().zip(counts) {
        let mut tokens = Vec::new();
        if failed > 0 {
            let rows = ctx
                .sql_with_options(&accounting::tokens_sql(select, target), read_only())
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
                        let token = datafusion::arrow::util::display::array_value_to_string(t, i)
                            .map_err(|e| Error::Batches(e.to_string()))?;
                        tokens.push((token, n.value(i) as u64));
                    }
                }
            }
        }
        checks.push(CastCheck {
            column: target.column.clone(),
            failed,
            tokens,
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
                seen: None,
            }),
        );
    }
    let df = ctx.sql_with_options(sql, read_only()).await?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    // A rehearsal is read at the door like any other answer, so it stops at
    // the door's cap — a probe without a LIMIT used to pull the whole
    // source into memory to show 200 rows of it.
    let mut stream = df.execute_stream().await?;
    let mut batches = Vec::new();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
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
/// (found 2026-08-06) — the statement allowlist never sees this SQL.
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
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let plan_err = |m: String| DataFusionError::Plan(m);
        let rel = match args {
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
            SourceKind::Csv => Arc::new(CsvFormat::default().with_has_header(true)),
            SourceKind::Json => Arc::new(JsonFormat::default()),
            SourceKind::RelationalDb => unreachable!("never registered"),
        };
        let mut options = ListingOptions::new(format);
        if rel.contains(['*', '?', '[']) {
            // the glob names the files; the extension filter would fight it
            options = options.with_file_extension("");
        }
        let url = ListingTableUrl::parse(target.display().to_string())?;

        let state = SessionContext::new().state();
        let inferred = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(options.infer_schema(&state as &dyn Session, &url))
        })?;
        let schema = if self.kind == SourceKind::Csv {
            Arc::new(Schema::new(
                inferred
                    .fields()
                    .iter()
                    .map(|f| Field::new(f.name(), DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ))
        } else {
            inferred
        };

        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema);
        let provider: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
        if let Some(seen) = &self.seen {
            seen.lock()
                .expect("seen")
                .push((rel.clone(), Arc::clone(&provider)));
        }
        Ok(provider)
    }
}
