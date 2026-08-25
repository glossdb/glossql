//! The cube: every grounded metric's cells — the total, the slices
//! along its judged dimensions, the disclosed rival — at the metric's
//! resolution, held in memory and served through two reads.
//!
//! A measurement is a claim about the data: small, adjudicated by a
//! witness, ranked by actor kind, contestable, its history the drift
//! record. The cube is the data at a grain — a GROUP BY result. It is
//! about nothing, judged by nobody, and an old cube is not drift (the
//! lake holds every snapshot). So it is a query result: **cached,
//! never recorded.** Nothing here writes.
//!
//! One table per metric, one cache entry per (dataset, metric, pin,
//! version) — the pair the session's `ReadContext` is already fresh
//! by. A moved pin or version is a miss, never an invalidation. The
//! fill is lazy and single-flight (moka's `get_with`): concurrent
//! readers of one key share one build, nothing recomputes eagerly,
//! and the build runs where the triggering read runs. The cache is the
//! Plane's, handed to each session at construction as the function
//! runtime is; a session built without a Plane carries its own.
//!
//! Resolution and window come from the `cube` FACT aspect the KPI kit
//! declares on the dataset: a metric's cells are at its judged cadence
//! (`temporal_profile`) and never finer than the declared floor; the
//! window is the ladder's rung for that resolution, measured back from
//! the data's own edge. Every coarser grain derives from the cells by
//! the metric's verb at read — flow sums, stock takes the bucket's last
//! period, ratio divides summed halves — so a day cube answers the
//! month, the quarter and the year without a second build, and a ratio
//! cell carries its halves at every dimension, the rival included.
//!
//! Admission is the judged surface, never the data's shape: the time
//! axis is the served date column whose `temporal_profile` names a
//! cadence (highest completeness first), a dimension is a served
//! column whose `dimension_relevance` is applicable (relevance orders,
//! four at most). The verdicts are the newest landed per column
//! whatever their pin — serve and mark: a measurement is reachable at
//! its own pin and every write moves the pin, so after a ruling or an
//! import the axes stand on verdicts from an earlier moment; the fact
//! row's `judged_current` says so, and a re-measure lands the next
//! ones. Counting only floors and buckets: at most 24 members are
//! named, above that the top 23 by weight plus `'other'`, and the fact
//! row names the bucketed dimensions so `'other'` is never read as a
//! business member.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, Value as SQLValue,
};
use serde_json::Value;

use crate::reads::{Served, Shared};
use crate::search::{QuerySlot, current_query_slots, int_column};
use crate::session::SessionError;

/// The process-wide byte cap when serverd is started without
/// `--cube-cache`, and what a session built without a Plane carries.
pub const DEFAULT_CUBE_CACHE_MB: u64 = 2048;

const DIMS_CAP: usize = 4;
const MEMBERS_CAP: i64 = 24;

/// The one refusal every judged reader shares.
const NO_JUDGED_TIME: &str = "no judged time column: no served date column carries an applicable \
     temporal_profile — run temporal() over the metric's date column first";

/// A calendar resolution — the rungs of the ladder, finest first, and
/// the grains a read may ask for. Ordered, so the coarser of two is
/// `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Resolution {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Resolution {
    const ALL: [Resolution; 7] = [
        Resolution::Minute,
        Resolution::Hour,
        Resolution::Day,
        Resolution::Week,
        Resolution::Month,
        Resolution::Quarter,
        Resolution::Year,
    ];

    pub(crate) fn parse(s: &str) -> Option<Resolution> {
        Self::ALL.into_iter().find(|r| r.as_str() == s)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Resolution::Minute => "minute",
            Resolution::Hour => "hour",
            Resolution::Day => "day",
            Resolution::Week => "week",
            Resolution::Month => "month",
            Resolution::Quarter => "quarter",
            Resolution::Year => "year",
        }
    }

    /// A judged cadence (`temporal_profile.granularity`) as a
    /// resolution. `second` is finer than the finest rung, so it reads
    /// as the finest and the floor decides; `irregular` and `unknown`
    /// name no cadence — such a column anchors at the floor.
    fn cadence(granularity: &str) -> Option<Resolution> {
        match granularity {
            "second" => Some(Resolution::Minute),
            other => Self::parse(other),
        }
    }
}

/// What the `cube` aspect declares for a dataset: the floor and the
/// ladder — the schema's defaults, overridden by the collapsed gloss
/// on the dataset where one stands.
#[derive(Debug, Clone)]
struct Settings {
    floor: Resolution,
    windows: HashMap<Resolution, String>,
}

async fn settings(
    shared: &Arc<Shared>,
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
) -> Result<Settings, SessionError> {
    let aspect = rctx
        .aspects
        .iter()
        .find(|a| a.name == "cube")
        .ok_or_else(|| {
            SessionError::BadSubject(
                "no `cube` aspect is declared — the KPI kit ships it: the resolution \
                 floor and the window ladder the cube is computed under"
                    .into(),
            )
        })?;
    let schema: Value = serde_json::from_str(&aspect.schema).unwrap_or(Value::Null);
    let mut floor = schema["properties"]["resolution"]["default"]
        .as_str()
        .and_then(Resolution::parse)
        .unwrap_or(Resolution::Minute);
    let mut windows: HashMap<Resolution, String> = Resolution::ALL
        .into_iter()
        .filter_map(|r| {
            let w =
                schema["properties"]["windows"]["properties"][r.as_str()]["default"].as_str()?;
            Some((r, w.to_string()))
        })
        .collect();
    // The dataset's own gloss, collapsed like any read: human over
    // agent, a witness honoured if one is ever declared on it.
    let scope = glossql_glossary::Scope::Subject(dataset.to_string());
    let verdicts = crate::reads::verdicts(shared, rctx, dataset, &scope, Some("cube")).await?;
    let row =
        glossql_glossary::Store::collapsed_read(dataset, &scope, Some("cube"), rctx, &verdicts)
            .into_iter()
            .find(|r| r.subject == dataset && r.aspect == "cube" && r.state == "current");
    if let Some(body) = row
        .and_then(|r| r.value)
        .and_then(|v| serde_json::from_str::<Value>(&v).ok())
    {
        if let Some(r) = body["resolution"].as_str().and_then(Resolution::parse) {
            floor = r;
        }
        if let Some(rungs) = body["windows"].as_object() {
            for (name, w) in rungs {
                if let (Some(r), Some(w)) = (Resolution::parse(name), w.as_str()) {
                    windows.insert(r, w.to_string());
                }
            }
        }
    }
    Ok(Settings { floor, windows })
}

/// One metric's fact row: what the cube admitted and why not.
#[derive(Debug, Clone)]
pub(crate) struct Fact {
    pub metric: String,
    pub applicable: bool,
    /// Whether every verdict the build admitted on stands at this pin.
    /// False after a write or an import moved the pin since the
    /// profilers ran — the numbers are current, the axes may not be.
    pub judged_current: bool,
    pub reason: Option<String>,
    pub behavior: Option<String>,
    /// Where the verb came from: `ratio` when the frame served both
    /// halves and the marker was never consulted, `marked` when the
    /// grounding carried `behavior`, `default` when it did not and the
    /// metric is summed as a flow because nothing said otherwise. The
    /// default is the common case and usually right — the point is
    /// that reading it as a flow stops being a silent assumption.
    pub behavior_basis: Option<&'static str>,
    pub resolution: Option<Resolution>,
    pub window: Option<String>,
    pub dims: Vec<String>,
    pub bucketed: Vec<String>,
    pub alternative: Option<String>,
    pub alternative_error: Option<String>,
}

impl Fact {
    fn abstain(metric: &str, reason: String) -> Fact {
        Fact {
            metric: metric.to_string(),
            applicable: false,
            judged_current: true,
            reason: Some(reason),
            behavior: None,
            behavior_basis: None,
            resolution: None,
            window: None,
            dims: Vec::new(),
            bucketed: Vec::new(),
            alternative: None,
            alternative_error: None,
        }
    }
}

/// One metric's cube: the fact row and the cells at its resolution, in
/// the shape `metric_series()` serves —
/// `(metric, dimension, member, period, value, num, den, behavior)`, dimension
/// `''` the total, `'alternative'` the rival, `behavior` the verb that
/// produced the row (a rival's may differ from the metric's).
#[derive(Debug)]
pub(crate) struct Cube {
    pub fact: Fact,
    pub cells: RecordBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CubeKey {
    dataset: String,
    metric: String,
    pin: String,
    version: String,
}

/// The cache: LRU by bytes (generational keys want recency, not
/// frequency), weighed by each entry's Arrow footprint, capped at the
/// process-wide byte budget. moka evicts in pending tasks, so the cap
/// is approximate — fine for a compute cache.
#[derive(Debug, Clone)]
pub struct CubeCache {
    inner: moka::future::Cache<CubeKey, Arc<Cube>>,
    builds: Arc<AtomicU64>,
}

impl CubeCache {
    pub fn new(megabytes: u64) -> Self {
        let inner = moka::future::Cache::builder()
            .max_capacity(megabytes.saturating_mul(1024 * 1024))
            .eviction_policy(moka::policy::EvictionPolicy::lru())
            .weigher(|_: &CubeKey, cube: &Arc<Cube>| {
                // The fact row and the key ride beside the cells; a
                // flat allowance keeps an empty entry from weighing
                // nothing.
                u32::try_from(cube.cells.get_array_memory_size() + 512).unwrap_or(u32::MAX)
            })
            .build();
        CubeCache {
            inner,
            builds: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many builds this cache has run — one per miss, whatever the
    /// number of readers that shared it.
    pub fn builds(&self) -> u64 {
        self.builds.load(Ordering::Relaxed)
    }

    /// Entries standing once moka's pending evictions have run.
    pub async fn entries(&self) -> u64 {
        self.inner.run_pending_tasks().await;
        self.inner.entry_count()
    }
}

/// One landed verdict: its body, and whether it stands at this pin.
pub(crate) struct Verdict {
    body: Value,
    current: bool,
}

/// The judged surface the build reads: verdicts by column subject,
/// under the shipped bootstrap's aspect names.
struct Judged {
    temporal: HashMap<String, Verdict>,
    relevance: HashMap<String, Verdict>,
}

/// One measurement aspect's verdicts: the newest landing per subject
/// by any function returning the aspect, whatever its pin, marked
/// current when it stands at this one — the read context's own serve-
/// and-mark rule (SPEC.md §7). Only functions speak on a measurement
/// aspect (§5.2), so there is no collapse to run; a verdict judged at
/// an earlier pin still admits an axis, and the fact row says it is
/// not current.
pub(crate) fn judged_bodies(
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
    aspect: &str,
) -> HashMap<String, Verdict> {
    let mut out: HashMap<String, (String, Verdict)> = HashMap::new();
    let returning = rctx.functions.iter().filter(|f| {
        f.returns.as_deref() == Some(aspect)
            && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
    });
    for f in returning {
        for (row, current) in glossql_glossary::Store::measurements_in(rctx, dataset, &f.name) {
            let Ok(body) = serde_json::from_str::<Value>(&row.body) else {
                continue;
            };
            if out
                .get(&row.subject)
                .is_none_or(|(at, _)| row.computed_at > *at)
            {
                out.insert(row.subject, (row.computed_at, Verdict { body, current }));
            }
        }
    }
    out.into_iter().map(|(s, (_, v))| (s, v)).collect()
}

/// The judged time axis over a served frame: the date column whose
/// `temporal_profile` is applicable — a named cadence before none,
/// highest completeness first, schema order on a tie — with its
/// cadence (none for `irregular` and `unknown`, which anchor at the
/// floor) and whether the verdict is current. A column without a
/// verdict is a gap, not a candidate.
pub(crate) fn judged_time_column(
    fields: &datafusion::common::DFSchemaRef,
    subjects: &HashMap<String, String>,
    temporal: &HashMap<String, Verdict>,
) -> Option<(String, Option<Resolution>, bool)> {
    let mut best: Option<(String, Option<Resolution>, bool, (bool, f64))> = None;
    for f in fields.fields() {
        if !crate::whatif::is_temporal(f.data_type()) {
            continue;
        }
        let Some(v) = subjects.get(f.name()).and_then(|s| temporal.get(s)) else {
            continue;
        };
        if v.body["applicable"].as_bool() != Some(true) {
            continue;
        }
        let cadence = v.body["granularity"].as_str().and_then(Resolution::cadence);
        // A cadence-less verdict carries no completeness: it ranks
        // below every named cadence, and by nothing among its own.
        let rank = (
            cadence.is_some(),
            v.body["completeness"]["ratio"].as_f64().unwrap_or(0.0),
        );
        if best.as_ref().is_none_or(|(.., b)| rank > *b) {
            best = Some((f.name().clone(), cadence, v.current, rank));
        }
    }
    best.map(|(c, r, current, _)| (c, r, current))
}

/// Every current grounding's cube, built where missing. The slots are
/// the store's collapsed read — contested out, human over agent — so
/// the enumeration itself costs no build; the judged surface is read
/// only when some metric misses.
async fn cubes(shared: &Arc<Shared>) -> Result<Vec<Arc<Cube>>, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let rctx = shared.read_context().await?;
    let slots = current_query_slots(shared, &rctx, &dataset).await?;
    let cache = shared.cube();
    let ctx = shared.session_ctx();
    let mut judged: Option<(Judged, Settings)> = None;
    let mut out = Vec::with_capacity(slots.len());
    for slot in &slots {
        let key = CubeKey {
            dataset: dataset.clone(),
            metric: slot.aspect.clone(),
            pin: rctx.pin.text.clone(),
            version: rctx.version.clone(),
        };
        if let Some(cube) = cache.inner.get(&key).await {
            out.push(cube);
            continue;
        }
        if judged.is_none() {
            judged = Some((
                Judged {
                    temporal: judged_bodies(&rctx, &dataset, "temporal_profile"),
                    relevance: judged_bodies(&rctx, &dataset, "dimension_relevance"),
                },
                settings(shared, &rctx, &dataset).await?,
            ));
        }
        let (judged, settings) = judged.as_ref().expect("loaded above");
        let cube = cache
            .inner
            .get_with(key, async {
                cache.builds.fetch_add(1, Ordering::Relaxed);
                // Boxed: the build's future carries the whole frame —
                // schema, subjects, cells, the fact — and it is awaited
                // inside moka's own, inside the read's. Left on the
                // stack it overflows a test thread's 2 MB, which is the
                // same reason every `build_plan` call below is pinned.
                Arc::new(
                    Box::pin(build_metric(shared, &ctx, &dataset, slot, judged, settings)).await,
                )
            })
            .await;
        out.push(cube);
    }
    Ok(out)
}

/// One metric's cube at this pin. A grounding that cannot serve — no
/// JSON, no `sql`, no value column, no judged time axis, a plan or run
/// the engine refuses — abstains with the reason, and the abstention
/// is the entry: the same pin gives the same answer.
async fn build_metric(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    dataset: &str,
    slot: &QuerySlot,
    judged: &Judged,
    settings: &Settings,
) -> Cube {
    match build(shared, ctx, dataset, slot, judged, settings).await {
        Ok(cube) => cube,
        Err(Abstain(reason)) => Cube {
            fact: Fact::abstain(&slot.aspect, reason),
            cells: RecordBatch::new_empty(series_schema()),
        },
    }
}

/// Why a metric's cube is not built — the text the fact row carries.
struct Abstain(String);

impl From<SessionError> for Abstain {
    fn from(e: SessionError) -> Self {
        Abstain(e.to_string())
    }
}

/// One cell before it is a row of the batch.
struct Cell {
    dimension: String,
    member: String,
    period: i64,
    value: f64,
    num: Option<f64>,
    den: Option<f64>,
    behavior: &'static str,
}

/// One row of a series query: `(period, member, value, num, den)`.
type SeriesRow = (i64, Option<String>, f64, Option<f64>, Option<f64>);

async fn build(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    dataset: &str,
    slot: &QuerySlot,
    judged: &Judged,
    settings: &Settings,
) -> Result<Cube, Abstain> {
    let metric = slot.aspect.as_str();
    let body: Value = serde_json::from_str(&slot.body)
        .map_err(|e| Abstain(format!("the grounding is not JSON: {e}")))?;
    let sql = body
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| Abstain("the grounding carries no `sql`".into()))?;
    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain("no value column".into()));
    }
    // Which table column each served field descends from — the judged
    // verdicts key by subject, the frame names aliases.
    let subjects = crate::provenance::served_subjects(&probe, dataset);
    let (time_column, cadence, time_current) =
        judged_time_column(&fields, &subjects, &judged.temporal)
            .ok_or_else(|| Abstain(NO_JUDGED_TIME.into()))?;
    let tcol = time_column.as_str();
    // Whether every verdict admitted on stands at this pin — the time
    // axis now, each admitted dimension below.
    let mut judged_current = time_current;
    // The coarser of the judged cadence and the declared floor — the
    // floor alone where the verdict names no cadence; the window is
    // that rung of the ladder.
    let resolution = cadence.map_or(settings.floor, |c| c.max(settings.floor));
    let window = settings.windows.get(&resolution).cloned();

    // A ratio declares itself by serving both halves of its division —
    // checked before the stock marker so a ratio over stock components
    // cannot be mistaken for a stock. The marker is the grounding's
    // top-level `behavior` (the metrics skill's convention).
    let is_ratio = has("num") && has("den");
    let marker = body.get("behavior").and_then(Value::as_str);
    let (verb, behavior_basis): (&'static str, &'static str) = if is_ratio {
        ("ratio", "ratio")
    } else if marker == Some("stock") {
        ("stock", "marked")
    } else if marker == Some("flow") {
        ("flow", "marked")
    } else {
        ("flow", "default")
    };

    // Judged dimensions: a served column (neither the value nor
    // time-typed nor a ratio's own halves) enters when its collapsed
    // dimension_relevance is applicable — relevance orders the
    // admitted, fewest members break a tie, the cap keeps the top
    // four. Counting admits nothing; its two jobs are the served-frame
    // floor and the bucketing split, one aggregate pass.
    let cand: Vec<(String, f64, bool)> = fields
        .fields()
        .iter()
        .filter(|f| {
            let n = f.name().as_str();
            if n == "value" || (is_ratio && (n == "num" || n == "den")) {
                return false;
            }
            !crate::whatif::is_temporal(f.data_type())
        })
        .filter_map(|f| {
            let v = subjects
                .get(f.name())
                .and_then(|s| judged.relevance.get(s))?;
            if v.body["applicable"].as_bool() != Some(true) {
                return None;
            }
            Some((
                f.name().clone(),
                v.body["relevance"].as_f64().unwrap_or(0.0),
                v.current,
            ))
        })
        .collect();
    let mut counts: Vec<(String, f64, i64, bool)> = Vec::new();
    if !cand.is_empty() {
        let parts: Vec<String> = cand
            .iter()
            .enumerate()
            .map(|(i, (c, ..))| format!("count(DISTINCT \"{c}\") AS \"n_{i}\""))
            .collect();
        let batches = run(
            shared,
            ctx,
            &format!("SELECT {} FROM ({sql})", parts.join(", ")),
        )
        .await?;
        for (i, (c, r, current)) in cand.iter().enumerate() {
            let n = int_column(&batches, &format!("n_{i}")).map_err(Abstain)?[0];
            counts.push((c.clone(), *r, n, *current));
        }
    }
    counts.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.2.cmp(&b.2))
            .then(a.0.cmp(&b.0))
    });
    let mut dims: Vec<String> = Vec::new();
    let mut bucketed: Vec<String> = Vec::new();
    for (c, _, n, current) in &counts {
        if dims.len() >= DIMS_CAP {
            break;
        }
        if *n < 2 {
            continue;
        }
        dims.push(c.clone());
        judged_current &= *current;
        if *n > MEMBERS_CAP {
            bucketed.push(c.clone());
        }
    }

    // The window, measured from the data's own edge: the bucket of
    // the latest observation less the rung's interval, and every
    // series keeps the buckets after it. An unbounded rung keeps all.
    let since = match &window {
        Some(w) => {
            let q = format!(
                "SELECT {} - INTERVAL '{}' AS since FROM ({sql})",
                period_expr(&format!("max(\"{tcol}\")"), resolution),
                w.replace('\'', "''")
            );
            let batches = run(shared, ctx, &q).await?;
            batches
                .iter()
                .find(|b| b.num_rows() > 0)
                .and_then(|b| {
                    let col = b.column_by_name("since")?;
                    (!col.is_null(0))
                        .then(|| array_value_to_string(col, 0).ok())
                        .flatten()
                })
                .map(|t| format!("TIMESTAMP '{t}'"))
        }
        None => None,
    };

    let mut cells: Vec<Cell> = Vec::new();
    let push =
        |cells: &mut Vec<Cell>, dimension: &str, rows: Vec<SeriesRow>, verb: &'static str| {
            for (period, member, value, num, den) in rows {
                cells.push(Cell {
                    dimension: dimension.to_string(),
                    member: member.unwrap_or_default(),
                    period,
                    value,
                    num,
                    den,
                    behavior: verb,
                });
            }
        };

    // The total series.
    let total = series(
        shared,
        ctx,
        &total_sql(sql, tcol, verb, resolution, since.as_deref()),
        false,
    )
    .await?;
    push(&mut cells, "", total, verb);

    // Member series per admitted dimension, same verb, same window —
    // independent plans, driven concurrently. A bucketed dimension
    // names its top members by weight and folds the rest into
    // 'other'; the set is resolved first and spliced as literals,
    // deterministic (weight, then name) so two builds at one pin agree.
    let members = dims.iter().map(|dcol| {
        let bucketed = bucketed.iter().any(|b| b == dcol);
        let since = since.as_deref();
        async move {
            let member = if bucketed {
                let weight = if verb == "ratio" {
                    "sum(den)"
                } else {
                    "sum(value)"
                };
                let clause = since.map_or(String::new(), |s| {
                    format!(
                        " AND {} > {s}",
                        period_expr(&format!("\"{tcol}\""), resolution)
                    )
                });
                let q = format!(
                    "SELECT CAST(\"{dcol}\" AS VARCHAR) AS mc_member FROM ({sql}) \
                     WHERE \"{dcol}\" IS NOT NULL{clause} GROUP BY 1 \
                     ORDER BY {weight} DESC NULLS LAST, mc_member LIMIT {}",
                    MEMBERS_CAP - 1
                );
                let mut named = Vec::new();
                for b in run(shared, ctx, &q)
                    .await?
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                {
                    let col = b
                        .column_by_name("mc_member")
                        .ok_or_else(|| Abstain("the member pass served no mc_member".into()))?;
                    for i in 0..b.num_rows() {
                        let m =
                            array_value_to_string(col, i).map_err(|e| Abstain(e.to_string()))?;
                        named.push(format!("'{}'", m.replace('\'', "''")));
                    }
                }
                format!(
                    "CASE WHEN CAST(\"{dcol}\" AS VARCHAR) IN ({}) \
                     THEN CAST(\"{dcol}\" AS VARCHAR) ELSE 'other' END",
                    named.join(", ")
                )
            } else {
                format!("CAST(\"{dcol}\" AS VARCHAR)")
            };
            let rows = series(
                shared,
                ctx,
                &member_sql(sql, tcol, dcol, &member, verb, resolution, since),
                true,
            )
            .await?;
            Ok::<_, Abstain>((dcol.clone(), rows))
        }
    });
    for (dcol, rows) in futures::future::try_join_all(members).await? {
        push(&mut cells, &dcol, rows, verb);
    }

    // The named rival, when a grounding assumption discloses one. The
    // rival SQL is authored but never admission-validated: it runs
    // behind a guard and a refusal is reported in the fact row, never
    // thrown. Its verb is its own — a rival serving num/den is a ratio
    // whatever the metric is — and its time axis is judged like the
    // chosen reading's wherever a verdict stands, since the rival is a
    // comparison cell. Where the rival's frame carries one date column
    // and no verdict, that column is not a choice and is taken; where
    // it carries several unjudged, the rival is not served and the fact
    // row says why.
    let mut alternative = None;
    let mut alternative_error = None;
    if let Some(assumptions) = body.get("assumptions").and_then(Value::as_array) {
        for a in assumptions {
            let Some(alt_sql) = a.get("alternative_sql").and_then(Value::as_str) else {
                continue;
            };
            let rival = a
                .get("alternative")
                .and_then(Value::as_str)
                .unwrap_or("(rival)");
            match rival_series(
                shared,
                ctx,
                alt_sql,
                dataset,
                judged,
                verb,
                resolution,
                since.as_deref(),
            )
            .await
            {
                Ok((rows, rival_verb)) => {
                    for (period, _, value, num, den) in rows {
                        cells.push(Cell {
                            dimension: "alternative".into(),
                            member: rival.to_string(),
                            period,
                            value,
                            num,
                            den,
                            behavior: rival_verb,
                        });
                    }
                    alternative = Some(rival.to_string());
                }
                Err(Abstain(why)) => {
                    alternative_error = Some(format!("the rival is not served: {why}"));
                }
            }
            break;
        }
    }

    Ok(Cube {
        fact: Fact {
            metric: metric.to_string(),
            applicable: true,
            judged_current,
            reason: None,
            behavior: Some(verb.to_string()),
            behavior_basis: Some(behavior_basis),
            resolution: Some(resolution),
            window,
            dims,
            bucketed,
            alternative,
            alternative_error,
        },
        cells: cells_batch(&slot.aspect, &cells),
    })
}

/// The rival's series at the metric's resolution and window, at its
/// own verb: a rival that serves num/den totals as a ratio even where
/// the chosen reading does not, and the reverse. Its time axis is
/// judged on the same rule as the chosen reading wherever a verdict
/// stands, falling back to the frame's only date column and refusing
/// where several stand unjudged — a rival is a comparison cell, and an
/// anchor guessed among several beside a judged series compares
/// nothing. Every refusal carries its own reason for the fact row.
#[allow(clippy::too_many_arguments)]
async fn rival_series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    dataset: &str,
    judged: &Judged,
    chosen_verb: &str,
    resolution: Resolution,
    since: Option<&str>,
) -> Result<(Vec<SeriesRow>, &'static str), Abstain> {
    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain("it serves no `value` column".into()));
    }
    let subjects = crate::provenance::served_subjects(&probe, dataset);
    let tcol = match judged_time_column(&fields, &subjects, &judged.temporal) {
        Some((column, ..)) => column,
        // No verdict on any served date column — common, because a
        // rival routinely reads a table the metric does not, and that
        // table need never have been profiled. Where the rival's frame
        // carries exactly one date column there is no choice to get
        // wrong, so it is served on it. Where it carries several, the
        // anchor would be a guess standing beside a judged series, and
        // a guessed comparison is worse than none.
        None => {
            let mut dates = fields
                .fields()
                .iter()
                .filter(|f| crate::whatif::is_temporal(f.data_type()));
            match (dates.next(), dates.next()) {
                (Some(only), None) => only.name().clone(),
                (Some(_), Some(_)) => {
                    return Err(Abstain(
                        "it carries several date columns and none of them is judged, so its                          time axis beside a judged series would be a guess"
                            .into(),
                    ));
                }
                _ => return Err(Abstain("it serves no date column".into())),
            }
        }
    };
    let verb: &'static str = if has("num") && has("den") {
        "ratio"
    } else if chosen_verb == "stock" {
        "stock"
    } else {
        "flow"
    };
    let rows = series(
        shared,
        ctx,
        &total_sql(sql, &tcol, verb, resolution, since),
        false,
    )
    .await?;
    Ok((rows, verb))
}

/// The bucket start of a time expression at a resolution, as a plain
/// timestamp — one type for every cell whatever the column's own.
fn period_expr(time: &str, resolution: Resolution) -> String {
    format!(
        "CAST(date_trunc('{}', {time}) AS TIMESTAMP)",
        resolution.as_str()
    )
}

/// The three verbs at a resolution, the window applied on the bucket:
/// flows sum per period; a marked stock sums the rows standing at the
/// period's LATEST observed date; a ratio serves `num` and `den` and
/// the period reads as sum(num)/sum(den), the summed halves beside it
/// — the only material a coarser grain can re-derive the division
/// from.
fn total_sql(
    sql: &str,
    tcol: &str,
    verb: &str,
    resolution: Resolution,
    since: Option<&str>,
) -> String {
    let p = period_expr(&format!("\"{tcol}\""), resolution);
    let w = since.map_or(String::new(), |s| format!(" WHERE {p} > {s}"));
    match verb {
        "ratio" => format!(
            "SELECT {p} AS period, sum(num) / nullif(sum(den), 0) AS value, \
                    sum(num) AS num, sum(den) AS den \
             FROM ({sql}){w} GROUP BY 1 ORDER BY 1"
        ),
        "stock" => format!(
            "SELECT period, sum(value) AS value FROM (\
                SELECT {p} AS period, value, \
                       rank() OVER (PARTITION BY {p} ORDER BY \"{tcol}\" DESC) AS rk \
                FROM ({sql}){w}\
             ) WHERE rk = 1 GROUP BY period ORDER BY period"
        ),
        _ => format!(
            "SELECT {p} AS period, sum(value) AS value \
             FROM ({sql}){w} GROUP BY 1 ORDER BY 1"
        ),
    }
}

/// A member series at a verb: [`total_sql`]'s shapes sliced along one
/// dimension column, NULL members excluded. `member` is the member
/// expression — the cast column plainly, or the bucketing CASE. The
/// stock rank still partitions by the *raw* column, so a bucket's
/// value is the sum of its raw members' own latest observations, never
/// one arbitrary latest row of the whole bucket.
fn member_sql(
    sql: &str,
    tcol: &str,
    dcol: &str,
    member: &str,
    verb: &str,
    resolution: Resolution,
    since: Option<&str>,
) -> String {
    let p = period_expr(&format!("\"{tcol}\""), resolution);
    let w = since.map_or(String::new(), |s| format!(" AND {p} > {s}"));
    match verb {
        "ratio" => format!(
            "SELECT {p} AS period, {member} AS member, \
                    sum(num) / nullif(sum(den), 0) AS value, \
                    sum(num) AS num, sum(den) AS den \
             FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL{w} \
             GROUP BY 1, 2 ORDER BY 1, 2"
        ),
        "stock" => format!(
            "SELECT period, member, sum(value) AS value FROM (\
                SELECT {p} AS period, {member} AS member, value, \
                       rank() OVER (PARTITION BY {p}, \"{dcol}\" \
                                    ORDER BY \"{tcol}\" DESC) AS rk \
                FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL{w}\
             ) WHERE rk = 1 GROUP BY period, member ORDER BY period, member"
        ),
        _ => format!(
            "SELECT {p} AS period, {member} AS member, sum(value) AS value \
             FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL{w} \
             GROUP BY 1, 2 ORDER BY 1, 2"
        ),
    }
}

/// Plan through the session's own pipeline and run — a refusal at
/// either step is the metric's abstention, not the read's error.
async fn run(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, Abstain> {
    let plan = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    ctx.execute_logical_plan(plan)
        .await
        .map_err(|e| Abstain(format!("not served: {e}")))?
        .collect()
        .await
        .map_err(|e| Abstain(format!("not served: {e}")))
}

/// A series query's rows — NULL values dropped (a period the verb
/// could not value is no cell), periods as nanoseconds since the
/// epoch, halves where the query served them.
async fn series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    with_member: bool,
) -> Result<Vec<SeriesRow>, Abstain> {
    let batches = run(shared, ctx, sql).await?;
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let col = |n: &str| -> Result<&ArrayRef, Abstain> {
            b.column_by_name(n)
                .ok_or_else(|| Abstain(format!("the series served no {n}")))
        };
        let floats = |n: &str| -> Result<Option<Float64Array>, Abstain> {
            let Some(c) = b.column_by_name(n) else {
                return Ok(None);
            };
            let c = cast(c, &DataType::Float64).map_err(|e| Abstain(e.to_string()))?;
            c.as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .map(Some)
                .ok_or_else(|| Abstain(format!("{n} did not read as a number")))
        };
        let period = cast(
            col("period")?,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .map_err(|e| Abstain(e.to_string()))?;
        let period = period
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| Abstain("period did not read as a timestamp".into()))?;
        let member = if with_member {
            Some(col("member")?)
        } else {
            None
        };
        let value = floats("value")?.ok_or_else(|| Abstain("the series served no value".into()))?;
        let num = floats("num")?;
        let den = floats("den")?;
        let at = |c: &Option<Float64Array>, i: usize| {
            c.as_ref().and_then(|c| (!c.is_null(i)).then(|| c.value(i)))
        };
        for i in 0..b.num_rows() {
            if value.is_null(i) || period.is_null(i) {
                continue;
            }
            let m = match member {
                Some(m) => Some(array_value_to_string(m, i).map_err(|e| Abstain(e.to_string()))?),
                None => None,
            };
            out.push((period.value(i), m, value.value(i), at(&num, i), at(&den, i)));
        }
    }
    Ok(out)
}

// -- the shapes --------------------------------------------------------

fn utf8(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn period_field() -> Field {
    Field::new(
        "period",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )
}

fn cell_fields() -> Vec<Field> {
    vec![
        utf8("dimension"),
        utf8("member"),
        period_field(),
        Field::new("value", DataType::Float64, false),
        Field::new("num", DataType::Float64, true),
        Field::new("den", DataType::Float64, true),
        utf8("behavior"),
    ]
}

/// The `metric_series()` shape: the cells, each row naming its metric.
/// One schema for the process — every cube's cells carry it, and every
/// read hands it on rather than rebuilding it.
static SERIES: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = vec![utf8("metric")];
    fields.extend(cell_fields());
    Arc::new(Schema::new(fields))
});

fn series_schema() -> SchemaRef {
    Arc::clone(&SERIES)
}

/// A cube's cells as the read serves them, the metric named on every
/// row. Built once, at build time: what the cache holds is what a read
/// hands the planner, so a read allocates nothing for the cells.
fn cells_batch(metric: &str, cells: &[Cell]) -> RecordBatch {
    RecordBatch::try_new(
        series_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                metric,
                cells.len(),
            ))),
            Arc::new(StringArray::from_iter_values(
                cells.iter().map(|c| c.dimension.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                cells.iter().map(|c| c.member.as_str()),
            )),
            Arc::new(TimestampNanosecondArray::from_iter_values(
                cells.iter().map(|c| c.period),
            )),
            Arc::new(Float64Array::from_iter_values(
                cells.iter().map(|c| c.value),
            )),
            Arc::new(Float64Array::from_iter(cells.iter().map(|c| c.num))),
            Arc::new(Float64Array::from_iter(cells.iter().map(|c| c.den))),
            Arc::new(StringArray::from_iter_values(
                cells.iter().map(|c| c.behavior),
            )),
        ],
    )
    .expect("column shapes match the schema")
}

// -- the reads ---------------------------------------------------------

/// The `grain => '<grain>'` argument of `metric_series()`, or none.
pub(crate) fn grain_arg(args: &[FunctionArg]) -> Result<Option<Resolution>, SessionError> {
    let refuse = |what: String| SessionError::BadSubject(format!("metric_series({what})"));
    let names = "minute, hour, day, week, month, quarter, year";
    match args {
        [] => Ok(None),
        [
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(n),
                arg: FunctionArgExpr::Expr(v),
                ..
            },
        ]
        | [
            FunctionArg::Named {
                name: n,
                arg: FunctionArgExpr::Expr(v),
                ..
            },
        ] if n.value.eq_ignore_ascii_case("grain") => match v {
            SQLExpr::Value(v) => match &v.value {
                SQLValue::SingleQuotedString(s) => Resolution::parse(s)
                    .map(Some)
                    .ok_or_else(|| refuse(format!("grain => '{s}'): a grain is one of {names}"))),
                SQLValue::Placeholder(p) => Err(refuse(format!(
                    "grain => {p}): the grain is unbound — a frame binds it from its URL, \
                     a statement spells it: grain => 'month'"
                ))),
                other => Err(refuse(format!(
                    "grain => {other}): the grain is a quoted name — one of {names}"
                ))),
            },
            other => Err(refuse(format!(
                "grain => {other}): the grain is a quoted name — one of {names}"
            ))),
        },
        _ => Err(refuse(format!(
            "{}): the one argument is grain => '<grain>' — filters ride WHERE",
            args.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// `metric_series(grain => …)` — the cells of every current grounding:
/// `(metric, dimension, member, period, value, num, den, behavior)`.
/// Without a grain each metric serves at its own resolution; with one,
/// every metric at or finer than it is aggregated to it by the verb
/// each row carries, and a metric coarser than the asked grain serves
/// no rows — honest absence. `period` is the bucket's start, a typed
/// timestamp. A cache entry is never stale: it is a hit or a miss.
///
/// The cached cube is the table. Its cells are handed to the planner as
/// they sit in the cache — one batch per metric, `Arc`-shared, copied
/// into nothing — and a grain is a plan over them. The only rows a
/// read allocates are the ones it answers with.
pub(crate) async fn metric_series_batch(
    shared: &Arc<Shared>,
    grain: Option<Resolution>,
) -> Result<Served, SessionError> {
    let cubes = cubes(shared).await?;
    let partitions: Vec<RecordBatch> = cubes
        .iter()
        .filter(|c| match (grain, c.fact.resolution) {
            (Some(g), Some(r)) => r <= g,
            (None, Some(_)) => true,
            (_, None) => false,
        })
        .map(|c| c.cells.clone())
        .collect();
    let cells = Served {
        schema: series_schema(),
        partitions,
    };
    match grain {
        None => Ok(cells),
        Some(_) if cells.partitions.is_empty() => Ok(cells),
        Some(g) => aggregate(cells, g).await,
    }
}

/// The cells re-bucketed to a coarser grain, each series by its own
/// verb — the engine over the cached batches, the same three shapes
/// the build uses applied to cells: a flow sums, a stock takes the
/// bucket's last period, a ratio divides its summed halves and carries
/// them on.
async fn aggregate(cells: Served, grain: Resolution) -> Result<Served, SessionError> {
    let fail = |e: datafusion::common::DataFusionError| {
        SessionError::Runtime(format!("metric_series(grain => '{}'): {e}", grain.as_str()))
    };
    let schema = cells.schema.clone();
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema.clone(), vec![cells.partitions]).map_err(fail)?;
    ctx.register_table("cube_cells", Arc::new(table))
        .map_err(fail)?;
    let bucket = format!("date_trunc('{}', period)", grain.as_str());
    let sql = format!(
        "SELECT metric, dimension, member, period, value, num, den, behavior FROM (\
           SELECT metric, dimension, member, {bucket} AS period, \
                  CASE behavior \
                    WHEN 'ratio' THEN sum(num) / nullif(sum(den), 0) \
                    WHEN 'stock' THEN last_value(value ORDER BY period) \
                    ELSE sum(value) END AS value, \
                  CASE WHEN behavior = 'ratio' THEN sum(num) END AS num, \
                  CASE WHEN behavior = 'ratio' THEN sum(den) END AS den, \
                  behavior \
           FROM cube_cells \
           GROUP BY metric, dimension, member, {bucket}, behavior\
         ) WHERE value IS NOT NULL \
         ORDER BY metric, dimension, member, period"
    );
    let batches = ctx
        .sql_with_options(&sql, crate::session::read_only())
        .await
        .map_err(fail)?
        .collect()
        .await
        .map_err(fail)?;
    // The engine's output types follow its own rules; the read's shape
    // is fixed, so a batch whose types differ is cast back to it, batch
    // by batch — a cast to the type a column already has is a shared
    // pointer, not a copy. The batches stay the partitions they came
    // out as.
    let mut partitions = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.schema() == schema {
            partitions.push(batch);
            continue;
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let col = batch.column_by_name(field.name()).ok_or_else(|| {
                SessionError::Runtime(format!("metric_series: no {}", field.name()))
            })?;
            columns.push(
                cast(col, field.data_type()).map_err(|e| SessionError::Runtime(e.to_string()))?,
            );
        }
        partitions.push(
            RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| SessionError::Runtime(e.to_string()))?,
        );
    }
    Ok(Served { schema, partitions })
}

/// `metric_axes()` — one row per current grounding, the record read:
/// `(metric, applicable, judged_current, reason, behavior, resolution,
/// window, dims, bucketed, alternative, alternative_error)`. What the
/// cube admitted and why not, and whether the verdicts it admitted on
/// stand at this pin; served from the entry's fact row, so it builds
/// what is not built.
pub(crate) async fn metric_axes_batch(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let cubes = cubes(shared).await?;
    let facts: Vec<&Fact> = cubes.iter().map(|c| &c.fact).collect();
    let list = |pick: fn(&Fact) -> &Vec<String>| -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for f in &facts {
            for v in pick(f) {
                b.values().append_value(v);
            }
            b.append(true);
        }
        Arc::new(b.finish())
    };
    let text = |pick: fn(&Fact) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from_iter(facts.iter().map(|f| pick(f))))
    };
    let schema = Arc::new(Schema::new(vec![
        utf8("metric"),
        Field::new("applicable", DataType::Boolean, false),
        Field::new("judged_current", DataType::Boolean, false),
        Field::new("reason", DataType::Utf8, true),
        Field::new("behavior", DataType::Utf8, true),
        Field::new("behavior_basis", DataType::Utf8, true),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("window", DataType::Utf8, true),
        Field::new(
            "dims",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "bucketed",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("alternative", DataType::Utf8, true),
        Field::new("alternative_error", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                facts.iter().map(|f| f.metric.as_str()),
            )),
            Arc::new(BooleanArray::from_iter(
                facts.iter().map(|f| Some(f.applicable)),
            )),
            Arc::new(BooleanArray::from_iter(
                facts.iter().map(|f| Some(f.judged_current)),
            )),
            text(|f| f.reason.as_deref()),
            text(|f| f.behavior.as_deref()),
            text(|f| f.behavior_basis),
            text(|f| f.resolution.map(Resolution::as_str)),
            text(|f| f.window.as_deref()),
            list(|f| &f.dims),
            list(|f| &f.bucketed),
            text(|f| f.alternative.as_deref()),
            text(|f| f.alternative_error.as_deref()),
        ],
    )
    .map_err(|e| SessionError::Runtime(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cube(rows: usize) -> Arc<Cube> {
        let cells: Vec<Cell> = (0..rows)
            .map(|i| Cell {
                dimension: String::new(),
                member: String::new(),
                period: i as i64,
                value: 1.0,
                num: None,
                den: None,
                behavior: "flow",
            })
            .collect();
        Arc::new(Cube {
            fact: Fact::abstain("m", String::new()),
            cells: cells_batch("m", &cells),
        })
    }

    fn key(metric: &str) -> CubeKey {
        CubeKey {
            dataset: "d".into(),
            metric: metric.into(),
            pin: "p".into(),
            version: "v".into(),
        }
    }

    /// The cap is bytes, so a cache too small for what it holds
    /// evicts, and a complete key is a hit until then.
    #[tokio::test]
    async fn a_byte_cap_evicts_and_a_key_hits() {
        let roomy = CubeCache::new(64);
        let a = roomy.inner.get_with(key("a"), async { cube(1000) }).await;
        let again = roomy.inner.get_with(key("a"), async { cube(1) }).await;
        assert_eq!(
            again.cells.num_rows(),
            a.cells.num_rows(),
            "a hit serves the entry"
        );
        assert_eq!(roomy.entries().await, 1);

        let tiny = CubeCache::new(0);
        tiny.inner.get_with(key("a"), async { cube(1000) }).await;
        tiny.inner.get_with(key("b"), async { cube(1000) }).await;
        assert_eq!(tiny.entries().await, 0, "nothing fits under a zero cap");
    }

    #[test]
    fn resolutions_order_finest_first_and_cadences_map() {
        assert!(Resolution::Minute < Resolution::Day);
        assert!(Resolution::Day < Resolution::Year);
        assert_eq!(Resolution::cadence("second"), Some(Resolution::Minute));
        assert_eq!(Resolution::cadence("month"), Some(Resolution::Month));
        assert_eq!(Resolution::cadence("irregular"), None);
        assert_eq!(Resolution::cadence("unknown"), None);
        assert_eq!(Resolution::Day.max(Resolution::Hour), Resolution::Day);
    }

    #[test]
    fn the_window_clause_filters_on_the_bucket() {
        let q = total_sql(
            "SELECT d, value FROM t",
            "d",
            "flow",
            Resolution::Day,
            Some("TIMESTAMP '2024-12-15T00:00:00'"),
        );
        assert!(
            q.contains("WHERE CAST(date_trunc('day', \"d\") AS TIMESTAMP) > TIMESTAMP '2024-12-15T00:00:00'"),
            "{q}"
        );
        let q = member_sql(
            "SELECT d, value, r FROM t",
            "d",
            "r",
            "CAST(\"r\" AS VARCHAR)",
            "stock",
            Resolution::Month,
            None,
        );
        assert!(
            q.contains("PARTITION BY CAST(date_trunc('month', \"d\") AS TIMESTAMP), \"r\""),
            "{q}"
        );
        assert!(!q.contains(" AND "), "{q}");
    }
}
