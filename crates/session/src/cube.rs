//! The cube: every grounded metric's cells — the total, the slices
//! along its judged dimensions, the disclosed rival — at the metric's
//! resolution, or at the grain a read asks for, held in memory and
//! served through two reads.
//!
//! A measurement is a claim about the data: small, adjudicated by a
//! witness, ranked by actor kind, contestable, its history the drift
//! record. The cube is the data at a grain — a GROUP BY result. It is
//! about nothing, judged by nobody, and an old cube is not drift (the
//! lake holds every snapshot). So it is a query result: **cached,
//! never recorded.** Nothing here writes.
//!
//! One table per metric and grain, one cache entry per (dataset,
//! metric, grain, data legs, surface digest): the pin's parts for the dataset's own
//! tables, and everything else a build reads — the current groundings,
//! the judged surface, the cube settings — folded to one number
//! ([`surface_digest`]). A write that cannot reach any build — a
//! ruling, a note, a check's landing — changes neither, and every
//! entry stays a hit; a moved input is a miss, never an invalidation.
//! The one exception is a frame that itself scans a workspace
//! relation: its entry binds to the read context's version on top
//! ([`reads_the_workspace`]). The fill is lazy and single-flight
//! (moka's `get_with`): concurrent readers of one key share one
//! build, nothing recomputes eagerly, and the build runs where the
//! triggering read runs. The cache is the Plane's, handed to each
//! session at construction as the function runtime is; a session
//! built without a Plane carries its own.
//!
//! Resolution and window come from the `cube` FACT aspect the KPI kit
//! declares on the dataset: a metric's own cells are at its judged
//! cadence (`temporal_profile`) and never finer than the declared
//! floor; the window is the ladder's rung for that resolution,
//! measured back from the data's own edge. A read at a coarser grain
//! is its own build: the same grounding, the same verb and axes, at
//! the asked grain over that grain's rung — so a day metric's months
//! span the month rung, not the day rung — cached beside the metric's
//! own cells. A grain finer than the metric's resolution serves no
//! rows. A ratio cell carries its halves at every dimension, the
//! rival included.
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
     temporal_profile. For a series, serve the table's own date column and run temporal() over \
     it — a union of several date columns of one table serves when every one is judged; for a \
     current fact or a derived relation this is the right answer — read.<name>() serves it, and \
     no series is owed";

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
    let verdicts = crate::reads::verdicts(rctx, dataset, &scope, Some("cube")).await?;
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
    /// halves and nothing else was consulted, `marked` when the
    /// grounding carried `behavior`, `glossed` when the `behavior`
    /// gloss on the column the value is or sums decided, `evidence`
    /// when the `behavior_evidence` verdict on that column did,
    /// `default` when nothing said anything and the metric is summed
    /// as a flow. The default is usually right — the point is that
    /// reading it as a flow stops being a silent assumption.
    pub behavior_basis: Option<&'static str>,
    /// The declared row identity — the grounding's `grain` columns as
    /// served. Empty when the grounding declares none: the shape is
    /// undeclared and the build takes the frame as served.
    pub grain: Vec<String>,
    pub resolution: Option<Resolution>,
    pub window: Option<String>,
    pub dims: Vec<String>,
    /// Per admitted dimension, in `dims` order: the column subject whose
    /// verdict admitted it — its own, or the key column reached through
    /// a declared edge when the axis is a label in the key's table.
    pub basis: Vec<String>,
    /// Per admitted dimension, in `dims` order, what decided:
    /// `measurement` when the verdict alone did, `human` or `agent`
    /// when a `dimension` gloss admitted it or put it first.
    pub admitted_by: Vec<String>,
    pub bucketed: Vec<String>,
    /// The served columns the cube does not slice on — every one that
    /// is neither the value, a ratio's half nor time-typed and was not
    /// admitted — and, in `unadmitted_why` at the same index, what
    /// kept each out with the road back in: no verdict (run
    /// `dimension_relevance()` over the subject, or gloss `dimension`),
    /// an abstained verdict with no declared edge reaching a judged
    /// key, a `dimension` gloss of `none`, an expression no verdict
    /// can reach, one member across the frame, or a rank below the
    /// cap. The column names the gap; the reason names the act.
    pub unadmitted: Vec<String>,
    pub unadmitted_why: Vec<String>,
    pub alternative: Option<String>,
    /// The measured disagreement between the metric's total series and
    /// the rival's, over their shared periods — with an authored
    /// `tolerance` on the disclosing assumption, the count of periods
    /// breaching it; without one, the maximum relative gap. None when
    /// no rival is served.
    pub alternative_divergence: Option<String>,
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
            grain: Vec::new(),
            resolution: None,
            window: None,
            dims: Vec::new(),
            basis: Vec::new(),
            admitted_by: Vec::new(),
            bucketed: Vec::new(),
            unadmitted: Vec::new(),
            unadmitted_why: Vec::new(),
            alternative: None,
            alternative_divergence: None,
            alternative_error: None,
        }
    }
}

/// The plan stage of a build: what a plan and the judged surface
/// decide before any data is scanned — the time axis, the resolution
/// and window, the verb and its basis, the candidate axes and the
/// served columns that are not candidates, each with why. A
/// grounding's write answers with this stage alone
/// ([`fact_at_write`]); the cube's build goes on to count members and
/// compute cells.
struct Planned {
    body: Value,
    sql: String,
    tcol: String,
    /// The declared grain, every column verified served; empty when
    /// the grounding declares none.
    grain: Vec<String>,
    resolution: Resolution,
    window: Option<String>,
    verb: &'static str,
    behavior_basis: &'static str,
    /// The time axis's and the verb's currency, folded; each admitted
    /// dimension folds its own in later.
    judged_current: bool,
    candidates: Vec<Candidate>,
    unadmitted: Vec<(String, String)>,
    /// Whether the frame scans a workspace relation
    /// ([`reads_the_workspace`]) — the build binds such an entry to
    /// the read context's version.
    foreign: bool,
}

/// The candidate order the cube ranks by, less the member counts a
/// scan would add: a `primary` gloss first, then relevance, then the
/// column name so two readings of one pin agree.
fn rank_candidates(cand: &mut [Candidate]) {
    cand.sort_by(|a, b| {
        b.primary
            .cmp(&a.primary)
            .then(b.relevance.total_cmp(&a.relevance))
            .then(a.column.cmp(&b.column))
    });
}

impl Planned {
    /// The fact row as the plan stage knows it: the axes the verdicts
    /// admit, in rank order up to the cap, and everything left out
    /// with its reason. No member floor and no bucketing — those are
    /// the scan's — and no rival, which runs only in a build.
    fn fact(self, metric: &str) -> Fact {
        let mut cand = self.candidates;
        rank_candidates(&mut cand);
        let mut unadmitted = self.unadmitted;
        let mut judged_current = self.judged_current;
        let (mut dims, mut basis, mut admitted_by) = (Vec::new(), Vec::new(), Vec::new());
        for c in cand {
            if dims.len() >= DIMS_CAP {
                unadmitted.push((
                    c.column,
                    format!("ranked below the {DIMS_CAP} admitted axes"),
                ));
                continue;
            }
            dims.push(c.column);
            basis.push(c.basis);
            admitted_by.push(c.admitted_by.to_string());
            judged_current &= c.current;
        }
        let (unadmitted, unadmitted_why) = unadmitted.into_iter().unzip();
        Fact {
            metric: metric.to_string(),
            applicable: true,
            judged_current,
            reason: None,
            behavior: Some(self.verb.to_string()),
            behavior_basis: Some(self.behavior_basis),
            grain: self.grain,
            resolution: Some(self.resolution),
            window: self.window,
            dims,
            basis,
            admitted_by,
            bucketed: Vec::new(),
            unadmitted,
            unadmitted_why,
            alternative: None,
            alternative_divergence: None,
            alternative_error: None,
        }
    }
}

/// One served column on its way to being an axis.
struct Candidate {
    column: String,
    relevance: f64,
    current: bool,
    basis: String,
    admitted_by: &'static str,
    /// A `primary` gloss: ranks ahead of every measured relevance.
    primary: bool,
}

/// A label's admission through a declared edge: the served column
/// descends from `T.c`, and a relationship joins `T` to a table the
/// plan scans on a key column with an applicable verdict — that key's
/// relevance, current flag and subject. Several edges: the best
/// relevance.
fn through_edge(
    subject: &str,
    scanned: &std::collections::HashSet<String>,
    pointers: &[crate::behavior::Pointer],
    relevance: &HashMap<String, Verdict>,
) -> Option<(f64, bool, String)> {
    let (table, _) = subject.split_once('.')?;
    let mut best: Option<(f64, bool, String)> = None;
    for p in pointers {
        let key = if p.dst_t == table && p.src_t != table && scanned.contains(&p.src_t) {
            (&p.src_t, &p.src_cols)
        } else if p.src_t == table && p.dst_t != table && scanned.contains(&p.dst_t) {
            (&p.dst_t, &p.dst_cols)
        } else {
            continue;
        };
        let [column] = key.1.as_slice() else {
            continue;
        };
        let key_subject = format!("{}.{column}", key.0);
        let Some(v) = relevance.get(&key_subject) else {
            continue;
        };
        if v.body["applicable"].as_bool() != Some(true) {
            continue;
        }
        let r = v.body["relevance"].as_f64().unwrap_or(0.0);
        if best.as_ref().is_none_or(|(b, ..)| r > *b) {
            best = Some((r, v.current, key_subject));
        }
    }
    best
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
    /// The read context's version at build, where the frame (or its
    /// rival) scans a workspace relation — such a frame reads what
    /// writes move without touching the metric surface, so the entry
    /// serves only at the version it was built at. None for the
    /// ordinary frame over the dataset's own tables.
    pub version_bound: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CubeKey {
    dataset: String,
    metric: String,
    /// `None` for the metric's own cells at its resolution; `Some` for
    /// the cells built at a coarser grain a read asked for.
    grain: Option<Resolution>,
    /// The dataset's own table legs of the pin — the data. The
    /// workspace relations' legs stay out: every write moves those,
    /// and what of them a build reads is the digest's business.
    pin: String,
    /// Everything else the build reads, folded — [`surface_digest`].
    digest: u64,
}

/// Everything a build reads besides the dataset's data, folded to one
/// number: every current QUERY grounding (a metric's own frame, its
/// disclosed rival, any `read.` a frame expands — and the list
/// itself, since a slot leaving it contested changes what serves),
/// the judged surface, and the cube settings. Two contexts digesting
/// alike build alike, so a write that cannot reach any build — a
/// ruling, a note gloss, a reconciliation check's landing — keeps
/// every entry a hit. In-process only: the hash owes no stability
/// across runs. Completeness is checkable in one file: `plan` and
/// `build` read nothing of the store beyond (slot, judged, settings)
/// — the frame's own scans are [`reads_the_workspace`]'s to catch.
fn surface_digest(slots: &[QuerySlot], judged: &Judged, settings: &Settings) -> u64 {
    use std::hash::{Hash, Hasher};
    fn verdicts(h: &mut impl Hasher, m: &HashMap<String, Verdict>) {
        let mut rows: Vec<_> = m.iter().collect();
        rows.sort_by_key(|(k, _)| k.as_str());
        for (k, v) in rows {
            k.hash(h);
            v.body.to_string().hash(h);
            v.current.hash(h);
        }
    }
    fn glosses(h: &mut impl Hasher, m: &HashMap<String, (Value, u8)>) {
        let mut rows: Vec<_> = m.iter().collect();
        rows.sort_by_key(|(k, _)| k.as_str());
        for (k, (v, rank)) in rows {
            k.hash(h);
            v.to_string().hash(h);
            rank.hash(h);
        }
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for s in slots {
        s.subject.hash(&mut h);
        s.aspect.hash(&mut h);
        s.body.hash(&mut h);
    }
    verdicts(&mut h, &judged.temporal);
    verdicts(&mut h, &judged.relevance);
    verdicts(&mut h, &judged.behavior);
    glosses(&mut h, &judged.behavior_gloss);
    glosses(&mut h, &judged.dimension);
    for p in &judged.pointers {
        p.src_t.hash(&mut h);
        p.src_cols.hash(&mut h);
        p.dst_t.hash(&mut h);
        p.dst_cols.hash(&mut h);
    }
    settings.floor.as_str().hash(&mut h);
    let mut windows: Vec<_> = settings.windows.iter().collect();
    windows.sort_by_key(|(r, _)| **r);
    for (r, w) in windows {
        r.as_str().hash(&mut h);
        w.hash(&mut h);
    }
    h.finish()
}

/// Whether a frame's plan scans any workspace relation — a store
/// relation or a shipped read, by name. The reserved-name rule is
/// what makes a name check sound: no dataset table can bear one of
/// these names, so a match is never a false positive. The reads'
/// compute doors (`GLOSSARY()`, the cube's own functions) do not
/// serve a grounding's plan — such a frame abstains with the engine's
/// refusal, which no write can flip — so scans are the whole surface
/// to catch.
fn reads_the_workspace(plan: &datafusion::logical_expr::LogicalPlan) -> bool {
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::logical_expr::LogicalPlan;
    let reserved = |name: &str| {
        glossql_glossary::RELATIONS.iter().any(|r| r.name == name)
            || crate::library::LIBRARY.iter().any(|(n, _)| *n == name)
            || name == "current_dataset"
    };
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(t) = node
            && reserved(t.table_name.table())
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the visitor never errs");
    found
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
    pub(crate) body: Value,
    pub(crate) current: bool,
}

/// The judged surface the build reads: verdicts by column subject,
/// under the shipped bootstrap's aspect names — and beside them what
/// admission reads that no function measured: the collapsed `dimension`
/// gloss per column (human over agent) and the dataset's declared
/// edges, through which a label's admission borrows a key's verdict.
struct Judged {
    temporal: HashMap<String, Verdict>,
    relevance: HashMap<String, Verdict>,
    /// `behavior_evidence` per column — the verb's read where the
    /// grounding carries no marker and no gloss speaks.
    behavior: HashMap<String, Verdict>,
    /// The collapsed `behavior` gloss per column (human over agent) —
    /// the verb's read where the grounding carries no marker.
    behavior_gloss: HashMap<String, (Value, u8)>,
    dimension: HashMap<String, (Value, u8)>,
    pointers: Vec<crate::behavior::Pointer>,
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

/// When the newest landing of a measurement aspect on one subject was
/// computed — the row [`judged_bodies`] picks, by its `computed_at`;
/// None where none landed.
pub(crate) fn judged_at(
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
    aspect: &str,
) -> Option<String> {
    let returning = rctx.functions.iter().filter(|f| {
        f.returns.as_deref() == Some(aspect)
            && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
    });
    returning
        .flat_map(|f| glossql_glossary::Store::measurements_in(rctx, dataset, &f.name))
        .filter(|(row, _)| row.subject == dataset)
        .map(|(row, _)| row.computed_at)
        .max()
}

/// The judged time axis over a served frame: the date column whose
/// `temporal_profile` is applicable — a named cadence before none,
/// highest completeness first, schema order on a tie — with its
/// cadence (none for `irregular` and `unknown`, which anchor at the
/// floor) and whether the verdict is current. A column without a
/// verdict is a gap, not a candidate.
///
/// A served date that descends from several columns of one table — an
/// interval's `+1 at from_date, −1 at to_date` under a union — is the
/// axis when every one of them is judged applicable: its cadence is
/// the coarsest of theirs (the finer would fold the coarser branch at
/// a cadence it never had), its completeness the least, its currency
/// their fold. Columns of different tables are another shape and stay
/// a gap.
pub(crate) fn judged_time_column(
    fields: &datafusion::common::DFSchemaRef,
    sources: &HashMap<String, Vec<String>>,
    temporal: &HashMap<String, Verdict>,
) -> Option<(String, Option<Resolution>, bool)> {
    /// A judged column and the rank that put it ahead.
    struct Ranked {
        column: String,
        cadence: Option<Resolution>,
        current: bool,
        rank: (bool, f64),
    }
    let mut best: Option<Ranked> = None;
    for f in fields.fields() {
        if !crate::whatif::is_temporal(f.data_type()) {
            continue;
        }
        let Some(columns) = sources.get(f.name()) else {
            continue;
        };
        if !crate::provenance::one_table(columns) {
            continue;
        }
        let Some(verdicts) = columns
            .iter()
            .map(|s| temporal.get(s))
            .collect::<Option<Vec<&Verdict>>>()
        else {
            continue;
        };
        if !verdicts
            .iter()
            .all(|v| v.body["applicable"].as_bool() == Some(true))
        {
            continue;
        }
        let cadences: Vec<Option<Resolution>> = verdicts
            .iter()
            .map(|v| v.body["granularity"].as_str().and_then(Resolution::cadence))
            .collect();
        let cadence = cadences.iter().flatten().max().copied();
        // A cadence-less verdict carries no completeness: it ranks
        // below every named cadence, and by nothing among its own.
        let rank = (
            cadences.iter().all(Option::is_some),
            verdicts
                .iter()
                .map(|v| v.body["completeness"]["ratio"].as_f64().unwrap_or(0.0))
                .fold(f64::INFINITY, f64::min),
        );
        if best.as_ref().is_none_or(|b| rank > b.rank) {
            best = Some(Ranked {
                column: f.name().clone(),
                cadence,
                current: verdicts.iter().all(|v| v.current),
                rank,
            });
        }
    }
    best.map(|b| (b.column, b.cadence, b.current))
}

/// The verb a grounding folds by, and where it came from.
pub(crate) struct Verb {
    pub verb: &'static str,
    /// `ratio`, `marked`, `glossed`, `evidence` or `default` —
    /// `Fact::behavior_basis`.
    pub basis: &'static str,
    /// Whether the verdict read stands at this pin; true where none was.
    pub current: bool,
}

/// A grounding's verb: `ratio` when the frame serves both halves; else
/// the grounding's top-level `behavior` marker — its own word, which
/// outranks everything below; else the collapsed `behavior` gloss
/// (human over agent) on the column the value is, or is one `sum` of
/// (`provenance::summed_source`) — the kit's vocabulary, read as policy
/// the way a `dimension` gloss admits an axis; else the
/// `behavior_evidence` verdict on that column; else a flow, because
/// nothing said otherwise. One function for the cube and the walk, so
/// the two never fold one metric two ways.
pub(crate) fn verb_of(
    body: &Value,
    is_ratio: bool,
    probe: &datafusion::logical_expr::LogicalPlan,
    dataset: &str,
    behavior: &HashMap<String, Verdict>,
    glossed: &HashMap<String, (Value, u8)>,
) -> Verb {
    let verb = |verb, basis, current| Verb {
        verb,
        basis,
        current,
    };
    if is_ratio {
        return verb("ratio", "ratio", true);
    }
    match body.get("behavior").and_then(Value::as_str) {
        Some("stock") => return verb("stock", "marked", true),
        Some("flow") => return verb("flow", "marked", true),
        _ => {}
    }
    let source = crate::provenance::summed_source(probe, "value", dataset);
    match source
        .as_ref()
        .and_then(|subject| glossed.get(subject))
        .and_then(|(gloss, _)| gloss["value"].as_str())
    {
        Some("stock") => return verb("stock", "glossed", true),
        Some("flow") => return verb("flow", "glossed", true),
        _ => {}
    }
    let judged = source
        .and_then(|subject| behavior.get(&subject))
        .filter(|v| v.body["applicable"].as_bool() == Some(true));
    match judged.map(|v| (v.body["summary"]["verdict"].as_str(), v.current)) {
        Some((Some("stock"), current)) => verb("stock", "evidence", current),
        Some((Some("flow"), current)) => verb("flow", "evidence", current),
        _ => verb("flow", "default", true),
    }
}

/// Every current grounding's cube, built where missing. The slots are
/// the store's collapsed read — contested out, human over agent — so
/// the enumeration itself costs no build; the judged surface is read
/// only when some metric misses.
/// What one cube read loads once: the bound dataset's current
/// groundings, the judged surface and the settings folded to the
/// digest every key carries, and the cache the entries live in.
/// Loaded before any key, not on a miss — in-memory work over a
/// context already in hand, which is what buys the hit on every write
/// that cannot reach a build.
struct Surface {
    dataset: String,
    version: String,
    slots: Vec<QuerySlot>,
    judged: Judged,
    settings: Settings,
    digest: u64,
    pin: String,
    cache: CubeCache,
    ctx: SessionContext,
}

impl Surface {
    /// `None` with nothing grounded: honest absence stays honest —
    /// there is nothing to key, and a workspace without the `cube`
    /// aspect is not asked for it.
    async fn load(shared: &Arc<Shared>) -> Result<Option<Surface>, SessionError> {
        let dataset = shared
            .dataset
            .read()
            .expect("state lock")
            .clone()
            .ok_or(SessionError::NoDataset)?;
        let rctx = shared.read_context().await?;
        let slots = current_query_slots(&rctx, &dataset).await?;
        if slots.is_empty() {
            return Ok(None);
        }
        let (judged, settings) = judged_surface(shared, &rctx, &dataset).await?;
        let digest = surface_digest(&slots, &judged, &settings);
        let pin = glossql_glossary::data_legs(&rctx.pin.text, &dataset);
        Ok(Some(Surface {
            dataset,
            version: rctx.version.clone(),
            slots,
            judged,
            settings,
            digest,
            pin,
            cache: shared.cube(),
            ctx: shared.session_ctx(),
        }))
    }

    /// One metric's entry: its own cells (`None`), or the cells built
    /// at a coarser grain over that grain's rung. A hit, or one
    /// single-flight build shared by every reader of the key.
    async fn entry(
        &self,
        shared: &Arc<Shared>,
        slot: &QuerySlot,
        grain: Option<Resolution>,
    ) -> Arc<Cube> {
        let key = CubeKey {
            dataset: self.dataset.clone(),
            metric: slot.aspect.clone(),
            grain,
            pin: self.pin.clone(),
            digest: self.digest,
        };
        if let Some(cube) = self.cache.inner.get(&key).await {
            // A version-bound entry — its frame scans a workspace
            // relation — serves only at the version it was built at.
            if cube
                .version_bound
                .as_ref()
                .is_none_or(|v| *v == self.version)
            {
                return cube;
            }
            self.cache.inner.invalidate(&key).await;
        }
        self.cache
            .inner
            .get_with(key, async {
                self.cache.builds.fetch_add(1, Ordering::Relaxed);
                // Boxed: the build's future carries the whole frame —
                // schema, subjects, cells, the fact — and it is awaited
                // inside moka's own, inside the read's. Left on the
                // stack it overflows a test thread's 2 MB, which is the
                // same reason every `build_plan` call below is pinned.
                Arc::new(Box::pin(build_metric(shared, self, slot, grain)).await)
            })
            .await
    }
}

/// Every metric's own entry — what `metric_axes()` describes.
async fn cubes(shared: &Arc<Shared>) -> Result<Vec<Arc<Cube>>, SessionError> {
    let Some(surface) = Surface::load(shared).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(surface.slots.len());
    for slot in &surface.slots {
        out.push(surface.entry(shared, slot, None).await);
    }
    Ok(out)
}

/// The judged surface and the cube settings at a read context — what
/// admission reads, loaded once per enumeration and once per write.
async fn judged_surface(
    shared: &Arc<Shared>,
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
) -> Result<(Judged, Settings), SessionError> {
    let edges = shared.store.relation_rows("relationships").await?;
    Ok((
        Judged {
            temporal: judged_bodies(rctx, dataset, "temporal_profile"),
            relevance: judged_bodies(rctx, dataset, "dimension_relevance"),
            behavior: judged_bodies(rctx, dataset, "behavior_evidence"),
            behavior_gloss: crate::search::current_fact_values(rctx, dataset, "behavior").await?,
            dimension: crate::search::current_fact_values(rctx, dataset, "dimension").await?,
            pointers: crate::behavior::declared_pointers(&edges, dataset),
        },
        settings(rctx, dataset).await?,
    ))
}

/// The fact row a grounding's write answers with, in the
/// `metric_axes()` shape, at the pin the write moved to: whether the
/// SQL plans, the judged time axis, the verb and where it came from,
/// the axes the verdicts admit and every served column they do not,
/// each with its road back in. The plan stage alone — no data is
/// scanned, so the member floor, the bucketing and the rival are the
/// build's to add; everything else is what `metric_axes()` will say.
/// The row judges the grounding that serves: a human slot outranks
/// the agent's, and then it is the human's the row describes.
///
/// Nothing here fails the write. The gloss landed; a grounding that
/// cannot be judged abstains in the row and says why — the SQL does
/// not plan, no `cube` aspect is declared, or the call is bound to
/// another dataset or to none, so the grounding's table names do not
/// resolve from this channel.
pub(crate) async fn fact_at_write(
    shared: &Arc<Shared>,
    dataset: &str,
    subject: &str,
    aspect: &str,
) -> Result<RecordBatch, SessionError> {
    let fact = match write_fact(shared, dataset, subject, aspect).await {
        Ok(fact) => fact,
        Err(Abstain(reason)) => Fact::abstain(aspect, reason),
    };
    fact_batch(&[&fact])
}

async fn write_fact(
    shared: &Arc<Shared>,
    dataset: &str,
    subject: &str,
    aspect: &str,
) -> Result<Fact, Abstain> {
    let bound = shared.dataset.read().expect("state lock").clone();
    if bound.as_deref() != Some(dataset) {
        let here = bound.map_or("no dataset".to_string(), |b| format!("`{b}`"));
        return Err(Abstain(format!(
            "not judged from here: the call is bound to {here} — `USE {dataset};` and \
             metric_axes() judges it"
        )));
    }
    let withheld = || Abstain("no serving grounding: the slot is withheld as contested".into());
    let surface = Surface::load(shared).await?.ok_or_else(withheld)?;
    let slot = surface
        .slots
        .iter()
        .find(|s| s.subject == subject && s.aspect == aspect)
        .ok_or_else(withheld)?;
    let planned = Box::pin(plan(shared, &surface, slot, None)).await?;
    Ok(planned.fact(aspect))
}

/// One metric's cube at this pin. A grounding that cannot serve — no
/// JSON, no `sql`, no value column, no judged time axis, a plan or run
/// the engine refuses — abstains with the reason, and the abstention
/// is the entry: the same pin gives the same answer. A grounding the
/// author stopped abstains with the author's own reason.
async fn build_metric(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    asked: Option<Resolution>,
) -> Cube {
    match build(shared, surface, slot, asked).await {
        Ok(cube) => cube,
        // An abstention binds to no version: its reasons derive from
        // the plan over the digest-covered surface, so no write flips
        // one without missing the key. The grain check is the one
        // data-derived abstention, and `build` binds it itself.
        Err(Abstain(reason)) => Cube {
            fact: Fact::abstain(&slot.aspect, reason),
            cells: RecordBatch::new_empty(series_schema()),
            version_bound: None,
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

/// The plan stage — see [`Planned`]. Everything here is decided by
/// the plan's schema, its provenance and the judged surface; nothing
/// scans. `asked` is the coarser grain a read asked for, or none for
/// the metric's own resolution.
async fn plan(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    asked: Option<Resolution>,
) -> Result<Planned, Abstain> {
    let Surface {
        ctx,
        dataset,
        judged,
        settings,
        ..
    } = surface;
    let dataset = dataset.as_str();
    let body: Value = serde_json::from_str(&slot.body)
        .map_err(|e| Abstain(format!("the grounding is not JSON: {e}")))?;
    // The author's stop (SPEC.md §5.2): no number is served, and the
    // reason is theirs, carried as written.
    if let Some(why) = body.get("stopped").and_then(Value::as_str) {
        return Err(Abstain(format!("stopped: {why}")));
    }
    let sql = body
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| Abstain("the grounding carries no `sql`".into()))?;
    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    // Planned through to the physical plan as well: the engine admits
    // at the logical stage what it refuses at the physical one — a
    // scalar subquery inside an aggregate's argument arrives there as
    // `ScalarSubquery` and is refused ("Physical plan does not support
    // …") — and the row answers whether the SQL plans, so it answers
    // for both stages. The scans are the pinned providers' and plan
    // without I/O; nothing runs.
    ctx.state()
        .create_physical_plan(&probe)
        .await
        .map_err(|e| Abstain(format!("not served: {e}")))?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain("no value column".into()));
    }
    // The declared row identity: every grain column must be served —
    // a declaration over a column the frame does not carry judges
    // nothing.
    let grain: Vec<String> = body
        .get("grain")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if let Some(missing) = grain.iter().find(|c| !has(c)) {
        return Err(Abstain(format!(
            "the declared grain names `{missing}`, which the frame does not serve — \
             serve the column, or fix the declaration"
        )));
    }
    // Which table columns each served field descends from — the judged
    // verdicts key by subject, the frame names aliases. The time axis
    // reads every source; admission reads the fields with one.
    let sources = crate::provenance::served_sources(&probe, dataset);
    let subjects = crate::provenance::single(&sources);
    let (time_column, cadence, time_current) =
        judged_time_column(fields, &sources, &judged.temporal)
            .ok_or_else(|| Abstain(NO_JUDGED_TIME.into()))?;
    // Whether every verdict admitted on stands at this pin — the time
    // axis now, each admitted dimension below.
    let mut judged_current = time_current;
    // The metric's own resolution is the coarser of the judged cadence
    // and the declared floor — the floor alone where the verdict names
    // no cadence. A read at a coarser grain builds at that grain.
    // Either way the window is the ladder's rung for the resolution
    // built, so a month series spans the month rung whatever the
    // metric's own cadence.
    let own = cadence.map_or(settings.floor, |c| c.max(settings.floor));
    let resolution = asked.map_or(own, |g| g.max(own));
    let window = settings.windows.get(&resolution).cloned();

    // A ratio declares itself by serving both halves of its division —
    // checked before any marker so a ratio over stock components
    // cannot be mistaken for a stock.
    let is_ratio = has("num") && has("den");
    let Verb {
        verb,
        basis: behavior_basis,
        current: verb_current,
    } = verb_of(
        &body,
        is_ratio,
        &probe,
        dataset,
        &judged.behavior,
        &judged.behavior_gloss,
    );
    judged_current &= verb_current;

    // Judged dimensions: a served column (neither the value nor
    // time-typed nor a ratio's own halves) enters when a verdict admits
    // it — its own dimension_relevance, or, for a label whose own
    // verdict is a near-key in its table, the verdict on the key
    // column that reaches it through a declared edge. The collapsed
    // `dimension` gloss on the column is the read policy over that:
    // `none` closes the axis whatever was measured, `primary` admits
    // it and ranks it first, `supporting` admits it. Relevance orders
    // the admitted, fewest members break a tie, the cap keeps the top
    // four. Counting admits nothing; its two jobs are the served-frame
    // floor and the bucketing split, one aggregate pass.
    let scanned = crate::provenance::scanned_tables(&probe, dataset);
    let mut cand: Vec<Candidate> = Vec::new();
    let mut unadmitted: Vec<(String, String)> = Vec::new();
    for f in fields.fields() {
        let n = f.name().as_str();
        if n == "value"
            || (is_ratio && (n == "num" || n == "den"))
            || crate::whatif::is_temporal(f.data_type())
        {
            continue;
        }
        let Some(subject) = subjects.get(n) else {
            unadmitted.push((
                n.to_string(),
                "an expression, not a table column: no verdict can reach it — serve the \
                 column it derives from, or land it as a recipe column"
                    .into(),
            ));
            continue;
        };
        let gloss = judged.dimension.get(subject);
        let stance = gloss.and_then(|(v, _)| v["value"].as_str()).unwrap_or("");
        let speaker: &'static str = match gloss {
            Some((_, 0)) => "human",
            Some(_) => "agent",
            None => "measurement",
        };
        if stance == "none" {
            unadmitted.push((
                n.to_string(),
                format!("closed by a dimension gloss on {subject} ({speaker}: none)"),
            ));
            continue;
        }
        let measured = match judged
            .relevance
            .get(subject)
            .filter(|v| v.body["applicable"].as_bool() == Some(true))
        {
            Some(v) => Some((
                v.body["relevance"].as_f64().unwrap_or(0.0),
                v.current,
                subject.clone(),
            )),
            None => through_edge(subject, &scanned, &judged.pointers, &judged.relevance),
        };
        let candidate = match (measured, stance) {
            (Some((relevance, current, basis)), "primary") => Candidate {
                column: n.to_string(),
                relevance,
                current,
                basis,
                admitted_by: speaker,
                primary: true,
            },
            (Some((relevance, current, basis)), _) => Candidate {
                column: n.to_string(),
                relevance,
                current,
                basis,
                admitted_by: "measurement",
                primary: false,
            },
            // Admitted on the gloss alone: a gloss always stands, and
            // without a verdict `primary` leads, `supporting` trails.
            (None, "primary" | "supporting") => Candidate {
                column: n.to_string(),
                relevance: if stance == "primary" { 1.0 } else { 0.0 },
                current: true,
                basis: subject.clone(),
                admitted_by: speaker,
                primary: stance == "primary",
            },
            (None, _) => {
                let why = match judged.relevance.get(subject) {
                    Some(v) => format!(
                        "dimension_relevance abstained on {subject} ({}), and no declared \
                         relationship reaches it from a judged key the grounding scans — \
                         declare the edge, or gloss dimension on it",
                        v.body["reason"].as_str().unwrap_or("no reason given")
                    ),
                    None => format!(
                        "no verdict on {subject} — run dimension_relevance() over it, or \
                         gloss dimension on it"
                    ),
                };
                unadmitted.push((n.to_string(), why));
                continue;
            }
        };
        cand.push(candidate);
    }
    let sql = sql.to_string();
    Ok(Planned {
        body,
        sql,
        tcol: time_column,
        grain,
        resolution,
        window,
        verb,
        behavior_basis,
        judged_current,
        candidates: cand,
        unadmitted,
        foreign: reads_the_workspace(&probe),
    })
}

async fn build(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    asked: Option<Resolution>,
) -> Result<Cube, Abstain> {
    let Surface {
        ctx,
        dataset,
        judged,
        version,
        ..
    } = surface;
    let dataset = dataset.as_str();
    let metric = slot.aspect.as_str();
    let Planned {
        body,
        sql,
        tcol,
        grain,
        resolution,
        window,
        verb,
        behavior_basis,
        mut judged_current,
        candidates: cand,
        mut unadmitted,
        mut foreign,
    } = plan(shared, surface, slot, asked).await?;
    let sql = sql.as_str();
    let tcol = tcol.as_str();

    // The declared grain, validated where the frame is built: one row
    // per key, or the metric abstains — a frame that breaks its
    // declared identity multiplies every aggregating reader, and
    // nothing downstream can tell duplication from multi-entity.
    if !grain.is_empty() {
        let keys = grain
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!(
            "SELECT count(*) AS keys, coalesce(sum(c), 0) AS total FROM \
             (SELECT count(*) AS c FROM ({sql}) GROUP BY {keys})"
        );
        let batches = run(shared, ctx, &q).await?;
        let key_count = int_column(&batches, "keys").map_err(|e| Abstain(e.to_string()))?[0];
        let total = int_column(&batches, "total").map_err(|e| Abstain(e.to_string()))?[0];
        if total > key_count {
            let cols = grain.join(", ");
            // The one data-derived abstention: over a frame that also
            // scans a workspace relation, a write can flip it, so it
            // carries the binding a built cube would.
            return Ok(Cube {
                fact: Fact::abstain(
                    metric,
                    format!(
                        "the frame breaks its declared grain ({cols}): {total} rows over \
                         {key_count} distinct keys — serve one row per ({cols}), or fix the \
                         declaration"
                    ),
                ),
                cells: RecordBatch::new_empty(series_schema()),
                version_bound: foreign.then(|| version.to_string()),
            });
        }
    }
    let mut counts: Vec<(Candidate, i64)> = Vec::new();
    if !cand.is_empty() {
        let parts: Vec<String> = cand
            .iter()
            .enumerate()
            .map(|(i, c)| format!("count(DISTINCT \"{}\") AS \"n_{i}\"", c.column))
            .collect();
        let batches = run(
            shared,
            ctx,
            &format!("SELECT {} FROM ({sql})", parts.join(", ")),
        )
        .await?;
        for (i, c) in cand.into_iter().enumerate() {
            let n = int_column(&batches, &format!("n_{i}")).map_err(|e| Abstain(e.to_string()))?[0];
            counts.push((c, n));
        }
    }
    counts.sort_by(|(a, an), (b, bn)| {
        b.primary
            .cmp(&a.primary)
            .then(b.relevance.total_cmp(&a.relevance))
            .then(an.cmp(bn))
            .then(a.column.cmp(&b.column))
    });
    let mut dims: Vec<String> = Vec::new();
    let mut basis: Vec<String> = Vec::new();
    let mut admitted_by: Vec<String> = Vec::new();
    let mut bucketed: Vec<String> = Vec::new();
    for (c, n) in &counts {
        if dims.len() >= DIMS_CAP {
            unadmitted.push((
                c.column.clone(),
                format!("ranked below the {DIMS_CAP} admitted axes"),
            ));
            continue;
        }
        if *n < 2 {
            unadmitted.push((
                c.column.clone(),
                "one member across the frame: nothing to slice".into(),
            ));
            continue;
        }
        dims.push(c.column.clone());
        basis.push(c.basis.clone());
        admitted_by.push(c.admitted_by.to_string());
        judged_current &= c.current;
        if *n > MEMBERS_CAP {
            bucketed.push(c.column.clone());
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
    let mut alternative_divergence = None;
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
                Ok((rows, rival_verb, rival_foreign)) => {
                    foreign |= rival_foreign;
                    alternative_divergence = Some(divergence(
                        &cells,
                        &rows,
                        a.get("tolerance").and_then(Value::as_f64),
                    ));
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
            grain,
            resolution: Some(resolution),
            window,
            dims,
            basis,
            admitted_by,
            bucketed,
            unadmitted: unadmitted.iter().map(|(c, _)| c.clone()).collect(),
            unadmitted_why: unadmitted.into_iter().map(|(_, w)| w).collect(),
            alternative,
            alternative_divergence,
            alternative_error,
        },
        cells: cells_batch(&slot.aspect, &cells),
        version_bound: foreign.then(|| version.to_string()),
    })
}

/// The disagreement between the metric's total cells and the rival's
/// series, over their shared periods — the coordinates the docket's
/// question needs instead of two lines to eyeball. The gap is
/// relative, scaled by the larger magnitude, so it reads as a share of
/// the number itself. Agreement is a zero divergence, never silence;
/// no shared periods is its own answer.
fn divergence(cells: &[Cell], rival: &[SeriesRow], tolerance: Option<f64>) -> String {
    let total: std::collections::HashMap<i64, f64> = cells
        .iter()
        .filter(|c| c.dimension.is_empty())
        .map(|c| (c.period, c.value))
        .collect();
    let day = |p: i64| {
        chrono::DateTime::from_timestamp_nanos(p)
            .format("%Y-%m-%d")
            .to_string()
    };
    let mut shared = 0usize;
    let mut breaches = 0usize;
    let mut max: Option<(f64, i64)> = None;
    for (p, _, v, _, _) in rival {
        let Some(t) = total.get(p) else { continue };
        shared += 1;
        let scale = t.abs().max(v.abs());
        let gap = if scale == 0.0 {
            0.0
        } else {
            (t - v).abs() / scale
        };
        if max.is_none_or(|(g, _)| gap > g) {
            max = Some((gap, *p));
        }
        if tolerance.is_some_and(|tol| gap > tol) {
            breaches += 1;
        }
    }
    let Some((gap, at)) = max else {
        return "no shared periods".into();
    };
    match tolerance {
        Some(tol) => format!(
            "{breaches} of {shared} shared periods differ beyond {tol}; \
             max relative gap {gap:.4} at {}",
            day(at)
        ),
        None => format!(
            "max relative gap {gap:.4} at {} over {shared} shared periods",
            day(at)
        ),
    }
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
) -> Result<(Vec<SeriesRow>, &'static str, bool), Abstain> {
    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain("it serves no `value` column".into()));
    }
    let sources = crate::provenance::served_sources(&probe, dataset);
    let tcol = match judged_time_column(fields, &sources, &judged.temporal) {
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
    Ok((rows, verb, reads_the_workspace(&probe)))
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
/// Without a grain each metric serves its own cells, at its own
/// resolution over its own rung. With one, a metric at that resolution
/// serves its own cells; a finer metric serves the cells built at the
/// asked grain over that grain's rung — the same grounding, verb and
/// axes, a second entry beside the first; a metric coarser than the
/// asked grain serves no rows — honest absence. `period` is the
/// bucket's start, a typed timestamp. A cache entry is never stale:
/// it is a hit or a miss.
///
/// The cached cube is the table. Its cells are handed to the planner
/// as they sit in the cache — one batch per metric, `Arc`-shared,
/// copied into nothing. The only rows a read allocates are the ones
/// it answers with.
pub(crate) async fn metric_series_batch(
    shared: &Arc<Shared>,
    grain: Option<Resolution>,
) -> Result<Served, SessionError> {
    let mut partitions = Vec::new();
    if let Some(surface) = Surface::load(shared).await? {
        for slot in &surface.slots {
            let own = surface.entry(shared, slot, None).await;
            // An abstained metric has no resolution and no cells.
            let Some(resolution) = own.fact.resolution else {
                continue;
            };
            let cells = match grain {
                None => own.cells.clone(),
                Some(g) if g == resolution => own.cells.clone(),
                Some(g) if g > resolution => {
                    surface.entry(shared, slot, Some(g)).await.cells.clone()
                }
                Some(_) => continue,
            };
            partitions.push(cells);
        }
    }
    Ok(Served {
        schema: series_schema(),
        partitions,
    })
}

/// `metric_axes()` — one row per current grounding, the record read:
/// `(metric, applicable, judged_current, reason, behavior,
/// behavior_basis, grain, resolution, window, dims, basis,
/// admitted_by, bucketed, unadmitted, unadmitted_why, alternative,
/// alternative_divergence, alternative_error)`. What the cube
/// admitted and why not, and
/// whether the verdicts it admitted on stand at this pin; served from
/// the entry's fact row, so it builds what is not built.
pub(crate) async fn metric_axes_batch(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let cubes = cubes(shared).await?;
    let facts: Vec<&Fact> = cubes.iter().map(|c| &c.fact).collect();
    fact_batch(&facts)
}

/// Fact rows as the `metric_axes()` relation — one schema for the
/// read and for a grounding write's answer.
fn fact_batch(facts: &[&Fact]) -> Result<RecordBatch, SessionError> {
    let list = |pick: fn(&Fact) -> &Vec<String>| -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for f in facts {
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
        Field::new(
            "grain",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("window", DataType::Utf8, true),
        Field::new(
            "dims",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "basis",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "admitted_by",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "bucketed",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "unadmitted",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "unadmitted_why",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("alternative", DataType::Utf8, true),
        Field::new("alternative_divergence", DataType::Utf8, true),
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
            list(|f| &f.grain),
            text(|f| f.resolution.map(Resolution::as_str)),
            text(|f| f.window.as_deref()),
            list(|f| &f.dims),
            list(|f| &f.basis),
            list(|f| &f.admitted_by),
            list(|f| &f.bucketed),
            list(|f| &f.unadmitted),
            list(|f| &f.unadmitted_why),
            text(|f| f.alternative.as_deref()),
            text(|f| f.alternative_divergence.as_deref()),
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
            version_bound: None,
        })
    }

    fn key(metric: &str) -> CubeKey {
        CubeKey {
            dataset: "d".into(),
            metric: metric.into(),
            grain: None,
            pin: "p".into(),
            digest: 7,
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
