//! The cube: every grounded metric's cells — the total, the slices
//! along its judged dimensions, the disclosed rival — at the metric's
//! resolution, or at the grain a read asks for, held in memory and
//! served through two reads.
//!
//! A measurement is a claim about the data: small, adjudicated by a
//! witness, ranked by actor kind, contestable, its history the drift
//! record. The cube is the data at a grain — a GROUP BY result. It is
//! about nothing, judged by nobody, and an old cube is not drift (the
//! lake holds every snapshot). So it is a query result: **landed
//! under the cache, never recorded.** Nothing here writes the record.
//!
//! One head per metric: its cells at the floor grain over the ladder's
//! longest rung, landed in the catalog under the key (dataset, metric,
//! data legs, digest) — the pin's parts for the tables the metric's
//! frame scans, and everything else its build reads: the grounding,
//! the frame as planned, the verdicts and glosses on the columns it
//! serves, the edges on the tables it scans, the cube settings —
//! folded to one number ([`metric_digest`]). A write that cannot
//! reach the build — a ruling, a note, a check's landing, a gloss on
//! another metric's column, a landing on a table no judged time
//! column sits on — changes neither, and the entry stays a hit; a
//! moved input is a miss, never an invalidation. The one exception is
//! a frame that itself scans a workspace relation: its entry binds to
//! those relations' versions on top ([`workspace_reads`]). The fill is
//! lazy and single-flight
//! (moka's `get_with`): concurrent readers of one key share one
//! build, nothing recomputes eagerly, and the build runs where the
//! triggering read runs. The cache is the Plane's, handed to each
//! session at construction as the function runtime is; a session
//! built without a Plane carries its own.
//!
//! Every grain a read serves is a plan over the head, never a scan:
//! the cells at that grain, windowed to the ladder's rung for it, are
//! derived from the floor cells by the verb each row carries — a flow
//! sums, a ratio re-divides its summed halves, a stock takes the
//! latest floor cell in the bucket — and cached as their own entry
//! beside the head. A read at the metric's own resolution is the same
//! derivation; a grain finer than that resolution serves no rows.
//!
//! A flow's empty period is a zero cell: a sum over no rows is zero,
//! so the head carries one for every period at the metric's own
//! resolution, from the total's first to the data's edge — the latest
//! period any metric grounding's frame serves at the pin, measured on
//! the frames and never on a column of a table, so a calendar
//! dimension that runs decades ahead moves nothing — and a forecast
//! for that period is checked against it. The total and the rival
//! carry them; a member's gap stays a gap. A stock's or a ratio's
//! empty period stays a gap too: nothing stands, nothing divides.
//! Every flow's series reaches the same edge, and its window is
//! measured back from there; a plan's head (fixture 27) takes its own
//! frame's edge. The edge is a build input, so the key carries the
//! legs of every table the metrics' frames scan and the digest the
//! frames' names: a landing that moves the edge misses every head.
//!
//! Resolution, floor and windows come from the `cube` FACT aspect the
//! KPI kit declares on the dataset: a metric's own resolution is its
//! judged cadence (`temporal_profile`) and never finer than the
//! declared floor; the head stands at the floor, over the longest
//! window any grain from the resolution up may ask for, measured back
//! from the data's own edge; the ladder's rungs are the windows the
//! grains are served over. A ratio cell carries its halves at every
//! dimension, the rival included.
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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ScalarValue;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::error::DataFusionError;
use datafusion::functions::expr_fn::{abs, coalesce, greatest, nullif};
use datafusion::functions_aggregate::expr_fn::{count, count_distinct, max, sum};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, col, lit,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, Value as SQLValue,
};
use serde_json::Value;

use crate::period::{self, Resolution};
use crate::search::{QuerySlot, current_query_slots, int_column, int_value};
use crate::shared::{Held, Served, SessionError, Shared};
use crate::subject::qi;

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

/// Which set of numbers a cell belongs to. The actual world is what
/// the groundings compute from the data; the forecast world is the
/// newest `metric_forecast` run's medians, one cell per step; the
/// scenario world is a declared scenario read in two passes
/// (`crate::scenario`), named by `scenario => …`; the plan world is a
/// plan aspect's own cells served under the metric it names
/// (`crate::plan_world`, fixture 27), named by `plan => …`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum World {
    Actual,
    Forecast,
    Scenario,
    Plan,
}

impl World {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            World::Actual => "actual",
            World::Forecast => "forecast",
            World::Scenario => "scenario",
            World::Plan => "plan",
        }
    }

    fn parse(s: &str) -> Option<World> {
        match s {
            "actual" => Some(World::Actual),
            "forecast" => Some(World::Forecast),
            "scenario" => Some(World::Scenario),
            "plan" => Some(World::Plan),
            _ => None,
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
            SessionError::unknown(
                "cube",
                "no `cube` aspect is declared — the KPI kit ships it: the resolution \
                 floor and the window ladder the cube is computed under"
                    .to_string(),
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

/// One metric's fact row: what the cube admitted and why not. It
/// rides the landed cube as a tag, as JSON, which is why it derives
/// serde and owns its two basis words.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Fact {
    pub metric: String,
    /// The metric this row plans, for a plan aspect (`x-kind: plan`,
    /// fixture 27): its cells are served in the plan world under that
    /// name and in no other world. None for a metric.
    #[serde(default)]
    pub plan_of: Option<String>,
    pub applicable: bool,
    /// Whether every verdict the build admitted on stands at this pin.
    /// False after a write or an import moved the pin since the
    /// profilers ran — the numbers are current, the axes may not be.
    pub judged_current: bool,
    pub reason: Option<String>,
    pub behavior: Option<String>,
    /// Where the verb came from: `ratio` when the frame served both
    /// halves, `shape` when the value is a running total, `evidence`
    /// when the `behavior_evidence` verdict on the column the value is
    /// or sums decided (`evidence over marker` / `evidence over gloss`
    /// when the agent's word said otherwise), `marked` when the
    /// grounding's `stock` marker did, `glossed` when a `stock` gloss
    /// on that column did, `default` when nothing detected a stock and
    /// the metric is summed as a flow — the common case, and the row
    /// says so rather than leaving it a silent assumption.
    pub behavior_basis: Option<String>,
    /// The declared row identity — the grounding's `grain` columns as
    /// served. Empty when the grounding declares none: the shape is
    /// undeclared and the build takes the frame as served.
    pub grain: Vec<String>,
    pub resolution: Option<Resolution>,
    pub window: Option<String>,
    /// The periods of data at the resolution before the window — the
    /// buckets the data holds and the metric's own series does not
    /// serve. Zero when the data fits the rung; a `cube` gloss widening
    /// the rung is what moves it.
    #[serde(default)]
    pub outside: i64,
    pub dims: Vec<String>,
    /// Per admitted dimension, in `dims` order: the column subject whose
    /// verdict admitted it — its own, or the key column reached through
    /// a declared edge when the axis is a label in the key's table.
    pub basis: Vec<String>,
    /// Per admitted dimension, in `dims` order, what decided:
    /// `measurement` when the verdict alone did, `human` or `agent`
    /// when a `dimension` gloss or the grounding's `axes` admitted it.
    pub admitted_by: Vec<String>,
    /// What decides the axes: `authored` when the grounding lists them
    /// (`axes`), `measured` when the verdicts and the column glosses
    /// do, `measured over authored` when the grounding's empty list did
    /// not hold — it closes a distinct count or a ratio, the shapes no
    /// column slices whole, and on any other the verdicts decide.
    pub axes_basis: String,
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
    /// The act behind each, at the same index, as a keyed tag a route
    /// can read: `verdict` (no verdict yet — run `dimension_relevance()`
    /// over the subject, or gloss `dimension`), `abstained` (the
    /// verdict abstained — declare the edge, or gloss `dimension`),
    /// `none` (closed by a `dimension` gloss), `closed` (by the
    /// grounding's `axes` — `closed over verdict` or `closed over
    /// gloss` where a verdict or a `dimension` gloss admits the column
    /// the author closed), `unserved` (listed in `axes` and not a
    /// column the cube can slice on), `expression`, `single` and `cap`
    /// — the last five are terminal: nothing admits the column as it
    /// is served, and the grounding is where `closed` and `unserved`
    /// change.
    pub unadmitted_act: Vec<String>,
    /// The measurements this row reads and no function has landed —
    /// the function to run, and in `wanted_over` at the same index
    /// the column subject to run it over: the function returning
    /// `temporal_profile` over each source column of a served date
    /// without a verdict, the one returning `dimension_relevance`
    /// over each candidate column with neither a verdict nor a
    /// `dimension` gloss. `owed` lists them as `never measured`; the
    /// docket's re-measure runs them.
    pub wanted: Vec<String>,
    pub wanted_over: Vec<String>,
    /// Where the frame serves nothing the cube could slice on and the
    /// grounding lists no axes: the columns of the tables it scans
    /// that a `dimension` gloss or an applicable relevance verdict
    /// admits and the frame does not serve — the axis is judged, the
    /// road is to serve the column. Empty elsewhere.
    #[serde(default)]
    pub unserved: Vec<String>,
    pub alternative: Option<String>,
    /// The measured disagreement between the metric's total series and
    /// the rival's, over their shared periods — with an authored
    /// `tolerance` on the disclosing assumption, the count of periods
    /// breaching it; without one, the maximum relative gap. None when
    /// no rival is served.
    pub alternative_divergence: Option<String>,
    pub alternative_error: Option<String>,
    /// At a grounding write only: the gap between the serving frame's
    /// totals and the newest other writing on the same slot over their
    /// shared periods, and the periods one serves and the other does
    /// not — what the re-record changed, read at the decision moment.
    /// None on a read and on a first grounding.
    pub superseded_divergence: Option<String>,
}

impl Fact {
    fn abstain(metric: &str, reason: String) -> Fact {
        Fact {
            metric: metric.to_string(),
            plan_of: None,
            applicable: false,
            judged_current: true,
            reason: Some(reason),
            behavior: None,
            behavior_basis: None,
            grain: Vec::new(),
            resolution: None,
            window: None,
            outside: 0,
            dims: Vec::new(),
            basis: Vec::new(),
            admitted_by: Vec::new(),
            axes_basis: "measured".into(),
            bucketed: Vec::new(),
            unadmitted: Vec::new(),
            unadmitted_why: Vec::new(),
            unadmitted_act: Vec::new(),
            wanted: Vec::new(),
            wanted_over: Vec::new(),
            unserved: Vec::new(),
            alternative: None,
            alternative_divergence: None,
            alternative_error: None,
            superseded_divergence: None,
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
    /// The judged time axis; none is no series, and the fact row
    /// abstains with what it wants.
    tcol: Option<String>,
    /// The declared grain, every column verified served; empty when
    /// the grounding declares none.
    grain: Vec<String>,
    /// The metric's own resolution: the coarser of its judged cadence
    /// and the floor.
    resolution: Resolution,
    /// The ladder's rung for the resolution — the window the metric's
    /// own series is served over.
    window: Option<String>,
    verb: &'static str,
    behavior_basis: &'static str,
    /// The time axis's and the verb's currency, folded; each admitted
    /// dimension folds its own in later.
    judged_current: bool,
    candidates: Vec<Candidate>,
    unadmitted: Vec<(String, String, &'static str)>,
    axes_basis: &'static str,
    /// What the row reads and nobody measured — `(function, subject)`.
    wanted: Vec<(String, String)>,
    /// [`Fact::unserved`].
    unserved: Vec<String>,
    /// The workspace relations the frame scans ([`workspace_reads`])
    /// — the build binds such an entry to their versions.
    foreign: Vec<String>,
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
        let (wanted, wanted_over): (Vec<String>, Vec<String>) = self.wanted.iter().cloned().unzip();
        let Some(_) = self.tcol else {
            let mut fact = Fact::abstain(metric, NO_JUDGED_TIME.into());
            fact.wanted = wanted;
            fact.wanted_over = wanted_over;
            return fact;
        };
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
                    "cap",
                ));
                continue;
            }
            dims.push(c.column);
            basis.push(c.basis);
            admitted_by.push(c.admitted_by.to_string());
            judged_current &= c.current;
        }
        let (unadmitted, unadmitted_why, unadmitted_act) = split_unadmitted(unadmitted);
        Fact {
            metric: metric.to_string(),
            plan_of: None,
            applicable: true,
            judged_current,
            reason: None,
            behavior: Some(self.verb.to_string()),
            behavior_basis: Some(self.behavior_basis.to_string()),
            grain: self.grain,
            resolution: Some(self.resolution),
            window: self.window,
            outside: 0,
            dims,
            basis,
            admitted_by,
            axes_basis: self.axes_basis.to_string(),
            bucketed: Vec::new(),
            unadmitted,
            unadmitted_why,
            unadmitted_act,
            wanted,
            wanted_over,
            unserved: self.unserved,
            alternative: None,
            alternative_divergence: None,
            alternative_error: None,
            superseded_divergence: None,
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
/// `(metric, world, dimension, member, period, value, num, den, behavior)`, dimension
/// `''` the total, `'alternative'` the rival, `behavior` the verb that
/// produced the row (a rival's may differ from the metric's).
#[derive(Debug)]
pub(crate) struct Cube {
    pub fact: Fact,
    pub cells: RecordBatch,
    /// Where the frame (or its rival) scans workspace relations —
    /// such a frame reads what writes move without touching the
    /// metric surface — the relations it scans and their versions at
    /// build ([`glossql_glossary::version_view`]): the entry serves
    /// while those stand. None for the ordinary frame over the
    /// dataset's own tables.
    pub version_bound: Option<(Vec<String>, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CubeKey {
    dataset: String,
    metric: String,
    /// `None` for the head — the floor cells over the longest rung,
    /// the one entry that is built, loaded and landed; `Some` for the
    /// cells a read serves at a grain, a plan over the head cached
    /// beside it.
    grain: Option<Resolution>,
    /// The pin's legs for the tables the frame scans — the data. The
    /// other tables' legs and the workspace relations' stay out: a
    /// landing elsewhere and every write move those, and what of the
    /// relations a build reads is the digest's business.
    pin: String,
    /// Everything else the build reads, folded — [`metric_digest`] —
    /// as hex, stable across processes: it rides the landed cube as
    /// its key tag.
    digest: String,
}

impl CubeKey {
    /// The landed head's table, in the catalog's `main` schema. Only
    /// the head lands; a grain entry has no table.
    fn table(&self) -> String {
        format!("cube__{}__{}", self.dataset, self.metric)
    }

    /// What the head's key tag says: the data legs and the digest.
    fn tag(&self) -> String {
        format!("{}\n{}", self.pin, self.digest)
    }
}

/// The schema the landed cubes live in — schema id 0, outside the
/// mount, so a cube table is neither a dataset's nor a subject.
const CUBE_SCHEMA: &str = "main";
/// The head's tags: the key it was built at, and its fact row as JSON.
const CUBE_KEY_TAG: &str = "glossql.cube.key";
const CUBE_FACT_TAG: &str = "glossql.cube.fact";
/// The shape of a head, folded into every digest: a head landed under
/// another shape — cells at the metric's own resolution over its own
/// rung, once — misses and is built over.
const HEAD_SHAPE: &str =
    "floor cells over the longest rung, the world named, a flow's empty periods zero to the edge";

/// A `Hasher` over blake3: every `Hash` impl the digest folds writes
/// its bytes here, so the digest is the same in every process — the
/// standard library's hasher is not, and this one rides the catalog.
/// Integers hash in native byte order, as `Hash` writes them; every
/// host this runs on is little-endian.
struct Digest(blake3::Hasher);

impl std::hash::Hasher for Digest {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Unused: the digest is read as hex, never as a number.
    fn finish(&self) -> u64 {
        0
    }
}

/// What one metric's build reads of the dataset: the tables its frame
/// scans and the column subjects it judges — every source column of a
/// served field, the column the value sums, and the key columns of
/// the declared edges on a scanned table, through which a label
/// borrows a key's verdict. Empty for a frame that does not plan.
#[derive(Default)]
struct Reads {
    tables: Vec<String>,
    subjects: Vec<String>,
    /// Whether the frame serves any column the cube could slice on —
    /// neither the value, a half nor time-typed. A frame that serves
    /// none names the judged columns it leaves out
    /// ([`Fact::unserved`]), so its build reads every column of the
    /// tables it scans, and the digest folds them.
    sliceable: bool,
}

fn reads_of(
    plan: &datafusion::logical_expr::LogicalPlan,
    dataset: &str,
    pointers: &[crate::behavior::Pointer],
) -> Reads {
    let scanned = crate::provenance::scanned_tables(plan, dataset);
    let sliceable = plan.schema().fields().iter().any(|f| {
        !matches!(f.name().as_str(), "value" | "num" | "den")
            && !crate::plan::is_temporal(f.data_type())
    });
    let mut subjects: std::collections::BTreeSet<String> =
        crate::provenance::served_sources(plan, dataset)
            .into_values()
            .flatten()
            .collect();
    subjects.extend(crate::provenance::summed_source(plan, "value", dataset));
    for p in pointers {
        for (table, cols) in [(&p.src_t, &p.src_cols), (&p.dst_t, &p.dst_cols)] {
            if scanned.contains(table) {
                subjects.extend(cols.iter().map(|c| format!("{table}.{c}")));
            }
        }
    }
    let mut tables: Vec<String> = scanned.into_iter().collect();
    tables.sort();
    Reads {
        tables,
        subjects: subjects.into_iter().collect(),
        sliceable,
    }
}

/// Everything one metric's build reads besides the data, folded to
/// one number: its own grounding (the frame, its markers, its
/// disclosed rival), the frame as planned — which carries every
/// `read.` it expands — the verdicts and glosses on the subjects it
/// reads, the declared edges on the tables it scans, the functions
/// that return the verdicts, and the cube settings. Two contexts
/// digesting alike build alike, so a write that cannot reach this
/// build — a ruling, a note gloss, a check's landing, a gloss on a
/// column no served field descends from — keeps the entry a hit.
/// Stable across processes, so a head landed by one instance serves
/// another.
/// Completeness is checkable in one file: `plan` and `build` read
/// nothing of the store beyond (slot, judged, settings) and the data's
/// edge over the metrics' frames, whose names fold in here and whose
/// tables are key legs — the frame's own scans are
/// [`workspace_reads`]'s to catch.
fn metric_digest(
    slot: &QuerySlot,
    probe: Option<&datafusion::logical_expr::LogicalPlan>,
    reads: &Reads,
    judged: &Judged,
    settings: &Settings,
    shapes: &HashMap<String, glossql_catalog::Shape>,
    edge_frames: &[String],
) -> String {
    use std::hash::{Hash, Hasher};
    fn verdict(h: &mut impl Hasher, m: &HashMap<String, Verdict>, subject: &str) {
        if let Some(v) = m.get(subject) {
            v.body.to_string().hash(h);
            v.current.hash(h);
        } else {
            0u8.hash(h);
        }
    }
    fn gloss(h: &mut impl Hasher, m: &HashMap<String, (Value, u8)>, subject: &str) {
        if let Some((v, rank)) = m.get(subject) {
            v.to_string().hash(h);
            rank.hash(h);
        } else {
            0u8.hash(h);
        }
    }
    let mut h = Digest(blake3::Hasher::new());
    HEAD_SHAPE.hash(&mut h);
    for s in edge_frames {
        s.hash(&mut h);
    }
    slot.subject.hash(&mut h);
    slot.aspect.hash(&mut h);
    slot.body.hash(&mut h);
    if let Some(plan) = probe {
        plan.display_indent_schema().to_string().hash(&mut h);
    }
    for s in &reads.subjects {
        s.hash(&mut h);
        verdict(&mut h, &judged.temporal, s);
        verdict(&mut h, &judged.relevance, s);
        verdict(&mut h, &judged.behavior, s);
        gloss(&mut h, &judged.behavior_gloss, s);
        gloss(&mut h, &judged.dimension, s);
    }
    // A frame with nothing to slice on reads the admission of every
    // column of the tables it scans, to name what it leaves out.
    if !reads.sliceable {
        for t in &reads.tables {
            let Some(shape) = shapes.get(t) else {
                continue;
            };
            let mut columns: Vec<&String> = shape.columns.keys().collect();
            columns.sort();
            for c in columns {
                let s = format!("{t}.{c}");
                s.hash(&mut h);
                verdict(&mut h, &judged.relevance, &s);
                gloss(&mut h, &judged.dimension, &s);
            }
        }
    }
    for p in &judged.pointers {
        if !reads.tables.contains(&p.src_t) && !reads.tables.contains(&p.dst_t) {
            continue;
        }
        p.src_t.hash(&mut h);
        p.src_cols.hash(&mut h);
        p.dst_t.hash(&mut h);
        p.dst_cols.hash(&mut h);
    }
    judged.temporal_fn.hash(&mut h);
    judged.relevance_fn.hash(&mut h);
    judged.behavior_fn.hash(&mut h);
    settings.floor.as_str().hash(&mut h);
    let mut windows: Vec<_> = settings.windows.iter().collect();
    windows.sort_by_key(|(r, _)| **r);
    for (r, w) in windows {
        r.as_str().hash(&mut h);
        w.hash(&mut h);
    }
    h.0.finalize().to_hex().to_string()
}

/// The workspace relations a frame's plan scans, by name, sorted;
/// `*` where it scans a shipped read or `current_dataset`, whose own
/// reads are not enumerated here. The reserved-name rule is what
/// makes a name check sound: no dataset table can bear one of these
/// names, so a match is never a false positive. The reads' compute
/// doors (`GLOSSARY()`, the cube's own functions) do not serve a
/// grounding's plan — such a frame abstains with the engine's
/// refusal, which no write can flip — so scans are the whole surface
/// to catch.
fn workspace_reads(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<String> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::logical_expr::LogicalPlan;
    let mut found = std::collections::BTreeSet::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(t) = node {
            let name = t.table_name.table();
            if glossql_glossary::RELATIONS.iter().any(|r| r.name == name) {
                found.insert(name.to_string());
            } else if crate::library::LIBRARY.iter().any(|(n, _)| *n == name)
                || name == "current_dataset"
            {
                found.insert("*".to_string());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the visitor never errs");
    found.into_iter().collect()
}

/// The cache: LRU by bytes (generational keys want recency, not
/// frequency), weighed by each entry's Arrow footprint, capped at the
/// process-wide byte budget. moka evicts in pending tasks, so the cap
/// is approximate — fine for a compute cache.
#[derive(Debug, Clone)]
pub struct CubeCache {
    inner: moka::future::Cache<CubeKey, Arc<Cube>>,
    builds: Arc<AtomicU64>,
    /// Entries loaded from a landed head instead of built.
    loads: Arc<AtomicU64>,
}

impl Shared {
    /// The cube cache this session reads and fills — the Plane's,
    /// shared by every channel, or the session's own when it was built
    /// without one. The slot is untyped where it is held; this is the
    /// one place that reads it, beside the type it holds.
    pub fn cube(&self) -> CubeCache {
        self.cube
            .held()
            .downcast_ref::<CubeCache>()
            .cloned()
            .expect("the cube slot holds the cube cache, set at construction")
    }
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
            loads: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many builds this cache has run — one per miss no head
    /// answered, whatever the number of readers that shared it.
    pub fn builds(&self) -> u64 {
        self.builds.load(Ordering::Relaxed)
    }

    /// How many entries a landed head served in place of a build.
    pub fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
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
    /// The declared function returning each of the two — what a
    /// fact row names as wanted over a column nobody judged; none
    /// declared, nothing to want.
    temporal_fn: Option<String>,
    relevance_fn: Option<String>,
    /// `behavior_evidence` per column — the verb's read, ahead of the
    /// grounding's word; and the function returning it, which a
    /// grounding write runs over the column its value sums when no
    /// verdict stands there.
    behavior: HashMap<String, Verdict>,
    behavior_fn: Option<String>,
    /// The collapsed `behavior` gloss per column (human over agent) —
    /// a `stock` there is a stock where no verdict decided.
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

/// The judged surface a monthly reader anchors on, each by column
/// subject: temporal verdicts for the time axis, behavior verdicts and
/// the `behavior` glosses with their speaker rank for the verb. Read
/// once per door call, so a replay or a fingerprint never folds a
/// metric by a different word than its cube.
pub(crate) struct Anchors {
    pub(crate) temporal: HashMap<String, Verdict>,
    pub(crate) behavior: HashMap<String, Verdict>,
    pub(crate) behavior_gloss: HashMap<String, (Value, u8)>,
}

impl Anchors {
    pub(crate) async fn at(
        rctx: &glossql_glossary::ReadContext,
        dataset: &str,
    ) -> Result<Self, SessionError> {
        Ok(Self {
            temporal: judged_bodies(rctx, dataset, "temporal_profile"),
            behavior: judged_bodies(rctx, dataset, "behavior_evidence"),
            behavior_gloss: crate::search::current_fact_values(rctx, dataset, "behavior").await?,
        })
    }
}

/// The declared function that returns a measurement aspect from this
/// dataset — the first by name where several do; none where none is
/// declared.
pub(crate) fn returning(
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
    aspect: &str,
) -> Option<String> {
    rctx.functions
        .iter()
        .filter(|f| {
            f.returns.as_deref() == Some(aspect)
                && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
        })
        .map(|f| f.name.clone())
        .min()
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
        if !crate::plan::is_temporal(f.data_type()) {
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
    /// `Fact::behavior_basis`.
    pub basis: &'static str,
    /// Whether the verdict read stands at this pin; true where none was.
    pub current: bool,
}

/// A grounding's verb: a flow unless something detects a ratio or a
/// stock. A ratio serves both halves of its division. A stock is
/// detected by the SQL's shape — the value is a running total — or by
/// the `behavior_evidence` verdict on the column the value is, or is
/// one `sum` of (`provenance::summed_source`), or, where no verdict
/// decided, by the grounding's own `stock` marker or a `stock` gloss
/// on that column — the agent's word, read only where the data could
/// not speak. A verdict that decides also decides against the word,
/// and the basis says so. Nothing else is consulted: a `flow` marker
/// or gloss changes the basis, never the verb. One function for the
/// cube and the walk, so the two never fold one metric two ways.
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
    if crate::provenance::running_total(probe, "value") {
        return verb("stock", "shape", true);
    }
    let marker = body.get("behavior").and_then(Value::as_str);
    let source = crate::provenance::summed_source(probe, "value", dataset);
    let gloss = source
        .as_ref()
        .and_then(|subject| glossed.get(subject))
        .and_then(|(gloss, _)| gloss["value"].as_str());
    let judged = source
        .as_ref()
        .and_then(|subject| behavior.get(subject))
        .filter(|v| v.body["applicable"].as_bool() == Some(true));
    if let Some((verdict, current)) =
        judged.and_then(|v| Some((v.body["summary"]["verdict"].as_str()?, v.current)))
        && matches!(verdict, "stock" | "flow")
    {
        let word = marker.or(gloss).filter(|w| matches!(*w, "stock" | "flow"));
        let basis = match word {
            Some(w) if w != verdict && marker.is_some() => "evidence over marker",
            Some(w) if w != verdict => "evidence over gloss",
            _ => "evidence",
        };
        return verb(
            if verdict == "stock" { "stock" } else { "flow" },
            basis,
            current,
        );
    }
    if marker == Some("stock") {
        return verb("stock", "marked", true);
    }
    if gloss == Some("stock") {
        return verb("stock", "glossed", true);
    }
    match (marker, gloss) {
        (Some("flow"), _) => verb("flow", "marked", true),
        (_, Some("flow")) => verb("flow", "glossed", true),
        _ => verb("flow", "default", true),
    }
}

/// Every current grounding's cube, built where missing. The slots are
/// the store's collapsed read — contested out, human over agent — so
/// the enumeration itself costs no build; the judged surface is read
/// only when some metric misses.
/// What one cube read loads once: the bound dataset's current
/// groundings, the judged surface and the settings every key digests
/// its own reads of, the pin the data legs are cut from, and the
/// cache the entries live in. Loaded before any key, not on a miss —
/// in-memory work over a context already in hand, which is what buys
/// the hit on every write that cannot reach a build.
/// A value filled once per surface behind an async lock, shared by
/// every reader after: the frames and their edges.
type Once<T> = futures::lock::Mutex<Option<Arc<T>>>;

/// One grounding's frame as planned at a surface: the plan the build
/// takes along, the dataset tables it scans, and its judged time column
/// where the frame serves a judged date.
struct FrameInfo {
    probe: Option<datafusion::logical_expr::LogicalPlan>,
    scans: Vec<String>,
    tcol: Option<String>,
}

struct Surface {
    dataset: String,
    version: String,
    /// The metrics' groundings — the heads of the actual world.
    slots: Vec<QuerySlot>,
    /// The plan aspects' groundings (fixture 27): a head each, served
    /// in the plan world under the metric each names and in no other.
    plans: Vec<QuerySlot>,
    /// Every grounding's frame as planned at this surface, once: what
    /// the key legs and the data's edge are measured over. Filled on
    /// first use behind an async lock, as [`Surface::edges`] is.
    frames: Once<HashMap<String, FrameInfo>>,
    /// The last period each frame serves, once per surface, read when
    /// the first head builds: the data's edge is their latest.
    edges: Once<HashMap<String, Option<i64>>>,
    judged: Judged,
    settings: Settings,
    /// The bound dataset's tables and their columns at the pin — what
    /// a frame could serve and does not ([`Fact::unserved`]).
    shapes: HashMap<String, glossql_catalog::Shape>,
    pin_text: String,
    cache: CubeCache,
    ctx: SessionContext,
    lake: glossql_catalog::Lake,
}

impl Surface {
    /// `None` with nothing grounded: honest absence stays honest —
    /// there is nothing to key, and a workspace without the `cube`
    /// aspect is not asked for it.
    async fn load(shared: &Arc<Shared>) -> Result<Option<Surface>, SessionError> {
        let dataset = shared.dataset().ok_or(SessionError::NoDataset)?;
        let rctx = shared.read_context().await?;
        // The plan aspects apart (fixture 27): a plan is a grounding
        // with a head of its own and no metric of the actual world.
        let (plans, slots): (Vec<QuerySlot>, Vec<QuerySlot>) = current_query_slots(&rctx, &dataset)
            .await?
            .into_iter()
            .partition(QuerySlot::is_plan);
        if slots.is_empty() && plans.is_empty() {
            return Ok(None);
        }
        let (judged, settings) = judged_surface(shared, &rctx, &dataset).await?;
        Ok(Some(Surface {
            dataset,
            version: rctx.version.clone(),
            slots,
            plans,
            frames: futures::lock::Mutex::new(None),
            edges: futures::lock::Mutex::new(None),
            judged,
            settings,
            shapes: rctx.shapes.clone(),
            pin_text: rctx.pin.text.clone(),
            cache: shared.cube(),
            ctx: shared.session_ctx(),
            lake: shared.lake(),
        }))
    }

    /// Every grounding's frame planned once at this surface: the plan
    /// the build takes along, the dataset tables it scans, and its
    /// judged time column. Planning scans nothing; a frame that does
    /// not plan has no plan and no time column, and its build abstains
    /// with the planner's reason.
    async fn frames(&self, shared: &Arc<Shared>) -> Arc<HashMap<String, FrameInfo>> {
        let mut slot_guard = self.frames.lock().await;
        if let Some(frames) = slot_guard.as_ref() {
            return Arc::clone(frames);
        }
        let frames = Arc::new({
            let mut out = HashMap::new();
            for slot in self.slots.iter().chain(&self.plans) {
                let probe = match frame_sql(&slot.body) {
                    Some(sql) => crate::plan::build_plan(shared, &self.ctx, &sql).await.ok(),
                    None => None,
                };
                let (scans, tcol) = match &probe {
                    Some(plan) => {
                        let mut scans: Vec<String> =
                            crate::provenance::scanned_tables(plan, &self.dataset)
                                .into_iter()
                                .collect();
                        scans.sort();
                        let sources = crate::provenance::served_sources(plan, &self.dataset);
                        let tcol =
                            judged_time_column(plan.schema(), &sources, &self.judged.temporal)
                                .map(|(column, ..)| column)
                                .filter(|c| {
                                    plan.schema().fields().iter().any(|f| {
                                        f.name() == c && crate::plan::is_temporal(f.data_type())
                                    })
                                });
                        (scans, tcol)
                    }
                    None => (Vec::new(), None),
                };
                out.insert(slot.aspect.clone(), FrameInfo { probe, scans, tcol });
            }
            out
        });
        *slot_guard = Some(Arc::clone(&frames));
        frames
    }

    /// The frames the data's edge of one head is measured over: the
    /// metrics' for a metric's head, its own for a plan's (fixture 27:
    /// a budget landed to the period's end is no edge of the actuals).
    fn edge_slots<'a>(&'a self, slot: &'a QuerySlot) -> Vec<&'a QuerySlot> {
        if slot.is_plan() {
            vec![slot]
        } else {
            self.slots.iter().collect()
        }
    }

    /// The tables the edge's frames scan, sorted: key legs beside the
    /// frame's own, since a landing on any of them may move the edge.
    async fn edge_tables(&self, shared: &Arc<Shared>, slot: &QuerySlot) -> Vec<String> {
        let frames = self.frames(shared).await;
        let mut out: Vec<String> = self
            .edge_slots(slot)
            .into_iter()
            .filter_map(|s| frames.get(&s.aspect))
            .flat_map(|f| f.scans.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The last period each frame serves, in the period unit —
    /// `max` of its judged time column over the frame itself,
    /// so a calendar row no fact joins counts for nothing. Read once
    /// per surface, when the first head builds; one aggregate per
    /// frame. None for a frame without a judged time column, one that
    /// does not run, or one that serves no row.
    async fn edges(&self, shared: &Arc<Shared>) -> Arc<HashMap<String, Option<i64>>> {
        let mut guard = self.edges.lock().await;
        if let Some(edges) = guard.as_ref() {
            return Arc::clone(edges);
        }
        let frames = self.frames(shared).await;
        let edges = Arc::new({
            let mut out = HashMap::new();
            for slot in self.slots.iter().chain(&self.plans) {
                let edge = match (frames.get(&slot.aspect), frame_sql(&slot.body)) {
                    (
                        Some(FrameInfo {
                            tcol: Some(tcol), ..
                        }),
                        Some(sql),
                    ) => {
                        let q = format!(
                            "SELECT CAST(max({}) AS {}) AS edge FROM ({sql}) AS frame",
                            qi(tcol),
                            period::TYPE
                        );
                        match run(shared, &self.ctx, &q).await {
                            Ok(batches) => batches
                                .iter()
                                .find(|b| b.num_rows() > 0)
                                .and_then(|b| b.column_by_name("edge").cloned())
                                .filter(|c| !c.is_null(0))
                                .and_then(|c| period::column(&c).map(|a| a.value(0))),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                };
                out.insert(slot.aspect.clone(), edge);
            }
            out
        });
        *guard = Some(Arc::clone(&edges));
        edges
    }

    /// The data's edge for one head: the latest period the edge's
    /// frames serve; the plan that fills a flow's empty periods buckets
    /// it at the resolution. None where no frame serves a judged date.
    async fn data_edge(&self, shared: &Arc<Shared>, slot: &QuerySlot) -> Option<i64> {
        let edges = self.edges(shared).await;
        self.edge_slots(slot)
            .into_iter()
            .filter_map(|s| edges.get(&s.aspect).copied().flatten())
            .max()
    }

    /// One metric's key at this surface, with the frame as planned.
    /// The key is cut from the plan — planning scans nothing, and the
    /// build takes the plan along rather than planning twice; a frame
    /// that does not plan keys on the grounding alone, and its build
    /// abstains with the planner's reason.
    async fn key(
        &self,
        shared: &Arc<Shared>,
        slot: &QuerySlot,
        grain: Option<Resolution>,
    ) -> (CubeKey, Option<datafusion::logical_expr::LogicalPlan>) {
        let probe = self
            .frames(shared)
            .await
            .get(&slot.aspect)
            .and_then(|f| f.probe.clone());
        let reads = probe
            .as_ref()
            .map(|p| reads_of(p, &self.dataset, &self.judged.pointers))
            .unwrap_or_default();
        // The data's edge is a build input of every flow, so the legs
        // carry every table the edge's frames scan and the digest the
        // frames' names: a landing that moves the edge misses, and so
        // does a grounding declared or retired. A re-record of another
        // frame that moves the edge is caught by the next landing.
        let edge_frames: Vec<String> = self
            .edge_slots(slot)
            .into_iter()
            .map(|s| s.aspect.clone())
            .collect();
        let mut tables = reads.tables.clone();
        tables.extend(self.edge_tables(shared, slot).await);
        tables.sort();
        tables.dedup();
        let key = CubeKey {
            dataset: self.dataset.clone(),
            metric: slot.aspect.clone(),
            grain,
            pin: glossql_glossary::table_legs(&self.pin_text, &self.dataset, &tables),
            digest: metric_digest(
                slot,
                probe.as_ref(),
                &reads,
                &self.judged,
                &self.settings,
                &self.shapes,
                &edge_frames,
            ),
        };
        (key, probe)
    }

    /// The cached entry under `key`, where it stands. A version-bound
    /// entry — its frame scans workspace relations — serves while
    /// their versions stand, and is dropped when they moved.
    async fn standing(&self, key: &CubeKey) -> Option<Arc<Cube>> {
        let cube = self.cache.inner.get(key).await?;
        if cube.version_bound.as_ref().is_none_or(|(relations, at)| {
            *at == glossql_glossary::version_view(&self.version, relations)
        }) {
            return Some(cube);
        }
        self.cache.inner.invalidate(key).await;
        None
    }

    /// One metric's head: its cells at the floor over the longest
    /// rung. A hit, or one single-flight fill shared by every reader
    /// of the key — the landed head where one stands at this key, the
    /// build otherwise, landed as it finishes.
    async fn entry(&self, shared: &Arc<Shared>, slot: &QuerySlot) -> Arc<Cube> {
        let (key, probe) = self.key(shared, slot, None).await;
        if let Some(cube) = self.standing(&key).await {
            return cube;
        }
        self.cache
            .inner
            .get_with(key.clone(), async {
                // The head first: a cube landed at this key — by
                // another instance, or by this one before a restart —
                // is one file read, not a build.
                if let Some(cube) = self.load_head(&key).await {
                    self.cache.loads.fetch_add(1, Ordering::Relaxed);
                    return Arc::new(cube);
                }
                self.cache.builds.fetch_add(1, Ordering::Relaxed);
                let span = tracing::info_span!(
                    "cube",
                    dataset = %self.dataset,
                    metric = %slot.aspect,
                    rows = tracing::field::Empty,
                );
                // Boxed: the build's future carries the whole frame —
                // schema, subjects, cells, the fact — and it is awaited
                // inside moka's own, inside the read's. Left on the
                // stack it overflows a test thread's 2 MB, which is the
                // same reason every `build_plan` call below is pinned.
                let cube = tracing::Instrument::instrument(
                    Box::pin(build_metric(shared, self, slot, probe)),
                    span.clone(),
                )
                .await;
                span.record("rows", cube.cells.num_rows());
                self.land_head(&key, &cube).await;
                Arc::new(cube)
            })
            .await
    }

    /// One metric's cells at a grain: a plan over its head, windowed
    /// to the ladder's rung for the grain, cached beside the head
    /// under the same key legs. Never a build and never landed — the
    /// head is what lands, and a fresh instance derives from what it
    /// loads. A head that abstains serves as it is.
    async fn at_grain(
        &self,
        shared: &Arc<Shared>,
        slot: &QuerySlot,
        head: Arc<Cube>,
        grain: Resolution,
    ) -> Arc<Cube> {
        if !head.fact.applicable {
            return head;
        }
        let (key, _) = self.key(shared, slot, Some(grain)).await;
        if let Some(cube) = self.standing(&key).await {
            return cube;
        }
        let window = self.settings.windows.get(&grain).cloned();
        self.cache
            .inner
            .get_with(key, async {
                match derive(shared, &head, grain, window.as_deref()).await {
                    Ok(cube) => Arc::new(cube),
                    Err(abstained) => {
                        let mut fact = Fact::abstain(&slot.aspect, abstained.text());
                        fact.plan_of = slot.plan_of.clone();
                        Arc::new(Cube {
                            fact,
                            cells: RecordBatch::new_empty(series_schema()),
                            version_bound: head.version_bound.clone(),
                        })
                    }
                }
            })
            .await
    }

    /// The cube landed under `key`, if the head carries exactly that
    /// key: its cells read from the file, its fact row from the tag.
    /// Any other key, no table, or a head this build cannot read is
    /// none — the build runs, and lands over it. Type-erased, as
    /// [`Surface::land_head`] is: both carry the lake's writer and
    /// reader futures, and inside every reader's future those overflow
    /// the compiler's `Send` proof for the doors' handlers.
    fn load_head<'a>(
        &'a self,
        key: &'a CubeKey,
    ) -> std::pin::Pin<Box<dyn Future<Output = Option<Cube>> + Send + 'a>> {
        Box::pin(self.load_head_inner(key))
    }

    async fn load_head_inner(&self, key: &CubeKey) -> Option<Cube> {
        let table = key.table();
        let tags = self.lake.tags(CUBE_SCHEMA, &table).await.ok()?;
        if tags.get(CUBE_KEY_TAG) != Some(&key.tag()) {
            return None;
        }
        let fact: Fact = serde_json::from_str(tags.get(CUBE_FACT_TAG)?).ok()?;
        let pinned = self
            .lake
            .pin_tables(CUBE_SCHEMA, std::slice::from_ref(&table))
            .await
            .ok()
            .flatten()?
            .pop()?;
        let batches = self
            .ctx
            .read_table(pinned.provider)
            .ok()?
            .collect()
            .await
            .ok()?;
        let cells = datafusion::arrow::compute::concat_batches(&series_schema(), &batches).ok()?;
        tracing::info!(cube = %table, rows = cells.num_rows(), "cube loaded from its head");
        Some(Cube {
            fact,
            cells,
            version_bound: None,
        })
    }

    /// The built cube landed as the head under its key, replaced in one
    /// commit so a reader attached to the catalog sees the current
    /// cube at the current snapshot. An abstention and an entry bound
    /// to the record's version stay in memory only: the first has no
    /// cells, the second no key a catalog reader could check. A
    /// landing that fails is logged; the entry serves either way.
    fn land_head<'a>(
        &'a self,
        key: &'a CubeKey,
        cube: &'a Cube,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(self.land_head_inner(key, cube))
    }

    async fn land_head_inner(&self, key: &CubeKey, cube: &Cube) {
        if !cube.fact.applicable || cube.version_bound.is_some() {
            return;
        }
        let Ok(fact) = serde_json::to_string(&cube.fact) else {
            return;
        };
        let table = key.table();
        let landed = async {
            let schema = cube.cells.schema();
            let rows = Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    futures::stream::iter([Ok(cube.cells.clone())]),
                ),
            );
            let written = self
                .lake
                .write(CUBE_SCHEMA, &table, Arc::clone(&schema), rows)
                .await?;
            let landing = if self.lake.table_exists(CUBE_SCHEMA, &table).await? {
                glossql_catalog::Landing::Replace
            } else {
                glossql_catalog::Landing::Create
            };
            self.lake
                .commit_tagged(
                    CUBE_SCHEMA,
                    &table,
                    &schema,
                    written,
                    landing,
                    &HashMap::new(),
                    &[
                        (CUBE_KEY_TAG.to_string(), key.tag()),
                        (CUBE_FACT_TAG.to_string(), fact),
                    ],
                )
                .await
        }
        .await;
        match landed {
            Ok(version) => tracing::info!(cube = %table, version, "cube landed"),
            Err(e) => tracing::warn!(cube = %table, "the cube was not landed: {e}"),
        }
    }
}

/// Every metric's head, and every plan's — what `metric_axes()`
/// describes. A landing calls it to rebuild and land the heads over
/// the tables it moved: every other entry is a hit, and every grain
/// follows from its head.
pub(crate) async fn cubes(shared: &Arc<Shared>) -> Result<Vec<Arc<Cube>>, SessionError> {
    let Some(surface) = Surface::load(shared).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(surface.slots.len() + surface.plans.len());
    for slot in surface.slots.iter().chain(&surface.plans) {
        out.push(surface.entry(shared, slot).await);
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
            temporal_fn: returning(rctx, dataset, "temporal_profile"),
            relevance_fn: returning(rctx, dataset, "dimension_relevance"),
            behavior: judged_bodies(rctx, dataset, "behavior_evidence"),
            behavior_fn: returning(rctx, dataset, "behavior_evidence"),
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
) -> Result<Fact, SessionError> {
    Ok(match write_fact(shared, dataset, subject, aspect).await {
        Ok(fact) => fact,
        Err(abstained) => Fact::abstain(aspect, abstained.text()),
    })
}

async fn write_fact(
    shared: &Arc<Shared>,
    dataset: &str,
    subject: &str,
    aspect: &str,
) -> Result<Fact, Abstain> {
    let bound = shared.dataset();
    if bound.as_deref() != Some(dataset) {
        let here = bound.map_or("no dataset".to_string(), |b| format!("`{b}`"));
        return Err(Abstain::shape(
            aspect,
            format!(
                "not judged from here: the call is bound to {here} — `USE {dataset};` and \
                 metric_axes() judges it"
            ),
        ));
    }
    let withheld = || {
        Abstain::shape(
            aspect,
            "no serving grounding: the slot is withheld as contested",
        )
    };
    let surface = Surface::load(shared).await?.ok_or_else(withheld)?;
    // A metric's slot or a plan's: a plan's grounding write is judged
    // as a metric's is (fixture 27).
    let slot = surface
        .slots
        .iter()
        .chain(&surface.plans)
        .find(|s| s.subject == subject && s.aspect == aspect)
        .ok_or_else(withheld)?;
    let planned = Box::pin(plan(shared, &surface, slot, None)).await?;
    let mut fact = planned.fact(aspect);
    // Beside the serving frame, the newest other writing on the slot:
    // the one this write superseded, or the standing human grounding
    // the agent's writing does not displace. Both frames build here,
    // uncached, and the row says what the write changed as the totals
    // over their shared months — serving an axis must keep the total,
    // and a join that drops rows shows at the write, not at the
    // read-back. A first grounding has no other writing and no row.
    // The slot's history, newest first — the raw read serves one row
    // per actor kind and never the one a write superseded.
    let rctx = shared.read_context().await?;
    let mut rows: Vec<_> = rctx
        .glossary
        .iter()
        .filter(|g| g.dataset == dataset && g.subject == subject && g.aspect == aspect)
        .collect();
    rows.sort_by_key(|g| std::cmp::Reverse(g.seq));
    let serving = rows.iter().position(|r| r.body == slot.body);
    let other = rows
        .iter()
        .enumerate()
        .find(|(i, _)| Some(*i) != serving)
        .map(|(_, r)| r);
    if let Some(other) = other
        && fact.applicable
    {
        let before = QuerySlot {
            subject: subject.to_string(),
            aspect: aspect.to_string(),
            body: other.body.clone(),
            rank: if other.actor_kind == "human" { 0 } else { 1 },
            plan_of: slot.plan_of.clone(),
            covers: slot.covers,
        };
        let now = Box::pin(build_metric(shared, &surface, slot, None)).await;
        let before = Box::pin(build_metric(shared, &surface, &before, None)).await;
        // Like against like: a ratio's monthly value and a flow's
        // total are different numbers, and the row says so instead.
        fact.superseded_divergence = Some(if !before.fact.applicable {
            format!(
                "the writing it supersedes served nothing: {}",
                before.fact.reason.as_deref().unwrap_or("no reason given")
            )
        } else if now.fact.behavior != before.fact.behavior {
            format!(
                "the verb changed against the writing it supersedes, {} against {}: totals not compared",
                now.fact.behavior.as_deref().unwrap_or("none"),
                before.fact.behavior.as_deref().unwrap_or("none")
            )
        } else {
            drift(shared, &now.cells, &before.cells).await?
        });
    }
    Ok(fact)
}

/// The gap between two frames' totals over their shared periods, in
/// the rival divergence's words, and the periods one serves and the
/// other does not. A full join of the two over the period: the counts
/// an aggregate, the widest gap the first row of a sort. Agreement is
/// a zero gap, never silence.
async fn drift(
    shared: &Arc<Shared>,
    now: &RecordBatch,
    before: &RecordBatch,
) -> Result<String, DataFusionError> {
    let before = scan_cells("b", before)?
        .filter(col("dimension").eq(lit("")))?
        .build()?;
    let joined = scan_cells("n", now)?
        .filter(col("dimension").eq(lit("")))?
        .join(
            before,
            JoinType::Full,
            (vec!["period"], vec!["period"]),
            None,
        )?
        .project(vec![
            col("n.period").alias("now"),
            col("b.period").alias("before"),
            gap(col("n.value"), col("b.value")).alias("gap"),
        ])?
        .build()?;
    let both = col("now").is_not_null().and(col("before").is_not_null());
    let stats = LogicalPlanBuilder::from(joined.clone())
        .aggregate(
            Vec::<Expr>::new(),
            vec![
                count(lit(1)).filter(both.clone()).build()?.alias("shared"),
                count(lit(1))
                    .filter(col("now").is_null())
                    .build()?
                    .alias("only_before"),
                count(lit(1))
                    .filter(col("before").is_null())
                    .build()?
                    .alias("only_now"),
            ],
        )?
        .build()?;
    let widest = LogicalPlanBuilder::from(joined)
        .filter(both)?
        .project(vec![col("now").alias("period"), col("gap")])?
        .sort(widest_first())?
        .limit(0, Some(1))?
        .build()?;
    let stats = run_cells(shared, stats).await?;
    let n = |name: &str| int_column(&stats, name).map(|v| v.first().copied().unwrap_or(0));
    let (shared_n, only_before, only_now) = (n("shared")?, n("only_before")?, n("only_now")?);
    let mut out = match peak(&run_cells(shared, widest).await?) {
        None => "no shared periods with the writing it supersedes".to_string(),
        Some((0.0, _)) => {
            format!("no gap against the writing it supersedes over {shared_n} shared periods")
        }
        Some((gap, at)) => format!(
            "the total moved {:.1} % at {} against the writing it supersedes, the widest gap \
             over {shared_n} shared periods",
            gap * 100.0,
            period::date(at)
        ),
    };
    if only_before > 0 {
        out.push_str(&format!(
            "; {only_before} periods only in the writing it supersedes"
        ));
    }
    if only_now > 0 {
        out.push_str(&format!("; {only_now} periods only now"));
    }
    Ok(out)
}

/// The relative gap between two values: the difference over the
/// larger magnitude, so it reads as a share of the number itself;
/// zero where both are zero.
fn gap(a: Expr, b: Expr) -> Expr {
    coalesce(vec![
        abs(a.clone() - b.clone()) / nullif(greatest(vec![abs(a), abs(b)]), lit(0.0)),
        lit(0.0),
    ])
}

/// The order that puts the widest gap first, the earliest period
/// among equals.
fn widest_first() -> Vec<SortExpr> {
    vec![
        SortExpr::new(col("gap"), false, false),
        SortExpr::new(col("period"), true, false),
    ]
}

/// The first row of a gap plan sorted [`widest_first`]: `(gap,
/// period)`; none where no period is shared.
fn peak(batches: &[RecordBatch]) -> Option<(f64, i64)> {
    let b = batches.iter().find(|b| b.num_rows() > 0)?;
    let gap = b
        .column_by_name("gap")?
        .as_any()
        .downcast_ref::<Float64Array>()?;
    let period = period::column(b.column_by_name("period")?)?;
    (!gap.is_null(0) && !period.is_null(0)).then(|| (gap.value(0), period.value(0)))
}

/// One metric's cube at this pin. A grounding that cannot serve — no
/// JSON, no `sql`, no value column, no judged time axis, a plan or run
/// the engine refuses — abstains with the reason, and the abstention
/// is the entry: the same pin gives the same answer. A grounding the
/// author stopped abstains with the author's own reason.
/// The frame a grounding serves, where the body is JSON carrying
/// `sql` and no author's stop — what the key plans; anything else
/// abstains at the plan stage with its own reason.
fn frame_sql(body: &str) -> Option<String> {
    let body: Value = serde_json::from_str(body).ok()?;
    if body.get("stopped").is_some() {
        return None;
    }
    body.get("sql").and_then(Value::as_str).map(str::to_string)
}

async fn build_metric(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    probe: Option<datafusion::logical_expr::LogicalPlan>,
) -> Cube {
    let mut cube = match build(shared, surface, slot, probe).await {
        Ok(cube) => cube,
        // An abstention binds to no version: its reasons derive from
        // the plan over the digest-covered surface, so no write flips
        // one without missing the key. The grain check is the one
        // data-derived abstention, and `build` binds it itself.
        Err(abstained) => Cube {
            fact: Fact::abstain(&slot.aspect, abstained.text()),
            cells: RecordBatch::new_empty(series_schema()),
            version_bound: None,
        },
    };
    // A plan's row names the metric it plans (fixture 27): the build
    // is a metric's build, and the tie rides the slot.
    cube.fact.plan_of = slot.plan_of.clone();
    cube
}

/// Why a metric's cube is not built — the text the fact row carries.
struct Abstain(SessionError);

impl Abstain {
    /// A grounding whose shape the cube cannot build on: `subject` the
    /// metric or the part of its frame the reason is about.
    fn shape(subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Abstain(SessionError::shape(subject, detail))
    }

    /// The reason as the fact row carries it: the refusal's own text
    /// where it is about one subject, the whole refusal otherwise.
    fn text(&self) -> String {
        self.0.abstention().unwrap_or_else(|| self.0.to_string())
    }
}

impl From<SessionError> for Abstain {
    fn from(e: SessionError) -> Self {
        Abstain(e)
    }
}

impl From<DataFusionError> for Abstain {
    fn from(e: DataFusionError) -> Self {
        Abstain(SessionError::DataFusion(e))
    }
}

/// One row of a series query: `(period, member, value, num, den)`.
pub(crate) type SeriesRow = (i64, Option<String>, f64, Option<f64>, Option<f64>);

/// A column left out of the axes: its name, the reason, and the act
/// the row names it by.
type Unadmitted = (String, String, &'static str);

/// The plan stage — see [`Planned`]. Everything here is decided by
/// the plan's schema, its provenance and the judged surface; nothing
/// scans. `probe` is the frame's plan where the caller built it for
/// the key.
async fn plan(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    probe: Option<datafusion::logical_expr::LogicalPlan>,
) -> Result<Planned, Abstain> {
    let Surface {
        ctx,
        dataset,
        judged,
        settings,
        shapes,
        ..
    } = surface;
    let dataset = dataset.as_str();
    let (body, sql, probe) = frame(shared, ctx, slot, probe).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain::shape(&slot.aspect, "no value column"));
    }
    let grain = declared_grain(&body, fields)?;
    // Which table columns each served field descends from — the judged
    // verdicts key by subject, the frame names aliases. The time axis
    // reads every source; admission reads the fields with one.
    let sources = crate::provenance::served_sources(&probe, dataset);
    let subjects = crate::provenance::single(&sources);
    let mut wanted = wanted_temporal(fields, &sources, judged);
    // No judged time axis is no series: the plan stage still names
    // what it wants, and the fact row abstains carrying it.
    let (time_column, cadence, time_current) =
        match judged_time_column(fields, &sources, &judged.temporal) {
            Some((column, cadence, current)) => (Some(column), cadence, current),
            None => (None, None, true),
        };
    // Whether every verdict admitted on stands at this pin — the time
    // axis now, each admitted dimension at the scan.
    let mut judged_current = time_current;
    // The metric's own resolution is the coarser of the judged cadence
    // and the declared floor — the floor alone where the verdict names
    // no cadence — and its window the ladder's rung for it. The head
    // itself stands at the floor, over the longest rung any grain from
    // this resolution up is served over ([`build`]).
    let resolution = cadence.map_or(settings.floor, |c| c.max(settings.floor));
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
    wanted_behavior(
        &probe,
        dataset,
        judged,
        is_ratio,
        behavior_basis,
        &mut wanted,
    );

    let scanned = crate::provenance::scanned_tables(&probe, dataset);
    let (authored, axes_basis) = authored_axes(&body, is_ratio, &probe);
    let admission = Admission {
        judged,
        subjects: &subjects,
        scanned: &scanned,
        author: if slot.rank == 0 { "human" } else { "agent" },
    };
    let (candidates, unadmitted) = admit_axes(
        &admission,
        fields,
        is_ratio,
        authored.as_deref(),
        &mut wanted,
    );
    let unserved = if candidates.is_empty() && authored.is_none() {
        unserved_axes(&scanned, shapes, &sources, judged)
    } else {
        Vec::new()
    };
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
        candidates,
        unadmitted,
        axes_basis,
        wanted,
        unserved,
        foreign: workspace_reads(&probe),
    })
}

/// The frame a grounding serves: its body, its `sql` and the plan.
/// The author's stop (SPEC.md §5.2) abstains first — no number is
/// served, and the reason is theirs, carried as written. Planned
/// through to the physical plan as well: the engine admits at the
/// logical stage what it refuses at the physical one — a scalar
/// subquery inside an aggregate's argument arrives there as
/// `ScalarSubquery` and is refused ("Physical plan does not support
/// …") — and the row answers whether the SQL plans, so it answers for
/// both stages. The scans are the pinned providers' and plan without
/// I/O; nothing runs.
async fn frame(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    slot: &QuerySlot,
    probe: Option<datafusion::logical_expr::LogicalPlan>,
) -> Result<(Value, String, datafusion::logical_expr::LogicalPlan), Abstain> {
    let body: Value = serde_json::from_str(&slot.body)
        .map_err(|e| Abstain::shape(&slot.aspect, format!("the grounding is not JSON: {e}")))?;
    if let Some(why) = body.get("stopped").and_then(Value::as_str) {
        return Err(Abstain::shape(&slot.aspect, format!("stopped: {why}")));
    }
    let sql = body
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| Abstain::shape(&slot.aspect, "the grounding carries no `sql`"))?
        .to_string();
    let probe = match probe {
        Some(p) => p,
        None => crate::plan::build_plan(shared, ctx, &sql).await?,
    };
    ctx.state()
        .create_physical_plan(&probe)
        .await
        .map_err(SessionError::not_served)?;
    Ok((body, sql, probe))
}

/// The declared row identity: every grain column must be served — a
/// declaration over a column the frame does not carry judges nothing.
fn declared_grain(
    body: &Value,
    fields: &datafusion::common::DFSchema,
) -> Result<Vec<String>, Abstain> {
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
    if let Some(missing) = grain
        .iter()
        .find(|c| !fields.fields().iter().any(|f| f.name() == *c))
    {
        return Err(Abstain::shape(
            missing,
            format!(
                "the declared grain names `{missing}`, which the frame does not serve — \
                 serve the column, or fix the declaration"
            ),
        ));
    }
    Ok(grain)
}

/// What the row reads and nobody measured: the source columns of
/// every served date of one table without a temporal verdict; the
/// candidate columns add theirs at admission.
fn wanted_temporal(
    fields: &datafusion::common::DFSchema,
    sources: &HashMap<String, Vec<String>>,
    judged: &Judged,
) -> Vec<(String, String)> {
    let mut wanted: Vec<(String, String)> = Vec::new();
    let Some(function) = &judged.temporal_fn else {
        return wanted;
    };
    for f in fields.fields() {
        if !crate::plan::is_temporal(f.data_type()) {
            continue;
        }
        let Some(columns) = sources.get(f.name()) else {
            continue;
        };
        if !crate::provenance::one_table(columns) {
            continue;
        }
        for column in columns {
            if !judged.temporal.contains_key(column) && !wanted.iter().any(|(_, s)| s == column) {
                wanted.push((function.clone(), column.clone()));
            }
        }
    }
    wanted
}

/// The verb's own measurement, where the value is or sums a column
/// nobody measured: the grounding write runs it, and re-measure runs
/// it. A shape or a ratio needs none.
fn wanted_behavior(
    probe: &datafusion::logical_expr::LogicalPlan,
    dataset: &str,
    judged: &Judged,
    is_ratio: bool,
    behavior_basis: &str,
    wanted: &mut Vec<(String, String)>,
) {
    if !is_ratio
        && behavior_basis != "shape"
        && let Some(function) = &judged.behavior_fn
        && let Some(column) = crate::provenance::summed_source(probe, "value", dataset)
        && !judged.behavior.contains_key(&column)
        && !wanted.iter().any(|(f, s)| f == function && *s == column)
    {
        wanted.push((function.clone(), column));
    }
}

/// The grounding's own word on its axes: `axes` lists the served
/// columns the metric is sliced by, in order, and closes every other
/// served column — the empty list closes them all, where it holds.
/// Its author's word admits a listed column whatever was measured, as
/// a `dimension` gloss would; a listed name the frame does not serve
/// as a sliceable column is named back. Absent, the verdicts and the
/// column glosses decide. The empty list holds for the two shapes no
/// column slices whole: a distinct count, whose members double-count
/// across any column, and a ratio, whose members do not add up to it.
/// On any other shape every member adds up to the total, so the
/// verdicts keep deciding, and the row says the word was measured
/// over.
fn authored_axes(
    body: &Value,
    is_ratio: bool,
    probe: &datafusion::logical_expr::LogicalPlan,
) -> (Option<Vec<String>>, &'static str) {
    let authored: Option<Vec<String>> = body.get("axes").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    });
    match authored {
        Some(list)
            if list.is_empty()
                && !is_ratio
                && !crate::provenance::distinct_count(probe, "value") =>
        {
            (None, "measured over authored")
        }
        Some(list) => (Some(list), "authored"),
        None => (None, "measured"),
    }
}

/// What admission reads of the surface: the verdicts and the glosses,
/// each served field's one source column, the tables the frame scans,
/// and who authored the grounding.
struct Admission<'a> {
    judged: &'a Judged,
    subjects: &'a HashMap<String, String>,
    scanned: &'a HashSet<String>,
    author: &'static str,
}

impl Admission<'_> {
    /// What the record says of a column without the author's word: a
    /// `dimension` gloss that admits it, a verdict that does — its own,
    /// or one reached through a declared edge — or nothing. A closed
    /// column's act carries it, so the line can name what the word
    /// closed over.
    fn admits(&self, subject: &str) -> Option<&'static str> {
        let judged = self.judged;
        match judged
            .dimension
            .get(subject)
            .and_then(|(v, _)| v["value"].as_str())
        {
            Some("none") => return None,
            Some("primary" | "supporting") => return Some("gloss"),
            _ => {}
        }
        let measured = judged
            .relevance
            .get(subject)
            .filter(|v| v.body["applicable"].as_bool() == Some(true))
            .is_some()
            || through_edge(subject, self.scanned, &judged.pointers, &judged.relevance).is_some();
        measured.then_some("verdict")
    }
}

/// Judged dimensions: a served column (neither the value nor
/// time-typed nor a ratio's own halves) enters when a verdict admits
/// it — its own dimension_relevance, or, for a label whose own
/// verdict is a near-key in its table, the verdict on the key column
/// that reaches it through a declared edge. The collapsed `dimension`
/// gloss on the column is the read policy over that: `none` closes
/// the axis whatever was measured, `primary` admits it and ranks it
/// first, `supporting` admits it. Relevance orders the admitted,
/// fewest members break a tie, the cap keeps the top four. Counting
/// admits nothing; its two jobs are the served-frame floor and the
/// bucketing split, one aggregate pass ([`admitted_axes`]). Under the
/// author's word ([`authored_axes`]) the list decides instead, and a
/// listed name no served column answers to is named back.
fn admit_axes(
    admission: &Admission,
    fields: &datafusion::common::DFSchema,
    is_ratio: bool,
    authored: Option<&[String]>,
    wanted: &mut Vec<(String, String)>,
) -> (Vec<Candidate>, Vec<Unadmitted>) {
    let mut cand: Vec<Candidate> = Vec::new();
    let mut unadmitted: Vec<Unadmitted> = Vec::new();
    let mut sliceable: Vec<&str> = Vec::new();
    for f in fields.fields() {
        let n = f.name().as_str();
        if n == "value"
            || (is_ratio && (n == "num" || n == "den"))
            || crate::plan::is_temporal(f.data_type())
        {
            continue;
        }
        sliceable.push(n);
        let axis = match authored {
            Some(listed) => authored_axis(admission, n, listed),
            None => measured_axis(admission, n, wanted),
        };
        match axis {
            Ok(candidate) => cand.push(candidate),
            Err(left_out) => unadmitted.push(left_out),
        }
    }
    if let Some(listed) = authored {
        for a in listed.iter().filter(|a| !sliceable.contains(&a.as_str())) {
            unadmitted.push((
                a.clone(),
                "listed in the grounding's axes and not a served column the cube can slice \
                 on — serve it, or drop it from `axes`"
                    .into(),
                "unserved",
            ));
        }
    }
    (cand, unadmitted)
}

/// One served column under the author's word: listed, it is admitted
/// in the list's order whatever was measured; unlisted, it is closed,
/// and the act names what the word closed over.
fn authored_axis(
    admission: &Admission,
    n: &str,
    listed: &[String],
) -> Result<Candidate, Unadmitted> {
    let author = admission.author;
    match listed.iter().position(|a| a == n) {
        Some(i) => Ok(Candidate {
            column: n.to_string(),
            relevance: (listed.len() - i) as f64,
            current: true,
            basis: admission
                .subjects
                .get(n)
                .cloned()
                .unwrap_or_else(|| n.to_string()),
            admitted_by: author,
            primary: false,
        }),
        None => {
            let (why, act) = match admission.subjects.get(n).and_then(|s| admission.admits(s)) {
                Some("gloss") => ("a dimension gloss admits it", "closed over gloss"),
                Some("verdict") => ("a verdict admits it", "closed over verdict"),
                _ => ("nothing measured admits it", "closed"),
            };
            Err((
                n.to_string(),
                format!("closed by the grounding's axes ({author}); {why}"),
                act,
            ))
        }
    }
}

/// One served column under the record alone: an expression no verdict
/// can reach is named back; a `dimension` gloss of `none` closes it;
/// a verdict — its own, or one reached through a declared edge —
/// admits it with its relevance, a `primary` gloss ranking it first;
/// a gloss alone admits it, `primary` leading and `supporting`
/// trailing; and a column nothing admits is named back with the road,
/// the verdict it wants added to the row's wants.
fn measured_axis(
    admission: &Admission,
    n: &str,
    wanted: &mut Vec<(String, String)>,
) -> Result<Candidate, Unadmitted> {
    let judged = admission.judged;
    let Some(subject) = admission.subjects.get(n) else {
        return Err((
            n.to_string(),
            "an expression, not a table column: no verdict can reach it — serve the \
             column it derives from, or land it as a recipe column"
                .into(),
            "expression",
        ));
    };
    let gloss = judged.dimension.get(subject);
    let stance = gloss.and_then(|(v, _)| v["value"].as_str()).unwrap_or("");
    let speaker: &'static str = match gloss {
        Some((_, 0)) => "human",
        Some(_) => "agent",
        None => "measurement",
    };
    if stance == "none" {
        return Err((
            n.to_string(),
            format!("closed by a dimension gloss on {subject} ({speaker}: none)"),
            "none",
        ));
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
        None => through_edge(
            subject,
            admission.scanned,
            &judged.pointers,
            &judged.relevance,
        ),
    };
    match (measured, stance) {
        (Some((relevance, current, basis)), "primary") => Ok(Candidate {
            column: n.to_string(),
            relevance,
            current,
            basis,
            admitted_by: speaker,
            primary: true,
        }),
        (Some((relevance, current, basis)), _) => Ok(Candidate {
            column: n.to_string(),
            relevance,
            current,
            basis,
            admitted_by: "measurement",
            primary: false,
        }),
        // Admitted on the gloss alone: a gloss always stands, and
        // without a verdict `primary` leads, `supporting` trails.
        (None, "primary" | "supporting") => Ok(Candidate {
            column: n.to_string(),
            relevance: if stance == "primary" { 1.0 } else { 0.0 },
            current: true,
            basis: subject.clone(),
            admitted_by: speaker,
            primary: stance == "primary",
        }),
        (None, _) => {
            let (why, act) = match judged.relevance.get(subject) {
                Some(v) => (
                    format!(
                        "dimension_relevance abstained on {subject} ({}), and no declared \
                         relationship reaches it from a judged key the grounding scans — \
                         declare the edge, or gloss dimension on it",
                        v.body["reason"].as_str().unwrap_or("no reason given")
                    ),
                    "abstained",
                ),
                None => {
                    if let Some(function) = &judged.relevance_fn {
                        wanted.push((function.clone(), subject.clone()));
                    }
                    (
                        format!(
                            "no verdict on {subject} — run dimension_relevance() over it, \
                             or gloss dimension on it"
                        ),
                        "verdict",
                    )
                }
            };
            Err((n.to_string(), why, act))
        }
    }
}

/// Where nothing served can be an axis and the author listed none,
/// the frame is a date and a value: the row names the columns of the
/// tables it scans that a gloss or a verdict already admits and it
/// does not serve — the axis is judged, the road is to serve it. The
/// gloss is the read policy over the verdict, here as in admission:
/// `none` closes the column, a word admits it, and without one the
/// verdict decides.
fn unserved_axes(
    scanned: &HashSet<String>,
    shapes: &HashMap<String, glossql_catalog::Shape>,
    sources: &HashMap<String, Vec<String>>,
    judged: &Judged,
) -> Vec<String> {
    let served: HashSet<&String> = sources.values().flatten().collect();
    let mut unserved: Vec<String> = Vec::new();
    for table in scanned {
        let Some(shape) = shapes.get(table) else {
            continue;
        };
        for column in shape.columns.keys() {
            let subject = format!("{table}.{column}");
            if served.contains(&subject) {
                continue;
            }
            let admitted = match judged
                .dimension
                .get(&subject)
                .and_then(|(v, _)| v["value"].as_str())
            {
                Some("none") => false,
                Some("primary" | "supporting") => true,
                _ => judged
                    .relevance
                    .get(&subject)
                    .is_some_and(|v| v.body["applicable"].as_bool() == Some(true)),
            };
            if admitted {
                unserved.push(subject);
            }
        }
    }
    unserved.sort();
    unserved
}

/// The build stage: the plan stage's answer carried into the scans —
/// the grain checked, the axes counted and admitted, the windows and
/// the reach read off the frame, every series as cells, the zeros, the
/// rival, and the head landed in one order under its fact row.
async fn build(
    shared: &Arc<Shared>,
    surface: &Surface,
    slot: &QuerySlot,
    probe: Option<datafusion::logical_expr::LogicalPlan>,
) -> Result<Cube, Abstain> {
    let Surface {
        ctx,
        version,
        settings,
        ..
    } = surface;
    let metric = slot.aspect.as_str();
    // The head's own grain: the floor, whatever the metric's cadence,
    // so every grain from the resolution up derives exactly — a weekly
    // series' months come from its days, never from its weeks.
    let floor = settings.floor;
    // Boxed: the plan stage's future is most of the build's, and a
    // build constructed on the stack under a write's depth must fit.
    let planned = Box::pin(plan(shared, surface, slot, probe)).await?;
    // What binds the entry, where the frame scans workspace relations.
    let bound = |foreign: &[String]| {
        (!foreign.is_empty()).then(|| {
            (
                foreign.to_vec(),
                glossql_glossary::version_view(version, foreign),
            )
        })
    };
    // No judged time axis: the entry is the plan stage's abstention,
    // carrying what the row wants — an abstention binds to no version.
    let Some(tcol) = planned.tcol.clone() else {
        return Ok(Cube {
            fact: planned.fact(metric),
            cells: RecordBatch::new_empty(series_schema()),
            version_bound: None,
        });
    };
    let Planned {
        body,
        sql,
        grain,
        resolution,
        window,
        verb,
        behavior_basis,
        mut judged_current,
        candidates,
        mut unadmitted,
        axes_basis,
        wanted,
        unserved,
        mut foreign,
        ..
    } = planned;
    let sql = sql.as_str();
    let tcol = tcol.as_str();

    if let Some(reason) = broken_grain(shared, ctx, sql, &grain).await? {
        // The one data-derived abstention: over a frame that also
        // scans a workspace relation, a write can flip it, so it
        // carries the binding a built cube would.
        return Ok(Cube {
            fact: Fact::abstain(metric, reason),
            cells: RecordBatch::new_empty(series_schema()),
            version_bound: bound(&foreign),
        });
    }
    let Admitted {
        dims,
        basis,
        admitted_by,
        bucketed,
    } = admitted_axes(
        shared,
        ctx,
        sql,
        candidates,
        &mut unadmitted,
        &mut judged_current,
    )
    .await?;
    let Reach {
        spanned,
        since_own,
        since,
    } = reach(shared, ctx, sql, tcol, resolution, settings).await?;
    let series = Series {
        metric,
        sql,
        tcol,
        verb,
        floor,
        resolution,
        since: since.map(period::literal),
        since_own: since_own.map(period::literal),
    };

    // Every series' batches, in the series schema: the total, the
    // members, the zeros, the rival — one head once sorted.
    let mut batches = total_series(shared, ctx, &series).await?;
    batches.extend(member_series(shared, ctx, &series, &dims, &bucketed).await?);

    // What the data holds inside the metric's own window, counted
    // before the zeros below: a zero cell is no period of data.
    let head = one_batch(&batches)?;
    let inside = periods_inside(shared, &head, resolution, since_own).await?;
    // A flow's empty periods are zero cells, from the total's first
    // period at the metric's resolution to the data's edge: a sum over
    // no rows is zero, and a forecast for that period is checked
    // against it. A stock's or a ratio's stay gaps — nothing stands,
    // nothing divides.
    let edge = surface.data_edge(shared, slot).await;
    if verb == "flow"
        && let Some(edge) = edge
    {
        batches.extend(zero_fill(shared, &head, resolution, edge).await?);
    }
    let head = one_batch(&batches)?;

    let Rival {
        alternative,
        alternative_divergence,
        alternative_error,
        cells: rival_cells,
        foreign: rival_foreign,
    } = rival(shared, surface, &series, &body, &head, edge).await?;
    batches.extend(rival_cells);
    if let Some(rival_foreign) = rival_foreign {
        foreign.extend(rival_foreign);
        foreign.sort();
        foreign.dedup();
    }

    let cells = landed(shared, &batches).await?;
    // What the data holds at the resolution and the window leaves out:
    // the periods the frame spans less the periods of data the total
    // series serves inside the metric's own window.
    let outside = (spanned - inside).max(0);

    Ok(Cube {
        fact: Fact {
            metric: metric.to_string(),
            plan_of: None,
            applicable: true,
            judged_current,
            reason: None,
            behavior: Some(verb.to_string()),
            behavior_basis: Some(behavior_basis.to_string()),
            grain,
            resolution: Some(resolution),
            window,
            outside,
            dims,
            basis,
            admitted_by,
            axes_basis: axes_basis.to_string(),
            bucketed,
            unadmitted: unadmitted.iter().map(|(c, _, _)| c.clone()).collect(),
            unadmitted_why: unadmitted.iter().map(|(_, w, _)| w.clone()).collect(),
            unadmitted_act: unadmitted
                .into_iter()
                .map(|(_, _, a)| a.to_string())
                .collect(),
            wanted: wanted.iter().map(|(f, _)| f.clone()).collect(),
            wanted_over: wanted.into_iter().map(|(_, s)| s).collect(),
            unserved,
            alternative,
            alternative_divergence,
            alternative_error,
            superseded_divergence: None,
        },
        cells,
        version_bound: bound(&foreign),
    })
}

/// The declared grain, validated where the frame is built: one row
/// per key, or the metric abstains — a frame that breaks its declared
/// identity multiplies every aggregating reader, and nothing
/// downstream can tell duplication from multi-entity. The abstention's
/// text where it breaks; none where it holds or nothing is declared.
/// One aggregate over the frame, never a count over the frame grouped
/// again: the engine's projection pruner keeps only the group keys the
/// input's functional dependencies call sufficient when the parent
/// reads no key (`optimize_projections`), and an aggregate over a join
/// whose one side is DISTINCT mints a dependency from that side's key
/// to the whole row, which is false — the grouped shape then counts
/// one side's keys. At this pin and on upstream main.
async fn broken_grain(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    grain: &[String],
) -> Result<Option<String>, Abstain> {
    if grain.is_empty() {
        return Ok(None);
    }
    let keys = grain.iter().map(|c| qi(c)).collect::<Vec<_>>().join(", ");
    let q =
        format!("SELECT count(*) AS total, count(DISTINCT struct({keys})) AS keys FROM ({sql})");
    let batches = run(shared, ctx, &q).await?;
    let key_count = int_value(&batches, "keys")?;
    let total = int_value(&batches, "total")?;
    if total <= key_count {
        return Ok(None);
    }
    let cols = grain.join(", ");
    Ok(Some(format!(
        "the frame breaks its declared grain ({cols}): {total} rows over {key_count} distinct \
         keys — serve one row per ({cols}), or fix the declaration"
    )))
}

/// The axes the scan admits: the dimension columns in rank order, each
/// with its basis and who admitted it, and among them the ones past
/// the member cap, which bucket.
struct Admitted {
    dims: Vec<String>,
    basis: Vec<String>,
    admitted_by: Vec<String>,
    bucketed: Vec<String>,
}

/// The candidates counted and admitted: each candidate's members in
/// one aggregate pass over the frame, the candidates ranked — primary
/// first, then relevance, then fewest members, then name — and the
/// top four kept; a column with one member slices nothing, and one
/// past the member cap is bucketed. A candidate left out joins the
/// unadmitted with its act, and an admitted one judged at an earlier
/// pin marks the row not current.
async fn admitted_axes(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    cand: Vec<Candidate>,
    unadmitted: &mut Vec<Unadmitted>,
    judged_current: &mut bool,
) -> Result<Admitted, Abstain> {
    let mut counts: Vec<(Candidate, i64)> = Vec::new();
    if !cand.is_empty() {
        let parts: Vec<String> = cand
            .iter()
            .enumerate()
            .map(|(i, c)| format!("count(DISTINCT {}) AS \"n_{i}\"", qi(&c.column)))
            .collect();
        let batches = run(
            shared,
            ctx,
            &format!("SELECT {} FROM ({sql})", parts.join(", ")),
        )
        .await?;
        for (i, c) in cand.into_iter().enumerate() {
            let n = int_value(&batches, &format!("n_{i}"))?;
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
    let mut admitted = Admitted {
        dims: Vec::new(),
        basis: Vec::new(),
        admitted_by: Vec::new(),
        bucketed: Vec::new(),
    };
    for (c, n) in &counts {
        if admitted.dims.len() >= DIMS_CAP {
            unadmitted.push((
                c.column.clone(),
                format!("ranked below the {DIMS_CAP} admitted axes"),
                "cap",
            ));
            continue;
        }
        if *n < 2 {
            unadmitted.push((
                c.column.clone(),
                "one member across the frame: nothing to slice".into(),
                "single",
            ));
            continue;
        }
        admitted.dims.push(c.column.clone());
        admitted.basis.push(c.basis.clone());
        admitted.admitted_by.push(c.admitted_by.to_string());
        *judged_current &= c.current;
        if *n > MEMBERS_CAP {
            admitted.bucketed.push(c.column.clone());
        }
    }
    Ok(admitted)
}

/// What one pass over the frame says of its reach: the periods the
/// data holds at the metric's resolution, the metric's own window —
/// what its series is served over, what the members rank over, and
/// what `outside` counts against — and the head's, the earliest
/// window of the served grains.
struct Reach {
    spanned: i64,
    since_own: Option<i64>,
    since: Option<i64>,
}

/// The windows and the data's reach, one pass over the frame: the
/// edge (the latest observation) and the periods the data holds at
/// the metric's resolution. A grain's window is the edge's bucket at
/// that grain less the grain's rung; the head keeps every floor
/// bucket after the earliest window of the grains from the resolution
/// up, so each of them derives from it whole. A grain without a rung
/// is unbounded, and so is the head.
async fn reach(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    tcol: &str,
    resolution: Resolution,
    settings: &Settings,
) -> Result<Reach, Abstain> {
    let p_own = period::bucket(&qi(tcol), resolution);
    let served: Vec<Resolution> = Resolution::ALL
        .into_iter()
        .filter(|r| *r >= resolution)
        .collect();
    let unbounded = served.iter().any(|r| !settings.windows.contains_key(r));
    let sinces: String = served
        .iter()
        .filter_map(|r| Some((r, settings.windows.get(r)?)))
        .map(|(r, w)| {
            format!(
                ", CAST({} - INTERVAL '{}' AS {}) AS \"since_{}\"",
                period::bucket("edge", *r),
                w.replace('\'', "''"),
                period::TYPE,
                r.as_str()
            )
        })
        .collect();
    let q = format!(
        "SELECT edge, spanned{sinces} FROM (\
            SELECT max({tcol_q}) AS edge, \
                   count(DISTINCT {p_own}) FILTER (WHERE value IS NOT NULL) AS spanned \
            FROM ({sql}))",
        tcol_q = qi(tcol)
    );
    let batches = run(shared, ctx, &q).await?;
    let row = batches.iter().find(|b| b.num_rows() > 0);
    let since_at = |r: Resolution| -> Option<i64> {
        let col = row?
            .column_by_name(&format!("since_{}", r.as_str()))
            .and_then(period::column)?;
        (!col.is_null(0)).then(|| col.value(0))
    };
    let spanned = int_column(&batches, "spanned")?
        .first()
        .copied()
        .unwrap_or(0);
    let since_own = since_at(resolution);
    let since = if unbounded {
        None
    } else {
        served.iter().filter_map(|r| since_at(*r)).min()
    };
    Ok(Reach {
        spanned,
        since_own,
        since,
    })
}

/// What every series plan of one build reads: the metric, its frame
/// and time column, the verb, the floor the head stands at, the
/// metric's own resolution, and the two windows as the plans spell
/// them — the head's (`since`) and the metric's own (`since_own`).
struct Series<'a> {
    metric: &'a str,
    sql: &'a str,
    tcol: &'a str,
    verb: &'static str,
    floor: Resolution,
    resolution: Resolution,
    since: Option<String>,
    since_own: Option<String>,
}

/// The total series, at the floor over the head's window.
async fn total_series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    s: &Series<'_>,
) -> Result<Vec<RecordBatch>, Abstain> {
    cells(
        shared,
        ctx,
        &cells_sql(
            s.metric,
            "",
            "''",
            s.verb,
            &total_sql(s.sql, s.tcol, s.verb, s.floor, s.since.as_deref()),
        ),
    )
    .await
}

/// Member series per admitted dimension, same verb, same window —
/// independent plans, driven concurrently.
async fn member_series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    s: &Series<'_>,
    dims: &[String],
    bucketed: &[String],
) -> Result<Vec<RecordBatch>, Abstain> {
    let members = dims
        .iter()
        .map(|dcol| member_cells(shared, ctx, s, dcol, bucketed.iter().any(|b| b == dcol)));
    Ok(futures::future::try_join_all(members)
        .await?
        .into_iter()
        .flatten()
        .collect())
}

/// One dimension's series: the member expression plain, or the
/// bucketing CASE for a dimension past the member cap.
async fn member_cells(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    s: &Series<'_>,
    dcol: &str,
    bucketed: bool,
) -> Result<Vec<RecordBatch>, Abstain> {
    let member = if bucketed {
        bucketed_member(shared, ctx, s, dcol).await?
    } else {
        format!("CAST({dcol_q} AS VARCHAR)", dcol_q = qi(dcol))
    };
    cells(
        shared,
        ctx,
        &cells_sql(
            s.metric,
            dcol,
            "member",
            s.verb,
            &member_sql(
                s.sql,
                s.tcol,
                dcol,
                &member,
                s.verb,
                s.floor,
                s.since.as_deref(),
            ),
        ),
    )
    .await
}

/// A bucketed dimension's member expression: its top members by weight
/// over the metric's own window, and the rest folded into 'other'. The
/// set is resolved first and spliced as literals, deterministic
/// (weight, then name) so two builds at one pin agree. Where no member
/// stands inside the window — every row of the column there is NULL —
/// there is nothing to name and no cell to serve: the plain cast,
/// never an empty IN list, which is not a query.
async fn bucketed_member(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    s: &Series<'_>,
    dcol: &str,
) -> Result<String, Abstain> {
    let weight = if s.verb == "ratio" {
        "sum(den)"
    } else {
        "sum(value)"
    };
    let p_own = period::bucket(&qi(s.tcol), s.resolution);
    let clause = s
        .since_own
        .as_deref()
        .map_or(String::new(), |since| format!(" AND {p_own} > {since}"));
    let q = format!(
        "SELECT CAST({dcol_q} AS VARCHAR) AS mc_member FROM ({sql}) \
         WHERE {dcol_q} IS NOT NULL{clause} GROUP BY 1 \
         ORDER BY {weight} DESC NULLS LAST, mc_member LIMIT {}",
        MEMBERS_CAP - 1,
        sql = s.sql,
        dcol_q = qi(dcol)
    );
    let mut named = Vec::new();
    for b in run(shared, ctx, &q)
        .await?
        .iter()
        .filter(|b| b.num_rows() > 0)
    {
        let col = b
            .column_by_name("mc_member")
            .ok_or_else(|| Abstain::shape(dcol, "the member pass served no mc_member"))?;
        for i in 0..b.num_rows() {
            let m = array_value_to_string(col, i).map_err(SessionError::from)?;
            named.push(ql(&m));
        }
    }
    Ok(if named.is_empty() {
        format!("CAST({dcol_q} AS VARCHAR)", dcol_q = qi(dcol))
    } else {
        format!(
            "CASE WHEN CAST({dcol_q} AS VARCHAR) IN ({}) \
             THEN CAST({dcol_q} AS VARCHAR) ELSE 'other' END",
            named.join(", "),
            dcol_q = qi(dcol)
        )
    })
}

/// The rival as the fact row carries it: its name, its divergence from
/// the metric's total, or why it is not served; its cells beside the
/// metric's, and the workspace relations its frame scans, where it
/// served.
struct Rival {
    alternative: Option<String>,
    alternative_divergence: Option<String>,
    alternative_error: Option<String>,
    cells: Vec<RecordBatch>,
    foreign: Option<Vec<String>>,
}

/// The named rival, when a grounding assumption discloses one. The
/// rival SQL is authored but never admission-validated: it runs behind
/// a guard and a refusal is reported in the fact row, never thrown.
/// Its verb is its own — a rival serving num/den is a ratio whatever
/// the metric is — and its time axis is judged like the chosen
/// reading's wherever a verdict stands, since the rival is a
/// comparison cell. Where the rival's frame carries one date column
/// and no verdict, that column is not a choice and is taken; where it
/// carries several unjudged, the rival is not served and the fact row
/// says why. The rival's cells stand at its own verb, its empty
/// periods zero where it is a flow, so the two lines compare like
/// with like.
async fn rival(
    shared: &Arc<Shared>,
    surface: &Surface,
    s: &Series<'_>,
    body: &Value,
    head: &RecordBatch,
    edge: Option<i64>,
) -> Result<Rival, Abstain> {
    let mut out = Rival {
        alternative: None,
        alternative_divergence: None,
        alternative_error: None,
        cells: Vec::new(),
        foreign: None,
    };
    let Some(assumptions) = body.get("assumptions").and_then(Value::as_array) else {
        return Ok(out);
    };
    let Some((a, alt_sql)) = assumptions
        .iter()
        .find_map(|a| Some((a, a.get("alternative_sql").and_then(Value::as_str)?)))
    else {
        return Ok(out);
    };
    let name = a
        .get("alternative")
        .and_then(Value::as_str)
        .unwrap_or("(rival)");
    match rival_series(shared, surface, s, name, alt_sql).await {
        Ok((mut rows, rival_verb, rival_foreign)) => {
            if rival_verb == "flow"
                && let Some(edge) = edge
            {
                let zeros = zero_fill(shared, &one_batch(&rows)?, s.resolution, edge).await?;
                rows.extend(zeros);
            }
            let rival_cells = one_batch(&rows)?;
            out.alternative_divergence = Some(
                divergence(
                    shared,
                    head,
                    &rival_cells,
                    a.get("tolerance").and_then(Value::as_f64),
                )
                .await?,
            );
            out.cells.push(rival_cells);
            out.alternative = Some(name.to_string());
            out.foreign = Some(rival_foreign);
        }
        Err(why) => {
            out.alternative_error = Some(format!("the rival is not served: {}", why.text()));
        }
    }
    Ok(out)
}

/// The head in one order ([`head_order`]): a sort over every series'
/// batches, the one pass the engine makes over the head.
async fn landed(shared: &Arc<Shared>, batches: &[RecordBatch]) -> Result<RecordBatch, Abstain> {
    let sorted = scan_cells("h", &one_batch(batches)?)?
        .sort(head_order())?
        .build()?;
    Ok(one_batch(&run_cells(shared, sorted).await?)?)
}

/// A cube's cells as a plan source under `name`: the cached batch
/// mounted as a `MemTable`, copied into nothing. Every grain, every
/// count and every comparison over cells is a plan on one of these,
/// run on the plane's pool ([`run_cells`]).
fn scan_cells(name: &str, cells: &RecordBatch) -> Result<LogicalPlanBuilder, DataFusionError> {
    let table = MemTable::try_new(cells.schema(), vec![vec![cells.clone()]])?;
    LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)
}

/// A plan over cells, run on the plane's pool and spill space: its
/// batches.
async fn run_cells(
    shared: &Arc<Shared>,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    shared
        .scratch_ctx()
        .execute_logical_plan(plan)
        .await?
        .collect()
        .await
}

/// The batches a plan over cells produced, as one batch in the series
/// schema.
fn one_batch(batches: &[RecordBatch]) -> Result<RecordBatch, DataFusionError> {
    let conformed = batches.iter().map(conform).collect::<Result<Vec<_>, _>>()?;
    Ok(datafusion::arrow::compute::concat_batches(
        &series_schema(),
        &conformed,
    )?)
}

/// A batch a plan served in the series' columns, under the series
/// schema: a column the plan typed otherwise — a string a planner
/// spelled as a view — is cast to the schema's type; a column the
/// schema holds non-null the plan has already filtered.
fn conform(batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
    let schema = series_schema();
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| {
            if c.data_type() == f.data_type() {
                Ok(Arc::clone(c))
            } else {
                cast(c, f.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// The series schema's columns as a projection over cells carrying
/// the seven cell columns: the metric and the world as literals.
fn series_columns(metric: &str, world: World) -> Vec<Expr> {
    vec![
        lit(metric).alias("metric"),
        lit(world.as_str()).alias("world"),
        col("dimension"),
        col("member"),
        col("period"),
        col("value"),
        col("num"),
        col("den"),
        col("behavior"),
    ]
}

/// The order a head lands in: by period, so a reader's window prunes
/// the file's row groups, then dimension and member, so the cells
/// read in one order wherever they are served from.
fn head_order() -> Vec<SortExpr> {
    ["period", "dimension", "member"]
        .into_iter()
        .map(|c| SortExpr::new(col(c), true, false))
        .collect()
}

/// A cube's cells under another name and world: a plan aspect's cells
/// served as the plan world of the metric it names (fixture 27). A
/// projection with two literal columns; the seven cell columns pass
/// through, never copied.
async fn relabel(
    shared: &Arc<Shared>,
    cells: &RecordBatch,
    metric: &str,
    world: World,
) -> Result<RecordBatch, DataFusionError> {
    let plan = scan_cells("h", cells)?
        .project(series_columns(metric, world))?
        .build()?;
    one_batch(&run_cells(shared, plan).await?)
}

/// The last period of a cube's total cells — the data's edge for the
/// metric's own series — as an aggregate over the cells.
async fn last_period(
    shared: &Arc<Shared>,
    cells: &RecordBatch,
) -> Result<Option<i64>, DataFusionError> {
    let plan = scan_cells("h", cells)?
        .filter(col("dimension").eq(lit("")))?
        .aggregate(Vec::<Expr>::new(), vec![max(col("period")).alias("last")])?
        .build()?;
    Ok(first_period(&run_cells(shared, plan).await?, "last"))
}

/// The period in the first row of a one-row answer, by column.
fn first_period(batches: &[RecordBatch], name: &str) -> Option<i64> {
    let column = batches
        .iter()
        .find(|b| b.num_rows() > 0)?
        .column_by_name(name)
        .and_then(period::column)?;
    (!column.is_null(0)).then(|| column.value(0))
}

/// A plan's total cells beside the metric's at one grain: the plan's
/// periods left-joined to the actual's, `(period, value, actual)` in
/// period order — a join of two cubes' cells with a projection.
async fn aligned(
    shared: &Arc<Shared>,
    plan: &RecordBatch,
    actual: &RecordBatch,
) -> Result<Vec<(i64, f64, Option<f64>)>, DataFusionError> {
    let actual = scan_cells("a", actual)?
        .filter(col("dimension").eq(lit("")))?
        .build()?;
    let joined = scan_cells("p", plan)?
        .filter(col("dimension").eq(lit("")))?
        .join(
            actual,
            JoinType::Left,
            (vec!["period"], vec!["period"]),
            None,
        )?
        .project(vec![
            col("p.period").alias("period"),
            col("p.value").alias("value"),
            col("a.value").alias("actual"),
        ])?
        .sort(vec![SortExpr::new(col("period"), true, false)])?
        .build()?;
    let mut out = Vec::new();
    for b in run_cells(shared, joined)
        .await?
        .iter()
        .filter(|b| b.num_rows() > 0)
    {
        let floats = |n: &str| {
            b.column_by_name(n)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        };
        let period = b.column_by_name("period").and_then(period::column);
        let (Some(period), Some(value), Some(actual)) = (period, floats("value"), floats("actual"))
        else {
            continue;
        };
        for i in 0..b.num_rows() {
            if period.is_null(i) || value.is_null(i) {
                continue;
            }
            out.push((
                period.value(i),
                value.value(i),
                (!actual.is_null(i)).then(|| actual.value(i)),
            ));
        }
    }
    Ok(out)
}

/// The periods of a head's total series at a resolution, after
/// `since` — what the metric's own window serves; every period when
/// the window is unbounded.
async fn periods_inside(
    shared: &Arc<Shared>,
    cells: &RecordBatch,
    resolution: Resolution,
    since: Option<i64>,
) -> Result<i64, DataFusionError> {
    if cells.num_rows() == 0 {
        return Ok(0);
    }
    let bucket = period::bucket_expr(col("period"), resolution);
    let mut plan = scan_cells("h", cells)?.filter(col("dimension").eq(lit("")))?;
    if let Some(since) = since {
        plan = plan.filter(bucket.clone().gt(period::scalar(since)))?;
    }
    let plan = plan
        .aggregate(Vec::<Expr>::new(), vec![count_distinct(bucket).alias("n")])?
        .build()?;
    Ok(int_column(&run_cells(shared, plan).await?, "n")?
        .first()
        .copied()
        .unwrap_or(0))
}

/// The cells at a grain, derived from a head: each row folded into
/// its bucket at the grain by the verb it carries — a flow sums, a
/// ratio re-divides its summed halves, a stock takes the bucket's
/// latest floor cell — over the grain's window, measured back from
/// the head's own edge. The head holds every floor bucket any window
/// from the metric's resolution up can keep ([`build`]), so nothing
/// here scans. A bucketed stock member (`other`) takes the bucket's
/// latest `other` cell, where the raw query would take each of its
/// raw members' own latest; the fact row's `bucketed` names it. The
/// cells are what the plan produced ([`derive_plan`]).
async fn derive(
    shared: &Arc<Shared>,
    head: &Cube,
    grain: Resolution,
    window: Option<&str>,
) -> Result<Cube, Abstain> {
    // The window's start: the edge's bucket at the grain, less the
    // grain's rung.
    let since = match window {
        Some(w) if head.cells.num_rows() > 0 => {
            let plan = scan_cells("h", &head.cells)?
                .aggregate(Vec::<Expr>::new(), vec![max(col("period")).alias("edge")])?
                .project(vec![
                    datafusion::logical_expr::cast(
                        period::bucket_expr(col("edge"), grain) - period::rung(w)?,
                        period::data_type(),
                    )
                    .alias("since"),
                ])?
                .build()?;
            first_period(&run_cells(shared, plan).await?, "since")
        }
        _ => None,
    };
    let plan = derive_plan(&head.cells, &head.fact.metric, grain, since)?;
    let cells = one_batch(&run_cells(shared, plan).await?)?;
    let mut fact = head.fact.clone();
    fact.resolution = Some(grain);
    fact.window = window.map(str::to_string);
    Ok(Cube {
        cells,
        fact,
        version_bound: head.version_bound.clone(),
    })
}

/// The plan behind [`derive`]: the head's cells whose bucket at the
/// grain is after `since` — the cut is on the bucket, never on the
/// cell's own date, so a rung of `n` periods is `n` whole buckets
/// ending at the edge's, and the floor cells of the bucket at `since`
/// are left out whole — each verb's arm folded into its bucket at the
/// grain — a flow sums, a ratio re-divides its summed halves, a stock
/// takes the bucket's latest floor cell — the arms united, NULL values
/// and periods dropped, named for the metric and the actual world, in
/// the order a head lands.
fn derive_plan(
    cells: &RecordBatch,
    metric: &str,
    grain: Resolution,
    since: Option<i64>,
) -> Result<LogicalPlan, DataFusionError> {
    let mut base = scan_cells("h", cells)?;
    if let Some(since) = since {
        base = base.filter(period::bucket_expr(col("period"), grain).gt(period::scalar(since)))?;
    }
    let base = base.build()?;
    let arm =
        |verb: &str| LogicalPlanBuilder::from(base.clone()).filter(col("behavior").eq(lit(verb)));
    let bucket = || period::bucket_expr(col("period"), grain);
    let group = || {
        vec![
            col("dimension"),
            col("member"),
            bucket().alias("period"),
            col("behavior"),
        ]
    };
    let none = || lit(ScalarValue::Float64(None));
    let flow = arm("flow")?
        .aggregate(group(), vec![sum(col("value")).alias("value")])?
        .project(vec![
            col("dimension"),
            col("member"),
            col("period"),
            col("value"),
            none().alias("num"),
            none().alias("den"),
            col("behavior"),
        ])?
        .build()?;
    let ratio = arm("ratio")?
        .aggregate(
            group(),
            vec![sum(col("num")).alias("num"), sum(col("den")).alias("den")],
        )?
        .project(vec![
            col("dimension"),
            col("member"),
            col("period"),
            (col("num") / nullif(col("den"), lit(0.0))).alias("value"),
            col("num"),
            col("den"),
            col("behavior"),
        ])?
        .build()?;
    let latest = row_number()
        .partition_by(vec![col("dimension"), col("member"), bucket()])
        .order_by(vec![SortExpr::new(col("period"), false, false)])
        .build()?
        .alias("rn");
    let stock = arm("stock")?
        .window(vec![latest])?
        .filter(col("rn").eq(lit(1_i64)))?
        .project(vec![
            col("dimension"),
            col("member"),
            bucket().alias("period"),
            col("value"),
            col("num"),
            col("den"),
            col("behavior"),
        ])?
        .build()?;
    LogicalPlanBuilder::from(flow)
        .union(ratio)?
        .union(stock)?
        .filter(col("value").is_not_null().and(col("period").is_not_null()))?
        .project(series_columns(metric, World::Actual))?
        .sort(head_order())?
        .build()
}

/// The disagreement between the metric's total cells and the rival's,
/// over their shared periods — the coordinates the docket's question
/// needs instead of two lines to eyeball. A join of the two over the
/// period, the relative gap ([`gap`]) a projection, the counts an
/// aggregate and the widest gap the first row of a sort. Agreement is
/// a zero divergence, never silence; no shared periods is its own
/// answer.
async fn divergence(
    shared: &Arc<Shared>,
    cells: &RecordBatch,
    rival: &RecordBatch,
    tolerance: Option<f64>,
) -> Result<String, DataFusionError> {
    let total = scan_cells("t", cells)?
        .filter(col("dimension").eq(lit("")))?
        .build()?;
    let joined = scan_cells("r", rival)?
        .filter(col("dimension").eq(lit("alternative")))?
        .join(
            total,
            JoinType::Inner,
            (vec!["period"], vec!["period"]),
            None,
        )?
        .project(vec![
            col("r.period").alias("period"),
            gap(col("t.value"), col("r.value")).alias("gap"),
        ])?
        .build()?;
    let mut counts = vec![count(lit(1)).alias("shared")];
    if let Some(tol) = tolerance {
        counts.push(
            count(lit(1))
                .filter(col("gap").gt(lit(tol)))
                .build()?
                .alias("breaches"),
        );
    }
    let stats = LogicalPlanBuilder::from(joined.clone())
        .aggregate(Vec::<Expr>::new(), counts)?
        .build()?;
    let widest = LogicalPlanBuilder::from(joined)
        .sort(widest_first())?
        .limit(0, Some(1))?
        .build()?;
    let stats = run_cells(shared, stats).await?;
    let n = |name: &str| int_column(&stats, name).map(|v| v.first().copied().unwrap_or(0));
    let shared_n = n("shared")?;
    let Some((gap, at)) = peak(&run_cells(shared, widest).await?) else {
        return Ok("no shared periods".into());
    };
    Ok(match tolerance {
        Some(tol) => format!(
            "{} of {shared_n} shared periods differ beyond {tol}; max relative gap {gap:.4} at {}",
            n("breaches")?,
            period::date(at)
        ),
        None => format!(
            "max relative gap {gap:.4} at {} over {shared_n} shared periods",
            period::date(at)
        ),
    })
}

/// A flow's empty periods as zero cells, for the total series and the
/// rival's (`dimension` `''` or `'alternative'`): every period at the
/// metric's resolution from the series' first to the edge's bucket
/// that no floor cell falls in gets one cell at the period's start,
/// at zero — a sum over no rows. The grid is the engine's: a series
/// over the period range at the resolution's step, joined to the
/// buckets the cells hold, and the periods the join leaves empty are
/// the zeros, served as batches in the series schema beside the cells
/// they fill. The head stays at the floor: one zero cell per empty
/// period folds to zero at the resolution and above, and the floor is
/// never served below the resolution. A member's gap stays a gap, and
/// so does a stock's or a ratio's; the caller filters by verb.
async fn zero_fill(
    shared: &Arc<Shared>,
    cells: &RecordBatch,
    resolution: Resolution,
    edge: i64,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if cells.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let ctx = shared.scratch_ctx();
    ctx.register_table(
        "h",
        Arc::new(MemTable::try_new(
            cells.schema(),
            vec![vec![cells.clone()]],
        )?),
    )?;
    let q = format!(
        "WITH present AS (\
            SELECT metric, world, dimension, member, {p} AS period FROM h \
            WHERE behavior = 'flow' AND dimension IN ('', 'alternative') \
            GROUP BY 1, 2, 3, 4, 5), \
         grid AS (\
            SELECT metric, world, dimension, member, CAST(period AS {ty}) AS period FROM (\
                SELECT metric, world, dimension, member, \
                       unnest(generate_series(first, {to}, {step})) AS period \
                FROM (SELECT metric, world, dimension, member, min(period) AS first \
                      FROM present GROUP BY 1, 2, 3, 4))) \
         SELECT g.metric, g.world, g.dimension, g.member, g.period, 0.0 AS value, \
                CAST(NULL AS DOUBLE) AS num, CAST(NULL AS DOUBLE) AS den, 'flow' AS behavior \
         FROM grid g \
         LEFT JOIN present ON present.dimension = g.dimension \
                          AND present.member = g.member AND present.period = g.period \
         WHERE present.period IS NULL ORDER BY 3, 4, 5",
        p = period::bucket("period", resolution),
        ty = period::TYPE,
        to = period::bucket(&period::literal(edge), resolution),
        step = period::interval(resolution, 1),
    );
    ctx.sql(&q)
        .await?
        .collect()
        .await?
        .iter()
        .map(conform)
        .collect()
}

/// The rival's series at the metric's resolution and window, at its
/// own verb: a rival that serves num/den totals as a ratio even where
/// the chosen reading does not, and the reverse. Its time axis is
/// judged on the same rule as the chosen reading wherever a verdict
/// stands, falling back to the frame's only date column and refusing
/// where several stand unjudged — a rival is a comparison cell, and an
/// anchor guessed among several beside a judged series compares
/// nothing. Every refusal carries its own reason for the fact row.
async fn rival_series(
    shared: &Arc<Shared>,
    surface: &Surface,
    s: &Series<'_>,
    rival: &str,
    sql: &str,
) -> Result<(Vec<RecordBatch>, &'static str, Vec<String>), Abstain> {
    let Surface {
        ctx,
        dataset,
        judged,
        ..
    } = surface;
    let probe = crate::plan::build_plan(shared, ctx, sql).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    if !has("value") {
        return Err(Abstain::shape(s.metric, "it serves no `value` column"));
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
                .filter(|f| crate::plan::is_temporal(f.data_type()));
            match (dates.next(), dates.next()) {
                (Some(only), None) => only.name().clone(),
                (Some(_), Some(_)) => {
                    return Err(Abstain::shape(
                        s.metric,
                        "it carries several date columns and none of them is judged, so its \
                         time axis beside a judged series would be a guess",
                    ));
                }
                _ => return Err(Abstain::shape(s.metric, "it serves no date column")),
            }
        }
    };
    let verb: &'static str = if has("num") && has("den") {
        "ratio"
    } else if s.verb == "stock" {
        "stock"
    } else {
        "flow"
    };
    let rows = cells(
        shared,
        ctx,
        &cells_sql(
            s.metric,
            "alternative",
            &ql(rival),
            verb,
            &total_sql(sql, &tcol, verb, s.floor, s.since.as_deref()),
        ),
    )
    .await?;
    Ok((rows, verb, workspace_reads(&probe)))
}

/// The three verbs at a resolution, the window applied on the bucket:
/// flows sum per period; a marked stock sums the rows standing at the
/// period's LATEST observed date; a ratio serves `num` and `den` and
/// the period reads as sum(num)/sum(den), the summed halves beside it
/// — the only material a coarser grain can re-derive the division
/// from.
pub(crate) fn total_sql(
    sql: &str,
    tcol: &str,
    verb: &str,
    resolution: Resolution,
    since: Option<&str>,
) -> String {
    let p = period::bucket(&qi(tcol), resolution);
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
                       rank() OVER (PARTITION BY {p} ORDER BY {tcol_q} DESC) AS rk \
                FROM ({sql}){w}\
             ) WHERE rk = 1 GROUP BY period ORDER BY period",
            tcol_q = qi(tcol)
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
    let p = period::bucket(&qi(tcol), resolution);
    let w = since.map_or(String::new(), |s| format!(" AND {p} > {s}"));
    match verb {
        "ratio" => format!(
            "SELECT {p} AS period, {member} AS member, \
                    sum(num) / nullif(sum(den), 0) AS value, \
                    sum(num) AS num, sum(den) AS den \
             FROM ({sql}) WHERE {dcol_q} IS NOT NULL{w} \
             GROUP BY 1, 2 ORDER BY 1, 2",
            dcol_q = qi(dcol)
        ),
        "stock" => format!(
            "SELECT period, member, sum(value) AS value FROM (\
                SELECT {p} AS period, {member} AS member, value, \
                       rank() OVER (PARTITION BY {p}, {dcol_q} \
                                    ORDER BY {tcol_q} DESC) AS rk \
                FROM ({sql}) WHERE {dcol_q} IS NOT NULL{w}\
             ) WHERE rk = 1 GROUP BY period, member ORDER BY period, member",
            dcol_q = qi(dcol),
            tcol_q = qi(tcol)
        ),
        _ => format!(
            "SELECT {p} AS period, {member} AS member, sum(value) AS value \
             FROM ({sql}) WHERE {dcol_q} IS NOT NULL{w} \
             GROUP BY 1, 2 ORDER BY 1, 2",
            dcol_q = qi(dcol)
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
    let plan = crate::plan::build_plan(shared, ctx, sql).await?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(SessionError::not_served)?
        .collect()
        .await
        .map_err(SessionError::not_served)?;
    Ok(batches)
}

/// A series plan's rows as cells: planned through the session's own
/// pipeline, run, and held under the series schema.
async fn cells(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, Abstain> {
    run(shared, ctx, sql)
        .await?
        .iter()
        .map(|b| conform(b).map_err(Abstain::from))
        .collect()
}

/// A series plan's rows in the series schema: the metric and the world
/// as literals, the dimension named and the member an expression over
/// the plan's columns, the value and the halves as doubles, NULL
/// values and periods dropped — a period the verb could not value is
/// no cell. `verb` is the cells' behavior; a ratio's plan serves its
/// halves.
fn cells_sql(metric: &str, dimension: &str, member: &str, verb: &str, series: &str) -> String {
    let (num, den) = if verb == "ratio" {
        ("CAST(num AS DOUBLE)", "CAST(den AS DOUBLE)")
    } else {
        ("CAST(NULL AS DOUBLE)", "CAST(NULL AS DOUBLE)")
    };
    format!(
        "SELECT {} AS metric, 'actual' AS world, {} AS dimension, {member} AS member, \
                period, CAST(value AS DOUBLE) AS value, {num} AS num, {den} AS den, \
                {} AS behavior \
         FROM ({series}) WHERE value IS NOT NULL AND period IS NOT NULL",
        ql(metric),
        ql(dimension),
        ql(verb)
    )
}

/// A string as a plan literal.
fn ql(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A series query planned, replayed under `overrides` (`crate::replay`)
/// and run: the rows [`series`] reads, over the rewritten scans. With
/// no overrides, the plain series.
pub(crate) async fn replayed_series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    overrides: &[crate::replay::Override],
) -> Result<Vec<SeriesRow>, SessionError> {
    let plan = crate::plan::build_plan(shared, ctx, sql).await?;
    let plan = crate::replay::apply_overrides(plan, overrides)?;
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    collect_series(&batches, false).map_err(|Abstain(e)| e)
}

fn collect_series(batches: &[RecordBatch], with_member: bool) -> Result<Vec<SeriesRow>, Abstain> {
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let col = |n: &str| -> Result<&ArrayRef, Abstain> {
            b.column_by_name(n)
                .ok_or_else(|| Abstain::shape("the series", format!("the series served no {n}")))
        };
        let floats = |n: &str| -> Result<Option<Float64Array>, Abstain> {
            let Some(c) = b.column_by_name(n) else {
                return Ok(None);
            };
            let c = cast(c, &DataType::Float64).map_err(SessionError::from)?;
            c.as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .map(Some)
                .ok_or_else(|| {
                    Abstain::shape("the series", format!("{n} did not read as a number"))
                })
        };
        let period = period::column(col("period")?)
            .ok_or_else(|| Abstain::shape("the series", "period did not read as a period"))?;
        let member = if with_member {
            Some(col("member")?)
        } else {
            None
        };
        let value = floats("value")?
            .ok_or_else(|| Abstain::shape("the series", "the series served no value"))?;
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
                Some(m) => Some(array_value_to_string(m, i).map_err(SessionError::from)?),
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
    period::field("period", false)
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

/// The `metric_series()` shape: the cells, each row naming its metric
/// and its world. One schema for the process — every cube's cells
/// carry it, and every read hands it on rather than rebuilding it.
static SERIES: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields = vec![utf8("metric"), utf8("world")];
    fields.extend(cell_fields());
    Arc::new(Schema::new(fields))
});

fn series_schema() -> SchemaRef {
    Arc::clone(&SERIES)
}

/// A world's cells from a kernel's answer — the forecast's medians,
/// the scenario's points — as one batch in the series schema: the
/// total series only, the metric, the world and the verb named on
/// every row.
fn world_batch(
    metric: &str,
    world: World,
    behavior: &str,
    rows: &[(i64, f64, Option<f64>, Option<f64>)],
) -> RecordBatch {
    let n = rows.len();
    let text = |s: &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(s, n)))
    };
    RecordBatch::try_new(
        series_schema(),
        vec![
            text(metric),
            text(world.as_str()),
            text(""),
            text(""),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.0),
            )),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.3))),
            text(behavior),
        ],
    )
    .expect("column shapes match the schema")
}

// -- the reads ---------------------------------------------------------

/// The named arguments of `metric_series()`: `grain => '<grain>'`,
/// `world => '<world>'` and, in the scenario world, `scenario =>
/// '<name>'`, in the plan world `plan => '<name>'`, in any order.
/// Absent, a metric's own resolution and the actual world. The third
/// of the answer is the named world's name — the scenario's or the
/// plan's, the world says which.
pub(crate) fn series_args(
    args: &[FunctionArg],
) -> Result<(Option<Resolution>, World, Option<String>), SessionError> {
    let refuse =
        |what: String| SessionError::shape("metric_series", format!("metric_series({what})"));
    let grains = "minute, hour, day, week, month, quarter, year";
    let worlds = "actual, forecast, scenario or plan";
    let spelled = || {
        args.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Anything but the two named arguments is refused with the read
    // the caller meant, spelled out: the grain they named, the metric
    // as a filter.
    let meant = || {
        let named = |wanted: &str| {
            args.iter().find_map(|a| match a {
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(SQLExpr::Value(v)),
                    ..
                }
                | FunctionArg::ExprNamed {
                    name: SQLExpr::Identifier(name),
                    arg: FunctionArgExpr::Expr(SQLExpr::Value(v)),
                    ..
                } if name.value.eq_ignore_ascii_case(wanted) => match &v.value {
                    SQLValue::SingleQuotedString(s) => Some(s.clone()),
                    _ => None,
                },
                _ => None,
            })
        };
        let grain = named("grain").unwrap_or_else(|| "month".into());
        let filter = named("metric")
            .map(|m| format!(" WHERE metric = '{m}'"))
            .unwrap_or_default();
        refuse(format!(
            "{}): the arguments are the grain and the world — `SELECT metric, period, value FROM \
             metric_series(grain => '{grain}'){filter}`, `metric_series(world => 'forecast')`, \
             `metric_series(world => 'scenario', scenario => '<name>')`, \
             `metric_series(world => 'plan', plan => '<name>')`; filters ride WHERE. \
             The columns are metric, world, dimension, member, period, \
             value, num, den, behavior: the time column is `period`, the total is \
             `dimension = ''`",
            spelled()
        ))
    };
    let mut grain = None;
    let mut world = World::Actual;
    let mut scenario: Option<String> = None;
    let mut plan: Option<String> = None;
    for arg in args {
        let (name, value) = match arg {
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(n),
                arg: FunctionArgExpr::Expr(v),
                ..
            }
            | FunctionArg::Named {
                name: n,
                arg: FunctionArgExpr::Expr(v),
                ..
            } => (n.value.to_ascii_lowercase(), v),
            _ => return Err(meant()),
        };
        let (names, example) = match name.as_str() {
            "grain" => (grains, "month"),
            "world" => (worlds, "forecast"),
            "scenario" => ("the declared scenarios", "price_hike"),
            "plan" => ("the declared plans", "revised_budget"),
            _ => return Err(meant()),
        };
        let text = match value {
            SQLExpr::Value(v) => match &v.value {
                SQLValue::SingleQuotedString(s) => s.clone(),
                SQLValue::Placeholder(p) => {
                    return Err(refuse(format!(
                        "{name} => {p}): the {name} is unbound — a frame binds it from its URL, \
                         a statement spells it: {name} => '{example}'"
                    )));
                }
                other => {
                    return Err(refuse(format!(
                        "{name} => {other}): the {name} is a quoted name — one of {names}"
                    )));
                }
            },
            other => {
                return Err(refuse(format!(
                    "{name} => {other}): the {name} is a quoted name — one of {names}"
                )));
            }
        };
        match name.as_str() {
            "grain" => {
                grain = Some(Resolution::parse(&text).ok_or_else(|| {
                    refuse(format!("grain => '{text}'): a grain is one of {grains}"))
                })?);
            }
            "scenario" => scenario = Some(text),
            "plan" => plan = Some(text),
            _ => {
                world = World::parse(&text)
                    .ok_or_else(|| refuse(format!("world => '{text}'): a world is {worlds}")))?;
            }
        }
    }
    // A scenario and a plan are read by name, each in its own world.
    let name = match (world, scenario, plan) {
        (_, Some(s), Some(p)) => {
            return Err(refuse(format!(
                "scenario => '{s}', plan => '{p}'): one name — a scenario is read in the \
                 scenario world, a plan in the plan world"
            )));
        }
        (World::Scenario, Some(name), None) | (World::Plan, None, Some(name)) => Some(name),
        (_, Some(name), None) => {
            return Err(refuse(format!(
                "scenario => '{name}'): a scenario is read in the scenario world — \
                 `metric_series(world => 'scenario', scenario => '{name}')`"
            )));
        }
        (_, None, Some(name)) => {
            return Err(refuse(format!(
                "plan => '{name}'): a plan is read in the plan world — \
                 `metric_series(world => 'plan', plan => '{name}')`"
            )));
        }
        (World::Scenario, None, None) => {
            return Err(refuse(
                "world => 'scenario'): a scenario is read by name — \
                 `metric_series(world => 'scenario', scenario => '<name>')`"
                    .into(),
            ));
        }
        (World::Plan, None, None) => {
            return Err(refuse(
                "world => 'plan'): a plan is read by name — \
                 `metric_series(world => 'plan', plan => '<name>')`"
                    .into(),
            ));
        }
        (_, None, None) => None,
    };
    Ok((grain, world, name))
}

/// The verb as the cells carry it; a word the cube never writes is no
/// verb.
fn verb_static(verb: &str) -> &'static str {
    match verb {
        "flow" => "flow",
        "stock" => "stock",
        "ratio" => "ratio",
        _ => "",
    }
}

/// `metric_series(grain => …, world => …)` — the cells of every
/// current grounding: `(metric, world, dimension, member, period,
/// value, num, den, behavior)`. Without a grain each metric serves its
/// cells at its own resolution over its own rung; with one, at the
/// asked grain over that grain's rung — either a plan over the
/// metric's head, cached beside it. A metric coarser than the asked
/// grain serves no rows — honest absence. `period` is the bucket's
/// start, a typed timestamp. A cache entry is never stale: it is a hit
/// or a miss.
///
/// Without a world, the actual world: every read that stood before
/// the column serves the same rows. The forecast world is the newest
/// `metric_forecast` run per metric, its steps as cells with the
/// median for the value, at the metric's own resolution only — a
/// median re-bucketed is not a median, so another grain serves no
/// forecast rows, and a run at a grain the metric no longer has serves
/// none. Its totals are not cells. The scenario world is the named
/// scenario's cells (`crate::scenario`): the replayed values and the
/// estimated medians, at the metric's own resolution only, like the
/// forecast's; `scenario_points()` has the detail. The plan world is
/// the named plan aspect's own head (`crate::plan_world`, fixture 27),
/// a metric's head in every way, served under the metric the plan
/// names at the plan's resolution or any coarser grain asked — a plan
/// is a landed table's numbers, so its grains derive as a metric's do.
///
/// The cached cube is the table. Its cells are handed to the planner
/// as they sit in the cache — one batch per metric, `Arc`-shared,
/// copied into nothing. The only rows a read allocates are the ones
/// it answers with: the forecast world's, a dozen per metric, and the
/// plan world's two label columns over shared cells.
pub(crate) async fn metric_series_batch(
    shared: &Arc<Shared>,
    grain: Option<Resolution>,
    world: World,
    name: Option<&str>,
) -> Result<Served, SessionError> {
    let mut partitions = Vec::new();
    let surface = Surface::load(shared).await?;
    // The plan world (fixture 27): one plan aspect's cells, served
    // under the metric it names. The plan's head is a metric's head,
    // read at the plan's own resolution or any coarser grain asked;
    // a finer grain serves no rows, as for a metric.
    if world == World::Plan {
        let name = name.expect("series_args pairs the plan world with its name");
        let Some((surface, slot)) = surface
            .as_ref()
            .and_then(|s| s.plans.iter().find(|p| p.aspect == name).map(|p| (s, p)))
        else {
            return Err(crate::plan_world::unknown(shared, name).await);
        };
        let head = surface.entry(shared, slot).await;
        if let (Some(resolution), Some(metric)) = (head.fact.resolution, slot.plan_of.as_deref()) {
            let asked = grain.unwrap_or(resolution);
            if asked >= resolution {
                let cells = surface.at_grain(shared, slot, head, asked).await;
                partitions.push(relabel(shared, &cells.cells, metric, World::Plan).await?);
            }
        }
        return Ok(Served {
            schema: series_schema(),
            partitions,
        });
    }
    if let Some(surface) = surface {
        let ahead = match world {
            World::Forecast => crate::search::forecast_world_cells(shared).await?,
            World::Actual | World::Scenario | World::Plan => HashMap::new(),
        };
        // The scenario's cells, read once the first metric at the asked
        // grain needs them — a grain no metric has asks the service
        // nothing. Boxed: the scenario read reads the actual world
        // through this same door for its origins.
        let mut declared: Option<crate::scenario::WorldCells> = None;
        for slot in &surface.slots {
            let head = surface.entry(shared, slot).await;
            // An abstained metric has no resolution and no cells.
            let Some(resolution) = head.fact.resolution else {
                continue;
            };
            match world {
                World::Actual => {
                    let asked = grain.unwrap_or(resolution);
                    if asked < resolution {
                        continue;
                    }
                    partitions.push(
                        surface
                            .at_grain(shared, slot, head, asked)
                            .await
                            .cells
                            .clone(),
                    );
                }
                World::Forecast => {
                    if grain.is_some_and(|g| g != resolution) {
                        continue;
                    }
                    let Some((run_grain, points)) = ahead.get(&slot.aspect) else {
                        continue;
                    };
                    if run_grain != resolution.as_str() {
                        continue;
                    }
                    let behavior = head.fact.behavior.as_deref().map(verb_static).unwrap_or("");
                    let rows: Vec<_> = points.iter().map(|&(p, v)| (p, v, None, None)).collect();
                    partitions.push(world_batch(&slot.aspect, World::Forecast, behavior, &rows));
                }
                World::Plan => unreachable!("the plan world is served above"),
                World::Scenario => {
                    if grain.is_some_and(|g| g != resolution) {
                        continue;
                    }
                    let Some(name) = name else {
                        continue;
                    };
                    if declared.is_none() {
                        declared =
                            Some(Box::pin(crate::scenario::world_cells(shared, name)).await?);
                    }
                    let Some(rows) = declared.as_ref().and_then(|d| d.get(&slot.aspect)) else {
                        continue;
                    };
                    let behavior = head.fact.behavior.as_deref().map(verb_static).unwrap_or("");
                    partitions.push(world_batch(&slot.aspect, World::Scenario, behavior, rows));
                }
            }
        }
    }
    Ok(Served {
        schema: series_schema(),
        partitions,
    })
}

/// One plan's series beside its metric's, for `plan_points()`
/// (`crate::plan_world`): per grain the plan's total cells and the
/// metric's at that grain where the metric is as fine or finer, and
/// the metric's origin — the last period of its own series at its own
/// resolution, the data's edge. Two grains stand: the plan's own —
/// its resolution, or the grain its rows are booked against where the
/// declaration says so (`x-covers`: the adopted budget on the January
/// row covers the year) — and the year where that is finer.
pub(crate) struct PlanSeries {
    pub plan: String,
    pub metric: String,
    pub metric_resolution: Option<Resolution>,
    /// The metric's verb — `flow`, `stock` or `ratio` — which decides
    /// what a plan at a coarser grain compares to.
    pub metric_behavior: Option<String>,
    pub origin: Option<i64>,
    pub grains: Vec<PlanGrain>,
}

pub(crate) struct PlanGrain {
    pub resolution: Resolution,
    /// The plan's total cells beside the metric's at this grain,
    /// `(period, value, actual)` in period order; no actual where the
    /// metric is coarser than the grain or serves none for the period.
    pub rows: Vec<(i64, f64, Option<f64>)>,
}

/// Every served plan's series beside its metric's (fixture 27, #117).
/// A plan whose head abstains, or whose metric has no serving head,
/// has no entry.
pub(crate) async fn plan_series(shared: &Arc<Shared>) -> Result<Vec<PlanSeries>, SessionError> {
    let mut out = Vec::new();
    let Some(surface) = Surface::load(shared).await? else {
        return Ok(out);
    };
    for slot in &surface.plans {
        let head = surface.entry(shared, slot).await;
        let (Some(resolution), Some(metric)) = (head.fact.resolution, slot.plan_of.clone()) else {
            continue;
        };
        let Some(mslot) = surface.slots.iter().find(|s| s.aspect == metric) else {
            continue;
        };
        let mhead = surface.entry(shared, mslot).await;
        let metric_resolution = mhead.fact.resolution;
        let metric_behavior = mhead.fact.behavior.clone();
        let mut origin = None;
        if let Some(mres) = metric_resolution {
            let own = surface
                .at_grain(shared, mslot, Arc::clone(&mhead), mres)
                .await;
            origin = last_period(shared, &own.cells).await?;
        }
        // The plan's own grain: its rows' resolution, or the coarser
        // grain they are booked against. A word finer than the rows
        // changes nothing.
        let own = slot.covers.map_or(resolution, |c| c.max(resolution));
        let mut grains = Vec::new();
        for grain in [own, Resolution::Year] {
            if grain == Resolution::Year && own == Resolution::Year && !grains.is_empty() {
                break;
            }
            let plan = surface
                .at_grain(shared, slot, Arc::clone(&head), grain)
                .await;
            let actual = match metric_resolution {
                Some(mres) if grain >= mres => surface
                    .at_grain(shared, mslot, Arc::clone(&mhead), grain)
                    .await
                    .cells
                    .clone(),
                _ => RecordBatch::new_empty(series_schema()),
            };
            let rows = aligned(shared, &plan.cells, &actual).await?;
            grains.push(PlanGrain {
                resolution: grain,
                rows,
            });
        }
        out.push(PlanSeries {
            plan: slot.aspect.clone(),
            metric,
            metric_resolution,
            metric_behavior,
            origin,
            grains,
        });
    }
    Ok(out)
}

/// What the scenario read needs of a metric to replay it
/// (`crate::scenario`): its grounding and time column, its verb and
/// resolution, and the tables it scans — where a lever's table is among
/// them, the arithmetic reaches it.
pub(crate) struct Frame {
    pub(crate) metric: String,
    pub(crate) sql: String,
    pub(crate) tcol: String,
    pub(crate) verb: &'static str,
    pub(crate) resolution: Resolution,
    pub(crate) scans: HashSet<String>,
}

/// Every applicable metric's frame. The time column is the judged one
/// as the cube picks it, or the frame's only date column; a metric
/// without one has no series and no frame.
pub(crate) async fn scenario_frames(shared: &Arc<Shared>) -> Result<Vec<Frame>, SessionError> {
    let mut out = Vec::new();
    let Some(surface) = Surface::load(shared).await? else {
        return Ok(out);
    };
    for slot in &surface.slots {
        let head = surface.entry(shared, slot).await;
        if !head.fact.applicable {
            continue;
        }
        let (Some(resolution), Some(behavior)) =
            (head.fact.resolution, head.fact.behavior.as_deref())
        else {
            continue;
        };
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };
        let Ok(probe) = crate::plan::build_plan(shared, &surface.ctx, sql).await else {
            continue;
        };
        let fields = probe.schema();
        let sources = crate::provenance::served_sources(&probe, &surface.dataset);
        let tcol = match judged_time_column(fields, &sources, &surface.judged.temporal) {
            Some((column, ..)) => column,
            None => {
                let mut dates = fields
                    .fields()
                    .iter()
                    .filter(|f| crate::plan::is_temporal(f.data_type()));
                match (dates.next(), dates.next()) {
                    (Some(only), None) => only.name().clone(),
                    _ => continue,
                }
            }
        };
        out.push(Frame {
            metric: slot.aspect.clone(),
            sql: sql.to_string(),
            tcol,
            verb: verb_static(behavior),
            resolution,
            scans: crate::provenance::scanned_tables(&probe, &surface.dataset),
        });
    }
    Ok(out)
}

/// `metric_axes()` — one row per current grounding, the record read:
/// `(metric, applicable, judged_current, reason, behavior,
/// behavior_basis, grain, resolution, window, outside, dims, basis,
/// admitted_by, bucketed, unadmitted, unadmitted_why, unadmitted_act, wanted,
/// wanted_over, unserved, alternative, alternative_divergence,
/// alternative_error, superseded_divergence, plan_of)`. A plan aspect's
/// row names the metric it plans in `plan_of`. What the cube
/// admitted and why not, and
/// whether the verdicts it admitted on stand at this pin; served from
/// the entry's fact row, so it builds what is not built.
pub(crate) async fn metric_axes_batch(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let cubes = cubes(shared).await?;
    let facts: Vec<&Fact> = cubes.iter().map(|c| &c.fact).collect();
    fact_batch(&facts)
}

/// Every measurement the bound dataset's fact rows and its witnesses
/// read that no function has landed, as `(function, subject)`: each
/// row's wants over its served columns, then — while a grounding
/// stands — every declared function returning a witnessed measurement
/// aspect at dataset grain with no voice here (the bands walk, the
/// forecast). A witness without a detector counts: it names the
/// measurement as standing, and a forecast has nothing to band. The
/// same rows `owed` derives in SQL as `never measured`; re-measure
/// runs these.
pub(crate) async fn wanted(shared: &Arc<Shared>) -> Result<Vec<(String, String)>, SessionError> {
    let dataset = shared.dataset().ok_or(SessionError::NoDataset)?;
    let mut out: Vec<(String, String)> = Vec::new();
    for cube in cubes(shared).await? {
        for (function, subject) in cube.fact.wanted.iter().zip(&cube.fact.wanted_over) {
            let want = (function.clone(), subject.clone());
            if !out.contains(&want) {
                out.push(want);
            }
        }
    }
    let rctx = shared.read_context().await?;
    let grounded = current_query_slots(&rctx, &dataset)
        .await?
        .iter()
        .any(|s| serde_json::from_str::<Value>(&s.body).is_ok_and(|b| b.get("sql").is_some()));
    if !grounded {
        return Ok(out);
    }
    let here = |f: &&glossql_glossary::FunctionRow| {
        f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
    };
    for f in rctx.functions.iter().filter(here) {
        let Some(aspect) = f.returns.as_deref() else {
            continue;
        };
        let at_dataset = rctx.aspects.iter().any(|a| {
            a.name == aspect && a.kind == "measurement" && a.grains.as_deref() == Some("dataset")
        });
        let witnessed = rctx.witnesses.iter().any(|w| w.aspect == aspect);
        if at_dataset
            && witnessed
            && glossql_glossary::Store::measurements_in(&rctx, &dataset, &f.name).is_empty()
        {
            out.push((f.name.clone(), dataset.clone()));
        }
    }
    Ok(out)
}

/// Fact rows as the `metric_axes()` relation — one schema for the
/// read and for a grounding write's answer.
/// The three columns of the unadmitted list, in one order.
fn split_unadmitted(
    unadmitted: Vec<(String, String, &'static str)>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut columns = Vec::with_capacity(unadmitted.len());
    let mut whys = Vec::with_capacity(unadmitted.len());
    let mut acts = Vec::with_capacity(unadmitted.len());
    for (column, why, act) in unadmitted {
        columns.push(column);
        whys.push(why);
        acts.push(act.to_string());
    }
    (columns, whys, acts)
}

pub(crate) fn fact_batch(facts: &[&Fact]) -> Result<RecordBatch, SessionError> {
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
        Field::new("outside", DataType::Int64, false),
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
        Field::new("axes_basis", DataType::Utf8, true),
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
        Field::new(
            "unadmitted_act",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "wanted",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "wanted_over",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "unserved",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("alternative", DataType::Utf8, true),
        Field::new("alternative_divergence", DataType::Utf8, true),
        Field::new("alternative_error", DataType::Utf8, true),
        Field::new("superseded_divergence", DataType::Utf8, true),
        Field::new("plan_of", DataType::Utf8, true),
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
            text(|f| f.behavior_basis.as_deref()),
            list(|f| &f.grain),
            text(|f| f.resolution.map(Resolution::as_str)),
            text(|f| f.window.as_deref()),
            Arc::new(datafusion::arrow::array::Int64Array::from_iter_values(
                facts.iter().map(|f| f.outside),
            )),
            list(|f| &f.dims),
            list(|f| &f.basis),
            list(|f| &f.admitted_by),
            text(|f| Some(f.axes_basis.as_str())),
            list(|f| &f.bucketed),
            list(|f| &f.unadmitted),
            list(|f| &f.unadmitted_why),
            list(|f| &f.unadmitted_act),
            list(|f| &f.wanted),
            list(|f| &f.wanted_over),
            list(|f| &f.unserved),
            text(|f| f.alternative.as_deref()),
            text(|f| f.alternative_divergence.as_deref()),
            text(|f| f.alternative_error.as_deref()),
            text(|f| f.superseded_divergence.as_deref()),
            text(|f| f.plan_of.as_deref()),
        ],
    )
    .map_err(SessionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cube(rows: usize) -> Arc<Cube> {
        let rows: Vec<_> = (0..rows)
            .map(|i| (crate::shared::integer(i), 1.0, None, None))
            .collect();
        Arc::new(Cube {
            fact: Fact::abstain("m", String::new()),
            cells: world_batch("m", World::Actual, "flow", &rows),
            version_bound: None,
        })
    }

    /// The derive plan, pinned: a change to its shape is a visible
    /// diff, not a quiet one.
    #[test]
    fn the_derive_plan_is_pinned() {
        let since = period::parse_date("2024-12-01").expect("a date");
        let plan =
            derive_plan(&cube(1).cells, "m", Resolution::Month, Some(since)).expect("a plan");
        insta::assert_snapshot!("derive_plan", plan.display_indent().to_string());
    }

    /// The series plans as the engine reads them over a frame with a
    /// date, a value, a member column and a ratio's halves, pinned the
    /// same way: the total and the member series at each verb.
    #[tokio::test]
    async fn the_series_plans_are_pinned() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("num", DataType::Float64, true),
            Field::new("den", DataType::Float64, true),
            Field::new("r", DataType::Utf8, true),
        ]));
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("a table")),
        )
        .expect("registers");
        let frame = "SELECT d, value, num, den, r FROM t";
        let since = Some("TIMESTAMP '2024-12-15T00:00:00'");
        let mut shown = String::new();
        for verb in ["flow", "ratio", "stock"] {
            let total = cells_sql(
                "m",
                "",
                "''",
                verb,
                &total_sql(frame, "d", verb, Resolution::Month, since),
            );
            let member = cells_sql(
                "m",
                "r",
                "member",
                verb,
                &member_sql(
                    frame,
                    "d",
                    "r",
                    "CAST(\"r\" AS VARCHAR)",
                    verb,
                    Resolution::Month,
                    since,
                ),
            );
            for (name, sql) in [("total", total), ("member", member)] {
                let plan = ctx.state().create_logical_plan(&sql).await.expect("plans");
                shown.push_str(&format!("-- {name} {verb}\n{}\n", plan.display_indent()));
            }
        }
        insta::assert_snapshot!("series_plans", shown);
    }

    fn key(metric: &str) -> CubeKey {
        CubeKey {
            dataset: "d".into(),
            metric: metric.into(),
            grain: None,
            pin: "p".into(),
            digest: "7".into(),
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
            q.contains("WHERE CAST(date_trunc('day', \"d\") AS TIMESTAMP(6)) > TIMESTAMP '2024-12-15T00:00:00'"),
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
            q.contains("PARTITION BY CAST(date_trunc('month', \"d\") AS TIMESTAMP(6)), \"r\""),
            "{q}"
        );
        assert!(!q.contains(" AND "), "{q}");
    }
}
