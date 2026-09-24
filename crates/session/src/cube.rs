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
//! another metric's column, a landing on another table — changes
//! neither, and the entry stays a hit; a moved input is a miss, never
//! an invalidation. The one exception is a frame that itself scans a
//! workspace relation: its entry binds to those relations' versions
//! on top ([`workspace_reads`]). The fill is lazy and single-flight
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

/// A calendar resolution — the rungs of the ladder, finest first, and
/// the grains a read may ask for. Ordered, so the coarser of two is
/// `max`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
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

/// One metric's fact row: what the cube admitted and why not. It
/// rides the landed cube as a tag, as JSON, which is why it derives
/// serde and owns its two basis words.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
/// `(metric, dimension, member, period, value, num, den, behavior)`, dimension
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
const HEAD_SHAPE: &str = "floor cells over the longest rung";

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
            && !crate::whatif::is_temporal(f.data_type())
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
/// nothing of the store beyond (slot, judged, settings) — the frame's
/// own scans are [`workspace_reads`]'s to catch.
fn metric_digest(
    slot: &QuerySlot,
    probe: Option<&datafusion::logical_expr::LogicalPlan>,
    reads: &Reads,
    judged: &Judged,
    settings: &Settings,
    shapes: &HashMap<String, glossql_catalog::Shape>,
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
struct Surface {
    dataset: String,
    version: String,
    slots: Vec<QuerySlot>,
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
        Ok(Some(Surface {
            dataset,
            version: rctx.version.clone(),
            slots,
            judged,
            settings,
            shapes: rctx.shapes.clone(),
            pin_text: rctx.pin.text.clone(),
            cache: shared.cube(),
            ctx: shared.session_ctx(),
            lake: shared.lake(),
        }))
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
        let probe = match frame_sql(&slot.body) {
            Some(sql) => crate::whatif::build_plan(shared, &self.ctx, &sql)
                .await
                .ok(),
            None => None,
        };
        let reads = probe
            .as_ref()
            .map(|p| reads_of(p, &self.dataset, &self.judged.pointers))
            .unwrap_or_default();
        let key = CubeKey {
            dataset: self.dataset.clone(),
            metric: slot.aspect.clone(),
            grain,
            pin: glossql_glossary::table_legs(&self.pin_text, &self.dataset, &reads.tables),
            digest: metric_digest(
                slot,
                probe.as_ref(),
                &reads,
                &self.judged,
                &self.settings,
                &self.shapes,
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
                match derive(&head, grain, window.as_deref()).await {
                    Ok(cube) => Arc::new(cube),
                    Err(Abstain(reason)) => Arc::new(Cube {
                        fact: Fact::abstain(&slot.aspect, reason),
                        cells: RecordBatch::new_empty(series_schema()),
                        version_bound: head.version_bound.clone(),
                    }),
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

/// Every metric's head — what `metric_axes()` describes. A landing
/// calls it to rebuild and land the heads over the tables it moved:
/// every other entry is a hit, and every grain follows from its head.
pub(crate) async fn cubes(shared: &Arc<Shared>) -> Result<Vec<Arc<Cube>>, SessionError> {
    let Some(surface) = Surface::load(shared).await? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(surface.slots.len());
    for slot in &surface.slots {
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
        Err(Abstain(reason)) => Fact::abstain(aspect, reason),
    })
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
            drift(&now.cells, &before.cells)
        });
    }
    Ok(fact)
}

/// The totals of a cube's cells by period — the undimensioned rows.
fn totals(cells: &RecordBatch) -> HashMap<i64, f64> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};
    let dimension = cells.column(1).as_string::<i32>();
    let period = cells.column(3).as_primitive::<TimestampNanosecondType>();
    let value = cells.column(4).as_primitive::<Float64Type>();
    (0..cells.num_rows())
        .filter(|&i| dimension.value(i).is_empty())
        .map(|i| (period.value(i), value.value(i)))
        .collect()
}

/// The gap between two frames' totals over their shared periods, in
/// the rival divergence's words, and the periods one serves and the
/// other does not. Agreement is a zero gap, never silence.
fn drift(now: &RecordBatch, before: &RecordBatch) -> String {
    let (now, before) = (totals(now), totals(before));
    let day = |p: i64| {
        chrono::DateTime::from_timestamp_nanos(p)
            .format("%Y-%m-%d")
            .to_string()
    };
    let mut shared = 0usize;
    let mut max: Option<(f64, i64)> = None;
    for (p, v) in &now {
        let Some(b) = before.get(p) else { continue };
        shared += 1;
        let scale = v.abs().max(b.abs());
        let gap = if scale == 0.0 {
            0.0
        } else {
            (v - b).abs() / scale
        };
        if max.is_none_or(|(g, _)| gap > g) {
            max = Some((gap, *p));
        }
    }
    let mut out = match max {
        None => "no shared periods with the writing it supersedes".to_string(),
        Some((0.0, _)) => {
            format!("no gap against the writing it supersedes over {shared} shared periods")
        }
        Some((gap, at)) => format!(
            "the total moved {:.1} % at {} against the writing it supersedes, the widest gap \
             over {shared} shared periods",
            gap * 100.0,
            day(at)
        ),
    };
    let only_before = before.keys().filter(|p| !now.contains_key(p)).count();
    let only_now = now.keys().filter(|p| !before.contains_key(p)).count();
    if only_before > 0 {
        out.push_str(&format!(
            "; {only_before} periods only in the writing it supersedes"
        ));
    }
    if only_now > 0 {
        out.push_str(&format!("; {only_now} periods only now"));
    }
    out
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
    match build(shared, surface, slot, probe).await {
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
    let probe = match probe {
        Some(p) => p,
        None => crate::whatif::build_plan(shared, ctx, sql).await?,
    };
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
    // What the row reads and nobody measured: the source columns of
    // every served date of one table without a temporal verdict; the
    // candidate columns add theirs below.
    let mut wanted: Vec<(String, String)> = Vec::new();
    if let Some(function) = &judged.temporal_fn {
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
            for column in columns {
                if !judged.temporal.contains_key(column) && !wanted.iter().any(|(_, s)| s == column)
                {
                    wanted.push((function.clone(), column.clone()));
                }
            }
        }
    }
    // No judged time axis is no series: the plan stage still names
    // what it wants, and the fact row abstains carrying it.
    let (time_column, cadence, time_current) =
        match judged_time_column(fields, &sources, &judged.temporal) {
            Some((column, cadence, current)) => (Some(column), cadence, current),
            None => (None, None, true),
        };
    // Whether every verdict admitted on stands at this pin — the time
    // axis now, each admitted dimension below.
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
    // The verb's own measurement, where the value is or sums a column
    // nobody measured: the grounding write runs it, and re-measure
    // runs it. A shape or a ratio needs none.
    if !is_ratio
        && behavior_basis != "shape"
        && let Some(function) = &judged.behavior_fn
        && let Some(column) = crate::provenance::summed_source(&probe, "value", dataset)
        && !judged.behavior.contains_key(&column)
        && !wanted.iter().any(|(f, s)| f == function && *s == column)
    {
        wanted.push((function.clone(), column));
    }

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
    let mut unadmitted: Vec<(String, String, &'static str)> = Vec::new();
    // The grounding's own word on its axes: `axes` lists the served
    // columns the metric is sliced by, in order, and closes every other
    // served column — the empty list closes them all, where it holds.
    // Its author's word admits a listed column whatever was measured,
    // as a `dimension` gloss would; a listed name the frame does not
    // serve as a sliceable column is named back. Absent, the verdicts
    // and the column glosses decide below.
    let authored: Option<Vec<String>> = body.get("axes").and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    });
    // The empty list holds for the two shapes no column slices whole:
    // a distinct count, whose members double-count across any column,
    // and a ratio, whose members do not add up to it. On any other
    // shape every member adds up to the total, so the verdicts keep
    // deciding, and the row says the word was measured over.
    let (authored, axes_basis): (Option<Vec<String>>, &'static str) = match authored {
        Some(list)
            if list.is_empty()
                && !is_ratio
                && !crate::provenance::distinct_count(&probe, "value") =>
        {
            (None, "measured over authored")
        }
        Some(list) => (Some(list), "authored"),
        None => (None, "measured"),
    };
    // What the record says of a column without the author's word: a
    // `dimension` gloss that admits it, a verdict that does — its own,
    // or one reached through a declared edge — or nothing. A closed
    // column's act carries it, so the line can name what the word
    // closed over.
    let admits = |subject: &String| -> Option<&'static str> {
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
            || through_edge(subject, &scanned, &judged.pointers, &judged.relevance).is_some();
        measured.then_some("verdict")
    };
    let author: &'static str = if slot.rank == 0 { "human" } else { "agent" };
    let mut sliceable: Vec<&str> = Vec::new();
    for f in fields.fields() {
        let n = f.name().as_str();
        if n == "value"
            || (is_ratio && (n == "num" || n == "den"))
            || crate::whatif::is_temporal(f.data_type())
        {
            continue;
        }
        sliceable.push(n);
        if let Some(listed) = &authored {
            match listed.iter().position(|a| a == n) {
                Some(i) => cand.push(Candidate {
                    column: n.to_string(),
                    relevance: (listed.len() - i) as f64,
                    current: true,
                    basis: subjects.get(n).cloned().unwrap_or_else(|| n.to_string()),
                    admitted_by: author,
                    primary: false,
                }),
                None => {
                    let (why, act) = match subjects.get(n).and_then(admits) {
                        Some("gloss") => ("a dimension gloss admits it", "closed over gloss"),
                        Some("verdict") => ("a verdict admits it", "closed over verdict"),
                        _ => ("nothing measured admits it", "closed"),
                    };
                    unadmitted.push((
                        n.to_string(),
                        format!("closed by the grounding's axes ({author}); {why}"),
                        act,
                    ));
                }
            }
            continue;
        }
        let Some(subject) = subjects.get(n) else {
            unadmitted.push((
                n.to_string(),
                "an expression, not a table column: no verdict can reach it — serve the \
                 column it derives from, or land it as a recipe column"
                    .into(),
                "expression",
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
                "none",
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
                unadmitted.push((n.to_string(), why, act));
                continue;
            }
        };
        cand.push(candidate);
    }
    if let Some(listed) = &authored {
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
    // Where nothing served can be an axis and the author listed none,
    // the frame is a date and a value: the row names the columns of
    // the tables it scans that a gloss or a verdict already admits and
    // it does not serve — the axis is judged, the road is to serve it.
    let mut unserved: Vec<String> = Vec::new();
    if cand.is_empty() && authored.is_none() {
        let served: std::collections::HashSet<&String> = sources.values().flatten().collect();
        for table in &scanned {
            let Some(shape) = shapes.get(table) else {
                continue;
            };
            for column in shape.columns.keys() {
                let subject = format!("{table}.{column}");
                if served.contains(&subject) {
                    continue;
                }
                // The gloss is the read policy over the verdict, here
                // as in admission: `none` closes the column, a word
                // admits it, and without one the verdict decides.
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
        axes_basis,
        wanted,
        unserved,
        foreign: workspace_reads(&probe),
    })
}

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
        candidates: cand,
        mut unadmitted,
        axes_basis,
        wanted,
        unserved,
        mut foreign,
        ..
    } = planned;
    let sql = sql.as_str();
    let tcol = tcol.as_str();

    // The declared grain, validated where the frame is built: one row
    // per key, or the metric abstains — a frame that breaks its
    // declared identity multiplies every aggregating reader, and
    // nothing downstream can tell duplication from multi-entity.
    // One aggregate over the frame, never a count over the frame
    // grouped again: the engine's projection pruner keeps only the
    // group keys the input's functional dependencies call sufficient
    // when the parent reads no key (`optimize_projections`), and an
    // aggregate over a join whose one side is DISTINCT mints a
    // dependency from that side's key to the whole row, which is
    // false — the grouped shape then counts one side's keys. At this
    // pin and on upstream main.
    if !grain.is_empty() {
        let keys = grain.iter().map(|c| qi(c)).collect::<Vec<_>>().join(", ");
        let q = format!(
            "SELECT count(*) AS total, count(DISTINCT struct({keys})) AS keys FROM ({sql})"
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
                version_bound: bound(&foreign),
            });
        }
    }
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
        dims.push(c.column.clone());
        basis.push(c.basis.clone());
        admitted_by.push(c.admitted_by.to_string());
        judged_current &= c.current;
        if *n > MEMBERS_CAP {
            bucketed.push(c.column.clone());
        }
    }

    // The windows and the data's reach, one pass over the frame: the
    // edge (the latest observation) and the periods the data holds at
    // the metric's resolution. A grain's window is the edge's bucket
    // at that grain less the grain's rung; the head keeps every floor
    // bucket after the earliest window of the grains from the
    // resolution up, so each of them derives from it whole. A grain
    // without a rung is unbounded, and so is the head.
    let p_own = period_expr(&qi(tcol), resolution);
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
                ", {} - INTERVAL '{}' AS \"since_{}\"",
                period_expr("edge", *r),
                w.replace('\'', "''"),
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
    let text = |name: &str| -> Option<String> {
        let col = row?.column_by_name(name)?;
        (!col.is_null(0))
            .then(|| array_value_to_string(col, 0).ok())
            .flatten()
    };
    let spanned = int_column(&batches, "spanned")
        .map_err(|e| Abstain(e.to_string()))?
        .first()
        .copied()
        .unwrap_or(0);
    let since_at = |r: Resolution| text(&format!("since_{}", r.as_str()));
    // The metric's own window — what its series is served over, what
    // the members rank over, and what `outside` counts against.
    let since_own = since_at(resolution).map(|t| format!("TIMESTAMP '{t}'"));
    let since = if unbounded {
        None
    } else {
        served
            .iter()
            .filter_map(|r| since_at(*r))
            .min()
            .map(|t| format!("TIMESTAMP '{t}'"))
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

    // The total series, at the floor over the head's window.
    let total = series(
        shared,
        ctx,
        &total_sql(sql, tcol, verb, floor, since.as_deref()),
        false,
    )
    .await?;
    push(&mut cells, "", total, verb);

    // Member series per admitted dimension, same verb, same window —
    // independent plans, driven concurrently. A bucketed dimension
    // names its top members by weight over the metric's own window
    // and folds the rest into 'other'; the set is resolved first and
    // spliced as literals, deterministic (weight, then name) so two
    // builds at one pin agree.
    let members = dims.iter().map(|dcol| {
        let bucketed = bucketed.iter().any(|b| b == dcol);
        let since = since.as_deref();
        let since_own = since_own.as_deref();
        let p_own = p_own.as_str();
        async move {
            let member = if bucketed {
                let weight = if verb == "ratio" {
                    "sum(den)"
                } else {
                    "sum(value)"
                };
                let clause = since_own.map_or(String::new(), |s| format!(" AND {p_own} > {s}"));
                let q = format!(
                    "SELECT CAST({dcol_q} AS VARCHAR) AS mc_member FROM ({sql}) \
                     WHERE {dcol_q} IS NOT NULL{clause} GROUP BY 1 \
                     ORDER BY {weight} DESC NULLS LAST, mc_member LIMIT {}",
                    MEMBERS_CAP - 1,
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
                        .ok_or_else(|| Abstain("the member pass served no mc_member".into()))?;
                    for i in 0..b.num_rows() {
                        let m =
                            array_value_to_string(col, i).map_err(|e| Abstain(e.to_string()))?;
                        named.push(format!("'{}'", m.replace('\'', "''")));
                    }
                }
                if named.is_empty() {
                    // No member stands inside the window — every row
                    // of the column there is NULL — so there is
                    // nothing to name and no cell to serve; the plain
                    // cast, never an empty IN list, which is not a
                    // query.
                    format!("CAST({dcol_q} AS VARCHAR)", dcol_q = qi(dcol))
                } else {
                    format!(
                        "CASE WHEN CAST({dcol_q} AS VARCHAR) IN ({}) \
                         THEN CAST({dcol_q} AS VARCHAR) ELSE 'other' END",
                        named.join(", "),
                        dcol_q = qi(dcol)
                    )
                }
            } else {
                format!("CAST({dcol_q} AS VARCHAR)", dcol_q = qi(dcol))
            };
            let rows = series(
                shared,
                ctx,
                &member_sql(sql, tcol, dcol, &member, verb, floor, since),
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
            match rival_series(shared, surface, alt_sql, verb, floor, since.as_deref()).await {
                Ok((rows, rival_verb, rival_foreign)) => {
                    foreign.extend(rival_foreign);
                    foreign.sort();
                    foreign.dedup();
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

    // The head lands sorted by period, so a reader's window prunes
    // the file's row groups; the cells then read in one order
    // wherever they are served from.
    cells.sort_by(|a, b| {
        a.period
            .cmp(&b.period)
            .then_with(|| a.dimension.cmp(&b.dimension))
            .then_with(|| a.member.cmp(&b.member))
    });
    let cells = cells_batch(&slot.aspect, &cells);
    // What the data holds at the resolution and the window leaves out:
    // the periods the frame spans less the periods the total series
    // serves inside the metric's own window — counted over the head,
    // which holds every bucket the window could keep.
    let inside = periods_inside(&cells, resolution, since_own.as_deref()).await?;
    let outside = (spanned - inside).max(0);

    Ok(Cube {
        fact: Fact {
            metric: metric.to_string(),
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

/// A context over one cube's cells as the table `h` — the cached
/// batch mounted as a `MemTable`, copied into nothing; every grain
/// and every count over a head is a plan on it.
fn over_cells(cells: &RecordBatch) -> Result<SessionContext, Abstain> {
    let ctx = SessionContext::new();
    let table =
        datafusion::datasource::MemTable::try_new(cells.schema(), vec![vec![cells.clone()]])
            .map_err(|e| Abstain(e.to_string()))?;
    ctx.register_table("h", Arc::new(table))
        .map_err(|e| Abstain(e.to_string()))?;
    Ok(ctx)
}

/// The periods of a head's total series at a resolution, after
/// `since` — what the metric's own window serves; every period when
/// the window is unbounded.
async fn periods_inside(
    cells: &RecordBatch,
    resolution: Resolution,
    since: Option<&str>,
) -> Result<i64, Abstain> {
    if cells.num_rows() == 0 {
        return Ok(0);
    }
    let ctx = over_cells(cells)?;
    let p = period_expr("period", resolution);
    let w = since.map_or(String::new(), |s| format!(" AND {p} > {s}"));
    let batches = ctx
        .sql(&format!(
            "SELECT count(DISTINCT {p}) AS n FROM h WHERE dimension = ''{w}"
        ))
        .await
        .map_err(|e| Abstain(e.to_string()))?
        .collect()
        .await
        .map_err(|e| Abstain(e.to_string()))?;
    Ok(int_column(&batches, "n")
        .map_err(|e| Abstain(e.to_string()))?
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
/// raw members' own latest; the fact row's `bucketed` names it.
async fn derive(head: &Cube, grain: Resolution, window: Option<&str>) -> Result<Cube, Abstain> {
    let ctx = over_cells(&head.cells)?;
    let sql = |q: String| {
        let ctx = ctx.clone();
        async move {
            ctx.sql(&q)
                .await
                .map_err(|e| Abstain(e.to_string()))?
                .collect()
                .await
                .map_err(|e| Abstain(e.to_string()))
        }
    };
    let p = period_expr("period", grain);
    let since = match window {
        Some(w) if head.cells.num_rows() > 0 => {
            let edge = sql(format!(
                "SELECT {} - INTERVAL '{}' AS since FROM h",
                period_expr("max(period)", grain),
                w.replace('\'', "''")
            ))
            .await?;
            edge.iter()
                .find(|b| b.num_rows() > 0)
                .and_then(|b| {
                    let col = b.column_by_name("since")?;
                    (!col.is_null(0))
                        .then(|| array_value_to_string(col, 0).ok())
                        .flatten()
                })
                .map(|t| format!("TIMESTAMP '{t}'"))
        }
        _ => None,
    };
    let w = since.map_or(String::new(), |s| format!(" WHERE {p} > {s}"));
    let batches = sql(format!(
        "WITH w AS (SELECT * FROM h{w}) \
         SELECT dimension, member, {p} AS period, sum(value) AS value, \
                CAST(NULL AS DOUBLE) AS num, CAST(NULL AS DOUBLE) AS den, behavior \
         FROM w WHERE behavior = 'flow' GROUP BY 1, 2, 3, 7 \
         UNION ALL \
         SELECT dimension, member, {p} AS period, sum(num) / nullif(sum(den), 0) AS value, \
                sum(num) AS num, sum(den) AS den, behavior \
         FROM w WHERE behavior = 'ratio' GROUP BY 1, 2, 3, 7 \
         UNION ALL \
         SELECT dimension, member, period, value, num, den, behavior FROM (\
            SELECT dimension, member, {p} AS period, value, num, den, behavior, \
                   row_number() OVER (PARTITION BY dimension, member, {p} \
                                      ORDER BY period DESC) AS rn \
            FROM w WHERE behavior = 'stock') WHERE rn = 1 \
         ORDER BY period, dimension, member"
    ))
    .await?;
    let mut cells: Vec<Cell> = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let col = |n: &str| -> Result<&ArrayRef, Abstain> {
            b.column_by_name(n)
                .ok_or_else(|| Abstain(format!("the grain served no {n}")))
        };
        let floats = |n: &str| -> Result<Float64Array, Abstain> {
            cast(col(n)?, &DataType::Float64)
                .map_err(|e| Abstain(e.to_string()))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
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
        let (dimension, member, behavior) = (col("dimension")?, col("member")?, col("behavior")?);
        let (value, num, den) = (floats("value")?, floats("num")?, floats("den")?);
        let at = |c: &Float64Array, i: usize| (!c.is_null(i)).then(|| c.value(i));
        for i in 0..b.num_rows() {
            if value.is_null(i) || period.is_null(i) {
                continue;
            }
            let text =
                |c: &ArrayRef| array_value_to_string(c, i).map_err(|e| Abstain(e.to_string()));
            let behavior: &'static str = match text(behavior)?.as_str() {
                "stock" => "stock",
                "ratio" => "ratio",
                _ => "flow",
            };
            cells.push(Cell {
                dimension: text(dimension)?,
                member: text(member)?,
                period: period.value(i),
                value: value.value(i),
                num: at(&num, i),
                den: at(&den, i),
                behavior,
            });
        }
    }
    let mut fact = head.fact.clone();
    fact.resolution = Some(grain);
    fact.window = window.map(str::to_string);
    Ok(Cube {
        cells: cells_batch(&fact.metric, &cells),
        fact,
        version_bound: head.version_bound.clone(),
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
async fn rival_series(
    shared: &Arc<Shared>,
    surface: &Surface,
    sql: &str,
    chosen_verb: &str,
    resolution: Resolution,
    since: Option<&str>,
) -> Result<(Vec<SeriesRow>, &'static str, Vec<String>), Abstain> {
    let Surface {
        ctx,
        dataset,
        judged,
        ..
    } = surface;
    let probe = crate::whatif::build_plan(shared, ctx, sql).await?;
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
    Ok((rows, verb, workspace_reads(&probe)))
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
    let p = period_expr(&qi(tcol), resolution);
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
    let p = period_expr(&qi(tcol), resolution);
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
    let plan = crate::whatif::build_plan(shared, ctx, sql).await?;
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
        // Anything else is refused with the read the caller meant,
        // spelled out: the grain they named, the metric as a filter.
        _ => {
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
            Err(refuse(format!(
                "{}): the one argument is the grain — `SELECT metric, period, value FROM \
                 metric_series(grain => '{grain}'){filter}`; filters ride WHERE. The columns \
                 are metric, dimension, member, period, value, num, den, behavior: the time \
                 column is `period`, the total is `dimension = ''`",
                args.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }
}

/// `metric_series(grain => …)` — the cells of every current grounding:
/// `(metric, dimension, member, period, value, num, den, behavior)`.
/// Without a grain each metric serves its cells at its own resolution
/// over its own rung; with one, at the asked grain over that grain's
/// rung — either a plan over the metric's head, cached beside it. A
/// metric coarser than the asked grain serves no rows — honest
/// absence. `period` is the bucket's start, a typed timestamp. A cache
/// entry is never stale: it is a hit or a miss.
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
            let head = surface.entry(shared, slot).await;
            // An abstained metric has no resolution and no cells.
            let Some(resolution) = head.fact.resolution else {
                continue;
            };
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
    }
    Ok(Served {
        schema: series_schema(),
        partitions,
    })
}

/// `metric_axes()` — one row per current grounding, the record read:
/// `(metric, applicable, judged_current, reason, behavior,
/// behavior_basis, grain, resolution, window, outside, dims, basis,
/// admitted_by, bucketed, unadmitted, unadmitted_why, unadmitted_act, wanted,
/// wanted_over, unserved, alternative, alternative_divergence,
/// alternative_error)`. What the cube
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
/// aspect at dataset grain with no voice here (the bands walk). The
/// same rows `owed` derives in SQL as `never measured`; re-measure
/// runs these.
pub(crate) async fn wanted(shared: &Arc<Shared>) -> Result<Vec<(String, String)>, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
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
        let witnessed = rctx
            .witnesses
            .iter()
            .any(|w| w.aspect == aspect && w.detector.is_some());
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
        ],
    )
    .map_err(SessionError::from)
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
