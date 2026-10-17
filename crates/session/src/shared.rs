//! What every module of the crate shares, and nothing of what any one
//! of them does: the refusal, the outcome, the runtime seam, the state
//! a session holds for its channels, the shape a door serves and the
//! resolution a statement carries into planning. A leaf — it imports
//! no other module — so each of them reads, changes and tests against
//! this one alone, and a module that only refuses, reads state or
//! serves a shape depends on nothing else.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::parser::ParserError;
use glossql_catalog::{Lake, PinnedTable};
use glossql_glossary::{ReadContext, Store};
use serde_json::Value;

// -- the refusal -------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("parse: {0} — parsing precedes execution: nothing ran")]
    Parse(#[from] ParserError),
    #[error(transparent)]
    Store(#[from] glossql_glossary::Error),
    /// The engine's text, whole; a pool refusal carries the road out
    /// ([`memory_road`]).
    #[error("{}", memory_road(.0))]
    DataFusion(#[from] DataFusionError),
    #[error("no dataset in use — USE one first")]
    NoDataset,
    #[error("not a subject: {0}")]
    BadSubject(String),
    /// The planner's `table '…' not found`, carrying what the name could
    /// have been. The engine's text is kept whole in front.
    #[error("{0}")]
    UnknownTable(String),
    /// An unquoted column name that folds past two landed spellings
    /// (SPEC.md §1): the refusal names both.
    #[error("{0}")]
    AmbiguousName(String),
    #[error("unknown function `{0}` — DECLARE it (or check its FOR scope)")]
    UnknownFunction(String),
    #[error("output of `{function}` violates the schema of the aspect it RETURNS: {detail}")]
    OutputRejected { function: String, detail: String },
    #[error(
        "function `{0}` has no RETURNS — a detector runs through its witness, never through extraction"
    )]
    DetectorNotExtractable(String),
    #[error("function runtime: {0}")]
    Runtime(String),
    /// Arrow refused an array operation inside a door — a cast, a
    /// concatenation, a cell's rendering.
    #[error("function runtime: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// The engine refused a door's own plan. `context` is what the
    /// refusal reads as in front of the engine's text — the door's
    /// name, or `not served` — and the engine's error stays the source,
    /// so a pool refusal carries its road out here too.
    #[error("not a subject: {context}{}", memory_road(.source))]
    Engine {
        context: String,
        source: DataFusionError,
    },
    #[error("not an aspect name: `{0}`")]
    BadAspectName(String),
    #[error("the standing ruling slot is not a ruling body")]
    RulingSlotNotRulings,
    #[error("a body carrying `$$` cannot ride the dollar-quoted statement")]
    DollarQuotedBody,
    #[error(transparent)]
    Lake(#[from] glossql_catalog::Error),
    #[error(transparent)]
    Import(#[from] glossql_import::Error),
    #[error(
        "the substrate is not open for {0} — tables come from recipes; removal is DROP TABLE (SPEC.md §3)"
    )]
    SubstrateClosed(String),
    #[error(
        "DROP TABLE {table} refused: {reason} (replacement is postponed — declare under another name)"
    )]
    DropRefused { table: String, reason: String },
    #[error("streaming takes exactly one query — everything else answers through execute")]
    NotOneRead,
    #[error(
        "source `{name}`: `{location}` is a path on the server's own filesystem — on a deployment \
         a source is a location in an object store (s3://…, gs://…, abfss://…) or a relational \
         URI; a path is the laptop's shape, under --workspace"
    )]
    LocalSource { name: String, location: String },
    #[error("statement {index} of {total} refused: {source} — {context}")]
    Sequence {
        index: usize,
        total: usize,
        /// What landed and what never ran — "statements 1–2 landed;
        /// 4–7 not run". Without this, a mid-sequence refusal forces a
        /// read of `imports` just to learn what stood.
        context: String,
        source: Box<SessionError>,
        /// The outcomes of the statements that stood, in order — what
        /// a refusal must not discard: they landed.
        landed: Vec<Outcome>,
    },
}

impl SessionError {
    /// The engine refused a plan a door could not be served without.
    pub(crate) fn not_served(source: DataFusionError) -> Self {
        SessionError::Engine {
            context: "not served: ".into(),
            source,
        }
    }

    /// What a door that abstains per subject says in place of a row:
    /// the refusal's own text, for the two refusals that are about one
    /// subject rather than about the statement. Anything else is the
    /// statement's failure and has none.
    pub(crate) fn abstention(&self) -> Option<String> {
        match self {
            SessionError::BadSubject(reason) => Some(reason.clone()),
            SessionError::Engine { context, source } => {
                Some(format!("{context}{}", memory_road(source)))
            }
            _ => None,
        }
    }

    /// The engine refused `door`'s own plan.
    pub(crate) fn door(door: &str, source: DataFusionError) -> Self {
        SessionError::Engine {
            context: format!("{door}: "),
            source,
        }
    }
}

/// The engine's `Resources exhausted` with the shape that fits: a
/// DISTINCT or a wide GROUP BY keeps every value in memory per
/// partition, and neither the no-GROUP-BY aggregate nor a distinct
/// count's state can spill — the sketch or the two-step count can.
/// Every other engine error passes as it came.
pub(crate) fn memory_road(e: &DataFusionError) -> String {
    match e.find_root() {
        DataFusionError::ResourcesExhausted(_) => format!(
            "{e} — the statement outgrew the engine's memory pool: a `count(DISTINCT …)` or a \
             GROUP BY over a wide column keeps every value in memory per partition; \
             `approx_distinct(col)` sketches the count in fixed memory, or GROUP BY the keys \
             in a CTE and count the rows"
        ),
        _ => e.to_string(),
    }
}

/// What landed and what never ran, for a refusal at `index` (1-based)
/// of `total` — one outcome stands per completed statement, so the
/// caller's outcome count is the base.
pub(crate) fn sequence_context(index: usize, total: usize) -> String {
    let landed = match index {
        1 => "nothing landed before it".to_string(),
        2 => "statement 1 landed".to_string(),
        _ => format!("statements 1–{} landed", index - 1),
    };
    let tail = if index == total {
        String::new()
    } else if index + 1 == total {
        format!("; statement {total} not run")
    } else {
        format!("; statements {}–{} not run", index + 1, total)
    };
    format!("{landed}{tail}")
}

/// What one statement produced. `Rows` for anything that reads, `Affected`
/// for forwarded deletes, `Done` for declarations and writes.
#[derive(Debug)]
pub enum Outcome {
    Done(String),
    /// A read's rows. `truncated` says the read held more than the
    /// session's row cap and `batches` stops there — said by the session
    /// that cut, so no door infers it from a count.
    Rows {
        batches: Vec<RecordBatch>,
        truncated: bool,
    },
    Affected(u64),
}

// -- the runtime seam --------------------------------------------------

/// The service seam (`glossql-scripts`): the native statistical kernels
/// and aggregate registrations behind the doors, and the one table call
/// a service point answers. Function bodies are SQL the engine plans —
/// a measurement over data (§6), a detector over its witness's `slots`
/// — so nothing here evaluates a body; the seam carries only what SQL
/// cannot: the table call and the reconcile discriminator. The table
/// call is async — a service answers it over HTTP — and a runtime says
/// which points it fills, so a door refuses at plan time by the point's
/// name instead of failing in the read. Tests inject fakes.
///
/// A table answered by a service: its rows, and the answer object the
/// service put beside them — its name and version, the items that
/// failed alone (`errors`), what the answer rests on (`assumptions`).
#[derive(Clone, Debug)]
pub struct Answered {
    pub table: RecordBatch,
    pub answer: Value,
}

/// The points a deployment fills with a service, by name. A point's
/// name is the route the service serves and the stem of its variables:
/// `GLOSSQL_<POINT>_URL`, `_TOKEN`, `_AUDIENCE`, the name in capitals.
pub const POINTS: [&str; 3] = [FORECAST, MISFIT, WHATIF];

/// The forecast point: the metric-bands walk sends every metric's
/// monthly series and reads bands, actual and PIT per walked period.
pub const FORECAST: &str = "forecast";

/// The misfit point: `misfit.<frame>()` sends the frame and reads a
/// score per row.
pub const MISFIT: &str = "misfit";

/// The what-if point: the scenario read sends a metric's series with a
/// lever's, as planned and under the scenario, and reads the estimate.
pub const WHATIF: &str = "whatif";

/// The variable that names a point's service.
pub fn point_variable(point: &str, suffix: &str) -> String {
    format!("GLOSSQL_{}_{suffix}", point.to_ascii_uppercase())
}

/// The refusal a door gives when the runtime fills none of its point:
/// by name, with what to set. The server itself never carries a model;
/// a service the deployment names answers the point.
pub fn no_model(door: &str, point: &str) -> SessionError {
    SessionError::BadSubject(format!(
        "not served: {door} needs the {point} service and this server names none — \
         set {} (and {} or {}) to a service that serves /{point}",
        point_variable(point, "URL"),
        point_variable(point, "TOKEN"),
        point_variable(point, "AUDIENCE"),
    ))
}

#[async_trait::async_trait]
pub trait FunctionRuntime: Send + Sync + std::fmt::Debug {
    /// Whether a service stands behind `point` ([`POINTS`]). `false`
    /// refuses the point's doors at plan time ([`no_model`]); the call's
    /// own default below refuses too, for a runtime that lies.
    fn serves(&self, _point: &str) -> bool {
        false
    }

    /// The aggregate statistics the runtime ships (`profile` — the
    /// shape SQL lacks; `mad` and `entropy` ride inside its struct).
    /// Registered on the session's context when the runtime attaches,
    /// so a measurement body and an agent's own SQL name the same
    /// functions.
    fn udafs(&self) -> Vec<datafusion::logical_expr::AggregateUDF> {
        Vec::new()
    }

    /// The table call: `table` and `spec` go to the service behind
    /// `point`, column names and types with the rows; its rows and its
    /// answer object come back. What the table and the specification
    /// hold is the point's contract, read by the door that sends them;
    /// nothing here knows what the numbers mean. A scalar shape — a
    /// value per row, called from a recipe — is a second method here
    /// when its first user lands. The runtime that carries a service
    /// overrides this; the default refuses, and the door reports why.
    async fn table(
        &self,
        point: &str,
        _table: RecordBatch,
        _spec: Value,
    ) -> Result<Answered, String> {
        Err(format!("this runtime fills no {point} point"))
    }

    /// The stock/flow discriminator behind the behavior-evidence door:
    /// conventions — each term and every ordered pair
    /// difference — as one matrix product over the stacked entity
    /// series, residuals and gates per entity. The door aligns the two
    /// sides in the plan and hands one batch of `(e, b, yv,
    /// s_<term>…)` ordered by `(e, b)`, so the kernel reads segments
    /// off it and never pairs rows itself. `n_common` is the entity
    /// intersection, which the alignment cannot answer: an entity on
    /// both sides with no bucket in common carries no cell and still
    /// counts. Returns `{n_common, summaries}` as the script kernel
    /// did. The runtime that carries the kernel overrides this; the
    /// default refuses.
    fn reconcile(
        &self,
        _aligned: &[RecordBatch],
        _n_common: i64,
        _terms: &[String],
    ) -> Result<Value, String> {
        Err("this runtime carries no reconcile kernel".into())
    }
}

#[derive(Debug)]
pub struct NoRuntime;

#[async_trait::async_trait]
impl FunctionRuntime for NoRuntime {}

// -- the state ---------------------------------------------------------

/// State the planner shares with the router: the `USE`'d dataset, the data
/// plane, and the script runtime (reads run detectors).
pub(crate) struct Shared {
    pub store: Store,
    pub dataset: RwLock<Option<String>>,
    pub runtime: RwLock<Arc<dyn FunctionRuntime>>,
    /// The session's own context, set right after construction (the planner
    /// is built before the context exists). The metric bind plans each
    /// grounding through it as its own statement — `statement_to_plan`
    /// collects table references per statement, so a grounding's tables
    /// resolve even when the outer statement never names them.
    pub ctx: RwLock<Option<SessionContext>>,
    /// The cube cache — the Plane's, shared by every channel, or the
    /// session's own when it was built without one. A query result,
    /// never the record: it lives beside the store, not in it. Held
    /// untyped, since this module names no other; the cube reads it
    /// back as its own (`Shared::cube`, in the cube).
    pub cube: RwLock<Arc<dyn std::any::Any + Send + Sync>>,
    /// The catalog walk, held for the statement that asked for it and
    /// dropped when the next one begins ([`Session::execute_statements`]).
    ///
    /// A statement asks the same question of the catalog many times over:
    /// every plan built under it re-resolves the dataset's tables, and a
    /// cube read builds a plan per metric and per admitted dimension. The
    /// answer cannot change underneath it — no statement lands into the
    /// dataset and then reads it back, so the walk that served the first
    /// plan serves the last one.
    ///
    /// Keyed by dataset because a read can name one the session is not
    /// bound to (`GLOSSARY(fin, …)`).
    pub pins: RwLock<std::collections::HashMap<String, Arc<Vec<PinnedTable>>>>,
    /// The tables a statement loaded by name, beside the walk. A plan
    /// that names its tables takes them from the walk when one stands,
    /// from here otherwise, and loads from the catalog only what
    /// neither holds — a cube build plans one query per series, and
    /// each used to load its tables again. Dropped with the walk.
    pub named: RwLock<std::collections::HashMap<String, Named>>,
    /// The read context per dataset, held for the statement: built once
    /// and served to every arm and every cube build under it. Beside it
    /// the store's write count it was built at — a write of this store
    /// moves the count, and the entry is rebuilt. Dropped with the walk.
    pub contexts: RwLock<std::collections::HashMap<String, (u64, ReadContext)>>,
    /// The shipped reads served from memory — the Plane's, shared by
    /// every channel, or the session's own when built without one.
    /// Held untyped as the cube cache is (`Shared::shipped`, in memo).
    pub shipped: RwLock<Arc<dyn std::any::Any + Send + Sync>>,
    /// Whether this session's planner folds unquoted identifiers —
    /// `datafusion.sql_parser.enable_ident_normalization`, read off the
    /// config the session was built with. See [`Shared::idents`].
    pub normalize_idents: bool,
    /// The pages the door serves — the skills and their references,
    /// the docs, the language, the engine's SQL guide — as the rows of
    /// `pages()`. The Plane hands them in (the binary embeds them); a
    /// session built without one serves none.
    pub pages: RwLock<Arc<[DoorPage]>>,
    /// Whether a source may be a path on this machine — the laptop's
    /// shape. A deployment has no directory and refuses one: its
    /// filesystem is the process's own, the environment among it.
    pub local_sources: std::sync::atomic::AtomicBool,
}

/// One dataset's tables loaded by name, each as its pinned provider.
pub(crate) type Named =
    std::collections::HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>;

/// One page the door serves. `pages()` serves the resource URI, the
/// title and the body verbatim; a resource listing serves the URI
/// under `name`, with `description` saying when the page is worth
/// reading, and `mime` with the body.
#[derive(Debug, Clone)]
pub struct DoorPage {
    pub uri: String,
    pub name: String,
    pub title: String,
    pub description: String,
    pub mime: &'static str,
    pub body: String,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SessionContext is not Debug; the dataset is the identifying bit.
        f.debug_struct("Shared")
            .field("dataset", &self.dataset)
            .finish_non_exhaustive()
    }
}

impl Shared {
    /// How this session reads an identifier: quoted stays exact,
    /// unquoted folds with ASCII case, and the session's
    /// `enable_ident_normalization` decides whether the fold happens at
    /// all (`datafusion-sql` planner.rs, `IdentNormalizer`).
    ///
    /// DataFusion's own normalizer rather than the same rule written out
    /// here. Every seam in this crate that claims a name by matching it
    /// has to read that name the way the planner will: a seam that folds
    /// differently either claims a name the planner would have given to
    /// something else, or fails to claim one and lets the default path
    /// serve it — and the second is silent, because the default path
    /// answers with plausible rows.
    pub fn idents(&self) -> IdentNormalizer {
        IdentNormalizer::new(self.normalize_idents)
    }

    pub fn lake(&self) -> Lake {
        self.store.lake()
    }

    /// The dataset's tables from the catalog, walked once for the
    /// statement and served from [`Shared::pins`] after that.
    ///
    /// Two callers racing an empty entry both walk and both store. That
    /// is the arrangement [`glossql_glossary::Store`] already makes for
    /// the store's own head: a duplicate round trip is cheaper than an
    /// async-aware lock, and within one statement the two answers agree.
    pub(crate) async fn pinned(
        &self,
        dataset: &str,
    ) -> Result<Arc<Vec<PinnedTable>>, SessionError> {
        if let Some(held) = self.pins.read().expect("pins lock").get(dataset) {
            return Ok(Arc::clone(held));
        }
        let walked = Arc::new(self.lake().pin_dataset(dataset).await?);
        self.pins
            .write()
            .expect("pins lock")
            .insert(dataset.to_string(), Arc::clone(&walked));
        Ok(walked)
    }

    /// Everything held from the catalog, dropped so the next statement
    /// walks again. A landing one statement commits is visible to the
    /// next, which is the whole of what this has to guarantee.
    /// The bound dataset's groundings as the catalog lists them — the
    /// views under the metrics' names, read off the mount, which a
    /// grounding write rebuilds. Empty while nothing is bound.
    /// The bound dataset's groundings as the catalog lists them: each
    /// view's name to its definition, at the mount's generation.
    pub(crate) async fn views(
        &self,
    ) -> Result<std::collections::HashMap<String, String>, SessionError> {
        let Some(dataset) = self.dataset.read().expect("state lock").clone() else {
            return Ok(Default::default());
        };
        Ok(self.lake().provider().await?.views(&dataset))
    }

    pub fn forget_pins(&self) {
        self.pins.write().expect("pins lock").clear();
        self.named.write().expect("named lock").clear();
        self.contexts.write().expect("contexts lock").clear();
    }

    /// The bound dataset's tables, each pinned at its current snapshot —
    /// resolved once per statement, so two scans can never straddle a
    /// landing.
    pub async fn statement_pins(
        &self,
    ) -> Result<
        std::collections::HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>,
        SessionError,
    > {
        let Some(dataset) = self.dataset.read().expect("state lock").clone() else {
            return Ok(Default::default());
        };
        Ok(self
            .pinned(&dataset)
            .await?
            .iter()
            .map(|p| (p.name.clone(), Arc::clone(&p.provider)))
            .collect())
    }

    pub fn runtime(&self) -> Arc<dyn FunctionRuntime> {
        Arc::clone(&self.runtime.read().expect("runtime lock"))
    }

    /// The process's engine runtime — the one the session's own context
    /// is built on, handed to the scratch context a recipe or a probe
    /// runs in so its plan answers to the same memory pool and spill
    /// space.
    pub fn env(&self) -> Arc<RuntimeEnv> {
        self.session_ctx().runtime_env()
    }

    /// A scratch context on the plane's pool and spill space, with the
    /// session's configuration and none of its catalog: what a plan
    /// over a batch the session already holds — a cube's cells, a
    /// service's answer — runs through.
    pub fn scratch_ctx(&self) -> SessionContext {
        let session = self.session_ctx();
        SessionContext::new_with_config_rt(session.copied_config(), session.runtime_env())
    }

    /// The session's own context — set right after construction, so
    /// absence is a construction bug, never a runtime state.
    pub fn session_ctx(&self) -> SessionContext {
        self.ctx
            .read()
            .expect("ctx lock")
            .clone()
            .expect("the session context is set at construction")
    }

    /// What the store cannot know (SPEC.md §5.3): the subjects that exist —
    /// the recipe tables and their columns — and each table's current
    /// snapshot. Read from the catalog every time, so a landing is
    /// visible the moment it committed; the disclosure grid and the
    /// staleness comparison ride on this. What the *store* knows is
    /// held for us by [`Store::read_context`], keyed by its version.
    pub async fn read_context(&self) -> Result<ReadContext, SessionError> {
        let Some(dataset) = self.dataset.read().expect("state lock").clone() else {
            return Ok(ReadContext::default());
        };
        self.read_context_for(&dataset).await
    }

    /// The same, for a read that resolved its own dataset from an
    /// argument — `GLOSSARY(fin, …)` names `fin` whether or not a `USE`
    /// bound it. Taking the binding there would answer an empty context,
    /// and an empty context is not an error: it reads as "nothing is
    /// glossed" rather than "you are not where you think you are". The
    /// binding decides what an *unqualified* name means; a qualified one
    /// decides for itself.
    pub async fn read_context_for(&self, dataset: &str) -> Result<ReadContext, SessionError> {
        // Held for the statement at the store's write count: a write of
        // this store moves the count and the next ask rebuilds.
        let writes = self.store.writes();
        let held = self
            .contexts
            .read()
            .expect("contexts lock")
            .get(dataset)
            .filter(|(at, _)| *at == writes)
            .map(|(_, ctx)| ctx.clone());
        if let Some(ctx) = held {
            return Ok(ctx);
        }
        let ctx = self.build_context(dataset).await?;
        self.contexts
            .write()
            .expect("contexts lock")
            .insert(dataset.to_string(), (writes, ctx.clone()));
        Ok(ctx)
    }

    /// The context, built: the walk's subjects and snapshots, then the
    /// store's relations at their version.
    async fn build_context(&self, dataset: &str) -> Result<ReadContext, SessionError> {
        // Through the pin walk, which is the one walk over a dataset's
        // catalog: the snapshot and the columns are two fields of the
        // metadata it already loads. Asked separately they cost two more
        // loads of the same file per table. The providers it also builds
        // are dropped here — they are an Arrow schema conversion over
        // metadata in hand, no IO, which is why one function serves both
        // callers rather than two walking the same catalog.
        let mut universe = Vec::new();
        let mut snapshots = std::collections::HashMap::new();
        let mut shapes = std::collections::HashMap::new();
        for pinned in self.pinned(dataset).await?.iter() {
            // Only `Some`: a table nothing has landed into is a subject
            // that can be glossed but contributes no part to the pin
            // ([`glossql_glossary::Store::pin`]). Letting the `None`
            // through would rewrite every pin text in the workspace and
            // miss every measurement stored under the old ones.
            if let Some(snapshot) = pinned.snapshot_id {
                snapshots.insert(pinned.name.clone(), snapshot);
            }
            shapes.insert(pinned.name.clone(), pinned.shape.clone());
            for column in &pinned.columns {
                universe.push(format!("{}.{column}", pinned.name));
            }
            universe.push(pinned.name.clone());
        }
        Ok(self
            .store
            .read_context(dataset, universe, snapshots, shapes)
            .await?)
    }
}

/// What a compute door serves: a table, as the batches that make it up.
/// The planner mounts them as one `MemTable` partition, so the batches
/// are read in this order and none is copied into another — a door
/// that already holds its rows in memory (the cube) hands them over as
/// they are. A door with one batch is the one-batch case of this.
#[derive(Clone, Debug)]
pub(crate) struct Served {
    pub schema: SchemaRef,
    pub partitions: Vec<RecordBatch>,
}

impl From<RecordBatch> for Served {
    fn from(batch: RecordBatch) -> Self {
        Served {
            schema: batch.schema(),
            partitions: vec![batch],
        }
    }
}

// -- the resolution ----------------------------------------------------

/// What a statement resolved before planning: the bound dataset's tables
/// pinned at one snapshot each, every SQL-bodied door as a plan, and
/// every compute door as a batch. Immutable once built and handed to a
/// planner that only reads it — planning fetches nothing, computes
/// nothing, and never re-enters.
#[derive(Debug, Default, Clone)]
pub(crate) struct Resolved {
    pub(crate) plans: HashMap<String, Arc<LogicalPlan>>,
    pub(crate) pins: HashMap<String, Arc<dyn TableProvider>>,
    pub(crate) batches: HashMap<String, Served>,
    pub(crate) ctes: HashSet<String>,
    /// Whether anything this statement resolved — at any expansion
    /// depth — reads the glossary: the relation itself, the GLOSSARY/
    /// ATTEST doors, a served grounding, or a compute door over slots.
    /// Derived here because resolution is the one place every name
    /// passes through; nothing curates a list of frames. The app door
    /// serves it as the frame class (`record`/`data`): a `record`
    /// frame can change under a glossary write
    /// (a ruling), a `data` frame provably cannot.
    pub(crate) record: bool,
    /// What resolution read beyond the plans' own scans — compute
    /// doors and store relations, accumulated for the measurement
    /// record's `reads` legs (the currency rule). Derived here for the
    /// same reason as `record`; the data tables a body scans directly
    /// come from the executed plan instead.
    pub(crate) reads: ReadSet,
    /// The bound dataset's groundings as the catalog lists them: each
    /// view under its metric's name, with its definition. A bare or
    /// dataset-qualified name among them is the grounding; a shipped
    /// read's name and a CTE's stand over it.
    pub(crate) views: HashMap<String, String>,
    /// The groundings this statement names, each planned at its pins
    /// and held as a `ViewTable` over that plan — served under the
    /// metric's name as a pinned table is, and by `read.<name>()`.
    pub(crate) served: HashMap<String, Arc<dyn TableProvider>>,
}

/// What a statement's resolution read, named against the pin's legs:
/// data tables, store relations, or the whole dataset when a door
/// sweeps it. `everything` is the conservative floor — a door whose
/// reads cannot be enumerated records nothing, and the measurement
/// stands only at its exact pin.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReadSet {
    pub(crate) tables: HashSet<String>,
    pub(crate) relations: HashSet<String>,
    pub(crate) all_tables: bool,
    pub(crate) everything: bool,
}

impl ReadSet {
    /// Everything `other` read, added to this set.
    pub(crate) fn extend(&mut self, other: &ReadSet) {
        self.tables.extend(other.tables.iter().cloned());
        self.relations.extend(other.relations.iter().cloned());
        self.all_tables |= other.all_tables;
        self.everything |= other.everything;
    }
}

impl Resolved {
    /// A resolution holding only the given pins — what a door that
    /// names its own dataset reads, the statement's binding aside.
    pub(crate) fn over(pins: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        Self {
            pins,
            ..Self::default()
        }
    }

    pub(crate) fn plan(&self, key: &str) -> Option<Arc<LogicalPlan>> {
        self.plans.get(key).cloned()
    }

    pub(crate) fn pin(&self, table: &str) -> Option<Arc<dyn TableProvider>> {
        self.pins.get(table).cloned()
    }

    /// The pinned tables' names — the bound dataset's tables as this
    /// statement sees them.
    pub(crate) fn tables(&self) -> Vec<String> {
        self.pins.keys().cloned().collect()
    }

    pub(crate) fn batch(&self, key: &str) -> Option<&Served> {
        self.batches.get(key)
    }

    /// The grounding of that name as this statement serves it — its
    /// view over the definition planned at the statement's pins — once
    /// resolved; `None` for a name that is no grounding of the bound
    /// dataset.
    pub(crate) fn view(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.served.get(name).cloned()
    }

    /// Whether this factor is a name the statement binds as a CTE — see
    /// [`shadowed`].
    pub(crate) fn shadowed(&self, idents: &IdentNormalizer, f: &TableFactor) -> bool {
        shadowed(idents, &self.ctes, f)
    }

    /// Whether the statement reads the glossary anywhere in its
    /// expansion — the frame class, derived during resolution.
    pub(crate) fn touches_record(&self) -> bool {
        self.record
    }
}

/// Whether the factor is a bare reference to one of the statement's CTE
/// names — the planner leaves it alone so DataFusion's CTE lookup serves
/// it. A shipped read's name is never among them: the pre-pass keeps
/// those reserved over both tables and CTEs.
fn shadowed(idents: &IdentNormalizer, ctes: &HashSet<String>, f: &TableFactor) -> bool {
    let TableFactor::Table {
        name, args: None, ..
    } = f
    else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    let Some(ident) = part.as_ident() else {
        return false;
    };
    let name = idents.normalize(ident.clone());
    ctes.contains(&name)
}
