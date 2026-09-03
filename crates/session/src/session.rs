//! `Session`: the per-connection statement router.

use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion::common::{Column, DataFusionError, ParamValues};
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    FromTable, ObjectType, Statement as SQLStatement, TableFactor, visit_relations,
};
use datafusion::sql::sqlparser::parser::ParserError;
use futures::StreamExt as _;
use serde_json::Value;

use glossql_catalog::{IcebergCatalogProvider, Lake};
use glossql_glossary::{Actor, RecipeAdmission, Store, schemas};
use glossql_import::SourceSpec;
use glossql_parser::{
    Declaration, Extract, Gloss, GlossqlParser, Probe, RelOp, SettingValue, Statement, Subject,
};

use crate::reads::{GlossqlReads, Shared};
use crate::subject::{Resolved, pair_subject, resolve_endpoint, resolve_path};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("parse: {0} — parsing precedes execution: nothing ran")]
    Parse(#[from] ParserError),
    #[error(transparent)]
    Store(#[from] glossql_glossary::Error),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("no dataset in use — USE one first")]
    NoDataset,
    #[error("not a subject: {0}")]
    BadSubject(String),
    /// The planner's `table '…' not found`, carrying what the name could
    /// have been. The engine's text is kept whole in front.
    #[error("{0}")]
    UnknownTable(String),
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
    Rows(Vec<RecordBatch>),
    Affected(u64),
}

/// The kernel seam (`glossql-scripts`): the native statistical kernels
/// and aggregate registrations behind the doors. Function bodies are
/// SQL the engine plans — a measurement over data (§6), a detector over
/// its witness's `slots` — so nothing here evaluates a body; the seam
/// carries only what SQL cannot: the model forwards and the reconcile
/// discriminator. Tests inject fakes.
/// A row-major matrix borrowed from the caller: `rows × cols` values,
/// nulls as NaN where a kernel admits them.
#[derive(Clone, Copy, Debug)]
pub struct Matrix<'a> {
    pub data: &'a [f64],
    pub rows: usize,
    pub cols: usize,
}

pub trait FunctionRuntime: Send + Sync + std::fmt::Debug {
    /// The aggregate statistics the runtime ships (`profile` — the
    /// shape SQL lacks; `mad` and `entropy` ride inside its struct).
    /// Registered on the session's context when the runtime attaches,
    /// so a measurement body and an agent's own SQL name the same
    /// functions.
    fn udafs(&self) -> Vec<datafusion::logical_expr::AggregateUDF> {
        Vec::new()
    }

    /// The band kernel behind the `whatif.` door:
    /// fit one estimator on the replayed worlds, quantiles at `alphas`
    /// for every test row. `train` and `test` share their columns; the
    /// return is row-major (test rows × alphas). The runtime that
    /// carries the model overrides this (the ensemble — sparse replay
    /// grids are the regime it was ruled in for); the default refuses,
    /// and the door reports why.
    fn band_grid(
        &self,
        _train: Matrix<'_>,
        _train_y: &[f64],
        _test: Matrix<'_>,
        _alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        Err("this runtime carries no band kernel".into())
    }

    /// The misfit kernel behind the `misfit.` door (fixture 20): fit
    /// on the frame, score the same frame — one mean
    /// log density per row, log space end to end (higher = fits the
    /// frame better). Nulls ride as NaN. The runtime that carries the
    /// model overrides this; the default refuses, and the door reports
    /// why.
    fn misfit_scores(&self, _x: Matrix<'_>) -> Result<Vec<f64>, String> {
        Err("this runtime carries no misfit kernel".into())
    }

    /// The stock/flow discriminator behind the behavior-evidence door
    /// (stage 5): alignment by hash join on typed keys, conventions —
    /// each term and every ordered pair difference — as one matrix
    /// product over the stacked entity series, residuals and gates per
    /// entity. `y` rows are `(e, b, yv)`, `m` rows `(e, b, s_<term>…)`,
    /// both ordered by (e, b). Returns `{n_common, summaries}` as the
    /// script kernel did. The runtime that carries the kernel overrides
    /// this; the default refuses.
    fn reconcile(
        &self,
        _y: &[RecordBatch],
        _m: &[RecordBatch],
        _terms: &[String],
    ) -> Result<Value, String> {
        Err("this runtime carries no reconcile kernel".into())
    }

    /// One TabICL fit and read, behind the metric-bands walk (stage 5):
    /// train on `train`, predict the one test row `test_x` (its width),
    /// return the band value per alpha in order and the PIT — the
    /// quantile at which `actual` lands in the predicted distribution.
    /// The runtime that carries the model overrides this; the default
    /// refuses, and the door reports why.
    fn band_point(
        &self,
        _train: Matrix<'_>,
        _train_y: &[f64],
        _test_x: &[f64],
        _alphas: &[f64],
        _actual: f64,
    ) -> Result<(Vec<f64>, f64), String> {
        Err("this runtime carries no band kernel".into())
    }
}

#[derive(Debug)]
pub struct NoRuntime;

impl FunctionRuntime for NoRuntime {}

/// One substrate statement out of a SQL string — the internal reads
/// (the drop-table emptiness probe) plan through the same pre-pass as
/// everything else.
fn one_query(sql: &str) -> Result<DFStatement, SessionError> {
    let mut statements = GlossqlParser::parse_sql(sql)?;
    match statements.pop() {
        Some(Statement::Substrate(statement)) => Ok(*statement),
        _ => Err(SessionError::NotOneRead),
    }
}

/// The text as the one substrate query a read is, or
/// [`SessionError::NotOneRead`]: anything else — a sequence, a
/// statement of the language, a query that is not a `SELECT` —
/// belongs in [`Session::execute`].
fn one_read(sql: &str) -> Result<DFStatement, SessionError> {
    let mut statements = GlossqlParser::parse_sql(sql)?;
    let one_query = matches!(&statements[..], [Statement::Substrate(statement)]
        if matches!(&**statement, DFStatement::Statement(inner)
            if matches!(inner.as_ref(), SQLStatement::Query(_))));
    if !one_query {
        return Err(SessionError::NotOneRead);
    }
    match statements.pop() {
        Some(Statement::Substrate(statement)) => Ok(*statement),
        _ => unreachable!("just matched"),
    }
}

/// A script reads; it never writes. `SessionContext::sql` permits DDL, DML
/// and statements by default (datafusion context/mod.rs:614), which would
/// hand any declared function a door around the statement allowlist.
pub(crate) fn read_only() -> datafusion::prelude::SQLOptions {
    datafusion::prelude::SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

pub struct Session {
    ctx: SessionContext,
    shared: Arc<Shared>,
    actor: Actor,
    /// How many rows the reader will actually be shown. It bounds what the
    /// non-streaming paths ask the engine for; `usize::MAX` (the default)
    /// means the caller drains everything itself.
    row_cap: usize,
    /// Which generation of the lake's shared provider this session
    /// mounted — `mount_schema` re-registers when it changed.
    mounted_provider: std::sync::Mutex<Option<std::sync::Weak<IcebergCatalogProvider>>>,
}

impl Session {
    /// Who this session writes as.
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    /// A session on a runtime of its own — an unbounded memory pool, the
    /// OS temp directory, and a fresh set of file caches. It is what a
    /// test wants and what no server should run: the budget a runtime
    /// carries is only a budget if more than one session answers to it,
    /// and a server builds a session per call.
    pub fn new(store: Store, actor: Actor) -> Result<Self, SessionError> {
        Self::on_runtime(store, actor, Arc::new(RuntimeEnv::default()))
    }

    /// A session on a given execution runtime — the memory pool, the disk
    /// manager and the file caches every plan under it answers to.
    /// [`Plane`](crate::Plane) builds one at boot and hands it to every
    /// channel, which is what makes `--memory-limit` a ceiling on the
    /// process rather than on each call.
    pub fn on_runtime(
        store: Store,
        actor: Actor,
        env: Arc<RuntimeEnv>,
    ) -> Result<Self, SessionError> {
        let config = SessionConfig::new()
            .set_str("datafusion.sql_parser.dialect", "postgres")
            // Iceberg's arrow fields carry `PARQUET:field_id` metadata; any
            // expression derived from them (a cast, a common subexpression)
            // drops it logically but not physically, and the aggregate
            // schema check trips on the difference. The knob exists for
            // exactly this (datafusion-common config.rs:532).
            .set_bool(
                "datafusion.execution.skip_physical_aggregate_schema_check",
                true,
            );
        // Built after the config and from it: the seams in `reads` and
        // `prepass` match names ahead of the planner, so they have to fold
        // the way this config says the planner will.
        let shared = Arc::new(Shared {
            store,
            dataset: RwLock::new(None),
            runtime: RwLock::new(Arc::new(NoRuntime)),
            ctx: RwLock::new(None),
            cube: RwLock::new(crate::cube::CubeCache::new(
                crate::cube::DEFAULT_CUBE_CACHE_MB,
            )),
            pins: RwLock::new(Default::default()),
            normalize_idents: config.options().sql_parser.enable_ident_normalization,
        });
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            // Without this the builder makes its own (session_state.rs
            // `build`), and since a channel is built per call that would
            // be one unbounded memory pool per call — a limit set there
            // would bound nothing.
            .with_runtime_env(env)
            // The base planner resolves nothing: every statement builds its
            // own state carrying its own pre-pass result (`reads::state_with`).
            // This one only serves paths that name no SQL-bodied door.
            .with_relation_planners(vec![Arc::new(GlossqlReads {
                shared: Arc::clone(&shared),
                resolved: Arc::new(Default::default()),
            })])
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        datafusion_functions_json::register_all(&mut ctx)?;
        glossql_import::casts::register_try_functions(&ctx);
        // The staging ground for materializations: a session-local memory
        // schema, kept apart from the bound dataset's schema — a bare
        // registration there would create a real table in the lake.
        ctx.catalog("datafusion")
            .expect("default catalog")
            .register_schema("glossql_stage", Arc::new(MemorySchemaProvider::new()))?;
        // The planner was built before the context existed; close the loop
        // so the metric bind can plan groundings as their own statements.
        *shared.ctx.write().expect("ctx lock") = Some(ctx.clone());
        Ok(Session {
            ctx,
            shared,
            actor,
            row_cap: usize::MAX,
            mounted_provider: std::sync::Mutex::new(None),
        })
    }

    /// The door's row cap, so the engine is not asked for rows nobody will
    /// see (probes and statement sequences answer through `execute`).
    pub fn with_row_cap(mut self, cap: usize) -> Self {
        self.row_cap = cap;
        self
    }

    pub fn with_runtime(self, runtime: Arc<dyn FunctionRuntime>) -> Self {
        for udaf in runtime.udafs() {
            self.ctx.register_udaf(udaf);
        }
        *self.shared.runtime.write().expect("runtime lock") = runtime;
        self
    }

    /// The cube cache this session reads and fills — the Plane's, so
    /// every channel shares one set of entries. A session built
    /// without a Plane carries its own from construction.
    pub fn with_cube_cache(self, cache: crate::cube::CubeCache) -> Self {
        *self.shared.cube.write().expect("cube lock") = cache;
        self
    }

    /// Re-measure: re-run every measurement whose newest landing no
    /// longer stands — a leg it read has moved — each as the
    /// extraction an agent would run, `SELECT f() FROM subject`, built
    /// from the measurement row (the AST directly, never statement
    /// text). A voice served and marked (SPEC.md §7) is current again;
    /// each landing moves the version, and the next read — the
    /// witnesses, the cube — rebuilds on the new rows. A function no
    /// longer declared, or a subject the path grammar cannot name (a
    /// relationship), cannot re-run and stays marked. Every runnable
    /// extraction runs; the ones the engine refused are reported
    /// together afterwards. Returns how many landed.
    pub async fn remeasure(&self) -> Result<usize, SessionError> {
        use datafusion::sql::sqlparser::ast::Ident;
        let dataset = self.dataset().ok_or(SessionError::NoDataset)?;
        let ctx = self.shared.read_context().await?;
        let mut stale: Vec<(String, Vec<String>)> = ctx
            .measurements
            .iter()
            .filter(|r| {
                r.get(0) == Some(dataset.as_str())
                    && !glossql_glossary::measurement_stands(
                        &ctx.pin.text,
                        &dataset,
                        r.get(4),
                        r.get(7),
                    )
            })
            .filter_map(|r| {
                let (function, subject) = (r.get(1)?, r.get(2)?);
                let declared = ctx.functions.iter().any(|f| {
                    f.name == function
                        && f.returns.is_some()
                        && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
                });
                if !declared || subject.contains(' ') || subject.contains('(') {
                    return None;
                }
                Some((
                    function.to_string(),
                    subject.split('.').map(str::to_string).collect(),
                ))
            })
            .collect();
        stale.sort();
        let mut ran = 0;
        let mut refused = Vec::new();
        for (function, segments) in stale {
            let extract = Extract {
                calls: vec![Ident::new(function.clone())],
                subject: Subject::Path(glossql_parser::Path {
                    segments: segments.iter().map(|s| Ident::new(s.clone())).collect(),
                }),
            };
            match self.extract(extract).await {
                Ok(_) => ran += 1,
                Err(e) => refused.push(format!("{function}() over {}: {e}", segments.join("."))),
            }
        }
        if refused.is_empty() {
            Ok(ran)
        } else {
            Err(SessionError::Runtime(format!(
                "re-measure landed {ran} and was refused on {}",
                refused.join("; ")
            )))
        }
    }

    fn lake(&self) -> Lake {
        self.shared.lake()
    }

    /// A fixture landing for tests: the same storage path a recipe takes,
    /// without the recipe — the table is created in the bound dataset's
    /// namespace and the rows commit as one snapshot.
    pub async fn register_table(
        &self,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), SessionError> {
        let dataset = self.dataset().ok_or(SessionError::NoDataset)?;
        let schema = provider.schema();
        let batches = self.ctx.read_table(provider)?.collect().await?;
        self.land(&dataset, name, schema, &batches, Default::default())
            .await
    }

    /// One landing: the table created through the mounted schema (the
    /// framework's front door), the batches committed through iceberg-rust
    /// so `facts` ride the snapshot — DataFusion's INSERT cannot carry
    /// them, and facts about a write ride the write.
    async fn land(
        &self,
        dataset: &str,
        table: &str,
        schema: Arc<datafusion::arrow::datatypes::Schema>,
        batches: &[RecordBatch],
        facts: std::collections::HashMap<String, String>,
    ) -> Result<(), SessionError> {
        let lake = self.lake();
        lake.ensure_namespace(dataset, Default::default()).await?;
        let mounted = self.mount_schema(dataset).await?;
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let shape = MemTable::try_new(schema, vec![vec![empty]])?;
        mounted.register_table(table.to_string(), Arc::new(shape))?;
        lake.append_batches(dataset, table, batches, facts).await?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<Outcome>, SessionError> {
        self.execute_statements(GlossqlParser::parse_sql(sql)?)
            .await
    }

    /// The statement loop over parsed statements — the plane's channel
    /// router feeds it the runs between `USE`s. Each statement is a
    /// span under the call's: its place, its kind, the dataset it ran
    /// on.
    pub(crate) async fn execute_statements(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<Outcome>, SessionError> {
        self.refresh_mount().await?;
        let total = statements.len();
        let mut outcomes = Vec::with_capacity(total);
        for (idx, statement) in statements.into_iter().enumerate() {
            // The catalog walk is the statement's, never the sequence's:
            // a `DECLARE RECIPE` that lands is followed by statements
            // that must see what it landed.
            self.shared.forget_pins();
            let span = tracing::info_span!(
                "statement",
                index = idx + 1,
                total,
                kind = kind(&statement),
                dataset = %self.dataset().unwrap_or_default(),
            );
            let run = async {
                match statement {
                    Statement::Declare(d) => self.declare(*d).await,
                    Statement::Use(u) => self.use_dataset(&u.dataset.value).await,
                    Statement::Gloss(g) => self.gloss(g).await,
                    Statement::Extract(e) => self.extract(e).await,
                    Statement::Probe(p) => self.probe(p).await,
                    Statement::Substrate(s) => self.substrate(*s).await,
                }
            };
            let result = tracing::Instrument::instrument(run, span).await;
            match result {
                Ok(outcome) => outcomes.push(outcome),
                // A refusal in a sequence names its place, what landed
                // before it, and what never ran — the statements after
                // it are skipped, never attempted.
                Err(e) if total > 1 => {
                    return Err(SessionError::Sequence {
                        index: idx + 1,
                        total,
                        context: sequence_context(idx + 1, total),
                        source: Box::new(e),
                        landed: std::mem::take(&mut outcomes),
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(outcomes)
    }

    async fn declare(&self, declaration: Declaration) -> Result<Outcome, SessionError> {
        let store = &self.shared.store;
        let done = match &declaration {
            Declaration::Source(d) => {
                // SPEC.md §3 rules the `type` vocabulary; a spelling
                // outside it is refused here, where the typo is, rather
                // than at the first recipe against the source.
                let spelled = d
                    .settings
                    .iter()
                    .find(|s| s.key.value == "type")
                    .map(|s| match &s.value {
                        SettingValue::Name(n) => n.value.clone(),
                        SettingValue::String(t) | SettingValue::Number(t) => t.clone(),
                    });
                if spelled
                    .as_deref()
                    .and_then(glossql_import::SourceKind::parse)
                    .is_none()
                {
                    let types = glossql_import::SOURCE_TYPES;
                    return Err(glossql_import::Error::BadSource {
                        name: d.name.value.clone(),
                        detail: match spelled {
                            Some(t) => format!("unknown type `{t}` — one of {types}"),
                            None => format!("missing `type` — one of {types}"),
                        },
                    }
                    .into());
                }
                store.declare_source(d).await?;
                format!("DECLARE SOURCE {}", d.name.value)
            }
            Declaration::Dataset(d) => {
                store.declare_dataset(d).await?;
                self.mount_schema(&d.name.value).await?;
                format!("DECLARE DATASET {}", d.name.value)
            }
            Declaration::Recipe(d) => {
                let admission = store.recipe_admission(d).await?;
                let (dataset, table) = (d.dataset.value.as_str(), d.table.value.as_str());
                let lake = self.lake();
                if admission == RecipeAdmission::Unchanged
                    && lake.table_exists(dataset, table).await?
                {
                    format!("DECLARE RECIPE {table} ON {dataset} (unchanged)")
                } else {
                    // Supersede-and-reland: a changed
                    // recipe drops the old landing and its
                    // evidence, then lands fresh. Glosses stay — the
                    // snapshot id discloses their age.
                    //
                    // The new recipe runs *first*: until its SQL has
                    // produced batches there is nothing to replace the
                    // old landing with, and a recipe that errors must
                    // not have destroyed the table it was replacing.
                    let replaced = admission == RecipeAdmission::Replaced
                        && lake.table_exists(dataset, table).await?;
                    let landed = glossql_import::run_recipe(
                        &self.source_spec(&d.source.value).await?,
                        &d.sql,
                    )
                    .await?;
                    if replaced {
                        let mounted = self.mount_schema(dataset).await?;
                        mounted.deregister_table(table)?;
                    }
                    let (summary, casts) = self.materialize(dataset, table, landed).await?;
                    store.put_recipe(d).await?;
                    // The counts arrive at the decision moment: whether
                    // the dropped rows — and the cells the casts nulled
                    // — are acceptable is the author's call, made now.
                    // A multi-source recipe reports per-scan counts
                    // instead of a misleading difference — the sum
                    // across providers would read as one source's
                    // drop count.
                    let verb = if replaced {
                        "superseded and re-landed: "
                    } else {
                        ""
                    };
                    format!("DECLARE RECIPE {table} ON {dataset} ({verb}{summary}{casts})")
                }
            }
            Declaration::Relationship(d) => {
                let (left, op, right) = self.pair(&d.left, d.op, &d.right).await?;
                // An endpoint is a key column of a landed table (SPEC.md
                // §4). A column that is not there is refused here, where
                // the typo is, instead of reading later as an orphan rate
                // in `relationship_coherence`.
                for side in [&d.left, &d.right] {
                    let (table, columns) = endpoint_parts(side);
                    let table = table.last().expect("an endpoint names its table");
                    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
                    self.check_landed(&left.dataset, table, &columns).await?;
                }
                store
                    .declare_relationship(&left.dataset, &left.subject, op, &right.subject)
                    .await?;
                format!(
                    "DECLARE RELATIONSHIP {} {op} {}",
                    left.subject, right.subject
                )
            }
            Declaration::Aspect(d) => {
                store.declare_aspect(d).await?;
                format!("DECLARE ASPECT {}", d.name.value)
            }
            Declaration::Function(d) => {
                store.declare_function(d).await?;
                format!("DECLARE FUNCTION {}", d.name.value)
            }
            Declaration::Witness(d) => {
                store.declare_witness(d).await?;
                format!("DECLARE WITNESS {}", d.name.value)
            }
        };
        Ok(Outcome::Done(done))
    }

    /// The bound dataset — fixed at channel construction on the plane;
    /// a directly held session may move it with `USE`/[`Session::bind`].
    pub fn dataset(&self) -> Option<String> {
        self.shared.dataset.read().expect("state lock").clone()
    }

    async fn use_dataset(&self, name: &str) -> Result<Outcome, SessionError> {
        self.bind(name).await?;
        Ok(Outcome::Done(format!("USE {name}")))
    }

    /// Bind the session to a dataset — channel construction and `USE`
    /// share this. With a lake, the dataset's schema mounts from the
    /// lake's shared provider and becomes the session's default schema:
    /// bare names then resolve through the substrate's own resolution
    /// (datafusion-53.1.0 session_state.rs:295 reads the config per
    /// statement), so there is no per-table alias machinery and no
    /// per-session provider build.
    pub async fn bind(&self, name: &str) -> Result<(), SessionError> {
        if !self.shared.store.dataset_exists(name).await? {
            return Err(SessionError::Store(glossql_glossary::Error::Unknown {
                what: "dataset",
                name: name.into(),
            }));
        }
        self.mount_schema(name).await?;
        self.ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_schema = name.to_string();
        *self.shared.dataset.write().expect("state lock") = Some(name.to_string());
        Ok(())
    }

    /// Land what a recipe produced as its table: create the table through
    /// the mounted schema (live — no rebuild), then commit the batches
    /// through iceberg-rust with the landing's source-side facts riding
    /// the snapshot as properties — DataFusion's INSERT cannot carry
    /// them. One snapshot per materialization; the `imports` relation is
    /// these snapshots read back.
    async fn materialize(
        &self,
        dataset: &str,
        table: &str,
        landed: glossql_import::Landed,
    ) -> Result<(String, String), SessionError> {
        let summary = landed.row_summary();
        let mut facts = std::collections::HashMap::from([(
            glossql_glossary::LANDING_SCANS_PROP.to_string(),
            serde_json::Value::Array(
                landed
                    .source_scans
                    .iter()
                    .map(|(relation, held)| serde_json::json!({"relation": relation, "rows": held}))
                    .collect(),
            )
            .to_string(),
        )]);
        if let Some(dropped) = landed.dropped_rows() {
            facts.insert(
                glossql_glossary::LANDING_DROPPED_PROP.to_string(),
                dropped.to_string(),
            );
        }
        facts.insert(
            glossql_glossary::LANDING_CASTS_PROP.to_string(),
            landed.casts.to_json().to_string(),
        );
        self.land(
            dataset,
            table,
            Arc::clone(&landed.schema),
            &landed.batches,
            facts,
        )
        .await?;
        Ok((summary, cast_summary(&landed.casts)))
    }

    /// A probe (SPEC.md §3): the recipe rehearsal, executed at its source,
    /// landing nothing.
    async fn probe(&self, probe: Probe) -> Result<Outcome, SessionError> {
        let spec = self.source_spec(&probe.source.value).await?;
        Ok(Outcome::Rows(
            glossql_import::run_probe(&spec, &probe.sql, self.row_cap).await?,
        ))
    }

    async fn source_spec(&self, source: &str) -> Result<SourceSpec, SessionError> {
        let settings =
            self.shared
                .store
                .source_settings(source)
                .await?
                .ok_or(SessionError::Store(glossql_glossary::Error::Unknown {
                    what: "source",
                    name: source.into(),
                }))?;
        Ok(SourceSpec::from_settings(source, &settings)?)
    }

    /// The dataset's namespace as a schema in the session's default catalog
    /// — `fin.orders` resolves; after [`Session::bind`] it is also the
    /// default schema. The schema is an Arc clone of the lake's shared
    /// provider's, never a per-session build; a miss rebuilds the shared
    /// provider once, in case it predates the namespace. The mounted
    /// generation is tracked: when a namespace create rebuilt the shared
    /// provider, an already-mounted session re-registers from the current
    /// generation instead of resolving through the frozen one forever
    /// — otherwise a table landed on the new generation stays
    /// invisible to channels mounted before the invalidation.
    async fn mount_schema(&self, dataset: &str) -> Result<Arc<dyn SchemaProvider>, SessionError> {
        let lake = self.lake();
        let current = lake.provider().await?;
        let default = self.ctx.catalog("datafusion").expect("default catalog");
        let same_generation = self
            .mounted_provider
            .lock()
            .expect("mount lock")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|mounted| Arc::ptr_eq(&mounted, &current));
        if same_generation && let Some(existing) = default.schema(dataset) {
            return Ok(existing);
        }
        // The shared provider is invalidated by every namespace create,
        // so a miss here is a namespace that does not exist — the
        // error, not a rebuild.
        let provider = current;
        let schema = provider.schema(dataset).ok_or_else(|| {
            SessionError::Lake(glossql_catalog::Error::Workspace(format!(
                "namespace `{dataset}` is missing from the catalog"
            )))
        })?;
        default.register_schema(dataset, Arc::clone(&schema))?;
        // One provider serves every namespace, and a session is keyed to
        // one dataset, so a single tracked generation suffices.
        *self.mounted_provider.lock().expect("mount lock") = Some(Arc::downgrade(&provider));
        Ok(schema)
    }

    /// Re-mount the bound dataset when the lake's shared provider was
    /// rebuilt since this session mounted. Cheap when nothing changed:
    /// one lock read and an Arc pointer compare.
    async fn refresh_mount(&self) -> Result<(), SessionError> {
        let dataset = self.shared.dataset.read().expect("state lock").clone();
        if let Some(dataset) = dataset {
            self.mount_schema(&dataset).await?;
        }
        Ok(())
    }

    /// The subject's table snapshot at write time — `None` for dataset-level
    /// subjects, pair paths, or tables the lake does not hold.
    async fn stamp(&self, resolved: &Resolved) -> Result<Option<i64>, SessionError> {
        if resolved.subject == resolved.dataset || resolved.subject.contains(' ') {
            return Ok(None);
        }
        let lake = self.lake();
        let table = resolved
            .subject
            .split('.')
            .next()
            .expect("subjects are non-empty");
        Ok(lake.snapshot_id(&resolved.dataset, table).await?)
    }

    /// The gloss half of the landed-subject rule: when the subject's
    /// shape is a table or a column and the aspect admits that grain,
    /// the table (and the column) must be landed. A source name is
    /// SOURCE grain and the grain gate's business; a pair path is the
    /// relationship's, checked at its declaration.
    async fn check_glossed_subject(
        &self,
        resolved: &Resolved,
        grains: &str,
    ) -> Result<(), SessionError> {
        let grain = glossql_glossary::rules::grain_of(&resolved.dataset, &resolved.subject);
        if !matches!(grain, "table" | "column") || !grains.split(',').any(|g| g == grain) {
            return Ok(());
        }
        if self
            .shared
            .store
            .source_settings(&resolved.subject)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let (table, column) = match resolved.subject.split_once('.') {
            Some((table, column)) => (table, Some(column)),
            None => (resolved.subject.as_str(), None),
        };
        let columns: Vec<&str> = column.into_iter().collect();
        self.check_landed(&resolved.dataset, table, &columns).await
    }

    /// A subject that claims a landed table or column must find it.
    /// Refused with the road out — the dataset's tables, or the table's
    /// columns — the way the planner's own `No field named` names the
    /// valid fields. Reads the statement's catalog walk, so a check
    /// costs no extra round trip.
    async fn check_landed(
        &self,
        dataset: &str,
        table: &str,
        columns: &[&str],
    ) -> Result<(), SessionError> {
        let pins = self.shared.pinned(dataset).await?;
        let Some(pin) = pins.iter().find(|p| p.name == table) else {
            let mut names: Vec<&str> = pins.iter().map(|p| p.name.as_str()).collect();
            names.sort_unstable();
            let tables = if names.is_empty() {
                "none landed yet".to_string()
            } else {
                names.join(", ")
            };
            return Err(SessionError::BadSubject(format!(
                "`{table}` is not a landed table in `{dataset}` — tables: {tables}"
            )));
        };
        if let Some(missing) = columns
            .iter()
            .find(|c| !pin.columns.iter().any(|have| have == *c))
        {
            return Err(SessionError::BadSubject(format!(
                "`{table}.{missing}`: `{table}` has no column `{missing}` — columns: {}",
                pin.columns.join(", ")
            )));
        }
        Ok(())
    }

    async fn gloss(&self, gloss: Gloss) -> Result<Outcome, SessionError> {
        let resolved = self.subject(&gloss.subject).await?;
        let aspect = gloss.aspect.value.as_str();
        // An aspect declared ON TABLE or ON COLUMN says its subject is
        // one, so the subject must be landed — checked here, where the
        // typo is. An aspect with no grain clause claims nothing about
        // the schema: its subject is an address (an app's `app.page`
        // rides the column shape), and stands unchecked. An unknown
        // aspect is the store's refusal, below.
        if let Some((_, _, Some(grains))) = self.shared.store.aspect(aspect).await? {
            self.check_glossed_subject(&resolved, &grains).await?;
        }
        let snapshot = self.stamp(&resolved).await?;
        self.shared
            .store
            .gloss(
                &resolved.dataset,
                &self.actor,
                aspect,
                &resolved.subject,
                &gloss.body,
                snapshot,
            )
            .await?;
        // A grounding's write answers with the metric's fact row — the
        // `metric_axes()` shape at the pin the write moved to: whether
        // the SQL plans, the verb and where it came from, the axes the
        // verdicts admit and the served columns they do not, each with
        // its road back in. The recipe's outcome carries the cast
        // account at the decision moment (SPEC.md §3); this is the
        // same rule for the other authored SQL, so what the cube will
        // do with a grounding is read before the next write, not
        // discovered at the first read. Nothing is refused on it: the
        // language rules no shape for a grounding, and the row tells.
        // Every other gloss answers as it did.
        let is_grounding = self
            .shared
            .store
            .aspect(aspect)
            .await?
            .is_some_and(|(_, kind, _)| kind == "query");
        if is_grounding {
            let fact = crate::cube::fact_at_write(
                &self.shared,
                &resolved.dataset,
                &resolved.subject,
                aspect,
            )
            .await?;
            return Ok(Outcome::Rows(vec![fact]));
        }
        Ok(Outcome::Done(format!(
            "GLOSS {aspect} ON {}",
            resolved.subject
        )))
    }

    /// Extraction (SPEC.md §6): the compute act. A hit at the statement's
    /// pin serves; a miss computes, validates against the RETURNS
    /// aspect's schema, and lands one `measurements` row — the drift
    /// record's next point.
    async fn extract(&self, extract: Extract) -> Result<Outcome, SessionError> {
        let store = self.shared.store.clone();
        let resolved = self.subject(&extract.subject).await?;
        let ctx = self.shared.read_context().await?;
        let mut results = Vec::new();
        for call in &extract.calls {
            let name = call.value.clone();
            let function = store
                .function(&name, Some(&resolved.dataset))
                .await?
                .ok_or_else(|| SessionError::UnknownFunction(name.clone()))?;
            // Role by shape: a function without RETURNS
            // is a detector — it runs through its witness, never extraction.
            let Some(returns) = function.returns.clone() else {
                return Err(SessionError::DetectorNotExtractable(name.clone()));
            };
            let measured = glossql_glossary::Store::measurement_in(
                &ctx,
                &resolved.dataset,
                &resolved.subject,
                &name,
            );
            // The outcome says which happened: a hit serves the
            // recorded row at an unchanged pin, `computed_at` and all;
            // a miss computes now.
            let (row, computed) = match measured {
                Some(row) => (row, false),
                None => {
                    tracing::info!(
                        function = %name,
                        dataset = %resolved.dataset,
                        subject = %resolved.subject,
                        "measuring"
                    );
                    // A measurement's body is one SQL query the engine
                    // plans and runs (§6) — anything else is refused
                    // with the shape the declaration owes.
                    let body = crate::measure::sql_body(&function.script).ok_or_else(|| {
                        SessionError::Runtime(format!(
                            "`{name}`: a measurement's body is one SQL query"
                        ))
                    })?;
                    let (output, reads) = self
                        .compute_sql(body, &resolved.subject, &resolved.dataset)
                        .await?;
                    // The aspect's schema is the one contract: nothing lands
                    // under an aspect without validating against it.
                    let (schema, _, grains) = store.aspect(&returns).await?.ok_or_else(|| {
                        SessionError::Store(glossql_glossary::Error::Unknown {
                            what: "aspect",
                            name: returns.clone(),
                        })
                    })?;
                    // RETURNS lands under the aspect too: the extraction
                    // subject must sit in the aspect's declared grain.
                    let is_source = store.source_settings(&resolved.subject).await?.is_some();
                    glossql_glossary::admit_grain(
                        &returns,
                        grains.as_deref(),
                        &resolved.dataset,
                        &resolved.subject,
                        is_source,
                    )
                    .map_err(SessionError::Store)?;
                    schemas::validate_instance(&schema, &output).map_err(|e| {
                        SessionError::OutputRejected {
                            function: name.clone(),
                            detail: e.to_string(),
                        }
                    })?;
                    // An abstention naming absent inputs is an answer
                    // about the context, not a measurement of the
                    // subject — it serves without
                    // landing, so the retry recomputes once the
                    // producer has run.
                    let row = if output.get("missing_aspects").is_some() {
                        glossql_glossary::MeasurementRow {
                            subject: resolved.subject.clone(),
                            function: name.clone(),
                            body: output.to_string(),
                            computed_at: chrono::Utc::now()
                                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                                .to_string(),
                        }
                    } else {
                        // No cache to forget: the landing moves the
                        // store's version, so every channel — this one
                        // included — misses on its next read.
                        store
                            .measurement_put(
                                &resolved.dataset,
                                &name,
                                &resolved.subject,
                                &returns,
                                &ctx.pin,
                                &output.to_string(),
                                &read_names(&reads, &resolved.dataset),
                            )
                            .await?
                    };
                    (row, true)
                }
            };
            results.push((row, computed));
        }
        Ok(Outcome::Rows(vec![crate::reads::extraction_batch(results)]))
    }

    /// A SQL measurement body: the subject bound into the AST, planned
    /// through the statement pre-pass — same pin, same doors, same
    /// read-only guard as any read — executed, and shaped by the result
    /// rule (`measure::body_value`). Beside the value, what the body
    /// READ: the plan's own table scans plus what resolution
    /// accumulated (compute doors, relations) — the measurement
    /// record's legs.
    async fn compute_sql(
        &self,
        mut statement: DFStatement,
        subject: &str,
        dataset: &str,
    ) -> Result<(Value, crate::prepass::ReadSet), SessionError> {
        crate::measure::bind_subject(&mut statement, subject);
        let (plan, _, mut reads) = self.plan_statement_measured(statement).await?;
        // A scanned name is a data table unless it is a store
        // relation's; a door batch's scan (its factor string) matches
        // no pin leg and falls out at the filter.
        for name in crate::provenance::scanned_tables(&plan, dataset) {
            if glossql_glossary::relation_columns(&name).is_some() {
                reads.relations.insert(name);
            } else {
                reads.tables.insert(name);
            }
        }
        let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
        Ok((crate::measure::body_value(&batches)?, reads))
    }

    /// One query, streaming: batches flow as
    /// the engine produces them — memory rides one batch, and a dropped
    /// stream cancels the work upstream. `GLOSSARY()`/`ATTEST()` stream
    /// like any read; the planner did its part before the first batch.
    /// Anything that is not exactly one query refuses with
    /// [`SessionError::NotOneRead`] and belongs in [`Session::execute`].
    /// The result says whether the query reads only the store's
    /// relations — the doors' cap policy wants to know.
    /// Plan one statement: resolve its doors asynchronously, then run the
    /// sync planner over a state that already holds them. Nothing blocks a
    /// planner thread, and nothing re-enters — the two defects the
    /// expansion stack existed to survive.
    pub(crate) async fn plan_statement(
        &self,
        statement: datafusion::sql::parser::Statement,
    ) -> Result<datafusion::logical_expr::LogicalPlan, SessionError> {
        Ok(self.plan_statement_classed(statement).await?.0)
    }

    /// [`Session::plan_statement`] plus what resolution learned: whether
    /// the statement reads the glossary anywhere in its expansion — the
    /// frame class the app door serves.
    pub(crate) async fn plan_statement_classed(
        &self,
        statement: datafusion::sql::parser::Statement,
    ) -> Result<(datafusion::logical_expr::LogicalPlan, bool), SessionError> {
        let (plan, record, _) = self.plan_statement_measured(statement).await?;
        Ok((plan, record))
    }

    /// [`Session::plan_statement_classed`] plus the resolution's read
    /// set — what extraction records as the measurement's legs.
    pub(crate) async fn plan_statement_measured(
        &self,
        statement: datafusion::sql::parser::Statement,
    ) -> Result<
        (
            datafusion::logical_expr::LogicalPlan,
            bool,
            crate::prepass::ReadSet,
        ),
        SessionError,
    > {
        // Boxed for the reason `query_stream_with_params` is.
        tracing::Instrument::instrument(
            Box::pin(self.plan_classed(statement)),
            tracing::debug_span!("plan"),
        )
        .await
    }

    /// The planning, under its span.
    async fn plan_classed(
        &self,
        statement: datafusion::sql::parser::Statement,
    ) -> Result<
        (
            datafusion::logical_expr::LogicalPlan,
            bool,
            crate::prepass::ReadSet,
        ),
        SessionError,
    > {
        let resolved = crate::prepass::resolve(&self.shared, &self.ctx, &statement).await?;
        let record = resolved.touches_record();
        let reads = resolved.reads.clone();
        let tables = resolved.tables();
        let state = crate::reads::state_with(&self.ctx, &self.shared, resolved);
        let plan = match state.statement_to_plan(statement).await {
            Ok(plan) => plan,
            Err(e) => return Err(self.table_road_out(e, tables)),
        };
        // The substrate's own read-only verification, at the one place a
        // plan is made. Planning mints nothing — `execute_logical_plan`
        // does (datafusion context/mod.rs:699) — so the plan is what to
        // check and here is where to check it.
        //
        // A query that plans to DDL is `SELECT … INTO t` and nothing
        // else: the callers admit only queries, so that is the one
        // spelling that arrives as a Query and leaves as a
        // `CreateMemoryTable`. It is named by what the author wrote
        // rather than by the engine's node, and the engine's walk reaches
        // the nestings a check on the statement cannot — a derived table
        // in FROM, a CTE, a subquery.
        read_only()
            .verify_plan(&plan)
            .map_err(|_| SessionError::SubstrateClosed("SELECT INTO".into()))?;
        Ok((json_unions_as_text(plan)?, record, reads))
    }

    /// The planner's `table '…' not found` (datafusion-53.1.0
    /// session_state.rs:1824) with the road out: what the name could
    /// have been — the bound dataset's tables and the store's relations.
    /// The engine's text stays in front; every other planning error
    /// passes as it came.
    fn table_road_out(&self, e: DataFusionError, mut tables: Vec<String>) -> SessionError {
        let missing_table = matches!(
            e.find_root(),
            DataFusionError::Plan(m) if m.starts_with("table '") && m.ends_with("' not found")
        );
        if !missing_table {
            return e.into();
        }
        tables.sort_unstable();
        let tables = match (self.dataset(), tables.is_empty()) {
            (None, _) => "no dataset in use — USE one first".to_string(),
            (Some(d), true) => format!("no table landed in `{d}` yet"),
            (Some(d), false) => format!("tables in `{d}`: {}", tables.join(", ")),
        };
        let relations: Vec<&str> = glossql_glossary::RELATIONS.iter().map(|r| r.name).collect();
        SessionError::UnknownTable(format!(
            "{e} — {tables}; the store's relations: {}",
            relations.join(", ")
        ))
    }

    pub async fn query_stream(&self, sql: &str) -> Result<QueryStream, SessionError> {
        self.query_stream_with_params(sql, None).await
    }

    /// [`Session::query_stream`] with placeholder values: `$name` in the
    /// query binds from the map — typed values through the plan, never
    /// text spliced into SQL. The app door's frames ride this; a
    /// placeholder nobody bound fails at execution, which is the read
    /// telling the author what the URL owed it.
    pub async fn query_stream_with_params(
        &self,
        sql: &str,
        params: Option<ParamValues>,
    ) -> Result<QueryStream, SessionError> {
        // Before the span: the door tries every call as a read first,
        // and a sequence that goes on to execute is not a read that
        // failed — it never opens one.
        let statement = one_read(sql)?;
        let span = tracing::info_span!(
            "read",
            actor = %self.actor.kind,
            subject = %self.actor.id,
            dataset = %self.dataset().unwrap_or_default(),
            sql_digest = %crate::plane::sql_digest(sql),
            sql_len = sql.len(),
        );
        // Boxed: the read's future carries the pre-pass, every door body
        // it plans and the cube builds behind them. Held by value under
        // the span's wrapper, a debug build copies it onto the stack once
        // more at construction — the ceiling the cube suite runs at.
        tracing::Instrument::instrument(Box::pin(self.read_stream(statement, sql, params)), span)
            .await
    }

    /// The read, under its span.
    async fn read_stream(
        &self,
        mut statement: DFStatement,
        sql: &str,
        params: Option<ParamValues>,
    ) -> Result<QueryStream, SessionError> {
        tracing::debug!(text = %sql, "the read's text");
        // Named string params bind into the AST before the pre-pass, so
        // door arguments (`metric_series(grain => $grain)`) resolve;
        // everything else still binds through the plan below.
        if let Some(ParamValues::Map(map)) = &params {
            crate::measure::bind_params(&mut statement, map);
        }
        self.refresh_mount().await?;
        let metadata_only = reads_only_metadata(&statement);
        let (mut plan, record) = self.plan_statement_classed(statement).await?;
        if let Some(params) = params {
            plan = plan.with_param_values(params)?;
        }
        // The engine's own path, in two steps instead of
        // `DataFrame::execute_stream`, so the physical plan stays in
        // hand: its operators' counts are read when the stream ends.
        let physical = self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .create_physical_plan()
            .await?;
        let stream =
            datafusion::physical_plan::execute_stream(Arc::clone(&physical), self.ctx.task_ctx())?;
        Ok(QueryStream {
            stream: Box::pin(crate::execution::Metered::new(
                stream,
                physical,
                tracing::Span::current(),
            )),
            metadata_only,
            record,
        })
    }

    /// Substrate SQL runs behind an allowlist:
    /// queries pass, `DESCRIBE`/`EXPLAIN` pass as reads (`DESCRIBE`
    /// over any name a read can plan — [`Self::describe`]), the
    /// store's forwarded deletes pass, `DROP TABLE` routes to engine
    /// semantics — everything else that would alter the schema or data
    /// directly is refused. Tables come from recipes.
    async fn substrate(&self, statement: DFStatement) -> Result<Outcome, SessionError> {
        // Removal is SQL (SPEC.md §5.2, §6): the strike routes to the
        // store by name, which refuses until the substrate can remove
        // rows. DataFusion cannot execute DML against registered
        // providers anyway.
        if let Some(target) = store_delete(&statement) {
            let affected = self.shared.store.forward_delete(&target).await?;
            return Ok(Outcome::Affected(affected));
        }
        // DESCRIBE and EXPLAIN are reads — about a table's schema and a
        // plan — not manipulation, so they pass. EXPLAIN is the
        // substrate parser's own variant wrapping the statement it
        // explains (datafusion-sql-53.1.0 parser.rs:293); only a plain
        // query may ride it, so the allowlist repeats inside it instead
        // of being walked around. DESCRIBE arrives through sqlparser as
        // ExplainTable, below.
        if let DFStatement::Explain(explain) = &statement {
            match explain.statement.as_ref() {
                DFStatement::Statement(inner) => match inner.as_ref() {
                    SQLStatement::Query(_) => {}
                    other => {
                        return Err(SessionError::SubstrateClosed(format!(
                            "EXPLAIN {}",
                            verb_of(other)
                        )));
                    }
                },
                other => {
                    return Err(SessionError::SubstrateClosed(format!(
                        "EXPLAIN {}",
                        statement_verb(other)
                    )));
                }
            }
        } else {
            let DFStatement::Statement(inner) = &statement else {
                return Err(SessionError::SubstrateClosed(statement_verb(&statement)));
            };
            match inner.as_ref() {
                SQLStatement::Query(_) => {}
                SQLStatement::ExplainTable { table_name, .. } => {
                    return self.describe(&table_name.to_string()).await;
                }
                SQLStatement::Drop {
                    object_type, names, ..
                } if *object_type == ObjectType::Table && names.len() == 1 => {
                    let name = names[0].to_string();
                    return self.drop_table(&name).await;
                }
                other => return Err(SessionError::SubstrateClosed(verb_of(other))),
            }
        }
        let plan = self.plan_statement(statement).await?;
        let frame = self.ctx.execute_logical_plan(plan).await?;
        // The same two-step path as a streaming read, so the operators'
        // counts close the statement's span when the stream is done —
        // `USE ds; SELECT …`, the agent's usual call, reads here.
        let physical = frame.create_physical_plan().await?;
        let schema = physical.schema();
        let stream =
            datafusion::physical_plan::execute_stream(Arc::clone(&physical), self.ctx.task_ctx())?;
        let mut stream = Box::pin(crate::execution::Metered::new(
            stream,
            physical,
            tracing::Span::current(),
        ));
        // Bounded like the streaming door, for the same reason: the reader
        // sees at most its cap, so the engine should not be asked for more
        // than that. One row past the cap is kept, which is how the door
        // knows the answer was truncated; collecting the whole result
        // and trimming at render would defeat the cap.
        let mut batches = Vec::new();
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            rows += batch.num_rows();
            batches.push(batch);
            if rows > self.row_cap {
                break;
            }
        }
        if batches.is_empty() {
            // An empty result still carries the shape — the door serves
            // (name, type) columns from it, the LIMIT 0 rehearsal's point.
            batches.push(RecordBatch::new_empty(schema));
        }
        Ok(Outcome::Rows(batches))
    }

    /// `DESCRIBE <name>` for every name a read can plan — a landed
    /// table, a store relation, a shipped read, a cube read. The name
    /// is planned as `SELECT * FROM <name> LIMIT 0` through the same
    /// pre-pass a read takes, and the plan's schema is served in the
    /// engine's own DESCRIBE shape (`column_name`, `data_type`,
    /// `is_nullable`; datafusion-54.1.0 physical_planner.rs:2893). The
    /// engine's DESCRIBE sees only the mounted tables, which is why the
    /// statement does not reach it: the store's relations and the
    /// reads resolve in the pre-pass and are registered nowhere.
    async fn describe(&self, name: &str) -> Result<Outcome, SessionError> {
        let plan = self
            .plan_statement(one_query(&format!("SELECT * FROM {name} LIMIT 0"))?)
            .await?;
        let fields = plan.schema().fields();
        let column = |f: fn(&Field) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(
                fields.iter().map(|x| f(x.as_ref())),
            ))
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                column(|f| f.name().to_string()),
                column(|f| f.data_type().to_string()),
                column(|f| if f.is_nullable() { "YES" } else { "NO" }.to_string()),
            ],
        )
        .map_err(DataFusionError::from)?;
        Ok(Outcome::Rows(vec![batch]))
    }

    /// `DROP TABLE` (PoC rules): refused while the
    /// table holds data or glosses — replacement is postponed, so this only
    /// ever removes a mis-declared table. What it removes, it removes
    /// whole: the lake table, its recipe property, its landings, the
    /// import records.
    async fn drop_table(&self, name: &str) -> Result<Outcome, SessionError> {
        let dataset = self
            .shared
            .dataset
            .read()
            .expect("state lock")
            .clone()
            .ok_or(SessionError::NoDataset)?;
        let table = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
        let lake = self.lake();
        if !lake.table_exists(&dataset, table).await? {
            return Err(SessionError::Store(glossql_glossary::Error::Unknown {
                what: "table",
                name: table.into(),
            }));
        }
        let count = async {
            let plan = self
                .plan_statement(one_query(&format!(
                    "SELECT count(*) FROM \"{dataset}\".\"{table}\""
                ))?)
                .await?;
            let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
            Ok::<_, SessionError>(batches)
        }
        .await;
        let has_data = match count {
            Ok(batches) => batches.iter().any(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .is_some_and(|c| !c.is_empty() && c.value(0) > 0)
            }),
            Err(_) => true, // cannot verify: refuse
        };
        if has_data {
            return Err(SessionError::DropRefused {
                table: table.into(),
                reason: "it holds data".into(),
            });
        }
        let glosses = self.shared.store.glosses_under(&dataset, table).await?;
        if glosses > 0 {
            return Err(SessionError::DropRefused {
                table: table.into(),
                reason: format!("{glosses} gloss(es) sit under it"),
            });
        }
        // Through the mounted schema provider: iceberg-datafusion's
        // deregister drops the catalog table and updates its own map in one
        // move (iceberg-datafusion-0.10.1 schema.rs:215-236).
        let mounted = self.mount_schema(&dataset).await?;
        mounted.deregister_table(table)?;
        // The recipe and the import record die with the table (its
        // properties and snapshots); measurements that read it sit at
        // pins that no longer resolve.
        Ok(Outcome::Done(format!("DROP TABLE {table}")))
    }

    async fn subject(&self, subject: &Subject) -> Result<Resolved, SessionError> {
        let use_dataset = self.shared.dataset.read().expect("state lock").clone();
        let use_dataset = use_dataset.as_deref();
        match subject {
            Subject::Path(p) => {
                let segments: Vec<String> = p.segments.iter().map(|i| i.value.clone()).collect();
                resolve_path(&self.shared.store, use_dataset, &segments).await
            }
            Subject::Pair(pair) => {
                let (left, op, right) = self.pair(&pair.left, pair.op, &pair.right).await?;
                Ok(Resolved {
                    dataset: left.dataset.clone(),
                    subject: pair_subject(&left, op, &right),
                })
            }
        }
    }

    async fn pair(
        &self,
        left: &glossql_parser::RelSide,
        op: RelOp,
        right: &glossql_parser::RelSide,
    ) -> Result<(Resolved, &'static str, Resolved), SessionError> {
        let use_dataset = self.shared.dataset.read().expect("state lock").clone();
        let use_dataset = use_dataset.as_deref();
        let store = &self.shared.store;
        let (lt, lc) = endpoint_parts(left);
        let (rt, rc) = endpoint_parts(right);
        let l = resolve_endpoint(store, use_dataset, &lt, &lc).await?;
        let r = resolve_endpoint(store, use_dataset, &rt, &rc).await?;
        if l.dataset != r.dataset {
            return Err(SessionError::BadSubject(format!(
                "pair path spans datasets `{}` and `{}`",
                l.dataset, r.dataset
            )));
        }
        let op = match op {
            RelOp::ManyToOne => "->",
            RelOp::OneToOne => "<->",
        };
        Ok((l, op, r))
    }
}

/// The JSON functions' union — what `->` and `json_get` return — cannot
/// leave the engine: arrow's JSON writer refuses an Arrow Union at the
/// doors, and the parquet writer refuses it at a staging. A plan whose
/// output carries one is projected through the functions' own
/// `json_union_to_text`, so every consumer — a door, a measurement's
/// value, a materialization — reads canonical JSON text. Plans without
/// one pass untouched.
fn json_unions_as_text(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    use datafusion_functions_json::JSON_UNION_DATA_TYPE;
    let is_union = |dt: &DataType| dt == &*JSON_UNION_DATA_TYPE;
    if !plan.schema().fields().iter().any(|f| is_union(f.data_type())) {
        return Ok(plan);
    }
    let to_text = datafusion_functions_json::udfs::json_union_to_text_udf();
    let exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(Column::from((qualifier, field)));
            if is_union(field.data_type()) {
                to_text.call(vec![column]).alias(field.name())
            } else {
                column
            }
        })
        .collect();
    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}

/// An endpoint's `[dataset.]table` segments beside its key columns.
fn endpoint_parts(side: &glossql_parser::RelSide) -> (Vec<String>, Vec<String>) {
    let mut table = Vec::new();
    if let Some(d) = &side.dataset {
        table.push(d.value.clone());
    }
    table.push(side.table.value.clone());
    let columns = side.columns.iter().map(|c| c.value.clone()).collect();
    (table, columns)
}

/// A single query's batch stream, plus what it reads: `metadata_only`
/// marks a query whose every relation is the store's — `GLOSSARY()`,
/// `ATTEST()`, and the plain store relations. The doors' row-cap policy
/// exempts these: metadata is the agent's
/// map; the cap guards data reads.
pub struct QueryStream {
    pub stream: SendableRecordBatchStream,
    pub metadata_only: bool,
    /// Whether the statement reads the glossary anywhere in its
    /// expansion — derived by the pre-pass, not curated. The app door
    /// serves it as the frame class (`record`/`data`): a record frame
    /// can change under a glossary write, a data frame cannot, so the
    /// browser's frame store evicts only record entries on a ruling.
    /// Distinct from `metadata_only`, which is
    /// syntactic and cannot see through shipped reads.
    pub record: bool,
}

/// Every relation the query touches is a store read — and there is at
/// least one, so constant selects and VALUES stay on the capped path.
/// The store's RELATIONS table names the plain relations; `attest` is
/// the one read construct beside them (`glossary()` shares its name
/// with the relation).
/// The shape of a whole tool call, classified without executing — the
/// door's question-round cadence reads it.
/// `reviews` marks a call that reads the record: at least one metadata
/// read and no write — the brief sweep and the stage read-backs.
/// `writes` marks a call that moves the workspace: GLOSS, DECLARE,
/// PROBE, extraction, substrate DDL and DELETE. Forms ride reviewing
/// calls only, so a landing is never interrupted mid-flow; a plain
/// data SELECT is neither — judging work, not a review.
pub struct CallShape {
    pub reviews: bool,
    pub writes: bool,
}

/// A call that does not parse shapes as neither — the statement router
/// names the refusal when it runs.
pub fn call_shape(statements: &str) -> CallShape {
    let Ok(parsed) = GlossqlParser::parse_sql(statements) else {
        return CallShape {
            reviews: false,
            writes: false,
        };
    };
    let mut reads_record = false;
    let mut writes = false;
    for statement in &parsed {
        match statement {
            Statement::Use(_) => {}
            Statement::Substrate(df) => {
                if reads_only_metadata(df) {
                    reads_record = true;
                } else if !matches!(
                    df.as_ref(),
                    DFStatement::Statement(inner)
                        if matches!(inner.as_ref(), SQLStatement::Query(_))
                ) {
                    writes = true;
                }
            }
            _ => writes = true,
        }
    }
    CallShape {
        reviews: reads_record && !writes,
        writes,
    }
}

fn reads_only_metadata(statement: &DFStatement) -> bool {
    let DFStatement::Statement(inner) = statement else {
        return false;
    };
    let mut any = false;
    let mut all = true;
    let _ = visit_relations(inner.as_ref(), |name| {
        any = true;
        let metadata = name.0.len() == 1
            && name.0[0].as_ident().is_some_and(|i| {
                let name = i.value.to_lowercase();
                name == "attest" || glossql_glossary::relation_columns(&name).is_some()
            });
        if !metadata {
            all = false;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    any && all
}

/// The verb of a statement the allowlist refused, for the error message.
fn statement_verb(statement: &DFStatement) -> String {
    match statement {
        DFStatement::CreateExternalTable(_) => "CREATE EXTERNAL TABLE".into(),
        DFStatement::CopyTo(_) => "COPY".into(),
        DFStatement::Statement(inner) => verb_of(inner),
        other => format!("{other}")
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn verb_of(statement: &SQLStatement) -> String {
    statement
        .to_string()
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The cast account, rendered for the landing's outcome line: failing
/// columns with their top tokens (the full list persists in `imports`),
/// a clean bill, or the disclosed reason there is no account. Judging
/// the tokens is the reader's job — this line only makes them visible
/// at the decision moment.
/// The measurement record's `reads`: the NAMES of the legs the
/// computation read — `*` for the dataset's every table,
/// `<dataset>.<table>` and `glossql.<relation>` individually, `**`
/// when the reads could not be enumerated (exact-pin currency, the
/// conservative floor), empty for a body that read nothing and always
/// stands. Names, not snapshots: the currency rule
/// ([`glossql_glossary::measurement_stands`]) filters both the
/// recorded pin and the statement's by these names and compares, so a
/// leg that appears or disappears is a change like any other. A
/// scanned name that is no pin leg (a door batch's factor string)
/// selects nothing on either side and can never decide.
fn read_names(reads: &crate::prepass::ReadSet, dataset: &str) -> String {
    if reads.everything {
        return "**".into();
    }
    let mut names: Vec<String> = reads
        .relations
        .iter()
        .map(|r| format!("glossql.{r}"))
        .collect();
    if reads.all_tables {
        names.push("*".into());
    } else {
        names.extend(reads.tables.iter().map(|t| format!("{dataset}.{t}")));
    }
    names.sort();
    names.dedup();
    names.join(",")
}

fn cast_summary(casts: &glossql_import::CastAccounting) -> String {
    use glossql_import::CastAccounting;
    match casts {
        CastAccounting::Checked(checks) if checks.is_empty() => String::new(),
        CastAccounting::Checked(checks) => {
            let failing: Vec<String> = checks
                .iter()
                .filter(|c| c.failed > 0)
                .map(|c| {
                    let tokens = c
                        .tokens
                        .iter()
                        .take(3)
                        .map(|(t, n)| format!("'{t}' ×{n}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{}: {} [{}]", c.column, c.failed, tokens)
                })
                .collect();
            if failing.is_empty() {
                "; casts clean".into()
            } else {
                format!("; cast-nulled cells — {}", failing.join("; "))
            }
        }
        CastAccounting::Unchecked(note) => format!("; casts unaccounted — {note}"),
    }
}

/// `DELETE FROM glossary …` → the store's target name. Only the
/// glossary routes; a delete on anything else falls through to the
/// allowlist's own refusal.
fn store_delete(statement: &DFStatement) -> Option<String> {
    let DFStatement::Statement(inner) = statement else {
        return None;
    };
    let SQLStatement::Delete(delete) = inner.as_ref() else {
        return None;
    };
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let [table] = tables.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &table.relation else {
        return None;
    };
    if name.0.len() != 1 {
        return None;
    }
    let target = name.0[0].as_ident()?.value.to_lowercase();
    (target == "glossary").then_some(target)
}

/// A statement's kind, as its span names it.
fn kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::Declare(_) => "declare",
        Statement::Use(_) => "use",
        Statement::Gloss(_) => "gloss",
        Statement::Extract(_) => "extract",
        Statement::Probe(_) => "probe",
        Statement::Substrate(_) => "substrate",
    }
}
