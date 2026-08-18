//! `Session`: the per-connection statement router.

use std::sync::{Arc, RwLock};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, ParamValues};
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, FromTable, ObjectType, Query, SetExpr, Statement as SQLStatement, TableFactor,
    Value as SqlValue, visit_expressions_mut, visit_relations,
};
use datafusion::sql::sqlparser::parser::ParserError;
use futures::StreamExt as _;
use serde_json::Value;

use glossql_catalog::{IcebergCatalogProvider, Lake};
use glossql_glossary::{Actor, FunctionRow, RecipeAdmission, Store, schemas};
use glossql_import::SourceSpec;
use glossql_parser::{
    Declaration, Extract, Gloss, GlossqlParser, Probe, RelOp, Statement, Subject,
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
        /// 4–7 not run". A mid-sequence refusal without this cost the
        /// 2026-08-14 run a read of `imports` to learn what stood.
        context: String,
        source: Box<SessionError>,
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

/// The seam scripts plug into (rhai + arrow kernels, `glossql-scripts`).
/// Since stage 5 a script is a judge: it receives its `context` — a
/// detector's slots and threshold (§7.1), or a legacy measurement's
/// `ACCEPTS` document — and computes; the SQL door died with the last
/// shipped script measurement, so a script never re-enters the engine.
/// Tests inject fakes.
pub trait FunctionRuntime: Send + Sync + std::fmt::Debug {
    fn invoke(
        &self,
        function: &FunctionRow,
        subject: &str,
        context: &Value,
    ) -> Result<Value, String>;

    /// The aggregate statistics the runtime ships (`profile`, `mad`,
    /// `entropy` — the shapes SQL lacks). Registered on the session's
    /// context when the runtime attaches, so a measurement body and an
    /// agent's own SQL name the same functions.
    fn udafs(&self) -> Vec<datafusion::logical_expr::AggregateUDF> {
        Vec::new()
    }

    /// The band kernel behind the `whatif.` door (ruled 2026-08-11):
    /// fit one estimator on the replayed worlds, quantiles at `alphas`
    /// for every test row. Slices are row-major; the return is
    /// row-major (test_rows × alphas). The runtime that carries the
    /// model overrides this (the ensemble — sparse replay grids are the
    /// regime it was ruled in for); the default refuses, and the door
    /// reports why.
    #[allow(clippy::too_many_arguments)]
    fn band_grid(
        &self,
        _train_x: &[f64],
        _rows: usize,
        _cols: usize,
        _train_y: &[f64],
        _test_x: &[f64],
        _test_rows: usize,
        _alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        Err("this runtime carries no band kernel".into())
    }

    /// The misfit kernel behind the `misfit.` door (ruled 2026-08-11,
    /// fixture 20): fit on the frame, score the same frame — one mean
    /// log density per row, log space end to end (higher = fits the
    /// frame better). `x` is row-major (rows × cols), nulls as NaN.
    /// The runtime that carries the model overrides this; the default
    /// refuses, and the door reports why.
    fn misfit_scores(&self, _x: &[f64], _rows: usize, _cols: usize) -> Result<Vec<f64>, String> {
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
    /// train on `rows` × `cols` features (row-major), predict one test
    /// row, return the band value per alpha in order and the PIT — the
    /// quantile at which `actual` lands in the predicted distribution.
    /// The runtime that carries the model overrides this; the default
    /// refuses, and the door reports why.
    fn band_point(
        &self,
        _train_x: &[f64],
        _rows: usize,
        _cols: usize,
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

impl FunctionRuntime for NoRuntime {
    fn invoke(&self, function: &FunctionRow, _: &str, _: &Value) -> Result<Value, String> {
        Err(format!(
            "no function runtime configured — `{}` cannot run without scripts",
            function.name
        ))
    }
}

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
    pub fn new(store: Store, actor: Actor) -> Result<Self, SessionError> {
        let shared = Arc::new(Shared {
            store,
            dataset: RwLock::new(None),
            runtime: RwLock::new(Arc::new(NoRuntime)),
            context: RwLock::new(None),
            ctx: RwLock::new(None),
        });
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
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
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
    /// router feeds it the runs between `USE`s.
    pub(crate) async fn execute_statements(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<Outcome>, SessionError> {
        self.refresh_mount().await?;
        let total = statements.len();
        let mut outcomes = Vec::with_capacity(total);
        for (idx, statement) in statements.into_iter().enumerate() {
            let result = match statement {
                Statement::Declare(d) => self.declare(*d).await,
                Statement::Use(u) => self.use_dataset(&u.dataset.value).await,
                Statement::Gloss(g) => self.gloss(g).await,
                Statement::Extract(e) => self.extract(e).await,
                Statement::Probe(p) => self.probe(p).await,
                Statement::Substrate(s) => self.substrate(*s).await,
            };
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
                    // Supersede-and-reland (ruled 2026-08-06): a changed
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
                    // instead of a misleading difference (found
                    // 2026-08-12: the sum across providers read as one
                    // source's drop count).
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
        let rows: usize = landed.batches.iter().map(|b| b.num_rows()).sum();
        let summary = landed.row_summary(rows);
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
        if let Some(dropped) = landed.dropped_rows(rows) {
            facts.insert(
                glossql_glossary::LANDING_DROPPED_PROP.to_string(),
                dropped.to_string(),
            );
        }
        facts.insert(
            glossql_glossary::LANDING_CASTS_PROP.to_string(),
            landed.casts.to_json().to_string(),
        );
        self.land(dataset, table, Arc::clone(&landed.schema), &landed.batches, facts)
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
    /// (found 2026-08-12 — a table landed on the new generation was
    /// invisible to channels mounted before the invalidation).
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
        let (provider, schema) = match current.schema(dataset) {
            Some(schema) => (current, schema),
            None => {
                lake.invalidate_provider();
                let rebuilt = lake.provider().await?;
                let schema = rebuilt.schema(dataset).ok_or_else(|| {
                    SessionError::Lake(glossql_catalog::Error::Workspace(format!(
                        "namespace `{dataset}` is missing from the catalog"
                    )))
                })?;
                (rebuilt, schema)
            }
        };
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

    async fn gloss(&self, gloss: Gloss) -> Result<Outcome, SessionError> {
        let resolved = self.subject(&gloss.subject).await?;
        let snapshot = self.stamp(&resolved).await?;
        self.shared
            .store
            .gloss(
                &resolved.dataset,
                &self.actor,
                &gloss.aspect.value,
                &resolved.subject,
                &gloss.body,
                snapshot,
            )
            .await?;
        Ok(Outcome::Done(format!(
            "GLOSS {} ON {}",
            gloss.aspect.value, resolved.subject
        )))
    }

    /// Extraction (SPEC.md §6): the compute act. A hit at the statement's
    /// pin serves; a miss computes, validates against the RETURNS
    /// aspect's schema, and lands one `measurements` row — the drift
    /// record's next point. The context document holds one entry per
    /// `ACCEPTS` aspect: the nearest value walking up from the subject
    /// (subject, parent, dataset), null when nothing is glossed.
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
            // Role by shape (ruled 2026-08-04): a function without RETURNS
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
            let row = match measured {
                Some(row) => row,
                None => {
                    let mut context = serde_json::Map::new();
                    for aspect in &function.accepts {
                        let value = context_value(
                            &self.shared,
                            &ctx,
                            &resolved.dataset,
                            &resolved.subject,
                            aspect,
                        )
                        .await?;
                        context.insert(aspect.clone(), value);
                    }
                    let context = Value::Object(context);
                    // Role by shape, extended to the body (ruled
                    // 2026-08-17, §7e): a RETURNS function whose body is
                    // one SQL query is a measurement the engine runs. A
                    // script body still invokes — a legacy authored
                    // measurement computes from its context alone, and
                    // one that reaches for the dead SQL door fails with
                    // the runtime's own message.
                    let output = match crate::measure::sql_body(&function.script) {
                        Some(body) => self.compute_sql(body, &resolved.subject).await?,
                        None => self
                            .shared
                            .runtime()
                            .invoke(&function, &resolved.subject, &context)
                            .map_err(SessionError::Runtime)?,
                    };
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
                    let is_source = store
                        .source_settings(&resolved.subject)
                        .await?
                        .is_some();
                    glossql_glossary::admit_grain(
                        &returns,
                        grains.as_deref(),
                        &resolved.dataset,
                        &resolved.subject,
                        is_source,
                    )
                    .map_err(SessionError::Store)?;
                    schemas::validate_instance(&schema, &output).map_err(|detail| {
                        SessionError::OutputRejected {
                            function: name.clone(),
                            detail,
                        }
                    })?;
                    // An abstention naming absent inputs is an answer
                    // about the context, not a measurement of the
                    // subject (ruled 2026-08-04/16) — it serves without
                    // landing, so the retry recomputes once the
                    // producer has run.
                    if output.get("missing_aspects").is_some() {
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
                            )
                            .await?
                    }
                }
            };
            results.push(row);
        }
        Ok(Outcome::Rows(vec![crate::reads::extraction_batch(results)]))
    }

    /// A SQL measurement body: the subject bound into the AST, planned
    /// through the statement pre-pass — same pin, same doors, same
    /// read-only guard as any read — executed, and shaped by the result
    /// rule (`measure::body_value`).
    async fn compute_sql(
        &self,
        mut statement: DFStatement,
        subject: &str,
    ) -> Result<Value, SessionError> {
        crate::measure::bind_subject(&mut statement, subject);
        let resolved = crate::prepass::resolve(&self.shared, &self.ctx, &statement).await?;
        let plan = crate::reads::state_with(&self.ctx, &self.shared, resolved)
            .statement_to_plan(statement)
            .await?;
        read_only().verify_plan(&plan)?;
        let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
        crate::measure::body_value(&batches)
    }

    /// One query, streaming (project lead, 2026-08-04): batches flow as
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
        let resolved = crate::prepass::resolve(&self.shared, &self.ctx, &statement).await?;
        let state = crate::reads::state_with(&self.ctx, &self.shared, resolved);
        Ok(state.statement_to_plan(statement).await?)
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
        let mut statements = GlossqlParser::parse_sql(sql)?;
        let one_query = matches!(&statements[..], [Statement::Substrate(statement)]
            if matches!(&**statement, DFStatement::Statement(inner)
                if matches!(inner.as_ref(), SQLStatement::Query(_))));
        if !one_query {
            return Err(SessionError::NotOneRead);
        }
        let Some(Statement::Substrate(statement)) = statements.pop() else {
            unreachable!("just matched")
        };
        // `SELECT … INTO t` is a Query to the parser and a `CREATE MEMORY
        // TABLE` to the planner; the execute path refuses it and this path
        // must too — planned here it would mint a table and materialize the
        // whole source before the row cap applies (found 2026-08-12).
        if let DFStatement::Statement(inner) = &*statement
            && let SQLStatement::Query(q) = inner.as_ref()
            && selects_into(q)
        {
            return Err(SessionError::SubstrateClosed("SELECT INTO".into()));
        }
        self.refresh_mount().await?;
        let metadata_only = reads_only_metadata(&statement);
        let mut plan = self.plan_statement(*statement).await?;
        if let Some(params) = params {
            plan = plan.with_param_values(params)?;
        }
        Ok(QueryStream {
            stream: self
                .ctx
                .execute_logical_plan(plan)
                .await?
                .execute_stream()
                .await?,
            metadata_only,
        })
    }

    /// Substrate SQL runs behind an allowlist (project lead, 2026-08-04):
    /// queries pass, `DESCRIBE`/`EXPLAIN` pass as reads (2026-08-07), the
    /// store's forwarded deletes pass, `DROP TABLE` routes to engine
    /// semantics — everything else that would alter the schema or data
    /// directly is refused. Tables come from recipes.
    async fn substrate(&self, statement: DFStatement) -> Result<Outcome, SessionError> {
        // Removal is SQL (SPEC.md §5.2, §6): deletes on the store's two
        // relations run at the store. DataFusion cannot execute DML against
        // registered providers anyway.
        if let Some((target, text)) = store_delete(&statement) {
            let affected = self.shared.store.forward_delete(&target, &text).await?;
            return Ok(Outcome::Affected(affected));
        }
        // DESCRIBE and EXPLAIN are reads — about a table's schema and a
        // plan — not manipulation, so they pass (project lead,
        // 2026-08-07; the earlier refusal was the variant allowlist being
        // categorical, not a ruling against them). EXPLAIN is the
        // substrate parser's own variant wrapping the statement it
        // explains (datafusion-sql-53.1.0 parser.rs:293); only a plain
        // query may ride it, so the allowlist repeats inside it instead
        // of being walked around. DESCRIBE arrives through sqlparser as
        // ExplainTable, below.
        if let DFStatement::Explain(explain) = &statement {
            match explain.statement.as_ref() {
                DFStatement::Statement(inner) => match inner.as_ref() {
                    SQLStatement::Query(q) if selects_into(q) => {
                        return Err(SessionError::SubstrateClosed("SELECT INTO".into()));
                    }
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
                // `SELECT … INTO t` is a Query to the parser and a
                // `CREATE MEMORY TABLE` to the planner — the one spelling
                // that made tables without a recipe (found 2026-08-06).
                SQLStatement::Query(q) if selects_into(q) => {
                    return Err(SessionError::SubstrateClosed("SELECT INTO".into()));
                }
                SQLStatement::Query(_) => {}
                SQLStatement::ExplainTable { .. } => {}
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
        // Bounded like the streaming door, for the same reason: the reader
        // sees at most its cap, so the engine should not be asked for more
        // than that. One row past the cap is kept, which is how the door
        // knows the answer was truncated (found 2026-08-06: this path used
        // to collect the whole result and trim it at render).
        let mut stream = frame.execute_stream().await?;
        let schema = stream.schema();
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

    /// `DROP TABLE` (PoC rules, project lead 2026-08-04): refused while the
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

/// The nearest current value of `aspect`, walking up from the subject:
/// the subject itself, its parent, then the dataset. Null when nothing is
/// glossed — scripts are deterministic and handle absence themselves.
async fn context_value(
    shared: &Arc<Shared>,
    ctx: &glossql_glossary::ReadContext,
    dataset: &str,
    subject: &str,
    aspect: &str,
) -> Result<Value, SessionError> {
    let mut level = Some(subject.to_string());
    while let Some(current) = level {
        let scope = if current == dataset {
            glossql_glossary::Scope::Dataset
        } else {
            glossql_glossary::Scope::Subject(current.clone())
        };
        let verdicts = crate::reads::verdicts(shared, ctx, dataset, &scope, Some(aspect)).await?;
        let rows = glossql_glossary::Store::collapsed_read(dataset, &scope, Some(aspect), ctx, &verdicts);
        let target = if current == dataset {
            dataset
        } else {
            &current
        };
        if let Some(row) = rows.iter().find(|r| r.subject == target)
            && let Some(value) = &row.value
        {
            return Ok(serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.clone())));
        }
        level = parent_of(&current, dataset);
    }
    Ok(Value::Null)
}

/// `orders.amount` → `orders` → the dataset; tables and pair paths step
/// straight to the dataset.
fn parent_of(subject: &str, dataset: &str) -> Option<String> {
    if subject == dataset {
        None
    } else if subject.contains(' ') || !subject.contains('.') {
        Some(dataset.to_string())
    } else {
        Some(subject.rsplit_once('.').expect("has a dot").0.to_string())
    }
}

/// A single query's batch stream, plus what it reads: `metadata_only`
/// marks a query whose every relation is the store's — `GLOSSARY()`,
/// `ATTEST()`, and the plain store relations. The doors' row-cap policy
/// exempts these (project lead, 2026-08-04): metadata is the agent's
/// map; the cap guards data reads.
pub struct QueryStream {
    pub stream: SendableRecordBatchStream,
    pub metadata_only: bool,
}

/// Every relation the query touches is a store read — and there is at
/// least one, so constant selects and VALUES stay on the capped path.
/// The store's RELATIONS table names the plain relations; `attest` is
/// the one read construct beside them (`glossary()` shares its name
/// with the relation).
/// The shape of a whole tool call, classified without executing — the
/// door's question-round cadence reads it (ruled 2026-08-14).
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

/// `SELECT … INTO t` anywhere in a query's body.
fn selects_into(query: &Query) -> bool {
    fn body_selects_into(body: &SetExpr) -> bool {
        match body {
            SetExpr::Select(select) => select.into.is_some(),
            SetExpr::Query(q) => selects_into(q),
            SetExpr::SetOperation { left, right, .. } => {
                body_selects_into(left) || body_selects_into(right)
            }
            _ => false,
        }
    }
    body_selects_into(&query.body)
}

/// `DELETE FROM glossary …` → (target, SQL for the
/// store). The text is rendered from the AST with dollar-quoted literals
/// normalized to single quotes: the store speaks SQLite, which reads
/// `$tag$` as a bind parameter rather than a quote, so a dollar-quoted body
/// carrying a `;` used to arrive as a statement sequence (found
/// 2026-08-06). Single quotes render escaped by sqlparser and tokenize the
/// same in both dialects, which is what makes the round trip safe.
fn store_delete(statement: &DFStatement) -> Option<(String, String)> {
    let DFStatement::Statement(inner) = statement else {
        return None;
    };
    let SQLStatement::Delete(_) = inner.as_ref() else {
        return None;
    };
    let mut normalized = inner.as_ref().clone();
    let _ = visit_expressions_mut(&mut normalized, |expr| {
        if let Expr::Value(v) = expr
            && let SqlValue::DollarQuotedString(s) = &v.value
        {
            v.value = SqlValue::SingleQuotedString(s.value.clone());
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    let SQLStatement::Delete(delete) = &normalized else {
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
    (target == "glossary").then(|| (target, normalized.to_string()))
}
