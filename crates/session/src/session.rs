//! `Session`: the per-connection statement router.

use std::sync::{Arc, RwLock};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, ParamValues, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt as _;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, FromTable, ObjectType, Query, SetExpr, Statement as SQLStatement, TableFactor, Value as SqlValue,
    visit_expressions_mut, visit_relations,
};
use datafusion::sql::sqlparser::parser::ParserError;
use serde_json::Value;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, FunctionRow, RecipeAdmission, Store, schemas};
use glossql_import::SourceSpec;
use glossql_parser::{
    Declaration, Extract, Gloss, GlossqlParser, Probe, RelOp, Statement, Subject,
};

use crate::reads::{GlossqlReads, Shared};
use crate::subject::{Resolved, pair_subject, resolve_endpoint, resolve_path};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("parse: {0}")]
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
    #[error("DROP TABLE {table} refused: {reason} (replacement is postponed — declare under another name)")]
    DropRefused { table: String, reason: String },
    #[error("streaming takes exactly one query — everything else answers through execute")]
    NotOneRead,
}

/// What one statement produced. `Rows` for anything that reads, `Affected`
/// for forwarded deletes, `Done` for declarations and writes.
#[derive(Debug)]
pub enum Outcome {
    Done(String),
    Rows(Vec<RecordBatch>),
    Affected(u64),
}

/// The query capability every measurement invocation receives (SPEC.md §6 —
/// scripts run any SQL against the dataset). Sync because scripts are; the
/// session implements it over its context with the block-in-place bridge
/// the reads already use. Detectors get a door that refuses (§7.1).
///
/// Contract: an empty result still ships one empty batch carrying the
/// schema — the PROBE rule (§3), so a `LIMIT 0` through the door types
/// columns without scanning them.
pub trait SqlDoor: Send + Sync {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String>;

    /// Many statements, answered in order. The default runs them one by
    /// one; doors backed by a runtime overlap them — the script stays a
    /// sequential orchestrator, the fan-out lives below the seam
    /// (2026-08-06: v0.3 ran its pair scans on a thread pool; rhai has no
    /// threads, so the door carries the parallelism instead).
    fn sql_all(&self, queries: &[String]) -> Vec<Result<Vec<RecordBatch>, String>> {
        queries.iter().map(|q| self.sql(q)).collect()
    }
}

/// The seam scripts plug into (rhai + arrow kernels, `glossql-scripts`).
/// `context` is the document the server assembled from the function's
/// `ACCEPTS` aspects (SPEC.md §6) — or, for a detector, its slots and
/// threshold (§7.1). Tests inject fakes.
pub trait FunctionRuntime: Send + Sync + std::fmt::Debug {
    fn invoke(
        &self,
        function: &FunctionRow,
        subject: &str,
        context: &Value,
        door: Arc<dyn SqlDoor>,
    ) -> Result<Value, String>;
}

#[derive(Debug)]
pub struct NoRuntime;

impl FunctionRuntime for NoRuntime {
    fn invoke(
        &self,
        function: &FunctionRow,
        _: &str,
        _: &Value,
        _: Arc<dyn SqlDoor>,
    ) -> Result<Value, String> {
        Err(format!(
            "no function runtime configured — `{}` cannot run without scripts",
            function.name
        ))
    }
}

/// The session's own door: statements run against its context, so scripts
/// see the mounted lake tables, the derived views, and the read relations.
struct CtxDoor {
    ctx: SessionContext,
    handle: tokio::runtime::Handle,
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

impl SqlDoor for CtxDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        tokio::task::block_in_place(|| {
            self.handle.block_on(async {
                let df = self
                    .ctx
                    .sql_with_options(query, read_only())
                    .await
                    .map_err(|e| e.to_string())?;
                let schema = Arc::new(df.schema().as_arrow().clone());
                let mut batches = df.collect().await.map_err(|e| e.to_string())?;
                if batches.is_empty() {
                    batches.push(RecordBatch::new_empty(schema));
                }
                Ok(batches)
            })
        })
    }

    fn sql_all(&self, queries: &[String]) -> Vec<Result<Vec<RecordBatch>, String>> {
        // Waves of 4: enough overlap to hide per-query latency, small
        // enough that concurrent scans stay inside the process fd
        // budget. 16 crossed it (booksql run, 2026-08-07): every query
        // in a wave scans in parallel and a parquet scan holds around
        // target_partitions files open, so 16 dataset-grain queries
        // peaked the process past the macOS launchd soft limit of 256
        // (sampled 22→236→32 across one burst) and the sweep died on
        // "Too many open files". 4 concurrent scans leave that headroom
        // without serializing the batch.
        tokio::task::block_in_place(|| {
            self.handle.block_on(async {
                let mut out = Vec::with_capacity(queries.len());
                for wave in queries.chunks(4) {
                    let mut handles = Vec::with_capacity(wave.len());
                    for q in wave {
                        let ctx = self.ctx.clone();
                        let q = q.clone();
                        handles.push(tokio::spawn(async move {
                            let df = ctx
                                .sql_with_options(&q, read_only())
                                .await
                                .map_err(|e| e.to_string())?;
                            let schema = Arc::new(df.schema().as_arrow().clone());
                            let mut batches =
                                df.collect().await.map_err(|e| e.to_string())?;
                            if batches.is_empty() {
                                batches.push(RecordBatch::new_empty(schema));
                            }
                            Ok(batches)
                        }));
                    }
                    for h in handles {
                        out.push(h.await.unwrap_or_else(|e| Err(e.to_string())));
                    }
                }
                out
            })
        })
    }
}

pub struct Session {
    ctx: SessionContext,
    shared: Arc<Shared>,
    actor: Actor,
    /// How many rows the reader will actually be shown. It bounds what the
    /// non-streaming paths ask the engine for; `usize::MAX` (the default)
    /// means the caller drains everything itself.
    row_cap: usize,
}

impl Session {
    /// Must be called inside a multi-thread tokio runtime — read planning
    /// blocks in place on store queries.
    pub fn new(store: Store, actor: Actor) -> Result<Self, SessionError> {
        let shared = Arc::new(Shared {
            store,
            dataset: RwLock::new(None),
            handle: tokio::runtime::Handle::current(),
            lake: RwLock::new(None),
            runtime: RwLock::new(Arc::new(NoRuntime)),
            read_cache: RwLock::new(None),
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
            .with_relation_planners(vec![Arc::new(GlossqlReads {
                shared: Arc::clone(&shared),
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
        })
    }

    /// The door's row cap, so the engine is not asked for rows nobody will
    /// see (probes and statement sequences answer through `execute`).
    pub fn with_row_cap(mut self, cap: usize) -> Self {
        self.row_cap = cap;
        self
    }

    pub fn with_runtime(self, runtime: Arc<dyn FunctionRuntime>) -> Self {
        *self.shared.runtime.write().expect("runtime lock") = runtime;
        self
    }

    /// Attach the workspace data plane: recipes materialize, `USE` mounts
    /// the dataset's tables, gloss and cache writes carry snapshot ids.
    pub fn with_lake(self, lake: Lake) -> Self {
        *self.shared.lake.write().expect("lake lock") = Some(lake);
        self
    }

    fn lake(&self) -> Option<Lake> {
        self.shared.lake()
    }

    fn door(&self) -> CtxDoor {
        CtxDoor {
            ctx: self.ctx.clone(),
            handle: self.shared.handle.clone(),
        }
    }

    /// Data-plane tables come from recipes at M3; until then (and in tests)
    /// they are registered directly.
    pub fn register_table(
        &self,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), SessionError> {
        self.ctx.register_table(name, provider)?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<Outcome>, SessionError> {
        self.execute_statements(GlossqlParser::parse_sql(sql)?).await
    }

    /// The statement loop over parsed statements — the plane's channel
    /// router feeds it the runs between `USE`s.
    pub(crate) async fn execute_statements(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<Outcome>, SessionError> {
        let mut outcomes = Vec::with_capacity(statements.len());
        for statement in statements {
            outcomes.push(match statement {
                Statement::Declare(d) => self.declare(*d).await?,
                Statement::Use(u) => self.use_dataset(&u.dataset.value).await?,
                Statement::Gloss(g) => self.gloss(g).await?,
                Statement::Extract(e) => self.extract(e).await?,
                Statement::Probe(p) => self.probe(p).await?,
                Statement::Substrate(s) => self.substrate(*s).await?,
            });
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
                if let Some(lake) = self.lake() {
                    lake.ensure_namespace(&d.name.value).await?;
                    self.mount_schema(&d.name.value).await?;
                }
                format!("DECLARE DATASET {}", d.name.value)
            }
            Declaration::Recipe(d) => {
                let admission = store.recipe_admission(d).await?;
                let (dataset, table) = (d.dataset.value.as_str(), d.table.value.as_str());
                match self.lake() {
                    None => {
                        store.put_recipe(d).await?;
                        format!("DECLARE RECIPE {table} ON {dataset}")
                    }
                    Some(lake)
                        if admission == RecipeAdmission::Unchanged
                            && lake.table_exists(dataset, table).await? =>
                    {
                        format!("DECLARE RECIPE {table} ON {dataset} (unchanged)")
                    }
                    Some(lake) => {
                        // Supersede-and-reland (ruled 2026-08-06): a changed
                        // recipe drops the old landing and its cached
                        // evidence, then lands fresh. Glosses stay — the
                        // snapshot id discloses their age.
                        //
                        // The new recipe runs *first*: until its SQL has
                        // produced batches there is nothing to replace the
                        // old landing with, and a recipe that errors must
                        // not have destroyed the table it was replacing.
                        let replaced = admission == RecipeAdmission::Replaced
                            && lake.table_exists(dataset, table).await?;
                        let landed =
                            glossql_import::run_recipe(&self.source_spec(&d.source.value).await?, &d.sql)
                                .await?;
                        if replaced {
                            let mounted = self.mount_schema(dataset).await?;
                            mounted.deregister_table(table)?;
                            self.shared
                                .store
                                .invalidate_table_evidence(dataset, table)
                                .await?;
                        }
                        let (rows, dropped, casts) =
                            self.materialize(dataset, table, landed).await?;
                        store.put_recipe(d).await?;
                        // The counts arrive at the decision moment: whether
                        // the dropped rows — and the cells the casts nulled
                        // — are acceptable is the author's call, made now.
                        let verb = if replaced { "superseded and re-landed: " } else { "" };
                        format!(
                            "DECLARE RECIPE {table} ON {dataset} ({verb}{rows} rows landed, {dropped} dropped{casts})"
                        )
                    }
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
        if let Some(lake) = self.lake() {
            // A dataset declared while no lake was attached has no
            // namespace yet; creating it here keeps `USE` self-healing.
            lake.ensure_namespace(name).await?;
            self.mount_schema(name).await?;
            self.ctx
                .state_ref()
                .write()
                .config_mut()
                .options_mut()
                .catalog
                .default_schema = name.to_string();
        }
        *self.shared.dataset.write().expect("state lock") = Some(name.to_string());
        *self.shared.read_cache.write().expect("read cache") = None;
        Ok(())
    }

    /// Land what a recipe produced as its table: create the table through
    /// the mounted schema (live — no rebuild), append the batches through
    /// DataFusion's INSERT path, one snapshot per materialization. The
    /// recipe already ran at its source; the caller holds the result.
    async fn materialize(
        &self,
        dataset: &str,
        table: &str,
        landed: glossql_import::Landed,
    ) -> Result<(usize, u64, String), SessionError> {
        // The doors cannot guarantee statement order (M3 report), so the
        // staged name is unique per materialization, never per session.
        static STAGED_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let staged = format!(
            "__glossql_staged_{}",
            STAGED_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let lake = self.lake().expect("caller holds a lake");
        let rows: usize = landed.batches.iter().map(|b| b.num_rows()).sum();
        let dropped = landed.source_rows.saturating_sub(rows as u64);

        lake.ensure_namespace(dataset).await?;
        let mounted = self.mount_schema(dataset).await?;
        let empty = RecordBatch::new_empty(Arc::clone(&landed.schema));
        let shape = MemTable::try_new(Arc::clone(&landed.schema), vec![vec![empty]])?;
        mounted.register_table(table.to_string(), Arc::new(shape))?;

        if rows > 0 {
            let batches = MemTable::try_new(Arc::clone(&landed.schema), vec![landed.batches])?;
            // Qualified into the session's staging schema: a bound
            // session's bare names resolve into the dataset's schema,
            // where a registration would create a real lake table.
            let staged_ref = TableReference::partial("glossql_stage", staged.as_str());
            self.ctx
                .register_table(staged_ref.clone(), Arc::new(batches))?;
            let insert = format!(
                "INSERT INTO \"{dataset}\".\"{table}\" SELECT * FROM glossql_stage.{staged}"
            );
            let inserted = async {
                self.ctx.sql(&insert).await?.collect().await?;
                Ok::<(), DataFusionError>(())
            }
            .await;
            let _ = self.ctx.deregister_table(staged_ref);
            inserted?;
        }
        self.shared
            .store
            .import_put(
                dataset,
                table,
                landed.source_rows as i64,
                rows as i64,
                &landed.casts.to_json().to_string(),
            )
            .await?;
        *self.shared.read_cache.write().expect("read cache") = None;
        Ok((rows, dropped, cast_summary(&landed.casts)))
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
        let settings = self.shared.store.source_settings(source).await?.ok_or(
            SessionError::Store(glossql_glossary::Error::Unknown {
                what: "source",
                name: source.into(),
            }),
        )?;
        Ok(SourceSpec::from_settings(source, &settings)?)
    }

    /// The dataset's namespace as a schema in the session's default catalog
    /// — `fin.orders` resolves; after [`Session::bind`] it is also the
    /// default schema. The schema is an Arc clone of the lake's shared
    /// provider's, never a per-session build; a miss rebuilds the shared
    /// provider once, in case it predates the namespace.
    async fn mount_schema(&self, dataset: &str) -> Result<Arc<dyn SchemaProvider>, SessionError> {
        let default = self.ctx.catalog("datafusion").expect("default catalog");
        if let Some(existing) = default.schema(dataset) {
            return Ok(existing);
        }
        let lake = self.lake().expect("caller holds a lake");
        let mut schema = lake.provider().await?.schema(dataset);
        if schema.is_none() {
            lake.invalidate_provider();
            schema = lake.provider().await?.schema(dataset);
        }
        let schema = schema.ok_or_else(|| {
            SessionError::Lake(glossql_catalog::Error::Workspace(format!(
                "namespace `{dataset}` is missing from the catalog"
            )))
        })?;
        default.register_schema(dataset, Arc::clone(&schema))?;
        Ok(schema)
    }

    /// The subject's table snapshot at write time — `None` for dataset-level
    /// subjects, pair paths, tables the lake does not hold, or no lake.
    async fn stamp(&self, resolved: &Resolved) -> Result<Option<i64>, SessionError> {
        let Some(lake) = self.lake() else {
            return Ok(None);
        };
        if resolved.subject == resolved.dataset || resolved.subject.contains(' ') {
            return Ok(None);
        }
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

    /// Extraction (SPEC.md §6): first run computes and caches, later runs
    /// read the cache; re-running is `DELETE FROM cache WHERE …`. The
    /// context document holds one entry per `ACCEPTS` aspect: the nearest
    /// value walking up from the subject (subject, parent, dataset), null
    /// when nothing is glossed.
    async fn extract(&self, extract: Extract) -> Result<Outcome, SessionError> {
        let store = self.shared.store.clone();
        let resolved = self.subject(&extract.subject).await?;
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
            let cached = store
                .cache_get(&resolved.dataset, &resolved.subject, &name, None)
                .await?;
            let row = match cached {
                Some(row) => row,
                None => {
                    let mut context = serde_json::Map::new();
                    for aspect in &function.accepts {
                        // A declaration relation in ACCEPTS is an
                        // invalidation edge only (ruled 2026-08-05): the
                        // script reads it as a table, no context entry.
                        if glossql_glossary::accepts_relation(aspect) {
                            continue;
                        }
                        let value =
                            context_value(&store, &resolved.dataset, &resolved.subject, aspect)
                                .await?;
                        context.insert(aspect.clone(), value);
                    }
                    let context = Value::Object(context);
                    let output = self
                        .shared
                        .runtime()
                        .invoke(
                            &function,
                            &resolved.subject,
                            &context,
                            Arc::new(self.door()),
                        )
                        .map_err(SessionError::Runtime)?;
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
                    glossql_glossary::admit_grain(
                        &returns,
                        grains.as_deref(),
                        &resolved.dataset,
                        &resolved.subject,
                    )
                    .map_err(SessionError::Store)?;
                    schemas::validate_instance(&schema, &output).map_err(|detail| {
                        SessionError::OutputRejected {
                            function: name.clone(),
                            detail,
                        }
                    })?;
                    let snapshot = self.stamp(&resolved).await?;
                    store
                        .cache_put(
                            &resolved.dataset,
                            &resolved.subject,
                            &name,
                            None,
                            &output.to_string(),
                            snapshot,
                        )
                        .await?;
                    store
                        .cache_get(&resolved.dataset, &resolved.subject, &name, None)
                        .await?
                        .ok_or_else(|| {
                            SessionError::Runtime(format!(
                                "`{name}` wrote a value that was invalidated before it could be \
                                 read — check what it ACCEPTS"
                            ))
                        })?
                }
            };
            results.push(row);
        }
        Ok(Outcome::Rows(vec![crate::reads::extraction_batch(results)]))
    }

    /// One query, streaming (project lead, 2026-08-04): batches flow as
    /// the engine produces them — memory rides one batch, and a dropped
    /// stream cancels the work upstream. `GLOSSARY()`/`ATTEST()` stream
    /// like any read; the planner did its part before the first batch.
    /// Anything that is not exactly one query refuses with
    /// [`SessionError::NotOneRead`] and belongs in [`Session::execute`].
    /// The result says whether the query reads only the store's
    /// relations — the doors' cap policy wants to know.
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
        let metadata_only = reads_only_metadata(&statement);
        let mut plan = self.ctx.state().statement_to_plan(*statement).await?;
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
                SQLStatement::Drop { object_type, names, .. }
                    if *object_type == ObjectType::Table && names.len() == 1 =>
                {
                    let name = names[0].to_string();
                    return self.drop_table(&name).await;
                }
                other => return Err(SessionError::SubstrateClosed(verb_of(other))),
            }
        }
        let plan = self.ctx.state().statement_to_plan(statement).await?;
        let frame = self.ctx.execute_logical_plan(plan).await?;
        // Bounded like the streaming door, for the same reason: the reader
        // sees at most its cap, so the engine should not be asked for more
        // than that. One row past the cap is kept, which is how the door
        // knows the answer was truncated (found 2026-08-06: this path used
        // to collect the whole result and trim it at render).
        let mut stream = frame.execute_stream().await?;
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
        Ok(Outcome::Rows(batches))
    }

    /// `DROP TABLE` (PoC rules, project lead 2026-08-04): refused while the
    /// table holds data or glosses — replacement is postponed, so this only
    /// ever removes a mis-declared table. What it removes, it removes
    /// whole: the lake table, the recipe row, the cached evidence, the
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
        let Some(lake) = self.lake() else {
            return Err(SessionError::BadSubject(format!(
                "no lake — nothing to drop for `{table}`"
            )));
        };
        if !lake.table_exists(&dataset, table).await? {
            return Err(SessionError::Store(glossql_glossary::Error::Unknown {
                what: "table",
                name: table.into(),
            }));
        }
        let rows = self.door().sql(&format!("SELECT count(*) FROM \"{dataset}\".\"{table}\""));
        let has_data = match rows {
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
        self.shared.store.drop_table_records(&dataset, table).await?;
        *self.shared.read_cache.write().expect("read cache") = None;
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
    store: &glossql_glossary::Store,
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
        let rows = store
            .collapsed_read(dataset, &scope, Some(aspect), &Default::default())
            .await?;
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
        other => format!("{other}").split_whitespace().take(2).collect::<Vec<_>>().join(" "),
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

/// `DELETE FROM glossary … | DELETE FROM cache …` → (target, SQL for the
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
    (target == "glossary" || target == "cache").then(|| (target, normalized.to_string()))
}
