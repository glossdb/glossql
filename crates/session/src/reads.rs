//! `GLOSSARY()` / `ATTEST()` and the `glossary` / `cache` relations, planned
//! through DataFusion's `RelationPlanner` seam. The planner sees the raw
//! `TableFactor` before default planning, so named arguments (`all => true`),
//! zero-argument sweeps, and pair paths (`a.b <-> c.d`) all decode here —
//! structurally, from the sqlparser AST, which is also why the JSON `->`
//! operator (datafusion-functions-json) never collides with pair paths:
//! inside these factors `->` never reaches expression planning.

use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, Expr as SQLExpr, FunctionArg, FunctionArgExpr,
    TableAlias, TableFactor, Value as SQLValue,
};

use glossql_catalog::Lake;
use glossql_glossary::{AttestRow, CollapsedRow, RawRow, ReadContext, Scope, Store, schemas};
use serde_json::{Value, json};

use crate::session::{FunctionRuntime, SessionError, SqlDoor};
use crate::subject::{pair_subject, resolve_column_endpoint, resolve_path};

/// State the planner shares with the router: the `USE`'d dataset, the data
/// plane, and the script runtime (reads run detectors).
pub(crate) struct Shared {
    pub store: Store,
    pub dataset: RwLock<Option<String>>,
    pub handle: tokio::runtime::Handle,
    pub runtime: RwLock<Arc<dyn FunctionRuntime>>,
    /// The session's own context, set right after construction (the planner
    /// is built before the context exists). The metric bind plans each
    /// grounding through it as its own statement — `statement_to_plan`
    /// collects table references per statement, so a grounding's tables
    /// resolve even when the outer statement never names them.
    pub ctx: RwLock<Option<SessionContext>>,
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
    pub fn lake(&self) -> Lake {
        self.store.lake()
    }

    pub fn runtime(&self) -> Arc<dyn FunctionRuntime> {
        Arc::clone(&self.runtime.read().expect("runtime lock"))
    }

    /// What the store cannot know (SPEC.md §5.3): the subjects that exist —
    /// the recipe tables and their columns — and each table's current
    /// snapshot. Rebuilt per read from the catalog, so every channel sees
    /// a landing the moment it committed; the disclosure grid and the
    /// staleness comparison ride on this.
    pub async fn read_context(&self) -> Result<ReadContext, SessionError> {
        let mut ctx = ReadContext::default();
        let Some(dataset) = self.dataset.read().expect("state lock").clone() else {
            return Ok(ctx);
        };
        let lake = self.lake();
        for table in lake.table_names(&dataset).await? {
            if let Some(snapshot) = lake.snapshot_id(&dataset, &table).await? {
                ctx.snapshots.insert(table.clone(), snapshot);
            }
            for column in lake.table_columns(&dataset, &table).await? {
                ctx.universe.push(format!("{table}.{column}"));
            }
            ctx.universe.push(table);
        }
        Ok(ctx)
    }
}

/// What a detector gets instead of a SQL door: a refusal (SPEC.md §7.1 — a
/// detector receives the witness's slots and threshold, never table data).
struct DeniedDoor;

impl SqlDoor for DeniedDoor {
    fn sql(&self, _query: &str) -> Result<Vec<RecordBatch>, String> {
        Err("a detector sees slots and threshold, never table data (SPEC.md §7.1)".into())
    }
}

/// Detector freshness at read (project lead, 2026-08-04): a verdict missing
/// or older than the newest slot write recomputes here, is cached like any
/// function result, and `DELETE FROM cache` still forces it.
pub(crate) async fn ensure_verdicts(
    shared: &Shared,
    dataset: &str,
    scope: &Scope,
    aspect: Option<&str>,
) -> Result<(), SessionError> {
    for w in shared.store.witnesses_all().await? {
        if let Some(a) = aspect
            && w.aspect != a
        {
            continue;
        }
        let Some(detector) = w.detector.clone() else {
            continue;
        };
        let slots = shared
            .store
            .raw_read(dataset, scope, Some(&w.aspect))
            .await?;
        let mut newest: std::collections::BTreeMap<&str, &str> = Default::default();
        for s in &slots {
            let t = newest.entry(s.subject.as_str()).or_insert(&s.written_at);
            if s.written_at.as_str() > *t {
                *t = &s.written_at;
            }
        }
        for (subject, newest) in newest {
            // Keyed by the witness, not by the detector alone: the same
            // detector serving `role` and `behavior` computes a verdict for
            // each, from different slots against a different threshold
            // (defect found 2026-08-06 — one row was answering for both).
            let fresh = shared
                .store
                .cache_get(dataset, subject, &detector, Some(&w.name))
                .await?
                .is_some_and(|c| c.computed_at.as_str() >= newest);
            if fresh {
                continue;
            }
            let function = shared
                .store
                .function(&detector, Some(dataset))
                .await?
                .ok_or_else(|| SessionError::UnknownFunction(detector.clone()))?;
            let doc: Vec<Value> = slots
                .iter()
                .filter(|s| s.subject == subject)
                .map(|s| {
                    json!({
                        "speaker": s.speaker,
                        "actor": s.actor,
                        "body": serde_json::from_str::<Value>(&s.body)
                            .unwrap_or_else(|_| Value::String(s.body.clone())),
                        "written_at": s.written_at,
                    })
                })
                .collect();
            let context = json!({
                "subject": subject,
                "aspect": w.aspect,
                "witness": w.name,
                "slots": doc,
                "threshold": w.threshold,
            });
            let output = shared
                .runtime()
                .invoke(&function, subject, &context, Arc::new(DeniedDoor))
                .map_err(SessionError::Runtime)?;
            // A detector's output answers to the engine's attest contract
            // (SPEC.md §7.2) — role by shape, nothing authored.
            schemas::validate_instance(&schemas::attest_contract(), &output).map_err(|detail| {
                SessionError::OutputRejected {
                    function: detector.clone(),
                    detail,
                }
            })?;
            let snapshot = match glossary_table_of(subject) {
                Some(table) => shared.lake().snapshot_id(dataset, table).await?,
                None => None,
            };
            shared
                .store
                .cache_put(
                    dataset,
                    subject,
                    &detector,
                    Some(&w.name),
                    &output.to_string(),
                    snapshot,
                )
                .await?;
        }
    }
    Ok(())
}

/// The subject's table: its first path segment; pair paths have none.
fn glossary_table_of(subject: &str) -> Option<&str> {
    if subject.contains(' ') {
        return None;
    }
    subject.split('.').next()
}

#[derive(Debug)]
pub(crate) struct GlossqlReads {
    pub shared: Arc<Shared>,
    /// Everything the pre-pass resolved for THIS statement. Immutable,
    /// and never shared between statements — which is why concurrent
    /// reads on one session need no lock (`sql_all` runs four at once).
    pub resolved: Arc<crate::prepass::Resolved>,
}

/// A session state for one statement, carrying that statement's resolved
/// doors. Building from the existing state keeps the config, the
/// catalogs and the registered functions; only the planner differs.
pub(crate) fn state_with(
    ctx: &datafusion::prelude::SessionContext,
    shared: &Arc<Shared>,
    resolved: crate::prepass::Resolved,
) -> datafusion::execution::SessionState {
    datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state())
        .with_relation_planners(vec![Arc::new(GlossqlReads {
            shared: Arc::clone(shared),
            resolved: Arc::new(resolved),
        })])
        .build()
}

impl RelationPlanner for GlossqlReads {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> DFResult<RelationPlanning> {
        let TableFactor::Table {
            name, alias, args, ..
        } = &relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        // `read.<aspect>()` — the serve door, one generic prefix over
        // every QUERY gloss (value-at-read ruled 2026-08-06, bound
        // 2026-08-07; renamed from `metric.` 2026-08-11 — serving
        // declared SQL is one operation whatever flavor sits behind it,
        // the flavor lives in `x-kind`): the aspect's collapsed current
        // QUERY grounding expanded as a derived relation through the
        // full planner pipeline, so WHERE/GROUP BY compose around it and
        // a nested `read.` inside a recorded evaluation re-enters this
        // planner. v0.3's formula composer substituted each operand as a
        // scalar subquery; here the engine is the composer — the
        // substitution is this expansion. No script, no cache, no
        // parameters.
        if name.0.len() == 2
            && name.0[0]
                .as_ident()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("read"))
        {
            let (Some(aspect), Some(a)) = (name.0[1].as_ident().map(|i| i.value.clone()), args)
            else {
                return Ok(RelationPlanning::Original(Box::new(relation)));
            };
            if !a.args.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "read.{aspect}() takes no arguments — filters ride WHERE"
                )));
            }
            return self.planned(&format!("read.{aspect}"), alias.clone());
        }
        // `whatif.<scenario>()` — the scenario door (ruled 2026-08-11,
        // fixture 19): one operation-named prefix beside `read.`, serving
        // a declared scenario as bands over recipe replay. Computed at
        // plan time behind the cache, exactly as detector verdicts are.
        if name.0.len() == 2
            && name.0[0]
                .as_ident()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("whatif"))
        {
            let (Some(scenario), Some(a)) = (name.0[1].as_ident().map(|i| i.value.clone()), args)
            else {
                return Ok(RelationPlanning::Original(Box::new(relation)));
            };
            if !a.args.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "whatif.{scenario}() takes no arguments — the scenario body carries the \
                     overrides (fixture 19)"
                )));
            }
            let batch = self.run(crate::whatif::whatif_batch(&self.shared, &scenario))?;
            let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
            let plan = LogicalPlanBuilder::scan(
                format!("whatif.{scenario}()"),
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?;
            return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan,
                alias.clone(),
            ))));
        }
        // `misfit.<frame>()` — the ranking door (ruled 2026-08-11,
        // fixture 20): a declared sample frame served back with a
        // per-row misfit score from the density kernel. Computed at
        // plan time like the other doors; never cached — the ranking
        // is ephemeral by design, the judge's gloss is the record.
        if name.0.len() == 2
            && name.0[0]
                .as_ident()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("misfit"))
        {
            let (Some(frame), Some(a)) = (name.0[1].as_ident().map(|i| i.value.clone()), args)
            else {
                return Ok(RelationPlanning::Original(Box::new(relation)));
            };
            if !a.args.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "misfit.{frame}() takes no arguments — the frame is the aspect's \
                     grounding (fixture 20)"
                )));
            }
            let batch = self.run(crate::misfit::misfit_batch(&self.shared, &frame))?;
            let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
            let plan = LogicalPlanBuilder::scan(
                format!("misfit.{frame}()"),
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?;
            return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan,
                alias.clone(),
            ))));
        }
        if name.0.len() != 1 {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        let Some(fname) = name.0[0].as_ident().map(|i| i.value.to_lowercase()) else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let batch = match (fname.as_str(), args) {
            ("glossary", Some(a)) => self.run(glossary_read(&self.shared, &a.args))?,
            ("attest", Some(a)) => self.run(attest_read(&self.shared, &a.args))?,
            // `metric_series()` — the cube read (2026-08-13): the cached
            // `metric_cube` measurement flattened to long rows, so a
            // static frame slices any metric with plain value filters.
            ("metric_series", Some(a)) => {
                if !a.args.is_empty() {
                    return Err(DataFusionError::Plan(
                        "metric_series() takes no arguments — filters ride WHERE".into(),
                    ));
                }
                self.run(metric_series_read(&self.shared))?
            }
            // The store's relations, readable as plain tables; snapshot at
            // plan time, like every other read here. Which names qualify
            // lives in one place: the store's RELATIONS table.
            (name, None) if glossql_glossary::relation_columns(name).is_some() => {
                let table = fname.clone();
                self.run(async {
                    let rows = self.shared.store.relation_rows(&table).await?;
                    Ok(relation_batch(&table, rows))
                })?
            }
            // A shipped read (`crates/session/reads/*.sql`): a bare
            // relation whose body is SQL, expanded through the same
            // path a served grounding takes. Checked last, and a name
            // we do not ship falls through untouched — but a name we
            // do ship shadows a workspace table of that name, exactly
            // as the store's relations already do. The shipped names
            // are the reserved surface; keep them few and specific.
            (name, None) => match crate::library::read_sql(name) {
                Some(_) => return self.planned(&format!("read:{fname}"), alias.clone()),
                None => return Ok(RelationPlanning::Original(Box::new(relation))),
            },
            _ => return Ok(RelationPlanning::Original(Box::new(relation))),
        };

        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        let plan = LogicalPlanBuilder::scan(
            format!("{fname}()"),
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            alias.clone(),
        ))))
    }
}

impl GlossqlReads {
    /// Planning is sync; the store is async. Callers run inside the session's
    /// multi-thread runtime, so blocking in place is safe.
    fn run(
        &self,
        fut: impl Future<Output = Result<RecordBatch, SessionError>>,
    ) -> DFResult<RecordBatch> {
        tokio::task::block_in_place(|| self.shared.handle.block_on(fut))
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// A door whose body is SQL: the pre-pass planned it before the
    /// planner ran, so this is a lookup. Nothing here fetches, blocks or
    /// re-enters — which is why there is no expansion stack any more.
    ///
    /// A miss means the pre-pass did not see this reference. That can
    /// only happen if its traversal missed a position the planner
    /// reaches, so it says so rather than falling back to fetching.
    fn planned(&self, key: &str, alias: Option<TableAlias>) -> DFResult<RelationPlanning> {
        let plan = self.resolved.plan(key).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "`{key}` was not resolved before planning — the pre-pass missed this reference"
            ))
        })?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            (*plan).clone(),
            alias,
        ))))
    }
}

/// What a `read.<aspect>()` may expand: a QUERY aspect with a current
/// collapsed grounding on the `USE`'d dataset — human outranking agent,
/// so a pinned definition is literally what runs.
pub(crate) async fn served_grounding(shared: &Shared, aspect: &str) -> Result<String, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let Some((_, kind, _)) = shared.store.aspect(aspect).await? else {
        return Err(SessionError::BadSubject(format!(
            "read.{aspect}(): no aspect `{aspect}` is declared"
        )));
    };
    if kind != "query" {
        return Err(SessionError::BadSubject(format!(
            "read.{aspect}(): `{aspect}` is a {kind} aspect — GLOSSARY() reads it; \
             read. serves QUERY groundings"
        )));
    }
    let scope = Scope::Subject(dataset.clone());
    ensure_verdicts(shared, &dataset, &scope, Some(aspect)).await?;
    let rows = shared
        .store
        .collapsed_read(
            &dataset,
            &scope,
            Some(aspect),
            &shared.read_context().await?,
        )
        .await?;
    let row = rows
        .into_iter()
        .find(|r| r.subject == dataset && r.aspect == aspect && r.state != "unassessed");
    let Some(row) = row else {
        return Err(SessionError::BadSubject(format!(
            "read.{aspect}(): no current grounding on `{dataset}` — a derived metric's \
             definition stays in the formulas gloss until an evaluation is recorded"
        )));
    };
    if row.state != "current" {
        return Err(SessionError::BadSubject(format!(
            "read.{aspect}(): the grounding on `{dataset}` is {}",
            row.state
        )));
    }
    let value = row.value.ok_or_else(|| {
        SessionError::BadSubject(format!(
            "read.{aspect}(): the current grounding carries no value"
        ))
    })?;
    let body: Value = serde_json::from_str(&value).map_err(|e| {
        SessionError::BadSubject(format!("read.{aspect}(): the grounding is not JSON: {e}"))
    })?;
    body["sql"].as_str().map(str::to_string).ok_or_else(|| {
        SessionError::BadSubject(format!("read.{aspect}(): the grounding carries no `sql`"))
    })
}

// -- argument decoding ---------------------------------------------------

async fn glossary_read(shared: &Shared, args: &[FunctionArg]) -> Result<RecordBatch, SessionError> {
    let (subject, all) = split_args(args, true)?;
    let ((dataset, scope), aspect) = decode_scope(shared, subject).await?;
    let aspect = aspect.as_deref();
    if all {
        Ok(raw_batch(
            shared.store.raw_read(&dataset, &scope, aspect).await?,
        ))
    } else {
        ensure_verdicts(shared, &dataset, &scope, aspect).await?;
        Ok(collapsed_batch(
            shared
                .store
                .collapsed_read(&dataset, &scope, aspect, &shared.read_context().await?)
                .await?,
        ))
    }
}

/// The cube flattened: `(metric, dimension, member, period, value)`.
/// Dimension `''` is the monthly total, `'alternative'` the disclosed
/// rival reading, anything else a served dimension column with its
/// member in `member`. Cached-only by design — an empty relation means
/// the measurement has not run (`SELECT metric_cube() FROM <dataset>`),
/// the same honesty the bands tile keeps; nothing computes at page
/// load.
async fn metric_series_read(shared: &Shared) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let mut rows: Vec<(String, String, String, String, f64)> = Vec::new();
    if let Some(cached) = shared
        .store
        .cache_get(&dataset, &dataset, "metric_cube", None)
        .await?
    {
        let body: Value = serde_json::from_str(&cached.body).map_err(|e| {
            SessionError::BadSubject(format!("metric_series(): the cached cube is not JSON: {e}"))
        })?;
        for m in body["metrics"].as_array().into_iter().flatten() {
            let Some(metric) = m["metric"].as_str() else {
                continue;
            };
            for r in m["rows"].as_array().into_iter().flatten() {
                let (Some(dim), Some(member), Some(period), Some(value)) = (
                    r[0].as_str(),
                    r[1].as_str(),
                    r[2].as_str(),
                    r[3].as_f64(),
                ) else {
                    continue;
                };
                rows.push((
                    metric.to_string(),
                    dim.to_string(),
                    member.to_string(),
                    period.to_string(),
                    value,
                ));
            }
        }
    }
    let schema = Arc::new(Schema::new(vec![
        utf8("metric"),
        utf8("dimension"),
        utf8("member"),
        utf8("period"),
        Field::new("value", DataType::Float64, false),
    ]));
    Ok(batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.0.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.1.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.2.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.3.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.4))),
        ],
    ))
}

async fn attest_read(shared: &Shared, args: &[FunctionArg]) -> Result<RecordBatch, SessionError> {
    let (subject, _) = split_args(args, false)?;
    let ((dataset, scope), aspect) = decode_scope(shared, subject).await?;
    ensure_verdicts(shared, &dataset, &scope, aspect.as_deref()).await?;
    Ok(attest_batch(
        shared
            .store
            .attest_read(&dataset, &scope, aspect.as_deref())
            .await?,
    ))
}

/// Split a read's argument list into (optional subject, `all` flag).
fn split_args(
    args: &[FunctionArg],
    allow_all: bool,
) -> Result<(Option<&SQLExpr>, bool), SessionError> {
    let mut subject = None;
    let mut all = false;
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) if subject.is_none() => {
                subject = Some(e);
            }
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(n),
                arg: FunctionArgExpr::Expr(v),
                ..
            } if allow_all && n.value.eq_ignore_ascii_case("all") && is_true(v) => {
                all = true;
            }
            FunctionArg::Named { name, arg, .. }
                if allow_all
                    && name.value.eq_ignore_ascii_case("all")
                    && matches!(arg, FunctionArgExpr::Expr(v) if is_true(v)) =>
            {
                all = true;
            }
            other => {
                return Err(SessionError::BadSubject(format!(
                    "unsupported read argument `{other}`"
                )));
            }
        }
    }
    Ok((subject, all))
}

fn is_true(e: &SQLExpr) -> bool {
    matches!(e, SQLExpr::Value(v) if v.value == SQLValue::Boolean(true))
}

/// Decode a read subject into ((dataset, scope), aspect-filter). No subject
/// sweeps the `USE`'d dataset. `subject::aspect` narrows the read to one
/// declared aspect — the postgres cast spelling, so it arrives as
/// `Expr::Cast` and never collides with path segments.
async fn decode_scope(
    shared: &Shared,
    subject: Option<&SQLExpr>,
) -> Result<((String, Scope), Option<String>), SessionError> {
    let use_dataset = shared.dataset.read().expect("state lock").clone();
    let use_dataset = use_dataset.as_deref();
    let store = &shared.store;
    let Some(expr) = subject else {
        let dataset = use_dataset.ok_or(SessionError::NoDataset)?;
        return Ok(((dataset.to_string(), Scope::Dataset), None));
    };

    // `subject::aspect` — on the whole subject (`(a.b <-> c.d)::x` included).
    let (expr, aspect) = match expr {
        SQLExpr::Cast {
            expr: inner,
            data_type,
            ..
        } => (unnest(inner), Some(aspect_name(store, data_type).await?)),
        other => (other, None),
    };

    if let Some(segments) = path_segments(expr) {
        // A bare aspect name is a common mistake (`GLOSSARY(dso)`): it would
        // silently read an empty table named like the aspect.
        if let [only] = segments.as_slice()
            && aspect.is_none()
            && !store.dataset_exists(only).await?
            && store.aspect(only).await?.is_some()
        {
            return Err(SessionError::BadSubject(format!(
                "`{only}` names an aspect, not a subject — read it as `subject::{only}`"
            )));
        }
        let resolved = resolve_path(store, use_dataset, &segments).await?;
        return Ok(((resolved.dataset.clone(), resolved.scope()), aspect));
    }

    if let SQLExpr::BinaryOp { left, op, right } = expr
        && let Some(op) = rel_op(op)
    {
        // `::` binds tighter than `<->`, so `a.b <-> c.d::x` carries the
        // aspect on the right endpoint; it belongs to the pair.
        let (right, aspect) = match (right.as_ref(), aspect) {
            (
                SQLExpr::Cast {
                    expr: inner,
                    data_type,
                    ..
                },
                None,
            ) => (unnest(inner), Some(aspect_name(store, data_type).await?)),
            (other, aspect) => (other, aspect),
        };
        let left_segments = path_segments(left)
            .ok_or_else(|| SessionError::BadSubject(format!("`{left}` in a pair path")))?;
        let right_segments = path_segments(right)
            .ok_or_else(|| SessionError::BadSubject(format!("`{right}` in a pair path")))?;
        let l = resolve_column_endpoint(store, use_dataset, &left_segments).await?;
        let r = resolve_column_endpoint(store, use_dataset, &right_segments).await?;
        let pair = pair_subject(&l, op, &r);
        return Ok(((l.dataset, Scope::Subject(pair)), aspect));
    }

    Err(SessionError::BadSubject(format!(
        "`{expr}` is not a subject"
    )))
}

/// The `::aspect` name: a bare custom "type" naming a declared aspect.
async fn aspect_name(store: &Store, data_type: &SQLDataType) -> Result<String, SessionError> {
    let SQLDataType::Custom(name, _) = data_type else {
        return Err(SessionError::BadSubject(format!(
            "`::{data_type}` — the part after `::` must name a declared aspect"
        )));
    };
    let ident = match name.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
    };
    let Some(aspect) = ident.map(|i| i.value.clone()) else {
        return Err(SessionError::BadSubject(format!(
            "`::{name}` — the part after `::` must name a declared aspect"
        )));
    };
    if store.aspect(&aspect).await?.is_none() {
        return Err(SessionError::Store(glossql_glossary::Error::Unknown {
            what: "aspect",
            name: aspect,
        }));
    }
    Ok(aspect)
}

fn unnest(e: &SQLExpr) -> &SQLExpr {
    match e {
        SQLExpr::Nested(inner) => unnest(inner),
        other => other,
    }
}

fn path_segments(e: &SQLExpr) -> Option<Vec<String>> {
    match e {
        SQLExpr::Identifier(i) => Some(vec![i.value.clone()]),
        SQLExpr::CompoundIdentifier(parts) => Some(parts.iter().map(|i| i.value.clone()).collect()),
        _ => None,
    }
}

fn rel_op(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Arrow => Some("->"),
        BinaryOperator::LtDashGt => Some("<->"),
        _ => None,
    }
}

// -- batch shapes --------------------------------------------------------

fn utf8(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("column shapes match the schema")
}

/// `(subject, aspect, value, band, score, state)` — SPEC.md §5.3, collapsed.
fn collapsed_batch(rows: Vec<CollapsedRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("value"),
        utf8("band"),
        Field::new("score", DataType::Float64, true),
        utf8("state"),
    ]));
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.subject.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.aspect.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.value.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.band.as_deref()),
            )),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.score))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.state.as_str()),
            )),
        ],
    )
}

/// `(subject, aspect, kind, witness, actor, body, written_at)` — §5.3, raw.
fn raw_batch(rows: Vec<RawRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("kind"),
        utf8("witness"),
        utf8("actor"),
        utf8("body"),
        utf8("written_at"),
    ]));
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.subject.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.aspect.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.kind.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.witness.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.actor.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.body.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.written_at.as_str()),
            )),
        ],
    )
}

/// `(subject, aspect, witness, band, score, computed_at)` — §7.2.
fn attest_batch(rows: Vec<AttestRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("witness"),
        utf8("band"),
        Field::new("score", DataType::Float64, false),
        utf8("computed_at"),
    ]));
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.subject.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.aspect.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.witness.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.band.as_str()),
            )),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.score))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.computed_at.as_str()),
            )),
        ],
    )
}

/// What an extraction statement returns: one row per call, served from the
/// cache (whether this run computed it or a previous one did).
/// Extraction serves the function-authored `summary` when the body
/// carries one (ruled 2026-08-14: metric_cube's 54 KB body was
/// write-only through the door, and 65 profiles pushed their whole
/// bodies through the agent while warming). The full body stays in
/// the cache, read back uncapped via `GLOSSARY(subject::aspect)`.
fn served_body(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("summary").cloned())
        .filter(|s| s.is_object())
        .map(|s| s.to_string())
        .unwrap_or_else(|| body.to_string())
}

pub(crate) fn extraction_batch(rows: Vec<glossql_glossary::CacheRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("function"),
        utf8("subject"),
        utf8("body"),
        utf8("computed_at"),
    ]));
    let bodies: Vec<String> = rows.iter().map(|r| served_body(&r.body)).collect();
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.function.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.subject.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                bodies.iter().map(|b| b.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.computed_at.as_str()),
            )),
        ],
    )
}

fn relation_batch(table: &str, rows: Vec<Vec<Option<String>>>) -> RecordBatch {
    // The store's RELATIONS table is the one home of each shape; the
    // planner only routes names that table knows.
    let names = glossql_glossary::relation_columns(table).expect("planner routed a known relation");
    let schema = Arc::new(Schema::new(
        names.iter().map(|n| utf8(n)).collect::<Vec<_>>(),
    ));
    let columns = (0..names.len())
        .map(|i| Arc::new(StringArray::from_iter(rows.iter().map(|r| r[i].as_deref()))) as ArrayRef)
        .collect();
    batch(schema, columns)
}
