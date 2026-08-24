//! `GLOSSARY()` / `ATTEST()` and the store's relations, planned
//! through DataFusion's `RelationPlanner` seam. The planner sees the raw
//! `TableFactor` before default planning, so named arguments (`all => true`),
//! zero-argument sweeps, and pair paths (`a.b <-> c.d`) all decode here —
//! structurally, from the sqlparser AST, which is also why the JSON `->`
//! operator (datafusion-functions-json) never collides with pair paths:
//! inside these factors `->` never reaches expression planning.

use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, StringArray, StructArray,
};
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

use crate::session::{FunctionRuntime, SessionError};
use crate::subject::{pair_subject, resolve_column_endpoint, resolve_path};

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
    /// never the record: it lives beside the store, not in it.
    pub cube: RwLock<crate::cube::CubeCache>,
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
            .lake()
            .pin_dataset(&dataset)
            .await?
            .into_iter()
            .map(|p| (p.name, p.provider))
            .collect())
    }

    pub fn runtime(&self) -> Arc<dyn FunctionRuntime> {
        Arc::clone(&self.runtime.read().expect("runtime lock"))
    }

    pub fn cube(&self) -> crate::cube::CubeCache {
        self.cube.read().expect("cube lock").clone()
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
        let lake = self.lake();
        let mut universe = Vec::new();
        let mut snapshots = std::collections::HashMap::new();
        for table in lake.table_names(dataset).await? {
            if let Some(snapshot) = lake.snapshot_id(dataset, &table).await? {
                snapshots.insert(table.clone(), snapshot);
            }
            for column in lake.table_columns(dataset, &table).await? {
                universe.push(format!("{table}.{column}"));
            }
            universe.push(table);
        }
        Ok(self
            .store
            .read_context(dataset, universe, snapshots)
            .await?)
    }
}

/// Detector verdicts, computed at read (SPEC.md §7.2) and never
/// stored. Keyed by the witness, not the detector alone: one
/// detector serving two witnesses answers for each, from its own slots
/// against its own threshold. A detector's body is one SQL query over
/// the `slots` relation (§6), answering every subject in one plan.
pub(crate) async fn verdicts(
    shared: &Shared,
    ctx: &ReadContext,
    dataset: &str,
    scope: &Scope,
    aspect: Option<&str>,
) -> Result<glossql_glossary::Verdicts, SessionError> {
    let mut out = glossql_glossary::Verdicts::default();
    for w in shared.store.witnesses_all().await? {
        if let Some(a) = aspect
            && w.aspect != a
        {
            continue;
        }
        let Some(detector) = w.detector.clone() else {
            continue;
        };
        let slots = glossql_glossary::Store::raw_read(dataset, scope, Some(&w.aspect), ctx);
        // No slots, no question to adjudicate — and no rows to shape the
        // relation the body would plan over.
        if slots.is_empty() {
            continue;
        }
        // Served and marked: a verdict is current when every voice it
        // read stands at this pin — a voice landed at an earlier one
        // still speaks, and the attest row says so.
        let mut current: std::collections::HashMap<String, bool> = Default::default();
        for s in &slots {
            let entry = current.entry(s.subject.clone()).or_insert(true);
            *entry &= s.current;
        }
        let function = ctx
            .functions
            .iter()
            .find(|f| f.name == detector && f.scope_dataset.as_deref().is_none_or(|s| s == dataset))
            .cloned()
            .ok_or_else(|| SessionError::UnknownFunction(detector.clone()))?;
        let statement = crate::measure::sql_body(&function.script).ok_or_else(|| {
            SessionError::Runtime(format!(
                "`{detector}`: a detector's body is one SQL query over `slots`"
            ))
        })?;
        let rows = run_detector(&detector, statement, slots_batch(slots), w.threshold).await?;
        let computed_at = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        for (subject, band, score) in rows {
            let verdict = glossql_glossary::Verdict {
                witness: w.name.clone(),
                band,
                score,
                threshold: w.threshold,
                computed_at: computed_at.clone(),
                current: current.get(&subject).copied().unwrap_or(true),
            };
            out.entry((subject, w.aspect.clone()))
                .or_default()
                .push(verdict);
        }
    }
    Ok(out)
}

/// One detector run: its query planned over a fresh context holding
/// the witness's `slots` relation. `$threshold` binds as a
/// typed value through the plan. Each returned row answers to the §7.2
/// contract: `(subject, band, score)`; the engine completes the attest
/// row with witness, aspect, and its own clock.
async fn run_detector(
    detector: &str,
    statement: datafusion::sql::parser::Statement,
    slots: RecordBatch,
    threshold: Option<f64>,
) -> Result<Vec<(String, String, f64)>, SessionError> {
    use datafusion::common::{ParamValues, ScalarValue};
    let fail = |e: DataFusionError| SessionError::Runtime(format!("`{detector}`: {e}"));
    let ctx = SessionContext::new();
    ctx.register_batch("slots", slots).map_err(fail)?;
    let plan = ctx
        .state()
        .statement_to_plan(statement)
        .await
        .map_err(fail)?
        .with_param_values(ParamValues::from(std::collections::HashMap::from([(
            "threshold".to_string(),
            ScalarValue::Float64(threshold),
        )])))
        .map_err(fail)?;
    for column in ["subject", "band", "score"] {
        if plan.schema().field_with_unqualified_name(column).is_err() {
            return Err(SessionError::Runtime(format!(
                "`{detector}` output has no {column}"
            )));
        }
    }
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(fail)?
        .collect()
        .await
        .map_err(fail)?;
    let contract = schemas::attest_contract();
    let mut out = Vec::new();
    for batch in &batches {
        let column = |name: &str, to: &DataType| -> Result<ArrayRef, SessionError> {
            let array = batch
                .column_by_name(name)
                .expect("checked on the plan schema");
            datafusion::arrow::compute::cast(array, to)
                .map_err(|e| SessionError::Runtime(format!("`{detector}` output: {name} {e}")))
        };
        let subjects = column("subject", &DataType::Utf8)?;
        let bands = column("band", &DataType::Utf8)?;
        let scores = column("score", &DataType::Float64)?;
        let subjects = subjects
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast");
        let bands = bands.as_any().downcast_ref::<StringArray>().expect("cast");
        let scores = scores
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast");
        for i in 0..batch.num_rows() {
            let row = json!({
                "subject": (!subjects.is_null(i)).then(|| subjects.value(i)),
                "band": (!bands.is_null(i)).then(|| bands.value(i)),
                "score": (!scores.is_null(i)).then(|| scores.value(i)),
            });
            schemas::validate_instance(&contract, &row).map_err(|detail| {
                SessionError::OutputRejected {
                    function: detector.to_string(),
                    detail,
                }
            })?;
            out.push((
                subjects.value(i).to_string(),
                bands.value(i).to_string(),
                scores.value(i),
            ));
        }
    }
    Ok(out)
}

/// The `slots` relation a detector plans over: the §5.3 raw shape plus
/// `speaker`, one row per slot. `body` arrives shredded — typed nested
/// columns through arrow's own JSON reader — when every body is a JSON
/// object (an aspect's slots share its schema, so they shred together);
/// anything else keeps `body` as the text it is.
fn slots_batch(rows: Vec<RawRow>) -> RecordBatch {
    let body = shredded_bodies(&rows).unwrap_or_else(|| {
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.body.as_str()),
        )) as ArrayRef
    });
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("kind"),
        utf8("witness"),
        utf8("actor"),
        utf8("speaker"),
        Field::new("current", DataType::Boolean, false),
        utf8("written_at"),
        Field::new("body", body.data_type().clone(), true),
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
                rows.iter().map(|r| r.speaker.as_str()),
            )),
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|r| Some(r.current)),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.written_at.as_str()),
            )),
            body,
        ],
    )
}

/// Every body as one typed struct column, or `None` when the slot set
/// does not shred: a body that is not JSON, not an object, or an empty
/// object falls back to text, deterministically for the whole set.
fn shredded_bodies(rows: &[RawRow]) -> Option<ArrayRef> {
    let values: Vec<Value> = rows
        .iter()
        .map(|r| serde_json::from_str::<Value>(&r.body).ok())
        .collect::<Option<Vec<_>>>()?;
    if !values.iter().all(Value::is_object) {
        return None;
    }
    let schema =
        arrow_json::reader::infer_json_schema_from_iterator(values.iter().map(|v| Ok(v.clone())))
            .ok()?;
    if schema.fields().is_empty() {
        return None;
    }
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .ok()?;
    decoder.serialize(&values).ok()?;
    let decoded = decoder.flush().ok()??;
    (decoded.num_rows() == rows.len()).then(|| Arc::new(StructArray::from(decoded)) as ArrayRef)
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
        // every QUERY gloss (serving
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
        // A name the statement binds as a CTE is not ours to plan. This
        // seam runs *before* DataFusion's own CTE lookup (datafusion-sql
        // 54.1, src/relation/mod.rs:190) and RelationPlannerContext
        // cannot ask about CTE scope, so declining here is what keeps a
        // CTE shadowing a same-named table — SQL's precedence, which the
        // pin and batch arms below would otherwise silently invert.
        if self.resolved.shadowed(&relation) {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        // A compute door the pre-pass evaluated — `whatif.<x>()`,
        // `misfit.<x>()`, `glossary(...)`, `attest(...)`,
        // `metric_series(grain => …)`, the store's relations. Keyed by the factor's
        // own rendering, which is what arrives here again: a lookup —
        // nothing computes, fetches or blocks during planning.
        if let Some(batch) = self.resolved.batch(&relation.to_string()) {
            let provider = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?;
            let plan = LogicalPlanBuilder::scan(
                display_name(&relation),
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?;
            return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan,
                alias.clone(),
            ))));
        }
        // A shipped read (`crates/session/reads/*.sql`): a bare relation
        // whose body is SQL, expanded through the same path a served
        // grounding takes. A name we ship shadows a workspace table of
        // that name; the shipped names are the reserved surface, kept few
        // and specific.
        if let [part] = name.0.as_slice()
            && let Some(ident) = part.as_ident()
            && args.is_none()
            && crate::library::read_sql(&ident.value.to_lowercase()).is_some()
        {
            let key = format!("read:{}", ident.value.to_lowercase());
            return self.planned(&key, alias.clone());
        }
        // `subject_column('table.column')` — the pre-pass built the
        // projection from the pin; a malformed argument is refused here
        // with the same reader the pre-pass used.
        if let Some(arg) = crate::prepass::subject_column_arg(&relation) {
            let subject = arg.map_err(|e| DataFusionError::Plan(e.to_string()))?;
            return self.planned(&format!("subject_column:{subject}"), alias.clone());
        }
        // A data table of the bound dataset, pinned at the statement's
        // snapshot — bare or dataset-qualified. Another dataset's tables
        // fall through to the live provider.
        let pinned = match name.0.as_slice() {
            [t] if args.is_none() => t.as_ident().and_then(|i| self.resolved.pin(&i.value)),
            [d, t] if args.is_none() => match (d.as_ident(), t.as_ident()) {
                (Some(d), Some(t))
                    if self.shared.dataset.read().expect("state lock").as_deref()
                        == Some(d.value.as_str()) =>
                {
                    self.resolved.pin(&t.value)
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(provider) = pinned {
            let plan = LogicalPlanBuilder::scan(
                display_name(&relation),
                provider_as_source(provider),
                None,
            )?
            .build()?;
            return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan,
                alias.clone(),
            ))));
        }
        Ok(RelationPlanning::Original(Box::new(relation)))
    }
}

/// Whether a factor [`compute_batch`] resolves serves the record —
/// the glossary directly (`glossary`, `GLOSSARY()`, `ATTEST()`) or
/// what collapsed slots say (the walk reads groundings, collisions and
/// anchors read claims, `metric_axes()` says what the cube admitted).
/// The pre-pass derives the statement's frame class from this
/// (`record`/`data`): a `record` answer can change under a glossary
/// write, a `data` answer cannot. The class is about what a door
/// SERVES, not what its build consumed: `metric_series()` enumerates
/// groundings to find its cells, but what it serves is the data at a
/// grain, so it stays out. A new arm in the match below decides its
/// class here, in the same file.
pub(crate) fn reads_the_record(f: &TableFactor) -> bool {
    let TableFactor::Table { name, .. } = f else {
        return false;
    };
    let [part] = name.0.as_slice() else {
        return false;
    };
    part.as_ident().is_some_and(|i| {
        matches!(
            i.value.to_lowercase().as_str(),
            "glossary"
                | "attest"
                | "grounding_collisions"
                | "metric_band_walk"
                | "metric_axes"
                | "behavior_anchors"
        )
    })
}

/// The one quoted string argument of a door, if that is what the args
/// hold.
fn single_string_arg(a: &datafusion::sql::sqlparser::ast::TableFunctionArgs) -> Option<String> {
    use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value as SqlValue};
    if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] = a.args.as_slice()
        && let SqlValue::SingleQuotedString(s) = &v.value
    {
        return Some(s.clone());
    }
    None
}

/// The scan's display name: the factor without its alias, which rides
/// `PlannedRelation` instead.
fn display_name(factor: &TableFactor) -> String {
    match factor {
        TableFactor::Table {
            name, args: None, ..
        } => name.to_string(),
        TableFactor::Table { name, .. } => format!("{name}()"),
        other => other.to_string(),
    }
}

/// Evaluate one compute door ahead of planning — the async half behind
/// the planner's lookup. `None` for anything that is not a compute door.
/// `resolved` carries the statement's pins, which is how a search door
/// reads the same snapshot every other scan in the statement reads.
pub(crate) async fn compute_batch(
    shared: &Arc<Shared>,
    factor: &TableFactor,
    resolved: &crate::prepass::Resolved,
) -> Result<Option<RecordBatch>, SessionError> {
    let TableFactor::Table { name, args, .. } = factor else {
        return Ok(None);
    };
    if let [prefix, door] = name.0.as_slice()
        && let (Some(prefix), Some(door)) = (prefix.as_ident(), door.as_ident())
    {
        if prefix.value.eq_ignore_ascii_case("whatif") {
            let Some(a) = args else { return Ok(None) };
            if !a.args.is_empty() {
                return Err(SessionError::BadSubject(format!(
                    "whatif.{}() takes no arguments — the scenario body carries the \
                     overrides (fixture 19)",
                    door.value
                )));
            }
            return Ok(Some(
                Box::pin(crate::whatif::whatif_batch(shared, &door.value)).await?,
            ));
        }
        if prefix.value.eq_ignore_ascii_case("misfit") {
            let Some(a) = args else { return Ok(None) };
            if !a.args.is_empty() {
                return Err(SessionError::BadSubject(format!(
                    "misfit.{}() takes no arguments — the frame is the aspect's \
                     grounding (fixture 20)",
                    door.value
                )));
            }
            return Ok(Some(
                Box::pin(crate::misfit::misfit_batch(shared, &door.value)).await?,
            ));
        }
        return Ok(None);
    }
    let [part] = name.0.as_slice() else {
        return Ok(None);
    };
    let Some(fname) = part.as_ident().map(|i| i.value.to_lowercase()) else {
        return Ok(None);
    };
    match (fname.as_str(), args) {
        // The search doors: candidate enumeration over a table's own
        // columns, computed here because a static SQL body cannot spell
        // a schema it does not know (§7e).
        ("derivation_candidates", Some(a)) => {
            let table = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "derivation_candidates takes one quoted table: \
                     derivation_candidates('table')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::derivation_candidates(shared, resolved, &table).await?,
            ))
        }
        ("behavior_anchors", Some(a)) => {
            let subject = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "behavior_anchors takes one quoted subject: \
                     behavior_anchors('table.column')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::behavior::behavior_anchors(shared, resolved, &subject).await?,
            ))
        }
        ("metric_band_walk", Some(a)) => {
            let dataset = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "metric_band_walk takes one quoted dataset: \
                     metric_band_walk('dataset')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::metric_band_walk(shared, &dataset).await?,
            ))
        }
        ("relationship_checks", Some(a)) => {
            let dataset = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "relationship_checks takes one quoted dataset: \
                     relationship_checks('dataset')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::relationship_checks(shared, resolved, &dataset).await?,
            ))
        }
        ("grounding_collisions", Some(a)) => {
            let dataset = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "grounding_collisions takes one quoted dataset: \
                     grounding_collisions('dataset')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::grounding_collisions(shared, &dataset).await?,
            ))
        }
        ("relationship_candidates", Some(a)) => {
            let dataset = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "relationship_candidates takes one quoted dataset: \
                     relationship_candidates('dataset')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::relationship_candidates(shared, resolved, &dataset).await?,
            ))
        }
        ("hierarchy_candidates", Some(a)) => {
            let table = single_string_arg(a).ok_or_else(|| {
                SessionError::BadSubject(
                    "hierarchy_candidates takes one quoted table: \
                     hierarchy_candidates('table')"
                        .into(),
                )
            })?;
            Ok(Some(
                crate::search::hierarchy_candidates(shared, resolved, &table).await?,
            ))
        }
        ("glossary", Some(a)) => Ok(Some(glossary_read(shared, &a.args).await?)),
        ("attest", Some(a)) => Ok(Some(attest_read(shared, &a.args).await?)),
        // The cube's two reads (crate::cube): the cells at a grain and
        // the fact row per metric. Both build what is not built.
        ("metric_series", Some(a)) => {
            let grain = crate::cube::grain_arg(&a.args)?;
            Ok(Some(crate::cube::metric_series_batch(shared, grain).await?))
        }
        ("metric_axes", Some(a)) => {
            if !a.args.is_empty() {
                return Err(SessionError::BadSubject(
                    "metric_axes() takes no arguments — filters ride WHERE".into(),
                ));
            }
            Ok(Some(crate::cube::metric_axes_batch(shared).await?))
        }
        // The `USE`'d dataset, as a relation — the one thing a read
        // written in SQL cannot spell for itself, and the reason the
        // reads over the workspace-wide relations had no way to narrow
        // to the session they answer for.
        ("current_dataset", None) => Ok(Some(current_dataset_batch(shared))),
        // The store's relations, readable as plain tables. Which names
        // qualify lives in one place: the store's RELATIONS table.
        (name, None) if glossql_glossary::relation_columns(name).is_some() => {
            let rows = shared.store.relation_rows(&fname).await?;
            Ok(Some(relation_batch(&fname, rows)))
        }
        _ => Ok(None),
    }
}

impl GlossqlReads {
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
pub(crate) async fn served_grounding(
    shared: &Shared,
    aspect: &str,
) -> Result<String, SessionError> {
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
    let ctx = shared.read_context().await?;
    let verdicts = verdicts(shared, &ctx, &dataset, &scope, Some(aspect)).await?;
    let rows =
        glossql_glossary::Store::collapsed_read(&dataset, &scope, Some(aspect), &ctx, &verdicts);
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
    let ctx = shared.read_context_for(&dataset).await?;
    if all {
        Ok(raw_batch(glossql_glossary::Store::raw_read(
            &dataset, &scope, aspect, &ctx,
        )))
    } else {
        let verdicts = verdicts(shared, &ctx, &dataset, &scope, aspect).await?;
        Ok(collapsed_batch(glossql_glossary::Store::collapsed_read(
            &dataset, &scope, aspect, &ctx, &verdicts,
        )))
    }
}

async fn attest_read(shared: &Shared, args: &[FunctionArg]) -> Result<RecordBatch, SessionError> {
    let (subject, _) = split_args(args, false)?;
    let ((dataset, scope), aspect) = decode_scope(shared, subject).await?;
    let ctx = shared.read_context_for(&dataset).await?;
    let verdicts = verdicts(shared, &ctx, &dataset, &scope, aspect.as_deref()).await?;
    let mut rows: Vec<AttestRow> = verdicts
        .into_iter()
        .flat_map(|((subject, aspect), vs)| {
            vs.into_iter().map(move |v| AttestRow {
                subject: subject.clone(),
                aspect: aspect.clone(),
                witness: v.witness,
                band: v.band,
                score: v.score,
                computed_at: v.computed_at,
                current: v.current,
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        (&a.subject, &a.aspect, &a.witness).cmp(&(&b.subject, &b.aspect, &b.witness))
    });
    Ok(attest_batch(rows))
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
        Field::new("current", DataType::Boolean, false),
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
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|r| Some(r.current)),
            )),
        ],
    )
}

/// `(subject, aspect, witness, band, score, computed_at, current)` —
/// §7.2.
fn attest_batch(rows: Vec<AttestRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("witness"),
        utf8("band"),
        Field::new("score", DataType::Float64, false),
        utf8("computed_at"),
        Field::new("current", DataType::Boolean, false),
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
            Arc::new(BooleanArray::from_iter(
                rows.iter().map(|r| Some(r.current)),
            )),
        ],
    )
}

/// What an extraction statement returns: one row per call, served from
/// the measurement at the read's pin (whether this run landed it or an
/// earlier one did).
/// Extraction serves the function-authored `summary` when the body
/// carries one — a large body is write-only through the door, and a
/// warm-up run pushes every body through the agent. The full body
/// stays in the measurement, read back uncapped via
/// `GLOSSARY(subject::aspect)`.
fn served_body(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("summary").cloned())
        .filter(|s| s.is_object())
        .map(|s| s.to_string())
        .unwrap_or_else(|| body.to_string())
}

pub(crate) fn extraction_batch(rows: Vec<glossql_glossary::MeasurementRow>) -> RecordBatch {
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

/// `current_dataset` — the bound dataset as a one-row relation, or no
/// rows at all when nothing is bound.
///
/// Zero rows rather than a NULL or a refusal, and that is the whole
/// design: a read scopes itself by joining this, so an unbound session
/// answers nothing instead of answering for every dataset in the
/// workspace. `GLOSSARY()` refuses an unbound session outright
/// ([`SessionError::NoDataset`]) because a sweep with no scope is a
/// caller's mistake; a read that joins for its scope needs an empty
/// relation, not an error, or every read building on it would have to
/// handle the refusal.
///
/// It is session state, not the record: a glossary write cannot change
/// it, only `USE` can, which is why it is absent from
/// [`reads_the_record`].
fn current_dataset_batch(shared: &Shared) -> RecordBatch {
    let bound = shared.dataset.read().expect("state lock").clone();
    batch(
        Arc::new(Schema::new(vec![utf8("dataset")])),
        vec![Arc::new(StringArray::from_iter(bound.iter().map(Some))) as ArrayRef],
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

#[cfg(test)]
mod detector_tests {
    //! The shipped detector bodies against fabricated slots — the
    //! banding contract without a lake or weights. The bodies are the
    //! scripts crate's files, included by path: the harness lives here
    //! because the slots relation and the run are this module's.

    use super::*;

    const BAND_BREACH: &str = include_str!("../../scripts/functions/band_breach.sql");
    const RATE_TOLERANCE: &str = include_str!("../../scripts/functions/rate_tolerance.sql");
    const SLOT_ENTROPY: &str = include_str!("../../scripts/functions/slot_entropy.sql");

    fn slot(subject: &str, body: Value) -> RawRow {
        RawRow {
            subject: subject.into(),
            aspect: "a".into(),
            kind: "FACT".into(),
            witness: Some("w".into()),
            actor: "t".into(),
            body: body.to_string(),
            written_at: "2026-01-01T00:00:00.000Z".into(),
            speaker: "agent".into(),
            current: true,
        }
    }

    async fn detect(
        body: &str,
        rows: Vec<RawRow>,
        threshold: Option<f64>,
    ) -> Result<Vec<(String, String, f64)>, SessionError> {
        let statement = crate::measure::sql_body(body).expect("one query");
        run_detector("t", statement, slots_batch(rows), threshold).await
    }

    fn pits(values: &[f64]) -> Value {
        json!({"applicable": true, "metrics": values
            .iter()
            .map(|p| json!({"metric": "m", "points": [{"pit": p}]}))
            .collect::<Vec<_>>()})
    }

    #[tokio::test]
    async fn band_breach_adjudicates_the_worst_pit() {
        let read = |values: &'static [f64]| async {
            let rows = detect(BAND_BREACH, vec![slot("fin", pits(values))], None)
                .await
                .unwrap();
            rows.into_iter().next().expect("one subject, one verdict")
        };

        let (subject, band, score) = read(&[0.5, 0.62]).await;
        assert_eq!((subject.as_str(), band.as_str()), ("fin", "green"));
        assert!(score < 0.3);

        let (_, band, _) = read(&[0.5, 0.93]).await;
        assert_eq!(band, "yellow");

        let (_, band, score) = read(&[0.996, 0.5]).await;
        assert_eq!(band, "red");
        assert!(score > 0.98);
    }

    #[tokio::test]
    async fn band_breach_scores_zero_when_nothing_walked() {
        // A slot whose walk covered nothing (the measurement always
        // lands `metrics`, empty when no metric walked) is green at
        // 0.0 — nothing breached — not an absent verdict.
        let body = json!({"applicable": false, "metrics": []});
        let rows = detect(BAND_BREACH, vec![slot("fin", body)], None)
            .await
            .unwrap();
        assert_eq!(rows, vec![("fin".into(), "green".into(), 0.0)]);
    }

    #[tokio::test]
    async fn slot_entropy_scores_the_disagreement() {
        let agree = vec![
            slot("t.c", json!({"value": "flow"})),
            slot("t.c", json!({"value": "flow"})),
        ];
        let rows = detect(SLOT_ENTROPY, agree, Some(0.7)).await.unwrap();
        assert_eq!(rows, vec![("t.c".into(), "green".into(), 0.0)]);

        let split = vec![
            slot("t.c", json!({"value": "flow"})),
            slot("t.c", json!({"value": "stock"})),
        ];
        let rows = detect(SLOT_ENTROPY, split, Some(0.7)).await.unwrap();
        assert_eq!(rows, vec![("t.c".into(), "red".into(), 1.0)]);

        let leaning = vec![
            slot("t.c", json!({"value": "flow"})),
            slot("t.c", json!({"value": "flow"})),
            slot("t.c", json!({"value": "stock"})),
        ];
        let rows = detect(SLOT_ENTROPY, leaning, Some(0.7)).await.unwrap();
        assert_eq!(rows, vec![("t.c".into(), "yellow".into(), 0.5)]);
    }

    #[tokio::test]
    async fn rate_tolerance_reads_the_voice_against_the_expectation() {
        // Within tolerance: the authored 0.5 admits a 0.2 rate.
        let slots = vec![
            slot("v", json!({"tolerance": 0.5})),
            slot("v", json!({"breach_rate": 0.2})),
        ];
        let rows = detect(RATE_TOLERANCE, slots, None).await.unwrap();
        assert_eq!(rows, vec![("v".into(), "green".into(), 0.2)]);

        // The authored tolerance wins over the witness's THRESHOLD —
        // the expectation is the record's judgment on the subject; the
        // threshold is the witness's default for subjects without one.
        let slots = vec![
            slot("v", json!({"tolerance": 0.5})),
            slot("v", json!({"breach_rate": 0.2})),
        ];
        let rows = detect(RATE_TOLERANCE, slots, Some(0.05)).await.unwrap();
        assert_eq!(rows, vec![("v".into(), "green".into(), 0.2)]);
        let slots = vec![slot("v", json!({"tolerance": null, "breach_rate": 0.2}))];
        let rows = detect(RATE_TOLERANCE, slots, Some(0.05)).await.unwrap();
        assert_eq!(rows, vec![("v".into(), "red".into(), 0.2)]);
    }

    #[tokio::test]
    async fn rate_tolerance_without_a_voice_is_yellow() {
        // The expectation stands, the check has not spoken: yellow,
        // never green.
        let slots = vec![slot("v", json!({"tolerance": 0.1, "breach_rate": null}))];
        let rows = detect(RATE_TOLERANCE, slots, None).await.unwrap();
        assert_eq!(rows, vec![("v".into(), "yellow".into(), 0.0)]);
    }

    #[test]
    fn bodies_shred_only_when_uniformly_objects() {
        let shredded = slots_batch(vec![
            slot("s", json!({"value": "a"})),
            slot("s", json!({"value": "b", "grounds": "seen"})),
        ]);
        assert!(matches!(
            shredded.column_by_name("body").unwrap().data_type(),
            DataType::Struct(_)
        ));

        let mixed = slots_batch(vec![
            slot("s", json!({"value": "a"})),
            slot("s", json!("bare prose")),
        ]);
        assert_eq!(
            mixed.column_by_name("body").unwrap().data_type(),
            &DataType::Utf8
        );
    }
}
