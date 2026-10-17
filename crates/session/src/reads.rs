//! The record planner: `GLOSSARY()` / `ATTEST()`, the store's relations,
//! a served grounding and `read.<aspect>()`, planned
//! through DataFusion's `RelationPlanner` seam. The planner sees the raw
//! `TableFactor` before default planning, so named arguments (`all => true`),
//! zero-argument sweeps, and pair paths (`a.b <-> c.d`) all decode here —
//! structurally, from the sqlparser AST, which is also why the JSON `->`
//! operator (datafusion-functions-json) never collides with pair paths:
//! inside these factors `->` never reaches expression planning.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, Expr as SQLExpr, FunctionArg, FunctionArgExpr, Ident,
    TableAlias, TableFactor, Value as SQLValue,
};

use glossql_glossary::{AttestRow, CollapsedRow, RawRow, ReadContext, Scope, Store, schemas};
use serde_json::{Value, json};

use crate::shared::{Resolved, SessionError, Shared};
use crate::subject::{pair_subject, resolve_column_endpoint, resolve_path};

/// Detector verdicts, computed at read (SPEC.md §7.2) and never
/// stored. Keyed by the witness, not the detector alone: one
/// detector serving two witnesses answers for each, from its own slots
/// against its own threshold. A detector's body is one SQL query over
/// the `slots` relation (§6), answering every subject in one plan.
pub(crate) async fn verdicts(
    ctx: &ReadContext,
    dataset: &str,
    scope: &Scope,
    aspect: Option<&str>,
) -> Result<glossql_glossary::Verdicts, SessionError> {
    let mut out = glossql_glossary::Verdicts::default();
    // One slots pass serves every witness — each takes its aspect's
    // rows from it — and the witnesses are the context's own rows:
    // this read derives from the `ReadContext` alone.
    let all = glossql_glossary::Store::raw_read(dataset, scope, aspect, ctx);
    for w in ctx.witnesses.iter() {
        if let Some(a) = aspect
            && w.aspect != a
        {
            continue;
        }
        let Some(detector) = w.detector.clone() else {
            continue;
        };
        let slots: Vec<glossql_glossary::RawRow> = all
            .iter()
            .filter(|s| s.aspect == w.aspect)
            .cloned()
            .collect();
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
        // A detector that cannot answer refuses nothing: the failure
        // becomes this witness's verdict — band `error`, never a
        // judgment, so the §5.3 collapse never withholds on it — one
        // row per subject holding slots, and every other witness still
        // speaks. The triggers need no detector bug: `slots.body`
        // falls back to text when the bodies do not shred as one
        // schema, and a witness is workspace-global while its detector
        // may carry a `FOR` scope.
        let run = async {
            let function = ctx
                .functions
                .iter()
                .find(|f| {
                    f.name == detector && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
                })
                .cloned()
                .ok_or_else(|| SessionError::UnknownFunction(detector.clone()))?;
            let statement = crate::measure::sql_body(&function.script).ok_or_else(|| {
                SessionError::Runtime(format!(
                    "`{detector}`: a detector's body is one SQL query over `slots`"
                ))
            })?;
            run_detector(&detector, statement, slots_batch(slots), w.threshold).await
        };
        let computed_at = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        match run.await {
            Ok(rows) => {
                for (subject, band, score) in rows {
                    let verdict = glossql_glossary::Verdict {
                        witness: w.name.clone(),
                        band,
                        score,
                        threshold: w.threshold,
                        computed_at: computed_at.clone(),
                        current: current.get(&subject).copied().unwrap_or(true),
                        error: None,
                    };
                    out.entry((subject, w.aspect.clone()))
                        .or_default()
                        .push(verdict);
                }
            }
            Err(e) => {
                for (subject, cur) in &current {
                    let verdict = glossql_glossary::Verdict {
                        witness: w.name.clone(),
                        band: "error".into(),
                        score: 0.0,
                        threshold: w.threshold,
                        computed_at: computed_at.clone(),
                        current: *cur,
                        error: Some(e.to_string()),
                    };
                    out.entry((subject.clone(), w.aspect.clone()))
                        .or_default()
                        .push(verdict);
                }
            }
        }
    }
    Ok(out)
}

/// One detector run: its query planned over a fresh context holding
/// the witness's `slots` relation. `$threshold` binds as a
/// typed value through the plan. Each returned row answers to the §7.2
/// contract: `(subject, band, score)`; the engine completes the attest
/// row with witness, aspect, and its own clock.
/// The context a detector's query plans on: the engine's defaults over
/// the witness's `slots` relation and nothing else. The door's function
/// listing reads the same builder.
pub fn detector_ctx() -> SessionContext {
    SessionContext::new()
}

async fn run_detector(
    detector: &str,
    statement: datafusion::sql::parser::Statement,
    slots: RecordBatch,
    threshold: Option<f64>,
) -> Result<Vec<(String, String, f64)>, SessionError> {
    use datafusion::common::{ParamValues, ScalarValue};
    let fail = |e: DataFusionError| SessionError::Runtime(format!("`{detector}`: {e}"));
    let ctx = detector_ctx();
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
            schemas::validate_instance(&contract, &row).map_err(|e| {
                SessionError::OutputRejected {
                    function: detector.to_string(),
                    detail: e.to_string(),
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
    /// Everything the pre-pass resolved for THIS statement — built
    /// before planning, read by the planner, written by nothing after.
    /// Each statement's state carries its own, so concurrent statements
    /// on one session share no mutable planning state and need no lock.
    pub resolved: Arc<Resolved>,
}

/// A session state for one statement, carrying that statement's resolved
/// doors: a clone of the session's state — config, catalogs, registered
/// functions — with this statement's planner registered ahead of the
/// base one, which resolves nothing (`Session::open`). A clone, not a
/// rebuild: the builder re-registers every function and mints a new
/// session id per statement for the same result.
pub(crate) fn state_with(
    ctx: &datafusion::prelude::SessionContext,
    shared: &Arc<Shared>,
    resolved: Resolved,
) -> datafusion::execution::SessionState {
    let mut state = ctx.state();
    state
        .register_relation_planner(Arc::new(GlossqlReads {
            shared: Arc::clone(shared),
            resolved: Arc::new(resolved),
        }))
        .expect("registering a relation planner cannot fail (session_state.rs)");
    state
}

impl RelationPlanner for GlossqlReads {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
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
        // planner. The engine is the composer — the substitution is
        // this expansion. No script, no cache, no
        // parameters.
        if name.0.len() == 2
            && name.0[0]
                .as_ident()
                .is_some_and(|i| context.normalize_ident(i.clone()) == "read")
        {
            // Normalized, because the pre-pass keyed the resolved plan
            // by this same name and the two have to spell it the same
            // way — see `prepass::doors_in`.
            let (Some(aspect), Some(a)) = (
                name.0[1]
                    .as_ident()
                    .map(|i| context.normalize_ident(i.clone())),
                args,
            ) else {
                return Ok(RelationPlanning::Original(Box::new(relation)));
            };
            if !a.args.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "read.{aspect}() takes no arguments — filters ride WHERE"
                )));
            }
            let provider = self.resolved.view(&aspect).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "`read.{aspect}` was not resolved before planning — the pre-pass missed \
                     this reference"
                ))
            })?;
            return self.scan(aspect, provider, alias.clone());
        }
        // A name the statement binds as a CTE is not ours to plan. This
        // seam runs *before* DataFusion's own CTE lookup (datafusion-sql
        // `relation/mod.rs`, `create_relation`) and RelationPlannerContext
        // cannot ask about CTE scope, so declining here is what keeps a
        // CTE shadowing a same-named table — SQL's precedence, which the
        // pin and batch arms below would otherwise silently invert.
        if self.resolved.shadowed(&self.shared.idents(), &relation) {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        // A compute door the pre-pass evaluated — `misfit.<x>()`,
        // `glossary(...)`, `attest(...)`,
        // `metric_series(grain => …)`, the store's relations. Keyed by the factor's
        // own rendering, which is what arrives here again: a lookup —
        // nothing computes, fetches or blocks during planning.
        if let Some(served) = self.resolved.batch(&relation.to_string()) {
            // One partition holding the door's batches in order: read as
            // they stand, copied into nothing.
            let provider =
                MemTable::try_new(served.schema.clone(), vec![served.partitions.clone()])?;
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
        {
            let shipped = context.normalize_ident(ident.clone());
            if crate::library::read_sql(&shipped).is_some() {
                return self.planned(&format!("read:{shipped}"), alias.clone());
            }
        }
        // `subject_column('table.column')` — the pre-pass built the
        // projection from the pin; a malformed argument is refused here
        // with the same reader the pre-pass used.
        if let Some(arg) = crate::subject::subject_column_arg(&self.shared.idents(), &relation) {
            let subject = arg.map_err(|e| DataFusionError::Plan(e.to_string()))?;
            return self.planned(&format!("subject_column:{subject}"), alias.clone());
        }
        // A data table of the bound dataset, pinned at the statement's
        // snapshot, or a grounding of it planned at those pins — bare or
        // dataset-qualified. Another dataset's tables fall through to
        // the live provider.
        //
        // Through the planner's own normalizer, and that is not tidiness:
        // if this lookup spells a name differently from the way
        // DataFusion resolves it, the miss is silent. `FROM Orders` keyed
        // raw finds no pin, falls through to the default path, and the
        // catalog-backed provider serves it live — the same rows, and
        // the statement's snapshot gone. `normalize_ident` is what the
        // default path itself uses, so agreeing with it is the whole
        // requirement.
        let normal = |i: &Ident| context.normalize_ident(i.clone());
        let relation_of = |name: &str| self.resolved.pin(name).or_else(|| self.resolved.view(name));
        let pinned = match name.0.as_slice() {
            [t] if args.is_none() => t.as_ident().and_then(|i| relation_of(&normal(i))),
            [d, t] if args.is_none() => match (d.as_ident(), t.as_ident()) {
                (Some(d), Some(t))
                    if self.shared.dataset().as_deref() == Some(normal(d).as_str()) =>
                {
                    relation_of(&normal(t))
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(provider) = pinned {
            return self.scan(display_name(&relation), provider, alias.clone());
        }
        Ok(RelationPlanning::Original(Box::new(relation)))
    }
}

/// The scan's display name: the factor without its alias, which rides
/// `PlannedRelation` instead.
pub(crate) fn display_name(factor: &TableFactor) -> String {
    match factor {
        TableFactor::Table {
            name, args: None, ..
        } => name.to_string(),
        TableFactor::Table { name, .. } => format!("{name}()"),
        other => other.to_string(),
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

    /// The relation scanned as this statement holds it under `name`: a
    /// pinned table, or a grounding's view. A view's source carries its
    /// plan, and the builder inlines it under the name
    /// (`LogicalPlanBuilder::scan`, datafusion-expr `builder.rs`), so
    /// `revenue.value` qualifies as `orders.amount` does.
    fn scan(
        &self,
        name: String,
        provider: Arc<dyn TableProvider>,
        alias: Option<TableAlias>,
    ) -> DFResult<RelationPlanning> {
        let plan = LogicalPlanBuilder::scan(name, provider_as_source(provider), None)?.build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}

/// What a grounding's name, or `read.<aspect>()`, serves: the
/// definition the catalog's view under the metric's name carries — the
/// serving grounding after the record's collapse, human outranking
/// agent, landed with the gloss (`Session::sync_view`). A name with no
/// view is refused with the road, read from the record on that path
/// alone: the aspect is undeclared, is no QUERY aspect, is stopped, or
/// has no grounding.
pub(crate) async fn served_grounding(
    shared: &Shared,
    views: &std::collections::HashMap<String, String>,
    aspect: &str,
) -> Result<String, SessionError> {
    if let Some(sql) = views.get(aspect) {
        return Ok(sql.clone());
    }
    let dataset = shared.dataset().ok_or(SessionError::NoDataset)?;
    let Some((_, kind, _)) = shared.store.aspect(aspect).await? else {
        return Err(SessionError::unknown(
            aspect,
            format!("read.{aspect}(): no aspect `{aspect}` is declared"),
        ));
    };
    if kind != "query" {
        return Err(SessionError::shape(
            aspect,
            format!(
                "read.{aspect}(): `{aspect}` is a {kind} aspect — GLOSSARY() reads it; \
             read. serves QUERY groundings"
            ),
        ));
    }
    let scope = Scope::Subject(dataset.clone());
    let ctx = shared.read_context().await?;
    let verdicts = verdicts(&ctx, &dataset, &scope, Some(aspect)).await?;
    let rows =
        glossql_glossary::Store::collapsed_read(&dataset, &scope, Some(aspect), &ctx, &verdicts);
    let row = rows
        .into_iter()
        .find(|r| r.subject == dataset && r.aspect == aspect && r.state != "unassessed");
    let Some(row) = row else {
        return Err(SessionError::unknown(
            aspect,
            format!(
                "read.{aspect}(): no current grounding of `{aspect}` on `{dataset}` — \
             `GLOSS {aspect} ON {dataset} AS $${{\"sql\": …}}$$` grounds it"
            ),
        ));
    };
    let value = row.value.ok_or_else(|| {
        SessionError::shape(
            aspect,
            format!("read.{aspect}(): the current grounding carries no value"),
        )
    })?;
    let body: Value = serde_json::from_str(&value).map_err(|e| {
        SessionError::shape(
            aspect,
            format!("read.{aspect}(): the grounding is not JSON: {e}"),
        )
    })?;
    if let Some(why) = body["stopped"].as_str() {
        return Err(SessionError::shape(
            aspect,
            format!("read.{aspect}(): stopped — {why}"),
        ));
    }
    if body["sql"].as_str().is_none() {
        return Err(SessionError::shape(
            aspect,
            format!("read.{aspect}(): the grounding carries no `sql`"),
        ));
    }
    // The record serves a grounding the catalog does not list: its
    // view did not land with the gloss. The gloss is the road.
    Err(SessionError::shape(
        aspect,
        format!(
            "read.{aspect}(): the grounding on `{dataset}` has no view in the catalog — \
         `GLOSS {aspect} ON {dataset} AS $${{\"sql\": …}}$$` lands it"
        ),
    ))
}

// -- argument decoding ---------------------------------------------------

/// The context a record read runs in: the dataset's pins — or, when
/// the subject resolved to a declared source with no `USE` bound
/// ([`crate::subject::resolve_path`]), the store alone. Source-grain
/// slots are workspace rows and no table pin belongs to them; walked
/// as a dataset, the source's name is a namespace the catalog does not
/// hold, and the one read meant to run datasetless would refuse.
async fn read_context_at(shared: &Shared, dataset: &str) -> Result<ReadContext, SessionError> {
    let unbound = shared.dataset().is_none();
    if unbound
        && !shared.store.dataset_exists(dataset).await?
        && shared.store.source_settings(dataset).await?.is_some()
    {
        return Ok(shared
            .store
            .read_context(
                dataset,
                vec![dataset.to_string()],
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
            )
            .await?);
    }
    shared.read_context_for(dataset).await
}

pub(crate) async fn glossary_read(
    shared: &Shared,
    args: &[FunctionArg],
) -> Result<RecordBatch, SessionError> {
    let (subject, all) = split_args(&shared.idents(), args, true)?;
    let ((dataset, scope), aspect) = decode_scope(shared, subject).await?;
    let aspect = aspect.as_deref();
    let ctx = read_context_at(shared, &dataset).await?;
    if all {
        Ok(raw_batch(glossql_glossary::Store::raw_read(
            &dataset, &scope, aspect, &ctx,
        )))
    } else {
        let verdicts = verdicts(&ctx, &dataset, &scope, aspect).await?;
        Ok(collapsed_batch(glossql_glossary::Store::collapsed_read(
            &dataset, &scope, aspect, &ctx, &verdicts,
        )))
    }
}

pub(crate) async fn attest_read(
    shared: &Shared,
    args: &[FunctionArg],
) -> Result<RecordBatch, SessionError> {
    let (subject, _) = split_args(&shared.idents(), args, false)?;
    let ((dataset, scope), aspect) = decode_scope(shared, subject).await?;
    let ctx = read_context_at(shared, &dataset).await?;
    let verdicts = verdicts(&ctx, &dataset, &scope, aspect.as_deref()).await?;
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
                error: v.error,
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        (&a.subject, &a.aspect, &a.witness).cmp(&(&b.subject, &b.aspect, &b.witness))
    });
    Ok(attest_batch(rows))
}

/// Split a read's argument list into (optional subject, `all` flag).
fn split_args<'a>(
    idents: &IdentNormalizer,
    args: &'a [FunctionArg],
    allow_all: bool,
) -> Result<(Option<&'a SQLExpr>, bool), SessionError> {
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
            } if allow_all && idents.normalize(n.clone()) == "all" && is_true(v) => {
                all = true;
            }
            FunctionArg::Named { name, arg, .. }
                if allow_all
                    && idents.normalize(name.clone()) == "all"
                    && matches!(arg, FunctionArgExpr::Expr(v) if is_true(v)) =>
            {
                all = true;
            }
            other => {
                return Err(SessionError::shape(
                    other.to_string(),
                    format!("unsupported read argument `{other}`"),
                ));
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
    let use_dataset = shared.dataset();
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
        // silently read an empty table named like the aspect. A landed
        // table of the USE'd dataset wins over an aspect of its name —
        // the nearer scope (SPEC.md §3), as it does over a dataset's.
        if let [only] = segments.as_slice()
            && aspect.is_none()
            && !store.dataset_exists(only).await?
            && store.aspect(only).await?.is_some()
        {
            let landed = match use_dataset {
                Some(d) => store.table_exists(d, only).await?,
                None => false,
            };
            if !landed {
                return Err(SessionError::shape(
                    only,
                    format!(
                        "`{only}` names an aspect, not a subject — read it as `subject::{only}`"
                    ),
                ));
            }
        }
        let mut resolved = resolve_path(store, use_dataset, &segments).await?;
        respell_column_subject(shared, &mut resolved).await?;
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
        let left_segments = path_segments(left).ok_or_else(|| {
            SessionError::shape(left.to_string(), format!("`{left}` in a pair path"))
        })?;
        let right_segments = path_segments(right).ok_or_else(|| {
            SessionError::shape(right.to_string(), format!("`{right}` in a pair path"))
        })?;
        let l = resolve_column_endpoint(store, use_dataset, &left_segments).await?;
        let r = resolve_column_endpoint(store, use_dataset, &right_segments).await?;
        let pair = pair_subject(&l, op, &r);
        return Ok(((l.dataset, Scope::Subject(pair)), aspect));
    }

    // A subject written as a string literal — the one shape a path
    // never takes; the road names the bare forms.
    let road = match expr {
        SQLExpr::Value(v) if matches!(v.value, SQLValue::SingleQuotedString(_)) => {
            " — a subject is a bare path, never a string: GLOSSARY(orders), \
             GLOSSARY(orders.amount), ATTEST(fin::revenue)"
        }
        _ => "",
    };
    Err(SessionError::unknown(
        expr.to_string(),
        format!("`{expr}` is not a subject{road}"),
    ))
}

/// Every landed spelling that differs from `missing` only by case —
/// the columns an unquoted name may reach (SPEC.md §1): one is the
/// column meant, two are a refusal naming both.
pub(crate) fn case_matches<'a>(
    missing: &str,
    landed: impl IntoIterator<Item = &'a str>,
) -> Vec<&'a str> {
    let mut matches: Vec<&str> = landed
        .into_iter()
        .filter(|name| *name != missing && name.eq_ignore_ascii_case(missing))
        .collect();
    matches.sort_unstable();
    matches.dedup();
    matches
}

/// `column` as landed in `pin` (SPEC.md §1): its own spelling, or the
/// one spelling an unquoted name folded past. Refused with the road
/// out — the table's columns, or the two spellings the name could
/// mean — the way the planner's own `No field named` names the valid
/// fields.
pub(crate) fn landed_column(
    pin: &glossql_catalog::PinnedTable,
    column: &str,
) -> Result<String, SessionError> {
    let table = &pin.name;
    if pin.columns.iter().any(|have| have == column) {
        return Ok(column.to_string());
    }
    match case_matches(column, pin.columns.iter().map(String::as_str)).as_slice() {
        [one] => Ok((*one).to_string()),
        [] => Err(SessionError::unknown(
            format!("{table}.{column}"),
            format!(
                "`{table}.{column}`: `{table}` has no column `{column}` — columns: {}",
                pin.columns.join(", ")
            ),
        )),
        many => Err(SessionError::AmbiguousName(format!(
            "`{table}.{column}`: {} columns of `{table}` differ from `{column}` only by case: {} — quote the one you mean",
            many.len(),
            many.iter()
                .map(|m| format!("{table}.\"{m}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// A `table.column` subject under a landed table, respelled to the
/// column's landed spelling where the name folded past it (SPEC.md
/// §1). Any other subject — a dataset, a source, a table, a pair
/// path, a table the lake does not hold — stands as resolved; a
/// column the table does not have is refused here, where the name
/// is, with the table's columns.
pub(crate) async fn respell_column_subject(
    shared: &Shared,
    resolved: &mut crate::subject::Resolved,
) -> Result<(), SessionError> {
    let Some((table, column)) = resolved.subject.split_once('.') else {
        return Ok(());
    };
    if column.contains(' ')
        || column.contains('.')
        || !shared.store.dataset_exists(&resolved.dataset).await?
    {
        return Ok(());
    }
    let pins = shared.pinned(&resolved.dataset).await?;
    let Some(pin) = pins.iter().find(|p| p.name == table) else {
        return Ok(());
    };
    let spelling = landed_column(pin, column)?;
    if spelling != column {
        resolved.subject = format!("{table}.{spelling}");
    }
    Ok(())
}

/// The `::aspect` name: a bare custom "type" naming a declared aspect.
async fn aspect_name(store: &Store, data_type: &SQLDataType) -> Result<String, SessionError> {
    let SQLDataType::Custom(name, _) = data_type else {
        return Err(SessionError::unknown(
            data_type.to_string(),
            format!("`::{data_type}` — the part after `::` must name a declared aspect"),
        ));
    };
    let ident = match name.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
    };
    let Some(aspect) = ident.map(|i| i.value.clone()) else {
        return Err(SessionError::unknown(
            name.to_string(),
            format!("`::{name}` — the part after `::` must name a declared aspect"),
        ));
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

pub(crate) fn utf8(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

pub(crate) fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
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

/// `(subject, aspect, witness, band, score, computed_at, current,
/// error)` — §7.2; `error` null except where `band` is `error`.
fn attest_batch(rows: Vec<AttestRow>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("subject"),
        utf8("aspect"),
        utf8("witness"),
        utf8("band"),
        Field::new("score", DataType::Float64, false),
        utf8("computed_at"),
        Field::new("current", DataType::Boolean, false),
        Field::new("error", DataType::Utf8, true),
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
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.error.as_deref()),
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

/// The extraction outcome, one row per call: the measurement served,
/// and `computed` — true when this call computed it, false when the
/// recorded row was served at an unchanged pin, its `computed_at` the
/// earlier run's. A repeat with nothing moved is a serve, not a rerun,
/// and the row says which.
pub(crate) fn extraction_batch(rows: Vec<(glossql_glossary::MeasurementRow, bool)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        utf8("function"),
        utf8("subject"),
        utf8("body"),
        utf8("computed_at"),
        Field::new("computed", DataType::Boolean, false),
    ]));
    let bodies: Vec<String> = rows.iter().map(|(r, _)| served_body(&r.body)).collect();
    batch(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(r, _)| r.function.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(r, _)| r.subject.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                bodies.iter().map(|b| b.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(r, _)| r.computed_at.as_str()),
            )),
            Arc::new(datafusion::arrow::array::BooleanArray::from(
                rows.iter()
                    .map(|(_, computed)| *computed)
                    .collect::<Vec<bool>>(),
            )),
        ],
    )
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

    /// One metric per PIT, each walked to one complete point — the
    /// shape the walk lands, `period` and `partial` included, since
    /// the detector reads both as struct fields.
    fn pits(values: &[f64]) -> Value {
        json!({"applicable": true, "metrics": values
            .iter()
            .map(|p| json!({"metric": "m", "points": [
                {"period": "2025-06", "pit": p, "partial": false}
            ]}))
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

        // Two metrics scored: the worst displacement, squared.
        let (subject, band, score) = read(&[0.5, 0.62]).await;
        assert_eq!((subject.as_str(), band.as_str()), ("fin", "green"));
        assert!((score - 0.24f64.powi(2)).abs() < 1e-9, "{score}");

        // 0.93 alone would be yellow; among two it is 0.86² = 0.74,
        // green — the base rate of a worst-of-two.
        let (_, band, _) = read(&[0.5, 0.93]).await;
        assert_eq!(band, "green");
        let (_, band, _) = read(&[0.5, 0.97]).await;
        assert_eq!(band, "yellow");

        let (_, band, score) = read(&[0.996, 0.5]).await;
        assert_eq!(band, "red");
        assert!(score > 0.98, "{score}");
    }

    #[tokio::test]
    async fn band_breach_scores_the_newest_complete_point() {
        // The newest point is partial and would read red; the detector
        // skips it for the month before, which is at the median.
        let body = json!({"applicable": true, "metrics": [{"metric": "m", "points": [
            {"period": "2025-05", "pit": 0.5, "partial": false},
            {"period": "2025-06", "pit": null, "partial": true,
             "withheld": "partial: the extract ends 2025-06-11, before the period's last day 2025-06-30"}
        ]}]});
        let rows = detect(BAND_BREACH, vec![slot("fin", body)], None)
            .await
            .unwrap();
        assert_eq!(rows, vec![("fin".into(), "green".into(), 0.0)]);
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
