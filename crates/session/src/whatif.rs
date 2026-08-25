//! The `whatif.` door (fixture 19): a declared scenario —
//! a FACT aspect carrying column overrides — served as bands over recipe
//! replay. The server replays each concept's current QUERY
//! grounding with the overrides applied at a bracketing grid of strengths
//! (support worlds), the band kernel reads across the worlds with the
//! factors as features, and the declared point is always interpolation.
//! The override never touches storage: it is a plan rewrite — every scan
//! of an overridden table gains a projection scaling the overridden
//! column from the scenario's start month.
//!
//! Judgment rides the `basis` column, never a hidden guess: a concept
//! whose grounding is not current says so; one the overrides never move
//! is refused with the reason (no declared path — `detect_derivations`
//! proposes the missing identities); one whose grounding lacks a time
//! axis or a `value` column says which. The `replay` column carries the
//! mechanical recomputation at the declared factors — the arithmetic
//! half — beside the model's bands, so both halves stay visible.
//!
//! Recomputed per read, never stored: the replay is a search inside one
//! plan set, and an in-memory pin-keyed cache is a later, measured
//! question.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DFResult};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, lit, when};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use glossql_glossary::Scope;
use serde_json::Value;

use crate::reads::{Shared, verdicts};
use crate::session::{Matrix, SessionError};

const ALPHAS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
/// The bracket rule: strengths placed on both sides
/// of the declared factor, so the scenario's own read is interpolation.
const BRACKET: [f64; 5] = [-0.25, -0.10, -0.05, 0.05, 0.15];

/// One declared override: a real column, a factor, a start month.
#[derive(Clone)]
struct Override {
    table: String,
    column: String,
    factor: f64,
    from: String, // "YYYY-MM"
}

/// One served row; refusal rows carry only concept and basis.
struct Row {
    concept: String,
    month: Option<String>,
    replay: Option<f64>,
    q: Option<[f64; 5]>,
    basis: String,
}

pub(crate) async fn whatif_batch(
    shared: &Arc<Shared>,
    scenario: &str,
) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let bad = |detail: String| refused(scenario, &detail);

    let Some((_, kind, _)) = shared.store.aspect(scenario).await? else {
        return Err(bad(format!("no aspect `{scenario}` is declared")));
    };
    if kind != "fact" {
        return Err(bad(format!(
            "`{scenario}` is a {kind} aspect — a scenario is a FACT aspect carrying overrides \
             (fixture 19)"
        )));
    }

    // The scenario's collapsed current body, witness-gated and judged
    // like any read.
    let scope = Scope::Subject(dataset.clone());
    let ctx = shared.read_context().await?;
    let verdicts = verdicts(shared, &ctx, &dataset, &scope, Some(scenario)).await?;
    let collapsed =
        glossql_glossary::Store::collapsed_read(&dataset, &scope, Some(scenario), &ctx, &verdicts);
    let current = collapsed
        .into_iter()
        .find(|r| r.subject == dataset && r.aspect == scenario)
        .ok_or_else(|| bad(format!("no scenario is glossed on `{dataset}`")))?;
    if current.state != "current" {
        return Err(bad(format!(
            "the scenario on `{dataset}` is {}",
            current.state
        )));
    }
    let body: Value = current
        .value
        .as_deref()
        .and_then(|v| serde_json::from_str(v).ok())
        .ok_or_else(|| bad("the scenario body is not JSON".into()))?;
    let overrides = decode_overrides(scenario, &body)?;
    // Recomputed per read: the replay stays inside one plan set, and
    // whether repeated identical work earns an in-memory, pin-keyed
    // cache is a later, measured question.
    let rows = compute(shared, &dataset, scenario, &overrides).await?;
    Ok(row_batch(rows))
}

/// A scenario read refused, named by the door's call.
fn refused(scenario: &str, detail: &str) -> SessionError {
    SessionError::BadSubject(format!("whatif.{scenario}(): {detail}"))
}

fn decode_overrides(scenario: &str, body: &Value) -> Result<Vec<Override>, SessionError> {
    let bad = |detail: &str| refused(scenario, detail);
    let list = body["overrides"]
        .as_array()
        .filter(|l| !l.is_empty())
        .ok_or_else(|| bad("the scenario body carries no overrides"))?;
    list.iter()
        .map(|o| {
            let column = o["column"]
                .as_str()
                .ok_or_else(|| bad("an override names its `column` as `table.column`"))?;
            let (table, column) = column.split_once('.').ok_or_else(|| {
                bad(&format!(
                    "`{column}` — an override column is `table.column`"
                ))
            })?;
            let factor = o["factor"]
                .as_f64()
                .filter(|f| *f > 0.0)
                .ok_or_else(|| bad("an override carries a positive `factor`"))?;
            let from = o["from"]
                .as_str()
                .filter(|f| f.len() == 7 && f.as_bytes()[4] == b'-')
                .ok_or_else(|| bad("an override carries `from` as \"YYYY-MM\""))?;
            Ok(Override {
                table: table.to_string(),
                column: column.to_string(),
                factor,
                from: from.to_string(),
            })
        })
        .collect()
}

/// The replay: every current QUERY grounding, run over the support
/// worlds, banded by the runtime's kernel. One `Row` per (concept,
/// month) — or one refusal row per concept, with the reason.
async fn compute(
    shared: &Arc<Shared>,
    dataset: &str,
    scenario: &str,
    overrides: &[Override],
) -> Result<Vec<Row>, SessionError> {
    let ctx = shared.session_ctx();
    let bad = |detail: String| refused(scenario, &detail);

    // Every overridden column must exist, and its table must carry a
    // date column for the start-month condition — checked up front so a
    // misdeclared scenario fails whole, not per concept.
    for o in overrides {
        let frame = ctx
            .table(&o.table)
            .await
            .map_err(|_| bad(format!("`{}` names no table in the dataset", o.table)))?;
        let fields: Fields = frame.schema().fields().clone();
        if !fields.iter().any(|f| f.name() == &o.column) {
            return Err(bad(format!("`{}` has no column `{}`", o.table, o.column)));
        }
        if date_column(&fields).is_none() {
            return Err(bad(format!(
                "`{}` carries no date column to anchor `from` — override the landed table \
                 that carries the time",
                o.table
            )));
        }
    }

    // The support worlds: baseline, then each lever bracketed alone
    // (singles — the proven two-lever shape trains on baseline +
    // singles and reads the joint).
    let baseline: Vec<f64> = vec![1.0; overrides.len()];
    let mut worlds: Vec<Vec<f64>> = vec![baseline.clone()];
    for (i, o) in overrides.iter().enumerate() {
        for offset in BRACKET {
            let s = o.factor + offset;
            if s > 0.01 && (s - 1.0).abs() > 1e-9 {
                let mut w = baseline.clone();
                w[i] = s;
                if !worlds.contains(&w) {
                    worlds.push(w);
                }
            }
        }
    }
    let declared: Vec<f64> = overrides.iter().map(|o| o.factor).collect();
    let from_month = overrides
        .iter()
        .map(|o| o.from.as_str())
        .min()
        .expect("overrides checked non-empty")
        .to_string();

    // Latest current QUERY grounding per concept, judgment included.
    let scope = Scope::Subject(dataset.to_string());
    let kinds: std::collections::HashMap<String, String> = shared
        .store
        .relation_rows("aspects")
        .await?
        .into_iter()
        .filter_map(|r| Some((r[0].clone()?, r[1].clone()?)))
        .collect();
    let read_ctx = shared.read_context().await?;
    // The judged time axis, read once for every concept in the replay:
    // what-if is charted beside the cube's own series, so it anchors
    // where the cube anchors wherever a verdict stands.
    let judged_temporal = crate::cube::judged_bodies(&read_ctx, dataset, "temporal_profile");
    let all_verdicts = verdicts(shared, &read_ctx, dataset, &scope, None).await?;
    let collapsed =
        glossql_glossary::Store::collapsed_read(dataset, &scope, None, &read_ctx, &all_verdicts);

    let mut out = Vec::new();
    for c in collapsed {
        if c.subject != dataset || kinds.get(&c.aspect).map(String::as_str) != Some("query") {
            continue;
        }
        let refusal = |basis: String| Row {
            concept: c.aspect.clone(),
            month: None,
            replay: None,
            q: None,
            basis,
        };
        if c.state != "current" {
            out.push(refusal(format!("not served: the grounding is {}", c.state)));
            continue;
        }
        let Some(body) = c
            .value
            .as_deref()
            .and_then(|v| serde_json::from_str::<Value>(v).ok())
        else {
            out.push(refusal("not served: the grounding body is not JSON".into()));
            continue;
        };
        let Some(sql) = body["sql"].as_str().map(str::to_string) else {
            out.push(refusal("not served: the grounding carries no `sql`".into()));
            continue;
        };
        match concept_rows(
            shared,
            &ctx,
            dataset,
            &judged_temporal,
            &c.aspect,
            &sql,
            &body,
            overrides,
            &worlds,
            &declared,
            &from_month,
        )
        .await
        {
            Ok(rows) => out.extend(rows),
            Err(SessionError::BadSubject(detail)) => out.push(refusal(detail)),
            Err(e) => return Err(e),
        }
    }
    if out.is_empty() {
        return Err(bad(format!(
            "no QUERY grounding is current on `{dataset}` — there is nothing to replay"
        )));
    }
    Ok(out)
}

/// One concept through the pipeline: monthly series per world, the
/// unmoved check, the frame, the kernel. `BadSubject` here means a
/// refusal row, not a failed read.
#[allow(clippy::too_many_arguments)]
async fn concept_rows(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    dataset: &str,
    judged_temporal: &std::collections::HashMap<String, crate::cube::Verdict>,
    concept: &str,
    sql: &str,
    body: &Value,
    overrides: &[Override],
    worlds: &[Vec<f64>],
    declared: &[f64],
    from_month: &str,
) -> Result<Vec<Row>, SessionError> {
    let refuse = |detail: String| SessionError::BadSubject(detail);

    // The grounding's shape: a `value` column and a time axis, found by
    // dtype exactly as any reader finds them (the metric_bands probe) —
    // from the plan's schema, nothing scanned.
    let probe = build_plan(shared, ctx, sql).await?;
    let fields: Fields = probe.schema().fields().clone();
    if !fields.iter().any(|f| f.name() == "value") {
        return Err(refuse(
            "not served: the grounding carries no `value` column".into(),
        ));
    }
    // The judged axis where a verdict stands on a served date column,
    // the first date column by dtype where none does. The fallback is
    // allowed — a replay must still run on a grounding the cube would
    // abstain from — but it is never silent: `basis` names the axis and
    // says whether it was judged, because these rows are read beside
    // cube series that anchor on the judged one.
    let subjects = crate::provenance::served_subjects(&probe, dataset);
    let (tcol, axis_note) =
        match crate::cube::judged_time_column(probe.schema(), &subjects, judged_temporal) {
            Some((column, ..)) => {
                let note = format!("time axis `{column}`, judged");
                (column, note)
            }
            None => {
                let Some(column) = date_column(&fields) else {
                    return Err(refuse(
                        "not served: the grounding carries no time axis".into(),
                    ));
                };
                let note = format!(
                    "time axis `{column}`, by dtype — no served date column carries an \
                     applicable temporal_profile, so this replay does not anchor where the \
                     cube anchors"
                );
                (column, note)
            }
        };

    // The three verbs, the same ones the cube and metric_bands read.
    //
    // A RATIO declares itself by serving `num` and `den` beside `value`,
    // and reads as sum(num)/sum(den). Summing it instead adds member
    // ratios together: DSO replayed at 957 days against a true 76, its
    // grounding serving segment x region.
    //
    // A marked STOCK sums the rows standing at the month's LATEST
    // observed date. `row_number() = 1` kept ONE arbitrary row — a
    // receivables grounding emitting one row per open invoice replayed
    // as 4,325 against a true 42M, and inventory as 12k against 12.4M.
    let is_ratio =
        fields.iter().any(|f| f.name() == "num") && fields.iter().any(|f| f.name() == "den");
    let is_stock = !is_ratio && body["behavior"].as_str() == Some("stock");
    let verb = if is_ratio {
        "ratio"
    } else if is_stock {
        "stock"
    } else {
        "flow"
    };
    let series_sql = crate::search::monthly_sql(sql, &tcol, verb);

    let base = run_series(shared, ctx, &series_sql, None).await?;
    let post: Vec<usize> = (0..base.len())
        .filter(|i| base[*i].0.as_str() >= from_month)
        .collect();
    if post.is_empty() {
        let last = base.last().map(|(m, _)| m.as_str()).unwrap_or("nothing");
        return Err(refuse(format!(
            "not served: the scenario starts after the recorded history ends ({last}) — \
             replay needs recorded months from the start on"
        )));
    }

    // The kernel cap, the misfit door's measured bound: the training
    // frame is worlds x post months, and each world is a full replay
    // query — refused before any of that work starts.
    let train_rows = worlds.len() * post.len();
    if train_rows > crate::misfit::ROW_CAP {
        return Err(refuse(format!(
            "not served: the replay would train on {train_rows} rows ({} worlds x {} \
             months) — past the kernel cap ({}); narrow the scenario's date range or \
             reduce its overrides",
            worlds.len(),
            post.len(),
            crate::misfit::ROW_CAP
        )));
    }

    // The mechanical half: the exact recomputation at the declared
    // factors. Identical to baseline on every post month = the
    // overrides never reach this grounding — refused, never served
    // as a silently unchanged number.
    let joint = run_series(shared, ctx, &series_sql, Some((overrides, declared))).await?;
    if !same_roster(&joint, &base) {
        return Err(refuse(
            "not served: the replay changed the month roster — the grounding is not \
             stable under the override"
                .into(),
        ));
    }
    let moved = post.iter().any(|&i| {
        let (b, j) = (base[i].1, joint[i].1);
        (j - b).abs() > 1e-9 * b.abs().max(1.0)
    });
    let named_columns = overrides
        .iter()
        .map(|o| format!("{}.{}", o.table, o.column))
        .collect::<Vec<_>>()
        .join(", ");
    if !moved {
        return Err(refuse(format!(
            "unmoved by the overrides: no declared path from {named_columns} into this \
             grounding — declare the derivation (detect_derivations proposes candidates) \
             or gloss the assumption"
        )));
    }

    // The frame, the eval's shape: train on the support worlds' post
    // months, (factors…, month_index) → value; read at the declared
    // factors — inside the bracket by construction.
    let mut train_x = Vec::new();
    let mut train_y = Vec::new();
    let mut contributed: Vec<&Vec<f64>> = Vec::new();
    let mut skipped: Vec<&Vec<f64>> = Vec::new();
    for w in worlds {
        let owned;
        let s = if w.iter().all(|f| (*f - 1.0).abs() < 1e-9) {
            &base
        } else {
            owned = run_series(shared, ctx, &series_sql, Some((overrides, w))).await?;
            &owned
        };
        if !same_roster(s, &base) {
            skipped.push(w);
            continue;
        }
        contributed.push(w);
        for &i in &post {
            train_x.extend(w.iter().copied());
            train_x.push(i as f64);
            train_y.push(s[i].1);
        }
    }
    // Bands over baseline alone would claim support the grid never
    // gave — refused with the count instead.
    if contributed.len() < 2 {
        return Err(refuse(format!(
            "not served: every bracketed world changed the month roster under replay \
             ({} of {} worlds skipped) — no support beyond baseline to band on",
            skipped.len(),
            worlds.len()
        )));
    }
    let cols = declared.len() + 1;
    let rows = train_y.len();
    let mut test_x = Vec::new();
    for &i in &post {
        test_x.extend(declared.iter().copied());
        test_x.push(i as f64);
    }
    let q = shared
        .runtime()
        .band_grid(
            Matrix {
                data: &train_x,
                rows,
                cols,
            },
            &train_y,
            Matrix {
                data: &test_x,
                rows: post.len(),
                cols,
            },
            &ALPHAS,
        )
        .map_err(|e| refuse(format!("not served: the band kernel refused — {e}")))?;
    if q.len() != post.len() * ALPHAS.len() {
        return Err(SessionError::Runtime(format!(
            "the band kernel returned {} values for {} test rows",
            q.len(),
            post.len()
        )));
    }

    // The support claim is what actually trained: contributed worlds
    // by strength, skipped worlds named with the reason.
    let strengths = |w: &Vec<f64>| {
        w.iter()
            .filter(|s| (**s - 1.0).abs() > 1e-9)
            .map(|s| format!("{s:.2}"))
            .collect::<Vec<_>>()
            .join("*")
    };
    let grid: Vec<String> = contributed.iter().skip(1).map(|w| strengths(w)).collect();
    let skipped_note = if skipped.is_empty() {
        String::new()
    } else {
        format!(
            "; skipped x[{}]: roster changed",
            skipped
                .iter()
                .map(|w| strengths(w))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let asked: Vec<String> = declared.iter().map(|f| format!("{f:.2}")).collect();
    let basis = format!(
        "replayed x[{}] on {named_columns}; {axis_note}; {} of {} worlds x {} months\
         {skipped_note}; asked ({}) in support",
        grid.join(", "),
        contributed.len(),
        worlds.len(),
        post.len(),
        asked.join(", "),
    );
    Ok(post
        .iter()
        .enumerate()
        .map(|(t, &i)| Row {
            concept: concept.to_string(),
            month: Some(base[i].0.clone()),
            replay: Some(joint[i].1),
            q: Some([
                q[t * 5],
                q[t * 5 + 1],
                q[t * 5 + 2],
                q[t * 5 + 3],
                q[t * 5 + 4],
            ]),
            basis: basis.clone(),
        })
        .collect())
}

/// Plan a query through the session's own pipeline, so a nested `read.`
/// re-enters the relation planner. The `misfit.` door plans its frame
/// through the same gate.
pub(crate) async fn build_plan(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
) -> Result<LogicalPlan, SessionError> {
    // Parsed once, under the pre-pass's one-query rule; the same
    // statement is resolved and then planned.
    let query = crate::prepass::parse(sql, "the grounding")?;
    let statement = datafusion::sql::parser::Statement::Statement(Box::new(SQLStatement::Query(
        Box::new(query),
    )));
    // A grounding body may name `read.<x>()`; resolve before planning.
    let resolved = crate::prepass::resolve(shared, ctx, &statement).await?;
    crate::reads::state_with(ctx, shared, resolved)
        .statement_to_plan(statement)
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))
}

/// A monthly series: Vec<(period "YYYY-MM", value)>, optionally with
/// the override rewrite applied at the given strengths.
async fn run_series(
    shared: &Arc<Shared>,
    ctx: &SessionContext,
    sql: &str,
    overlay: Option<(&[Override], &[f64])>,
) -> Result<Vec<(String, f64)>, SessionError> {
    let mut plan = build_plan(shared, ctx, sql).await?;
    if let Some((overrides, factors)) = overlay {
        plan = apply_overrides(plan, overrides, factors)
            .map_err(|e| SessionError::Runtime(format!("the override rewrite failed: {e}")))?;
    }
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
        .collect()
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?;
    let mut out = Vec::new();
    for b in &batches {
        let period = b.column(0);
        let value = b
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                SessionError::BadSubject("not served: `value` does not read as a number".into())
            })?;
        for i in 0..b.num_rows() {
            if value.is_null(i) {
                continue;
            }
            let p = array_value_to_string(period, i)
                .map_err(|e| SessionError::Runtime(e.to_string()))?;
            // The YYYY-MM head — periods arrive in the column's
            // display form, as every monthly reader cuts them.
            let p = p.get(..7).map(str::to_string).unwrap_or(p);
            out.push((p, value.value(i)));
        }
    }
    Ok(out)
}

/// The replay rewrite: every `TableScan` of an overridden table gains a
/// projection scaling the overridden columns from the start month on,
/// re-aliased under the table's own name so every outer reference
/// resolves unchanged. Storage is never touched.
fn apply_overrides(
    plan: LogicalPlan,
    overrides: &[Override],
    factors: &[f64],
) -> DFResult<LogicalPlan> {
    plan.transform_up(|node| {
        let (table_name, fields) = match &node {
            LogicalPlan::TableScan(scan) => (
                scan.table_name.clone(),
                scan.projected_schema.fields().clone(),
            ),
            _ => return Ok(Transformed::no(node)),
        };
        let table = table_name.table().to_string();
        let active: Vec<(&Override, f64)> = overrides
            .iter()
            .zip(factors.iter().copied())
            .filter(|(o, f)| o.table == table && (*f - 1.0).abs() > 1e-9)
            .collect();
        if active.is_empty() {
            return Ok(Transformed::no(node));
        }
        let Some(tcol) = date_column(&fields) else {
            return Ok(Transformed::no(node));
        };
        let tcol_type = fields
            .iter()
            .find(|f| f.name() == &tcol)
            .expect("date column found above")
            .data_type()
            .clone();
        let qualified =
            |name: &str| Expr::Column(Column::new(Some(table_name.clone()), name.to_string()));
        let exprs: DFResult<Vec<Expr>> = fields
            .iter()
            .map(|field| {
                let mut expr = qualified(field.name());
                for (o, f) in &active {
                    if o.column != *field.name() {
                        continue;
                    }
                    let start = cast(lit(format!("{}-01", o.from)), tcol_type.clone());
                    let scaled = cast(expr.clone() * lit(*f), field.data_type().clone());
                    expr = when(qualified(&tcol).gt_eq(start), scaled).otherwise(expr)?;
                }
                Ok(expr.alias(field.name()))
            })
            .collect();
        let rebuilt = LogicalPlanBuilder::from(node)
            .project(exprs?)?
            .alias(table_name)?
            .build()?;
        Ok(Transformed::yes(rebuilt))
    })
    .map(|t| t.data)
}

/// Roster identity by month value — equal counts of different months
/// must not compare positionally.
fn same_roster(a: &[(String, f64)], b: &[(String, f64)]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.0 == y.0)
}

/// The first date-typed column, the same discovery every reader does.
/// The one temporal-column test every monthly reader shares.
pub(crate) fn is_temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

pub(crate) fn date_column(fields: &Fields) -> Option<String> {
    fields
        .iter()
        .find(|f| is_temporal(f.data_type()))
        .map(|f| f.name().clone())
}

/// `(concept, month, replay, p05, p10, p50, p90, p95, basis)`.
fn row_batch(rows: Vec<Row>) -> RecordBatch {
    let float = |name: &str| Field::new(name, DataType::Float64, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("concept", DataType::Utf8, false),
        Field::new("month", DataType::Utf8, true),
        float("replay"),
        float("p05"),
        float("p10"),
        float("p50"),
        float("p90"),
        float("p95"),
        Field::new("basis", DataType::Utf8, false),
    ]));
    let quant = |k: usize| -> ArrayRef {
        Arc::new(Float64Array::from_iter(
            rows.iter().map(|r| r.q.map(|q| q[k])),
        ))
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.concept.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.month.as_deref()),
            )),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.replay))),
            quant(0),
            quant(1),
            quant(2),
            quant(3),
            quant(4),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.basis.as_str()),
            )),
        ],
    )
    .expect("column shapes match the schema")
}
