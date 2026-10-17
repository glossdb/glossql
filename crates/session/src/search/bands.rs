//! The metric-bands walk and its points, and the period SQL every
//! monthly reader shares.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{cast, col, try_cast};
use serde_json::{Value, json};

use crate::shared::{Answered, FORECAST, SessionError, Shared};
use crate::subject::qi;

use super::{current_query_slots, rows_batch};

/// The three verbs at any calendar grain, shared by the metric doors:
/// flows sum per period; a marked stock sums the rows standing at the
/// period's LATEST observed date — one arbitrary last row is one item
/// of an inventory, never its total; a ratio serves `num` and `den`
/// and the period reads as sum(num)/sum(den), because a walk or a
/// slice over a summed ratio bands an artefact. With `halves` a ratio also serves
/// its summed num and den — the only material a coarser window can
/// re-derive the division from.
pub(crate) fn grain_sql(sql: &str, tcol: &str, verb: &str, grain: &str, halves: bool) -> String {
    match verb {
        "ratio" => {
            let h = if halves {
                ", sum(num) AS num, sum(den) AS den"
            } else {
                ""
            };
            format!(
                "SELECT date_trunc('{grain}', {tcol_q}) AS period, \
                        sum(num) / nullif(sum(den), 0) AS value{h} \
                 FROM ({sql}) GROUP BY 1 ORDER BY 1",
                tcol_q = qi(tcol)
            )
        }
        "stock" => format!(
            "SELECT period, sum(value) AS value FROM (\
                SELECT date_trunc('{grain}', {tcol_q}) AS period, value, \
                       rank() OVER (\
                           PARTITION BY date_trunc('{grain}', {tcol_q}) \
                           ORDER BY {tcol_q} DESC) AS rk \
                FROM ({sql})\
             ) WHERE rk = 1 GROUP BY period ORDER BY period",
            tcol_q = qi(tcol)
        ),
        _ => format!(
            "SELECT date_trunc('{grain}', {tcol_q}) AS period, sum(value) AS value \
             FROM ({sql}) GROUP BY 1 ORDER BY 1",
            tcol_q = qi(tcol)
        ),
    }
}

pub(crate) fn monthly_sql(sql: &str, tcol: &str, verb: &str) -> String {
    grain_sql(sql, tcol, verb, "month", false)
}

/// One period of a grain series: `(period, value, num, den)` — the
/// halves are present only where a ratio ran with `halves`.
type GrainRow = (String, Option<f64>, Option<f64>, Option<f64>);

/// A grounding's series at a grain. A period the verb could not value
/// is kept with a NULL value — the walk indexes by position. A NULL
/// period is dropped: a NULL date truncates to its own bucket, and a
/// NULL date is no period, as the cube's reader holds too. Periods
/// come back in the column's display form; callers cut the head they
/// need (YYYY-MM, YYYY-MM-DD) as the scripts did.
async fn run_grain(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    tcol: &str,
    verb: &str,
    grain: &str,
    halves: bool,
) -> Result<Vec<GrainRow>, SessionError> {
    use datafusion::arrow::array::Float64Array;

    let q = grain_sql(sql, tcol, verb, grain, halves);
    let plan = crate::plan::build_plan(shared, ctx, &q).await?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(SessionError::not_served)?
        .collect()
        .await
        .map_err(SessionError::not_served)?;
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let period = b.column(b.schema().index_of("period").map_err(SessionError::from)?);
        let float_col = |name: &str| -> Result<Option<Float64Array>, SessionError> {
            let Ok(i) = b.schema().index_of(name) else {
                return Ok(None);
            };
            let floats = cast_with_options(
                b.column(i),
                &DataType::Float64,
                &CastOptions {
                    safe: true,
                    ..Default::default()
                },
            )
            .map_err(SessionError::from)?;
            floats
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .map(Some)
                .ok_or_else(|| SessionError::Runtime(format!("{name} did not read as a number")))
        };
        let value = float_col("value")?
            .ok_or_else(|| SessionError::Runtime("value did not read as a number".into()))?;
        let num = float_col("num")?;
        let den = float_col("den")?;
        let at = |c: &Option<Float64Array>, i: usize| {
            c.as_ref().and_then(|c| (!c.is_null(i)).then(|| c.value(i)))
        };
        for i in 0..b.num_rows() {
            if period.is_null(i) {
                continue;
            }
            out.push((
                array_value_to_string(period, i).map_err(SessionError::from)?,
                (!value.is_null(i)).then(|| value.value(i)),
                at(&num, i),
                at(&den, i),
            ));
        }
    }
    Ok(out)
}

/// [`run_grain`] at month grain, values only — what the walk and the
/// rival read.
async fn run_monthly(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    tcol: &str,
    verb: &str,
) -> Result<Vec<(String, Option<f64>)>, SessionError> {
    Ok(run_grain(shared, ctx, sql, tcol, verb, "month", false)
        .await?
        .into_iter()
        .map(|(p, v, ..)| (p, v))
        .collect())
}

/// The extract's shape on its time column: the horizon — the last day
/// it holds, `YYYY-MM-DD` in the column's display form, None where the
/// extract is empty — and whether the rows land finer than monthly,
/// read from the extract itself: some month holds more than one
/// distinct day. The walk reads both to tell a partial trailing month
/// from a complete one: a period the horizon falls inside has not
/// finished landing when rows land through the month, while a
/// monthly-dated series is whole at its one row. The judged cadence
/// is not the evidence here — an event-stamp column judges
/// `irregular`, which names no cadence at all.
async fn extract_shape(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    tcol: &str,
) -> Result<(Option<String>, bool), SessionError> {
    use datafusion::arrow::array::ArrayRef;
    let q = format!(
        "SELECT max({tcol_q}) AS horizon, \
                count(DISTINCT date_trunc('day', {tcol_q})) AS days, \
                count(DISTINCT date_trunc('month', {tcol_q})) AS months \
         FROM ({sql})",
        tcol_q = qi(tcol)
    );
    let plan = crate::plan::build_plan(shared, ctx, &q).await?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(SessionError::not_served)?
        .collect()
        .await
        .map_err(SessionError::not_served)?;
    let Some(b) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok((None, false));
    };
    let column = |name: &str| -> Result<&ArrayRef, SessionError> {
        b.column_by_name(name)
            .ok_or_else(|| SessionError::Runtime(format!("the shape read served no {name}")))
    };
    let count = |name: &str| -> Result<i64, SessionError> {
        column(name)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|c| !c.is_null(0))
            .map(|c| c.value(0))
            .ok_or_else(|| SessionError::Runtime(format!("{name} did not read as a count")))
    };
    let sub_monthly = count("days")? > count("months")?;
    let col = column("horizon")?;
    if col.is_null(0) {
        return Ok((None, sub_monthly));
    }
    let shown = array_value_to_string(col, 0).map_err(SessionError::from)?;
    Ok((Some(shown.chars().take(10).collect()), sub_monthly))
}

/// The last day of a `YYYY-MM` period, `YYYY-MM-DD`, from the
/// calendar; None for text that is no month.
async fn month_end(period: &str) -> Result<Option<String>, SessionError> {
    let Some(first) = crate::period::parse_date(&format!("{period}-01")) else {
        return Ok(None);
    };
    let last = crate::period::over(
        &[first],
        &[format!(
            "p + {} - INTERVAL '1 day'",
            crate::period::interval(crate::period::Resolution::Month, 1)
        )],
    )
    .await
    .map_err(crate::period::refused)?;
    Ok(last
        .first()
        .and_then(|c| c.first().copied().flatten())
        .map(crate::period::date))
}

/// `metric_band_walk('dataset')` — for every grounded metric, walk the
/// recent months: from each of the last six months as an origin, the
/// forecast service says what range the month after it should have
/// landed in, given the history up to the origin. The protocol here is
/// the monthly read at the grounding's verb, the extract's shape, and
/// the `withheld` and `partial` rules; the service builds its own rows
/// from the series and reads the PIT against its own distribution. The
/// model call is one table call behind the runtime seam, never
/// reimplemented.
///
/// Each walked point records its bands, its actual and its PIT — the
/// quantile at which the actual landed, 0..1 and ordinal by
/// construction. The band_breach detector adjudicates PITs; this door
/// only reports.
pub(crate) async fn metric_band_walk(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let anchors = crate::cube::Anchors::at(&rctx, dataset).await?;
    let runtime = shared.runtime();
    if !runtime.serves(FORECAST) {
        return Err(crate::shared::no_model("metric_bands()", FORECAST));
    }

    // Every metric's series goes to the service together, once the
    // loop has read them all: one table answers a workspace, and the
    // service forecasts the series of one request together. A metric's
    // rows are built after the answer, in the loop's order.
    let mut out: Vec<Row> = Vec::new();
    for slot in current_query_slots(&rctx, dataset).await? {
        // A plan is walked by nobody: it is not a metric (fixture 27).
        if slot.is_plan() {
            continue;
        }
        let seq = out.len() as i64;
        if let Some(row) = walk_metric(shared, &ctx, dataset, &anchors, &slot, seq).await? {
            out.push(row);
        }
    }

    let walked: Vec<&Walked> = out
        .iter()
        .filter_map(|row| match row {
            Row::Walked(w) => Some(w),
            Row::Ready(_) => None,
        })
        .collect();
    let answered = if walked.is_empty() {
        None
    } else {
        let spec = json!({
            "targets": ["value"], "origins": ORIGINS, "horizon": 1, "levels": LEVELS,
        });
        Some(runtime.table(FORECAST, walk_table(&walked)?, spec).await?)
    };
    let (points, errors, served_by) = answer_of(shared, answered.as_ref(), &LEVELS).await?;

    let mut rows = Vec::new();
    for row in out {
        match row {
            Row::Ready(value) => rows.push(value),
            Row::Walked(walked) => {
                let served = served_points(&walked, &points).await?;
                rows.extend(metric_rows(walked, served, &errors, served_by.as_deref()));
            }
        }
    }
    if rows.is_empty() {
        rows.push(json!({}));
    }
    rows_batch(rows, band_shape())
}

const LEVELS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
/// Origins walked per metric: the last six months, each the end of
/// the history the month after it is forecast from.
const ORIGINS: usize = 6;
/// The months a metric needs to walk at all: one origin with the
/// six known values the service needs behind it. A metric with
/// fewer months than a full walk walks the origins it has.
const MIN_MONTHS: usize = 7;

/// One metric read for the walk, before the service answers.
struct Walked {
    seq: i64,
    aspect: String,
    aggregation: &'static str,
    tcol: String,
    axis_judged: bool,
    /// The monthly series, periods as `YYYY-MM`, a NULL where the
    /// verb could not value the month.
    series: Vec<(String, Option<f64>)>,
    /// The extract's last day, where its rows land finer than
    /// monthly.
    horizon: Option<String>,
}

/// One metric's place in the answer: a row already decided — an
/// abstention with its reason — or a series the service walks.
enum Row {
    Ready(Value),
    Walked(Walked),
}

/// One grounding read for the walk: its series and the extract's
/// shape, through the engine. The grounding is a grain-free extract
/// with a time axis and a `value` column — the judged column where a
/// verdict stands, the first date column by dtype where none does;
/// only the column is judged, the walk's cadence stays month, the
/// grain the service's intervals are measured at, and the measurement
/// names the axis it took. A grounding that does not plan fails the
/// walk whole; one the engine refuses at execution — a timestamp its
/// month bucketing cannot hold, a column it cannot truncate — abstains
/// with the engine's reason, and the other metrics walk. None for a
/// grounding the walk passes over: no body, no `value`, no time axis.
async fn walk_metric(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    anchors: &crate::cube::Anchors,
    slot: &crate::search::QuerySlot,
    seq: i64,
) -> Result<Option<Row>, SessionError> {
    let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
        return Ok(None);
    };
    let Some(sql) = body.get("sql").and_then(Value::as_str) else {
        return Ok(None);
    };
    let probe = crate::plan::build_plan(shared, ctx, sql).await?;
    let fields = probe.schema();
    let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
    if !has("value") {
        return Ok(None);
    }
    let sources = crate::provenance::served_sources(&probe, dataset);
    let judged = crate::cube::judged_time_column(fields, &sources, &anchors.temporal);
    let judged_axis = judged.as_ref().map(|(column, ..)| column.clone());
    let Some(tcol) = judged_axis
        .clone()
        .or_else(|| crate::plan::date_column(fields.fields()))
    else {
        return Ok(None);
    };
    let axis_judged = judged_axis.is_some();
    let is_ratio = has("num") && has("den");
    let verb = crate::cube::verb_of(
        &body,
        is_ratio,
        &probe,
        dataset,
        &anchors.behavior,
        &anchors.behavior_gloss,
    )
    .verb;
    // What one period's value is, named as the arithmetic: a flow
    // sums, a marked stock sums the rows at its latest date, a
    // ratio divides its summed halves — `grain_sql`'s three arms.
    let aggregation = match verb {
        "stock" => "latest-sum",
        "ratio" => "ratio-of-sums",
        _ => "sum",
    };
    let abstain = |reason: String| {
        Row::Ready(json!({
            "seq": seq, "metric": slot.aspect, "applicable": false, "reason": reason,
        }))
    };
    let served = async {
        let series = run_monthly(shared, ctx, sql, &tcol, verb).await?;
        let (horizon, sub_monthly) = extract_shape(shared, ctx, sql, &tcol).await?;
        Ok::<_, SessionError>((series, horizon, sub_monthly))
    }
    .await;
    let (series, horizon, sub_monthly) = match served {
        Ok(served) => served,
        Err(e) => {
            let Some(reason) = e.abstention() else {
                return Err(e);
            };
            return Ok(Some(abstain(reason)));
        }
    };
    let n = series.len();
    if n < MIN_MONTHS {
        return Ok(Some(abstain(format!("{n} months, need {MIN_MONTHS}"))));
    }
    // Rows landing through the month stop mid-month when the
    // extract does; a monthly-dated series is whole at its one
    // row, and its horizon says nothing.
    let horizon = horizon.filter(|_| sub_monthly);
    // The YYYY-MM head: periods arrive in the column's display
    // form, and a shorter one is no period the walk can read.
    let mut monthly = Vec::with_capacity(n);
    for (period, value) in series {
        let Some(month) = period.get(..7) else {
            return Err(SessionError::Runtime(format!(
                "period {period:?} has no month"
            )));
        };
        monthly.push((month.to_string(), value));
    }
    Ok(Some(Row::Walked(Walked {
        seq,
        aspect: slot.aspect.clone(),
        aggregation,
        tcol,
        axis_judged,
        series: monthly,
        horizon,
    })))
}

/// The one table, as the contract has the walk send it: `member` the
/// metric, `period` the month, one value column, every row actual.
/// The service walks the last six months of every member and answers
/// one row per member, walked month and level, with the actual and
/// the PIT; a member it cannot walk fails alone, in the answer's
/// `errors`.
fn walk_table(walked: &[&Walked]) -> Result<RecordBatch, SessionError> {
    let (mut members, mut periods, mut values) = (Vec::new(), Vec::new(), Vec::new());
    for w in walked {
        for (period, value) in &w.series {
            members.push(w.aspect.as_str());
            periods.push(period.as_str());
            values.push(*value);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("member", DataType::Utf8, false),
            Field::new("period", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from(members)),
            Arc::new(StringArray::from(periods)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .map_err(SessionError::from)
}

/// One metric's walked points as the rows carry them: each known
/// month the service answered, with its bands, its actual and its
/// PIT — withheld, with the reason, where the point says nothing.
/// The newest month is partial while the extract's horizon falls
/// inside it: a short sum against a corridor fitted on whole months
/// reads as a breach that is only the calendar, so the point serves
/// its bands and its actual so far and the detector scores the newest
/// complete month instead. A series that repeats values moves on a
/// grid (a ratio over a fixed field, a count): a corridor narrower
/// than the grid's step, with the actual inside a step of the median,
/// is the model's noise around a value the series takes exactly, and
/// the PIT read against it says nothing. An actual further off than a
/// step is a real move whatever the corridor; a series that never
/// repeats has no grid, and a tight corridor on it is earned.
async fn served_points(
    walked: &Walked,
    points: &HashMap<(String, String), Point>,
) -> Result<Vec<Value>, SessionError> {
    let Walked {
        aspect,
        series,
        horizon,
        ..
    } = walked;
    let n = series.len();
    let mut served: Vec<Value> = Vec::new();
    for (t, (period, value)) in series.iter().enumerate() {
        let Some(actual) = *value else { continue };
        let Some(point) = points.get(&(aspect.clone(), period.clone())) else {
            continue;
        };
        let q = point.levels;
        let partial = match (t == n - 1, horizon.as_deref()) {
            (true, Some(h)) => month_end(period)
                .await?
                .filter(|end| h < end.as_str())
                .map(|end| {
                    format!("partial: the extract ends {h}, before the period's last day {end}")
                }),
            _ => None,
        };
        let history: Vec<f64> = series[..t].iter().filter_map(|(_, v)| *v).collect();
        let corridor = q[4] - q[0];
        let withheld = match resolution_of(&history) {
            _ if partial.is_some() => partial.clone(),
            Some(res) if res > 0.0 && corridor < res && (actual - q[2]).abs() <= res => {
                Some(format!(
                    "corridor {corridor:.3e} is narrower than the series' resolution {res:.3e} and the actual sits within it"
                ))
            }
            Some(res) if res == 0.0 && history.first() == Some(&actual) => {
                Some("the training series is constant and the actual equals it".to_string())
            }
            _ => None,
        };
        served.push(json!({
            "period": period, "actual": actual,
            "p05": q[0], "p10": q[1], "p50": q[2], "p90": q[3], "p95": q[4],
            "pit": withheld.is_none().then_some(point.pit).flatten(),
            "withheld": withheld,
            // Always present: the detector reads it as a struct
            // field, and a field no point carries is not in the
            // body's struct at all.
            "partial": partial.is_some(),
        }));
    }
    Ok(served)
}

/// One metric's rows: a point each where the service walked it; its
/// head alone where it walked nowhere without a word (every recent
/// month unvalued); inapplicable with its reasons where it walked
/// nowhere and said why.
fn metric_rows(
    walked: Walked,
    served: Vec<Value>,
    errors: &HashMap<String, Vec<String>>,
    served_by: Option<&str>,
) -> Vec<Value> {
    let Walked {
        seq,
        aspect,
        aggregation,
        tcol,
        axis_judged,
        series,
        ..
    } = walked;
    let n = series.len();
    let mut rows = Vec::new();
    if served.is_empty()
        && let Some(reasons) = errors.get(&aspect)
    {
        rows.push(json!({
            "seq": seq, "metric": aspect, "applicable": false,
            "reason": reasons.join("; "),
            "served_by": served_by,
        }));
        return rows;
    }
    for (point_seq, p) in served.iter().enumerate() {
        let mut row = serde_json::Map::new();
        row.insert("seq".into(), json!(seq));
        row.insert("metric".into(), json!(aspect));
        row.insert("applicable".into(), json!(true));
        row.insert("grain".into(), json!("month"));
        row.insert("aggregation".into(), json!(aggregation));
        row.insert("trained_on".into(), json!(n as i64));
        row.insert("axis".into(), json!(tcol));
        row.insert("axis_judged".into(), json!(axis_judged));
        row.insert("served_by".into(), json!(served_by));
        row.insert("point_seq".into(), json!(point_seq as i64));
        for (k, val) in p.as_object().expect("point object") {
            row.insert(k.clone(), val.clone());
        }
        rows.push(Value::Object(row));
    }
    if served.is_empty() {
        rows.push(json!({
            "seq": seq, "metric": aspect, "applicable": true,
            "grain": "month",
            "aggregation": aggregation,
            "trained_on": n as i64,
            "axis": tcol, "axis_judged": axis_judged,
            "served_by": served_by,
        }));
    }
    rows
}

/// One walked point as the service answered it: the band value per
/// level asked, in the order asked, and the PIT where the actual was
/// known to the service.
struct Point {
    levels: [f64; 5],
    pit: Option<f64>,
}

/// The walk's answer as the rows read it: the points by member and
/// period, each member's errors, and who served; nothing where
/// nothing was asked.
async fn answer_of(
    shared: &Arc<Shared>,
    answered: Option<&Answered>,
    levels: &[f64; 5],
) -> Result<Answer, SessionError> {
    let Some(answered) = answered else {
        return Ok((HashMap::new(), HashMap::new(), None));
    };
    let points = walk_points(shared, &answered.table, levels)
        .await
        .map_err(|e| SessionError::Runtime(format!("the forecast service answered {e}")))?;
    let served_by = format!(
        "{} {}",
        answered.answer["service"].as_str().unwrap_or("?"),
        answered.answer["version"].as_str().unwrap_or("?")
    );
    Ok((points, walk_errors(&answered.answer), Some(served_by)))
}

/// The points by member and period, the errors by member, and who
/// served.
type Answer = (
    HashMap<(String, String), Point>,
    HashMap<String, Vec<String>>,
    Option<String>,
);

/// The answer's period rows, by member and period, one row per level
/// folded into one point: an aggregate over the answer batch — each
/// asked level's value its own column, the PIT beside them — run on
/// the plane's pool. A total row (`span` set) is none of the walk's
/// and is skipped; a level nobody asked, or a point missing a level,
/// is a wrong answer.
async fn walk_points(
    shared: &Arc<Shared>,
    answer: &RecordBatch,
    levels: &[f64; 5],
) -> Result<HashMap<(String, String), Point>, String> {
    let plan = super::answer_source(answer)
        .and_then(|a| {
            let mut columns = super::level_columns(levels)?;
            columns.push(max(try_cast(col("pit"), DataType::Float64)).alias("pit"));
            a.filter(
                col("span")
                    .is_null()
                    .and(col("period").is_not_null())
                    .and(col("member").is_not_null())
                    .and(col("level").is_not_null()),
            )?
            .aggregate(
                vec![
                    col("member"),
                    cast(col("period"), DataType::Utf8).alias("period"),
                ],
                columns,
            )?
            .build()
        })
        .map_err(|e| e.to_string())?;
    let mut out = HashMap::new();
    for b in super::run_answer(shared, plan)
        .await
        .map_err(|e| e.to_string())?
    {
        let column = |name: &str| {
            b.column_by_name(name)
                .ok_or_else(|| format!("the fold served no `{name}`"))
        };
        let (member, period) = (column("member")?, column("period")?);
        let pit = column("pit")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| "`pit` did not read as a number".to_string())?;
        for i in 0..b.num_rows() {
            let key = (
                array_value_to_string(member, i).map_err(|e| e.to_string())?,
                array_value_to_string(period, i).map_err(|e| e.to_string())?,
            );
            let levels = super::levels_of::<5>(&b, i, &format!("{} at {}", key.0, key.1))?;
            let pit = (!pit.is_null(i)).then(|| pit.value(i));
            out.insert(key, Point { levels, pit });
        }
    }
    Ok(out)
}

/// The answer's `errors` by member: what failed alone, in the service's
/// words, the origin named where one was.
pub(crate) fn walk_errors(answer: &Value) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for e in answer["errors"].as_array().into_iter().flatten() {
        let Some(member) = e["member"].as_str() else {
            continue;
        };
        let error = e["error"].as_str().unwrap_or("failed");
        let said = match e.get("origin").filter(|o| !o.is_null()) {
            Some(origin) => format!(
                "origin {}: {error}",
                origin
                    .as_str()
                    .map_or_else(|| origin.to_string(), str::to_string)
            ),
            None => error.to_string(),
        };
        out.entry(member.to_string()).or_default().push(said);
    }
    out
}

/// `band_points()` — the recorded walk, one row per metric per walked
/// point: what `metric_bands` last landed for the bound dataset,
/// flattened back to the walk's own rows, each point with its
/// displacement (|2·pit − 1|, what the detector scores — none where
/// the PIT is withheld) and the measurement's `computed_at` and
/// whether it stands at this pin. The record itself, never a re-run:
/// `metric_band_walk()` walks again and calls the model, this reads
/// what landed, so a red dataset verdict is diagnosable to its metric
/// and month in one read. A metric the walk found inapplicable is one
/// row with its reason and no point; a dataset never walked serves no
/// rows.
pub(crate) async fn band_points(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let rctx = shared.read_context().await?;
    let landed = crate::cube::judged_bodies(&rctx, &dataset, "metric_bands");
    let mut out = Vec::new();
    if let Some(verdict) = landed.get(&dataset) {
        let computed_at = crate::cube::judged_at(&rctx, &dataset, "metric_bands");
        let metrics = verdict.body["metrics"].as_array();
        for (seq, metric) in metrics.into_iter().flatten().enumerate() {
            let head = || {
                let mut row = serde_json::Map::new();
                row.insert("seq".into(), json!(seq as i64));
                for key in [
                    "metric",
                    "applicable",
                    "reason",
                    "grain",
                    "aggregation",
                    "trained_on",
                    "axis",
                    "axis_judged",
                    "served_by",
                ] {
                    row.insert(key.into(), metric[key].clone());
                }
                row.insert("computed_at".into(), json!(computed_at));
                row.insert("current".into(), json!(verdict.current));
                row
            };
            let points = metric["points"].as_array().filter(|p| !p.is_empty());
            let Some(points) = points else {
                out.push(Value::Object(head()));
                continue;
            };
            for (point_seq, point) in points.iter().enumerate() {
                let mut row = head();
                row.insert("point_seq".into(), json!(point_seq as i64));
                for (k, v) in point.as_object().into_iter().flatten() {
                    row.insert(k.clone(), v.clone());
                }
                row.insert(
                    "displacement".into(),
                    json!(point["pit"].as_f64().map(|pit| (2.0 * pit - 1.0).abs())),
                );
                out.push(Value::Object(row));
            }
        }
    }
    let mut fields = band_shape();
    fields.push(Field::new("displacement", DataType::Float64, true));
    fields.push(Field::new("computed_at", DataType::Utf8, true));
    fields.push(Field::new("current", DataType::Boolean, true));
    rows_batch(out, fields)
}

/// The series' resolution, where it has one: a series that repeats a
/// value moves on a grid, and the smallest gap between two of its
/// values is the grid's step — 0.0 when it takes one value only. A
/// series that never repeats is continuous as far as its history
/// shows, and a tight corridor on it is earned, not noise: None.
fn resolution_of(labels: &[f64]) -> Option<f64> {
    let mut sorted = labels.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.dedup();
    if sorted.len() == labels.len() {
        return None;
    }
    Some(
        sorted
            .windows(2)
            .map(|w| w[1] - w[0])
            .min_by(f64::total_cmp)
            .unwrap_or(0.0),
    )
}

fn band_shape() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("metric", DataType::Utf8, true),
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("grain", DataType::Utf8, true),
        Field::new("aggregation", DataType::Utf8, true),
        Field::new("trained_on", DataType::Int64, true),
        // The date column the walk anchored on, and whether a
        // temporal_profile verdict named it. Per metric, not per point.
        Field::new("axis", DataType::Utf8, true),
        Field::new("axis_judged", DataType::Boolean, true),
        // The service that answered, and its version: what the bands
        // rest on. Per metric, not per point.
        Field::new("served_by", DataType::Utf8, true),
        Field::new("point_seq", DataType::Int64, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("actual", DataType::Float64, true),
        Field::new("p05", DataType::Float64, true),
        Field::new("p10", DataType::Float64, true),
        Field::new("p50", DataType::Float64, true),
        Field::new("p90", DataType::Float64, true),
        Field::new("p95", DataType::Float64, true),
        Field::new("pit", DataType::Float64, true),
        // Why a point carries no PIT, where it carries none.
        Field::new("withheld", DataType::Utf8, true),
        // True on the newest point while the extract's horizon falls
        // inside its month — the detector skips it for the newest
        // complete one.
        Field::new("partial", DataType::Boolean, true),
    ]
}
