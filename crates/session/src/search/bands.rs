//! The metric-bands walk and its points, and the period SQL every
//! monthly reader shares.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::{FORECAST, SessionError};
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
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

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

/// The last day of a `YYYY-MM` period, `YYYY-MM-DD`.
fn month_end(period: &str) -> Option<String> {
    let (y, m) = period.split_once('-')?;
    let (y, m): (i32, u32) = (y.parse().ok()?, m.parse().ok()?);
    let next = if m == 12 {
        chrono::NaiveDate::from_ymd_opt(y + 1, 1, 1)
    } else {
        chrono::NaiveDate::from_ymd_opt(y, m + 1, 1)
    }?;
    Some((next - chrono::Duration::days(1)).to_string())
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
    const LEVELS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
    /// Origins walked per metric: the last six months, each the end of
    /// the history the month after it is forecast from.
    const ORIGINS: usize = 6;
    /// The months a metric needs to walk at all: one origin with the
    /// six known values the service needs behind it. A metric with
    /// fewer months than a full walk walks the origins it has.
    const MIN_MONTHS: usize = 7;

    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let anchors = crate::cube::Anchors::at(&rctx, dataset).await?;
    let runtime = shared.runtime();
    if !runtime.serves(FORECAST) {
        return Err(crate::session::no_model("metric_bands()", FORECAST));
    }

    // Every metric's series goes to the service together, once the
    // loop has read them all: one table answers a workspace, and the
    // service forecasts the series of one request together. A metric's
    // rows are built after the answer, in the loop's order.
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
    enum Row {
        Ready(Value),
        Walked(Walked),
    }

    let mut out: Vec<Row> = Vec::new();
    let mut seq = 0i64;
    for slot in current_query_slots(&rctx, dataset).await? {
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };

        // The grounding is a grain-free extract with a time axis and a
        // `value` column; the time column by dtype, as any reader finds
        // it. A grounding that does not plan fails the walk whole.
        let probe = crate::plan::build_plan(shared, &ctx, sql).await?;
        let fields = probe.schema();
        let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
        if !has("value") {
            continue;
        }
        // The judged column where a verdict stands, the first date
        // column by dtype where none does. Only the *column* is judged:
        // the walk's cadence stays month, the grain the service's
        // intervals are measured at. The measurement names the axis it
        // took, so a walk anchored on an unjudged column says so rather
        // than reading like the cube's.
        let sources = crate::provenance::served_sources(&probe, dataset);
        let judged = crate::cube::judged_time_column(fields, &sources, &anchors.temporal);
        let judged_axis = judged.as_ref().map(|(column, ..)| column.clone());
        let Some(tcol) = judged_axis
            .clone()
            .or_else(|| crate::plan::date_column(fields.fields()))
        else {
            continue;
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
        // The series and the extract's shape, through the engine. A
        // grounding the engine refuses at execution — a timestamp its
        // month bucketing cannot hold, a column it cannot truncate —
        // abstains on this metric with the engine's reason; the other
        // metrics walk. Only a plan that does not build fails the
        // walk whole, above.
        let served = async {
            let series = run_monthly(shared, &ctx, sql, &tcol, verb).await?;
            let (horizon, sub_monthly) = extract_shape(shared, &ctx, sql, &tcol).await?;
            Ok::<_, SessionError>((series, horizon, sub_monthly))
        }
        .await;
        let (series, horizon, sub_monthly) = match served {
            Ok(served) => served,
            Err(e) => {
                let Some(reason) = e.abstention() else {
                    return Err(e);
                };
                out.push(Row::Ready(json!({
                    "seq": seq, "metric": slot.aspect, "applicable": false,
                    "reason": reason,
                })));
                seq += 1;
                continue;
            }
        };
        let n = series.len();
        if n < MIN_MONTHS {
            out.push(Row::Ready(json!({
                "seq": seq, "metric": slot.aspect, "applicable": false,
                "reason": format!("{n} months, need {MIN_MONTHS}"),
            })));
            seq += 1;
            continue;
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
        out.push(Row::Walked(Walked {
            seq,
            aspect: slot.aspect.clone(),
            aggregation,
            tcol,
            axis_judged,
            series: monthly,
            horizon,
        }));
        seq += 1;
    }

    // The one table, as the contract has the walk send it: `member`
    // the metric, `period` the month, one value column, every row
    // actual. The service walks the last six months of every member
    // and answers one row per member, walked month and level, with the
    // actual and the PIT; a member it cannot walk fails alone, in the
    // answer's `errors`.
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
        let (mut members, mut periods, mut values) = (Vec::new(), Vec::new(), Vec::new());
        for w in &walked {
            for (period, value) in &w.series {
                members.push(w.aspect.as_str());
                periods.push(period.as_str());
                values.push(*value);
            }
        }
        let table = RecordBatch::try_new(
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
        .map_err(SessionError::from)?;
        let spec = json!({
            "targets": ["value"], "origins": ORIGINS, "horizon": 1, "levels": LEVELS,
        });
        Some(
            runtime
                .table(FORECAST, table, spec)
                .await
                .map_err(SessionError::Runtime)?,
        )
    };
    let (points, errors, served_by) = match &answered {
        Some(answered) => (
            walk_points(&answered.table, &LEVELS)
                .map_err(|e| SessionError::Runtime(format!("the forecast service answered {e}")))?,
            walk_errors(&answered.answer),
            Some(format!(
                "{} {}",
                answered.answer["service"].as_str().unwrap_or("?"),
                answered.answer["version"].as_str().unwrap_or("?")
            )),
        ),
        None => (HashMap::new(), HashMap::new(), None),
    };

    let mut rows = Vec::new();
    for row in out {
        let walked = match row {
            Row::Ready(value) => {
                rows.push(value);
                continue;
            }
            Row::Walked(walked) => walked,
        };
        let Walked {
            seq,
            aspect,
            aggregation,
            tcol,
            axis_judged,
            series,
            horizon,
        } = walked;
        let n = series.len();
        let mut served: Vec<Value> = Vec::new();
        for (t, (period, value)) in series.iter().enumerate() {
            let Some(actual) = *value else { continue };
            let Some(point) = points.get(&(aspect.clone(), period.clone())) else {
                continue;
            };
            let q = point.levels;
            // The newest month is partial while the extract's horizon
            // falls inside it: a short sum against a corridor fitted
            // on whole months reads as a breach that is only the
            // calendar. The point serves its bands and its actual so
            // far, withholds the PIT, and says so — the detector
            // scores the newest complete month instead.
            let partial = (t == n - 1)
                .then(|| horizon.as_deref().zip(month_end(period)))
                .flatten()
                .filter(|(h, end)| *h < end.as_str())
                .map(|(h, end)| {
                    format!("partial: the extract ends {h}, before the period's last day {end}")
                });
            // A series that repeats values moves on a grid (a ratio over
            // a fixed field, a count). A corridor narrower than the
            // grid's step, with the actual inside a step of the median,
            // is the model's noise around a value the series takes
            // exactly: the PIT read against it says nothing, so the
            // point serves its band and actual and withholds the PIT
            // with the reason. An actual further off than a step is a
            // real move whatever the corridor; a series that never
            // repeats has no grid, and a tight corridor on it is earned.
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

        // A metric the service walked nowhere, and said why, is
        // inapplicable with its reasons; one it walked nowhere without
        // a word (every recent month unvalued) serves its head alone.
        if served.is_empty()
            && let Some(reasons) = errors.get(&aspect)
        {
            rows.push(json!({
                "seq": seq, "metric": aspect, "applicable": false,
                "reason": reasons.join("; "),
                "served_by": served_by,
            }));
            continue;
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
    }
    if rows.is_empty() {
        rows.push(json!({}));
    }
    rows_batch(rows, band_shape())
}

/// One walked point as the service answered it: the band value per
/// level asked, in the order asked, and the PIT where the actual was
/// known to the service.
struct Point {
    levels: [f64; 5],
    pit: Option<f64>,
}

/// The answer's period rows, by member and period: one row per level
/// folded into one point. A total row (`span` set) is none of the
/// walk's and is skipped; a level nobody asked, or a point missing a
/// level, is a wrong answer.
fn walk_points(
    answer: &RecordBatch,
    levels: &[f64; 5],
) -> Result<HashMap<(String, String), Point>, String> {
    let column = |name: &str| {
        answer
            .column_by_name(name)
            .ok_or_else(|| format!("no `{name}` column"))
    };
    let floats = |name: &str| -> Result<Float64Array, String> {
        cast_with_options(
            column(name)?,
            &DataType::Float64,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )
        .map_err(|e| format!("`{name}` does not read as a number: {e}"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| format!("`{name}` does not read as a number"))
    };
    let (member, period, span) = (column("member")?, column("period")?, column("span")?);
    let (level, value, pit) = (floats("level")?, floats("value")?, floats("pit")?);
    let mut out: HashMap<(String, String), [Option<f64>; 5]> = HashMap::new();
    let mut pits: HashMap<(String, String), Option<f64>> = HashMap::new();
    for i in 0..answer.num_rows() {
        if !span.is_null(i) || period.is_null(i) || member.is_null(i) || level.is_null(i) {
            continue;
        }
        let key = (
            array_value_to_string(member, i).map_err(|e| e.to_string())?,
            array_value_to_string(period, i).map_err(|e| e.to_string())?,
        );
        let asked = levels
            .iter()
            .position(|l| (l - level.value(i)).abs() < 1e-9)
            .ok_or_else(|| format!("a level {} nobody asked", level.value(i)))?;
        out.entry(key.clone()).or_default()[asked] = (!value.is_null(i)).then(|| value.value(i));
        pits.insert(key, (!pit.is_null(i)).then(|| pit.value(i)));
    }
    out.into_iter()
        .map(|(key, q)| {
            let mut levels = [0.0; 5];
            for (slot, v) in levels.iter_mut().zip(q) {
                *slot =
                    v.ok_or_else(|| format!("{} at {} without every level asked", key.0, key.1))?;
            }
            let pit = pits.remove(&key).flatten();
            Ok((key, Point { levels, pit }))
        })
        .collect()
}

/// The answer's `errors` by member: what failed alone, in the service's
/// words, the origin named where one was.
pub(super) fn walk_errors(answer: &Value) -> HashMap<String, Vec<String>> {
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
