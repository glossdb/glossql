//! The metric-bands walk and its points, and the period SQL every
//! monthly reader shares.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::util::display::array_value_to_string;
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::{Matrix, SessionError};
use crate::subject::qi;

use super::{current_query_slots, rows_batch};

/// The three verbs at any calendar grain, shared by the metric doors:
/// flows sum per period; a marked stock sums the rows standing at the
/// period's LATEST observed date — one arbitrary last row read a
/// 480-row inventory as 94k against a true 12.4M; a ratio serves `num`
/// and `den` and the period reads as sum(num)/sum(den), because a walk
/// or a slice over a summed ratio bands an artefact (DSO at
/// 928.3 days against a true 75.6). With `halves` a ratio also serves
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
    let plan = crate::whatif::build_plan(shared, ctx, &q).await?;
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
    let plan = crate::whatif::build_plan(shared, ctx, &q).await?;
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
/// recent months and ask the TabICL forward what range each month
/// should have landed in, given everything before it. The protocol —
/// monthly read at the grounding's verb, the feature recipe, the
/// point-in-time fills, the walk — is authored policy; the model call
/// is one kernel behind the runtime seam, never reimplemented.
/// Evaluated against generated ground truth before it shipped.
///
/// The feature recipe and the fill are graded protocol, exact: the
/// trailing mean excludes the current month, every fill is
/// point-in-time, and no training row exists before the first
/// observed month. Change any of it only against a graded re-run —
/// each of these three has silently drifted once.
///
/// Each walked point records its bands and its PIT — the quantile at
/// which the actual landed, 0..1 and ordinal by construction (raw
/// densities never leave the kernel). The band_breach detector
/// adjudicates PITs; this door only reports.
pub(crate) async fn metric_band_walk(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    const ALPHAS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
    const MIN_TRAIN: usize = 5;
    const MAX_WALK: usize = 6;

    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let anchors = crate::cube::Anchors::at(&rctx, dataset).await?;
    let runtime = shared.runtime();
    if !runtime.carries_model() {
        return Err(crate::session::no_model("metric_bands()"));
    }

    // Median over the present values of one feature column. Even counts
    // average the two middles, as the graded protocol's pandas median
    // does.
    fn col_median(mut values: Vec<f64>) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let k = values.len();
        Some(if k % 2 == 1 {
            values[k / 2]
        } else {
            (values[k / 2 - 1] + values[k / 2]) / 2.0
        })
    }

    let mut out = Vec::new();
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
        // it. A grounding that does not plan fails the walk whole, as
        // the script it replaces did.
        let probe = crate::whatif::build_plan(shared, &ctx, sql).await?;
        let fields = probe.schema();
        let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
        if !has("value") {
            continue;
        }
        // The judged column where a verdict stands, the first date
        // column by dtype where none does. Only the *column* is judged:
        // the walk's cadence stays month because the feature recipe is
        // month-shaped (month-of-year, the 12-month lag), and
        // re-cadencing it would change the graded protocol above. The
        // measurement names the axis it took, so a walk anchored on an
        // unjudged column says so rather than reading like the cube's.
        let sources = crate::provenance::served_sources(&probe, dataset);
        let judged = crate::cube::judged_time_column(fields, &sources, &anchors.temporal);
        let judged_axis = judged.as_ref().map(|(column, ..)| column.clone());
        let Some(tcol) = judged_axis
            .clone()
            .or_else(|| crate::whatif::date_column(fields.fields()))
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
                out.push(json!({
                    "seq": seq, "metric": slot.aspect, "applicable": false,
                    "reason": reason,
                }));
                seq += 1;
                continue;
            }
        };
        // Rows landing through the month stop mid-month when the
        // extract does; a monthly-dated series is whole at its one
        // row, and its horizon says nothing.
        let horizon = horizon.filter(|_| sub_monthly);
        let v: Vec<Option<f64>> = series.iter().map(|(_, v)| *v).collect();
        let n = v.len();
        // Feature rows start at month 1 — month 0 has no lag of its own
        // and the graded protocol builds no row for it — so a walk point
        // at t trains on t-1 rows and the earliest walkable t is
        // MIN_TRAIN + 1.
        if n < MIN_TRAIN + 2 {
            out.push(json!({
                "seq": seq, "metric": slot.aspect, "applicable": false,
                "reason": format!("{n} months, need {}", MIN_TRAIN + 2),
            }));
            seq += 1;
            continue;
        }

        // Features for month i (row i-1): index, month-of-year, the 1-
        // and 12-month lags, and the trailing 3-month MEAN — `lag3m` in
        // the graded protocol, not the raw
        // t-3 value. Absent features stay absent; they fill per walk
        // step from training rows only.
        let mut feats: Vec<[Option<f64>; 5]> = Vec::new();
        let mut labels: Vec<Option<f64>> = Vec::new();
        for i in 1..n {
            // The period in its display form is at least `YYYY-MM-DD`;
            // a shorter one is no period the walk can read.
            let moy: f64 = series[i]
                .0
                .get(5..7)
                .and_then(|m| m.parse::<i64>().ok())
                .ok_or_else(|| {
                    SessionError::Runtime(format!("period {:?} has no month", series[i].0))
                })? as f64;
            let lag1 = v[i - 1];
            let lo = i.saturating_sub(3);
            let present: Vec<f64> = (lo..i).filter_map(|j| v[j]).collect();
            let lag3m =
                (!present.is_empty()).then(|| present.iter().sum::<f64>() / present.len() as f64);
            let lag12 = if i >= 12 { v[i - 12] } else { None };
            feats.push([Some(i as f64), Some(moy), lag1, lag3m, lag12]);
            labels.push(v[i]);
        }

        let mut points: Vec<Value> = Vec::new();
        let floor = MIN_TRAIN + 1;
        let start = if n.saturating_sub(MAX_WALK) > floor {
            n - MAX_WALK
        } else {
            floor
        };
        for t in start..n {
            let Some(actual) = v[t] else { continue };
            // Absent features fill from the training rows only, per
            // feature, recomputed at this step — so the fit stays
            // point-in-time. A feature absent throughout fills 0.0.
            let mut fills = [0.0f64; 5];
            for (c, fill) in fills.iter_mut().enumerate() {
                let col: Vec<f64> = (0..t.saturating_sub(1))
                    .filter_map(|r| feats[r][c])
                    .collect();
                if let Some(m) = col_median(col) {
                    *fill = m;
                }
            }
            let filled = |row: &[Option<f64>; 5]| -> Vec<f64> {
                row.iter()
                    .enumerate()
                    .map(|(c, v)| v.unwrap_or(fills[c]))
                    .collect()
            };
            let mut train_x = Vec::new();
            let mut train_y = Vec::new();
            for r in 0..t.saturating_sub(1) {
                let Some(label) = labels[r] else { continue };
                train_x.extend(filled(&feats[r]));
                train_y.push(label);
            }
            if train_y.len() < MIN_TRAIN {
                continue;
            }
            let train = Matrix {
                data: &train_x,
                rows: train_y.len(),
                cols: 5,
            };
            let (q, pit) = runtime
                .band_point(train, &train_y, &filled(&feats[t - 1]), &ALPHAS, actual)
                .await
                .map_err(SessionError::Runtime)?;
            // A series that repeats values moves on a grid (a ratio over
            // a fixed field, a count). A corridor narrower than the
            // grid's step, with the actual inside a step of the median,
            // is the model's noise around a value the series takes
            // exactly: the PIT read against it says nothing, so the
            // point serves its band and actual and withholds the PIT
            // with the reason. An actual further off than a step is a
            // real move whatever the corridor; a series that never
            // repeats has no grid, and a tight corridor on it is earned.
            let corridor = q[4] - q[0];
            // The newest month is partial while the extract's horizon
            // falls inside it: a short sum against a corridor fitted
            // on whole months reads as a breach that is only the
            // calendar. The point serves its bands and its actual so
            // far, withholds the PIT, and says so — the detector
            // scores the newest complete month instead.
            let partial = (t == n - 1)
                .then(|| horizon.as_deref().zip(month_end(&series[t].0[..7])))
                .flatten()
                .filter(|(h, end)| *h < end.as_str())
                .map(|(h, end)| {
                    format!("partial: the extract ends {h}, before the period's last day {end}")
                });
            let withheld = match resolution_of(&train_y) {
                _ if partial.is_some() => partial.clone(),
                Some(res) if res > 0.0 && corridor < res && (actual - q[2]).abs() <= res => {
                    Some(format!(
                        "corridor {corridor:.3e} is narrower than the series' resolution {res:.3e} and the actual sits within it"
                    ))
                }
                Some(res) if res == 0.0 && actual == train_y[0] => {
                    Some("the training series is constant and the actual equals it".to_string())
                }
                _ => None,
            };
            points.push(json!({
                "period": &series[t].0[..7], "actual": actual,
                "p05": q[0], "p10": q[1], "p50": q[2], "p90": q[3], "p95": q[4],
                "pit": withheld.is_none().then_some(pit),
                "withheld": withheld,
                // Always present: the detector reads it as a struct
                // field, and a field no point carries is not in the
                // body's struct at all.
                "partial": partial.is_some(),
            }));
        }

        for (point_seq, p) in points.iter().enumerate() {
            let mut row = serde_json::Map::new();
            row.insert("seq".into(), json!(seq));
            row.insert("metric".into(), json!(slot.aspect));
            row.insert("applicable".into(), json!(true));
            row.insert("grain".into(), json!("month"));
            row.insert("aggregation".into(), json!(aggregation));
            row.insert("trained_on".into(), json!(n as i64));
            row.insert("axis".into(), json!(tcol));
            row.insert("axis_judged".into(), json!(axis_judged));
            row.insert("point_seq".into(), json!(point_seq as i64));
            for (k, val) in p.as_object().expect("point object") {
                row.insert(k.clone(), val.clone());
            }
            out.push(Value::Object(row));
        }
        if points.is_empty() {
            out.push(json!({
                "seq": seq, "metric": slot.aspect, "applicable": true,
                "grain": "month",
                "aggregation": aggregation,
                "trained_on": n as i64,
                "axis": tcol, "axis_judged": axis_judged,
            }));
        }
        seq += 1;
    }
    if out.is_empty() {
        out.push(json!({}));
    }
    rows_batch(out, band_shape())
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
