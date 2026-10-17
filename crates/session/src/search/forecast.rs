//! The forecast as a measurement at an origin. `metric_forecast_ahead`
//! is the door behind the `metric_forecast` measurement;
//! `forecast_points()` and `forecast_coverage()` read what landed.
//!
//! Terms. The origin is the last period with an actual. A step is one
//! period past it. The horizon is how many steps are asked. A span is
//! the total of the first so many steps. A level is a quantile the
//! service answers, and the band between two levels is the range the
//! actual should land in that often. Coverage is the share of actuals
//! that landed inside a band.
//!
//! The door reads each applicable metric as a whole — the cube's
//! `dimension = ''` cells, not the members of any dimension — at its
//! own resolution over its rung, so a forecast period is a cube bucket.
//! It sends one table per grain and totals to the forecast point: the
//! history as `actual` rows, the horizon's periods ahead as `forecast`
//! rows with no value. What lands is the model's quantiles per step and
//! span, the origin and the service that answered; no actual and no
//! PIT, since neither is known at the origin. `forecast_points()` joins
//! the actual from the cube at read and says where it landed against
//! the bands; `forecast_coverage()` counts that over every forecast the
//! record holds. The PIT stays the walk's (`bands.rs`): the exact
//! quantile needs the model's distribution, which only the service has
//! at the time of the call.
//!
//! A range is served with how often such a range held. The service
//! answers, per step and span, the share of actual values its bands
//! held on held-out data at the grain (`coverage` in the answer
//! object); it lands with the point as `service_80` and `service_90`.
//! `forecast_points()` serves `held_80` and `held_90` from the metric's
//! own record where [`RECORD_FLOOR`] of its points have landed at that
//! grain, step and span, else from the dataset's, else the service's,
//! and `held_from` says which.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use serde_json::{Value, json};

use super::rows_batch;
use crate::cube::Resolution;
use crate::period;
use crate::reads::Shared;
use crate::session::{FORECAST, SessionError};

/// The levels asked, the walk's own: the 80% band is p10 to p90, the
/// 90% band p05 to p95.
const LEVELS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
/// The known values the service needs behind an origin.
const MIN_PERIODS: usize = 6;
/// The measurement aspect the door fills and the reads flatten.
const ASPECT: &str = "metric_forecast";
/// Landed points a record needs at one grain, step and span before its
/// own share stands in for the service's: at 30 the standard error of
/// a share near 0.8 is 0.07.
const RECORD_FLOOR: i64 = 30;

/// The five quantiles of one point, in the order of [`LEVELS`].
type Quantiles = [f64; 5];
/// A point in the answer: the member, the step and, for a total, the span.
type PointKey = (String, i64, Option<i64>);
/// The service's held-out share inside the 80% and the 90% band at one
/// grain, step and span; None where it measured none.
type Held = (Option<f64>, Option<f64>);
/// One request to the service: the grain and the spans asked, and the
/// metrics (as indices into the door's rows) that go in it.
type Group = ((&'static str, Vec<usize>), Vec<usize>);
/// A coverage row's key: the metric (`''` the dataset), the grain, the
/// step, the span and what it runs to.
type TallyKey = (String, String, i64, Option<i64>, Option<String>);

/// The horizon per grain, fixed where the service measured its
/// coverage: a year of months, a quarter of weeks, a month of days, a
/// year of quarters. A grain the service does not forecast is the
/// reason the metric is inapplicable.
fn horizon_of(resolution: Resolution) -> Result<(&'static str, usize), String> {
    match resolution {
        Resolution::Day => Ok(("day", 31)),
        Resolution::Week => Ok(("week", 13)),
        Resolution::Month => Ok(("month", 12)),
        Resolution::Quarter => Ok(("quarter", 4)),
        Resolution::Minute | Resolution::Hour => Err(format!(
            "the {} grain is finer than the service forecasts (day, week, month, quarter)",
            resolution.as_str()
        )),
        Resolution::Year => Err("a year grain has too few periods to forecast".into()),
    }
}

/// The totals a flow asks for: the periods left in the quarter and the
/// year the first period ahead falls in, to their end — a day metric
/// asks for its month to go — where they fit in the horizon. A step
/// belongs to a span while its bucket starts before the span's end.
/// A span the horizon cannot reach is not asked. Two spans of one
/// length are asked once and served under both names. `ahead` is the
/// horizon's steps and one more, so the reach is read off the list.
fn spans_of(
    ahead: &[i64],
    horizon: usize,
    ends: &[(Resolution, Option<i64>)],
) -> Vec<(usize, &'static str)> {
    let mut out = Vec::new();
    for (span, end) in ends {
        let Some(end) = end else { continue };
        let reached = ahead.iter().filter(|s| *s < end).count();
        let n = reached.min(horizon);
        if n >= 1 && reached <= horizon {
            out.push((n, span.as_str()));
        }
    }
    out
}

/// The spans a grain's flow asks for, each ending where the calendar
/// says: the month for a day, the quarter for a week, the quarter and
/// the year for a month, the year for a quarter.
fn span_grains(resolution: Resolution) -> &'static [Resolution] {
    match resolution {
        Resolution::Day => &[Resolution::Month],
        Resolution::Week => &[Resolution::Quarter],
        Resolution::Month => &[Resolution::Quarter, Resolution::Year],
        Resolution::Quarter => &[Resolution::Year],
        _ => &[],
    }
}

/// Where each span of the first period ahead ends: the start of the
/// span after it, from the calendar.
async fn span_ends(
    first: i64,
    resolution: Resolution,
) -> Result<Vec<(Resolution, Option<i64>)>, SessionError> {
    let spans = span_grains(resolution);
    if spans.is_empty() {
        return Ok(Vec::new());
    }
    let exprs: Vec<String> = spans
        .iter()
        .map(|s| format!("{} + {}", period::bucket("p", *s), period::interval(*s, 1)))
        .collect();
    let ends = period::over(&[first], &exprs)
        .await
        .map_err(period::refused)?;
    Ok(spans
        .iter()
        .zip(ends)
        .map(|(s, e)| (*s, e.first().copied().flatten()))
        .collect())
}

/// A metric's whole line — the `''` dimension — from the cube's cells
/// at its own resolution: `(period, value)` in period order. The cells
/// are the cube's own, bounded by the metric's rung.
pub(crate) fn whole_series(partitions: &[RecordBatch], metric: &str) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for batch in partitions {
        if batch.num_rows() == 0 {
            continue;
        }
        let text = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        };
        let (Some(metrics), Some(dimensions)) = (text("metric"), text("dimension")) else {
            continue;
        };
        if metrics.value(0) != metric {
            continue;
        }
        let periods = batch.column_by_name("period").and_then(period::column);
        let values = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let (Some(periods), Some(values)) = (periods, values) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if dimensions.value(i).is_empty() && !periods.is_null(i) && !values.is_null(i) {
                out.push((periods.value(i), values.value(i)));
            }
        }
    }
    out.sort_by_key(|(p, _)| *p);
    out.dedup_by_key(|(p, _)| *p);
    out
}

/// One metric the door forecasts: what it sends and how it labels the
/// answer.
struct Ahead {
    seq: i64,
    metric: String,
    grain: &'static str,
    horizon: usize,
    origin: i64,
    spans: Vec<(usize, &'static str)>,
    series: Vec<(i64, f64)>,
    /// The horizon's periods, the `k`th at index `k - 1`.
    ahead: Vec<i64>,
}

enum Row {
    Ready(Value),
    Ahead(Ahead),
}

/// `metric_forecast_ahead('<dataset>')` — the door behind the
/// `metric_forecast` measurement: one row per metric and point, the
/// model's quantiles for the periods ahead from the origin. Reads the
/// bound dataset's cube, so the argument must be the dataset in use.
pub(crate) async fn metric_forecast_ahead(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    let runtime = shared.runtime();
    if !runtime.serves(FORECAST) {
        return Err(crate::session::no_model("metric_forecast()", FORECAST));
    }
    let bound = shared.dataset.read().expect("state lock").clone();
    if bound.as_deref() != Some(dataset) {
        return Err(SessionError::BadSubject(format!(
            "metric_forecast_ahead('{dataset}') reads the cube of the dataset in use — USE {dataset} first"
        )));
    }
    let cubes = crate::cube::cubes(shared).await?;
    let served =
        crate::cube::metric_series_batch(shared, None, crate::cube::World::Actual, None).await?;

    let mut out: Vec<Row> = Vec::new();
    // A plan is forecast by nobody (fixture 27): its cells are the plan
    // world of the metric it names, not a metric.
    let metrics = cubes.iter().filter(|c| c.fact.plan_of.is_none());
    for (seq, cube) in metrics.enumerate() {
        let seq = seq as i64;
        let fact = &cube.fact;
        let not_applicable = |reason: String| {
            Row::Ready(json!({
                "seq": seq, "metric": fact.metric, "applicable": false, "reason": reason,
            }))
        };
        if !fact.applicable {
            out.push(not_applicable(
                fact.reason
                    .clone()
                    .unwrap_or_else(|| "the cube admits no series".into()),
            ));
            continue;
        }
        let Some(resolution) = fact.resolution else {
            out.push(not_applicable("the cube names no resolution".into()));
            continue;
        };
        let (grain, horizon) = match horizon_of(resolution) {
            Ok(h) => h,
            Err(reason) => {
                out.push(not_applicable(reason));
                continue;
            }
        };
        let series = whole_series(&served.partitions, &fact.metric);
        let n = series.len();
        if n < MIN_PERIODS {
            out.push(not_applicable(format!(
                "{n} {grain}s of history: a forecast needs {MIN_PERIODS}"
            )));
            continue;
        }
        let origin = series[n - 1].0;
        // The horizon's periods and one more, so a span's reach reads
        // off the list.
        let mut ahead = period::ahead(origin, resolution, horizon + 1)
            .await
            .map_err(period::refused)?;
        // A total is a sum of periods: a flow has one, a stock or a
        // ratio has none.
        let spans = match (fact.behavior.as_deref(), ahead.first()) {
            (Some("flow"), Some(&first)) => {
                spans_of(&ahead, horizon, &span_ends(first, resolution).await?)
            }
            _ => Vec::new(),
        };
        ahead.truncate(horizon);
        out.push(Row::Ahead(Ahead {
            seq,
            metric: fact.metric.clone(),
            grain,
            horizon,
            origin,
            spans,
            series,
            ahead,
        }));
    }

    // One table per grain and totals: the service applies one
    // specification to every member of a request, so metrics that ask
    // for different spans go in different requests.
    let mut groups: Vec<Group> = Vec::new();
    for (i, row) in out.iter().enumerate() {
        if let Row::Ahead(a) = row {
            let key = (a.grain, a.spans.iter().map(|(n, _)| *n).collect::<Vec<_>>());
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, members)) => members.push(i),
                None => groups.push((key, vec![i])),
            }
        }
    }
    let mut points: HashMap<PointKey, Quantiles> = HashMap::new();
    let mut coverage: HashMap<(&'static str, i64, Option<i64>), Held> = HashMap::new();
    let mut errors: HashMap<String, Vec<String>> = HashMap::new();
    let mut served_by: Option<String> = None;
    for ((grain, spans), members) in &groups {
        let (mut names, mut periods, mut values, mut status) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for &i in members {
            let Row::Ahead(a) = &out[i] else { continue };
            for (d, v) in &a.series {
                names.push(a.metric.as_str());
                periods.push(Some(*d));
                values.push(Some(*v));
                status.push("actual");
            }
            for d in &a.ahead {
                names.push(a.metric.as_str());
                periods.push(Some(*d));
                values.push(None);
                status.push("forecast");
            }
        }
        let table = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("member", DataType::Utf8, false),
                period::field("period", true),
                Field::new("value", DataType::Float64, true),
                Field::new("status", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(TimestampMicrosecondArray::from(periods)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(status)),
            ],
        )
        .map_err(SessionError::from)?;
        let horizon = members
            .iter()
            .filter_map(|&i| match &out[i] {
                Row::Ahead(a) => Some(a.horizon),
                Row::Ready(_) => None,
            })
            .max()
            .unwrap_or(1);
        let mut spec = json!({
            "targets": ["value"], "horizon": horizon, "levels": LEVELS, "grain": grain,
        });
        if !spans.is_empty() {
            spec["totals"] = json!(spans);
        }
        let answered = runtime
            .table(FORECAST, table, spec)
            .await
            .map_err(SessionError::Runtime)?;
        points
            .extend(fold_points(&answered.table).map_err(|e| {
                SessionError::Runtime(format!("the forecast service answered {e}"))
            })?);
        for ((step, span), held) in fold_coverage(&answered.answer) {
            coverage.insert((grain, step, span), held);
        }
        for (member, said) in super::bands::walk_errors(&answered.answer) {
            errors.entry(member).or_default().extend(said);
        }
        served_by.get_or_insert_with(|| {
            format!(
                "{} {}",
                answered.answer["service"].as_str().unwrap_or("?"),
                answered.answer["version"].as_str().unwrap_or("?")
            )
        });
    }

    let mut rows = Vec::new();
    for row in out {
        let a = match row {
            Row::Ready(value) => {
                rows.push(value);
                continue;
            }
            Row::Ahead(a) => a,
        };
        let mut served: Vec<Value> = Vec::new();
        let mut point = |step: usize, span: Option<usize>, to_go: Option<&str>| {
            let key = (a.metric.clone(), step as i64, span.map(|s| s as i64));
            let Some(q) = points.get(&key) else { return };
            let Some(&period) = step.checked_sub(1).and_then(|k| a.ahead.get(k)) else {
                return;
            };
            let held = coverage
                .get(&(a.grain, step as i64, span.map(|s| s as i64)))
                .copied()
                .unwrap_or_default();
            served.push(json!({
                "step": step as i64, "span": span.map(|s| s as i64), "to_go": to_go,
                "period": period::date(period),
                "p05": q[0], "p10": q[1], "p50": q[2], "p90": q[3], "p95": q[4],
                "service_80": held.0, "service_90": held.1,
            }));
        };
        for step in 1..=a.horizon {
            point(step, None, None);
        }
        for (span, name) in &a.spans {
            point(*span, Some(*span), Some(name));
        }
        if served.is_empty() {
            let reason = match errors.get(&a.metric) {
                Some(reasons) => reasons.join("; "),
                None => "the service answered no point for this metric".to_string(),
            };
            rows.push(json!({
                "seq": a.seq, "metric": a.metric, "applicable": false, "reason": reason,
                "grain": a.grain, "origin": period::date(a.origin), "served_by": served_by,
            }));
            continue;
        }
        for (point_seq, p) in served.iter().enumerate() {
            let mut row = serde_json::Map::new();
            row.insert("seq".into(), json!(a.seq));
            row.insert("metric".into(), json!(a.metric));
            row.insert("applicable".into(), json!(true));
            row.insert("grain".into(), json!(a.grain));
            row.insert("origin".into(), json!(period::date(a.origin)));
            row.insert("served_by".into(), json!(served_by));
            row.insert("point_seq".into(), json!(point_seq as i64));
            for (k, val) in p.as_object().expect("point object") {
                row.insert(k.clone(), val.clone());
            }
            rows.push(Value::Object(row));
        }
    }
    if rows.is_empty() {
        rows.push(json!({}));
    }
    rows_batch(rows, forecast_shape())
}

/// The answer's rows by member, step and span: one row per level
/// folded into one point. A level nobody asked, or a point missing a
/// level, is a wrong answer.
fn fold_points(answer: &RecordBatch) -> Result<HashMap<PointKey, Quantiles>, String> {
    let column = |name: &str| {
        answer
            .column_by_name(name)
            .ok_or_else(|| format!("no `{name}` column"))
    };
    let cast = |name: &str, to: DataType| {
        cast_with_options(
            column(name)?,
            &to,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )
        .map_err(|e| format!("`{name}` does not read as {to}: {e}"))
    };
    let member = column("member")?;
    let step = cast("step", DataType::Int64)?;
    let span = cast("span", DataType::Int64)?;
    let level = cast("level", DataType::Float64)?;
    let value = cast("value", DataType::Float64)?;
    let (step, span) = (
        step.as_any().downcast_ref::<Int64Array>().ok_or("`step`")?,
        span.as_any().downcast_ref::<Int64Array>().ok_or("`span`")?,
    );
    let (level, value) = (
        level
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("`level`")?,
        value
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("`value`")?,
    );
    let mut out: HashMap<PointKey, [Option<f64>; 5]> = HashMap::new();
    for i in 0..answer.num_rows() {
        if member.is_null(i) || step.is_null(i) || level.is_null(i) {
            continue;
        }
        let key = (
            array_value_to_string(member, i).map_err(|e| e.to_string())?,
            step.value(i),
            (!span.is_null(i)).then(|| span.value(i)),
        );
        let asked = LEVELS
            .iter()
            .position(|l| (l - level.value(i)).abs() < 1e-9)
            .ok_or_else(|| format!("a level {} nobody asked", level.value(i)))?;
        out.entry(key).or_default()[asked] = (!value.is_null(i)).then(|| value.value(i));
    }
    out.into_iter()
        .map(|(key, q)| {
            let mut levels = [0.0; 5];
            for (slot, v) in levels.iter_mut().zip(q) {
                *slot = v.ok_or_else(|| {
                    format!("{} at step {} without every level asked", key.0, key.1)
                })?;
            }
            Ok((key, levels))
        })
        .collect()
}

/// The answer object's `coverage`: per step and span, the service's
/// held-out share inside the 80% band (levels 0.10 to 0.90) and the
/// 90% band (0.05 to 0.95). An entry of another band, or one without
/// a step or a share, is left out.
fn fold_coverage(answer: &Value) -> HashMap<(i64, Option<i64>), Held> {
    let mut out: HashMap<(i64, Option<i64>), Held> = HashMap::new();
    for entry in answer["coverage"].as_array().into_iter().flatten() {
        let (Some(step), Some(share)) = (entry["step"].as_i64(), entry["share"].as_f64()) else {
            continue;
        };
        let (Some(low), Some(high)) = (entry["low"].as_f64(), entry["high"].as_f64()) else {
            continue;
        };
        let slot = out.entry((step, entry["span"].as_i64())).or_default();
        if (low - 0.10).abs() < 1e-9 && (high - 0.90).abs() < 1e-9 {
            slot.0 = Some(share);
        } else if (low - 0.05).abs() < 1e-9 && (high - 0.95).abs() < 1e-9 {
            slot.1 = Some(share);
        }
    }
    out
}

/// Which share a point is served with: the metric's own record where
/// [`RECORD_FLOOR`] of its points have landed at that grain, step and
/// span, else the dataset's where it reaches the floor, else the
/// service's held-out share. The source is named; none where no
/// source has a number.
fn held(
    metric: Option<&Tally>,
    dataset: Option<&Tally>,
    service: Held,
) -> (Option<f64>, Option<f64>, Option<&'static str>) {
    for (tally, from) in [(metric, "metric"), (dataset, "dataset")] {
        if let Some(t) = tally
            && t.points >= RECORD_FLOOR
        {
            let share = |n: i64| Some(n as f64 / t.points as f64);
            return (share(t.inside_80), share(t.inside_90), Some(from));
        }
    }
    match service {
        (None, None) => (None, None, None),
        (p80, p90) => (p80, p90, Some("service")),
    }
}

fn forecast_shape() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("metric", DataType::Utf8, true),
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        // The metric's own resolution, the grain the forecast is at.
        Field::new("grain", DataType::Utf8, true),
        // The last period with an actual, as the bucket's first day.
        Field::new("origin", DataType::Utf8, true),
        // The service that answered, and its version.
        Field::new("served_by", DataType::Utf8, true),
        Field::new("point_seq", DataType::Int64, true),
        // Periods past the origin; for a span, the span itself.
        Field::new("step", DataType::Int64, true),
        // Set on a total: the number of steps it sums.
        Field::new("span", DataType::Int64, true),
        // What a span runs to: `quarter`, `year` or `month`.
        Field::new("to_go", DataType::Utf8, true),
        // The period forecast; for a span, the period it ends at.
        Field::new("period", DataType::Utf8, true),
        Field::new("p05", DataType::Float64, true),
        Field::new("p10", DataType::Float64, true),
        Field::new("p50", DataType::Float64, true),
        Field::new("p90", DataType::Float64, true),
        Field::new("p95", DataType::Float64, true),
        // The service's held-out share of actuals inside the 80% band
        // (p10 to p90) and the 90% band (p05 to p95) at this grain,
        // step and span; null where it measured none.
        Field::new("service_80", DataType::Float64, true),
        Field::new("service_90", DataType::Float64, true),
    ]
}

/// Every metric's whole line from the cube at its own resolution, by
/// metric and period — what a recorded point is checked against at
/// read. Sized by the cube's rungs, never by the data.
async fn actuals(shared: &Arc<Shared>) -> Result<HashMap<String, HashMap<i64, f64>>, SessionError> {
    let served =
        crate::cube::metric_series_batch(shared, None, crate::cube::World::Actual, None).await?;
    let mut out: HashMap<String, HashMap<i64, f64>> = HashMap::new();
    for batch in &served.partitions {
        if batch.num_rows() == 0 {
            continue;
        }
        let Some(metrics) = batch
            .column_by_name("metric")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        else {
            continue;
        };
        let metric = metrics.value(0).to_string();
        let cells = whole_series(std::slice::from_ref(batch), &metric);
        out.entry(metric).or_default().extend(cells);
    }
    Ok(out)
}

/// A recorded point's actual, from the cube: the cell at the period
/// for a step; for a span, the sum of its periods' cells where every
/// one is known. `ahead` is the origin's periods ahead, the `k`th at
/// index `k - 1`.
fn actual_of(
    cells: Option<&HashMap<i64, f64>>,
    ahead: &[i64],
    step: i64,
    span: Option<i64>,
) -> Option<f64> {
    let cells = cells?;
    let steps: Vec<usize> = match span {
        Some(span) => (1..=usize::try_from(span).ok()?).collect(),
        None => vec![usize::try_from(step).ok()?],
    };
    let mut total = 0.0;
    for k in steps {
        total += cells.get(ahead.get(k.checked_sub(1)?)?)?;
    }
    Some(total)
}

/// Where an actual fell against the recorded bands.
fn landed(actual: f64, q: Quantiles) -> &'static str {
    if actual < q[0] {
        "below"
    } else if actual < q[1] {
        "low"
    } else if actual <= q[3] {
        "inside"
    } else if actual <= q[4] {
        "high"
    } else {
        "above"
    }
}

fn quantiles(point: &Value) -> Option<Quantiles> {
    Some([
        point["p05"].as_f64()?,
        point["p10"].as_f64()?,
        point["p50"].as_f64()?,
        point["p90"].as_f64()?,
        point["p95"].as_f64()?,
    ])
}

/// `forecast_points()` — the latest forecast, flattened: what
/// `metric_forecast` last landed for the bound dataset, one row per
/// metric and point, with the actual joined from the cube at read and
/// `landed` saying where it fell — `below`, `low`, `inside`, `high`
/// or `above` against p05, p10, p90 and p95 — where the actual is
/// known. The record itself, never a re-run. A metric the door found
/// inapplicable is one row with its reason and no point; a dataset
/// never forecast serves no rows.
/// The forecast world's cells for `metric_series(world => 'forecast')`:
/// per metric, the newest run's grain and its steps as `(period,
/// median)`, the period in the period unit. The totals are not
/// cells. Empty for a dataset never forecast; a metric the run
/// found inapplicable has no entry.
pub(crate) async fn world_cells(
    shared: &Arc<Shared>,
) -> Result<HashMap<String, (String, Vec<(i64, f64)>)>, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let rctx = shared.read_context().await?;
    let mut out = HashMap::new();
    let Some(verdict) = crate::cube::judged_bodies(&rctx, &dataset, ASPECT).remove(&dataset) else {
        return Ok(out);
    };
    for metric in verdict.body["metrics"].as_array().into_iter().flatten() {
        if metric["applicable"] != Value::Bool(true) {
            continue;
        }
        let (Some(name), Some(grain)) = (metric["metric"].as_str(), metric["grain"].as_str())
        else {
            continue;
        };
        let mut cells = Vec::new();
        for point in metric["points"].as_array().into_iter().flatten() {
            if !point["span"].is_null() {
                continue;
            }
            let period = point["period"].as_str().and_then(period::parse_date);
            let (Some(period), Some(median)) = (period, point["p50"].as_f64()) else {
                continue;
            };
            cells.push((period, median));
        }
        if !cells.is_empty() {
            out.insert(name.to_string(), (grain.to_string(), cells));
        }
    }
    Ok(out)
}

pub(crate) async fn forecast_points(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let rctx = shared.read_context().await?;
    let landed_bodies = crate::cube::judged_bodies(&rctx, &dataset, ASPECT);
    let mut out = Vec::new();
    if let Some(verdict) = landed_bodies.get(&dataset) {
        let computed_at = crate::cube::judged_at(&rctx, &dataset, ASPECT);
        let cells = actuals(shared).await?;
        let record: HashMap<TallyKey, Tally> = tallies(shared, &dataset, &cells)
            .await?
            .into_iter()
            .collect();
        for (seq, metric) in verdict.body["metrics"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            let head = || {
                let mut row = serde_json::Map::new();
                row.insert("seq".into(), json!(seq as i64));
                for key in [
                    "metric",
                    "applicable",
                    "reason",
                    "grain",
                    "origin",
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
            let name = metric["metric"].as_str().unwrap_or_default();
            let grain = metric["grain"].as_str().unwrap_or_default();
            // The origin's periods ahead, as far as the furthest point:
            // one pass of the calendar per metric.
            let furthest = points
                .iter()
                .filter_map(|p| Some(p["span"].as_i64().unwrap_or(0).max(p["step"].as_i64()?)))
                .max()
                .and_then(|k| usize::try_from(k).ok())
                .unwrap_or(0);
            let ahead = match (
                metric["origin"].as_str().and_then(period::parse_date),
                Resolution::parse(grain),
            ) {
                (Some(origin), Some(resolution)) => period::ahead(origin, resolution, furthest)
                    .await
                    .map_err(period::refused)?,
                _ => Vec::new(),
            };
            for (point_seq, point) in points.iter().enumerate() {
                let mut row = head();
                row.insert("point_seq".into(), json!(point_seq as i64));
                for (k, v) in point.as_object().into_iter().flatten() {
                    row.insert(k.clone(), v.clone());
                }
                let step = point["step"].as_i64();
                let span = point["span"].as_i64();
                let actual = step.and_then(|step| actual_of(cells.get(name), &ahead, step, span));
                row.insert("actual".into(), json!(actual));
                row.insert(
                    "landed".into(),
                    json!(actual.zip(quantiles(point)).map(|(a, q)| landed(a, q))),
                );
                let (held_80, held_90, held_from) = match step {
                    Some(step) => {
                        let to_go = point["to_go"].as_str().map(str::to_string);
                        let key = |who: &str| {
                            (
                                who.to_string(),
                                grain.to_string(),
                                step,
                                span,
                                to_go.clone(),
                            )
                        };
                        held(
                            record.get(&key(name)),
                            record.get(&key("")),
                            (point["service_80"].as_f64(), point["service_90"].as_f64()),
                        )
                    }
                    None => (None, None, None),
                };
                row.insert("held_80".into(), json!(held_80));
                row.insert("held_90".into(), json!(held_90));
                row.insert("held_from".into(), json!(held_from));
                out.push(Value::Object(row));
            }
        }
    }
    let mut fields = forecast_shape();
    fields.push(Field::new("actual", DataType::Float64, true));
    fields.push(Field::new("landed", DataType::Utf8, true));
    fields.push(Field::new("computed_at", DataType::Utf8, true));
    fields.push(Field::new("current", DataType::Boolean, true));
    // How often such a band held, and whose count it is: `metric`,
    // `dataset` or `service`.
    fields.push(Field::new("held_80", DataType::Float64, true));
    fields.push(Field::new("held_90", DataType::Float64, true));
    fields.push(Field::new("held_from", DataType::Utf8, true));
    rows_batch(out, fields)
}

#[derive(Default)]
struct Tally {
    points: i64,
    inside_80: i64,
    inside_90: i64,
    origins: HashSet<String>,
}

/// The record's count per metric (`''` the dataset as a whole), grain,
/// step, span and what it runs to, over every forecast the record holds
/// for the dataset, one forecast per metric and origin: the newest run
/// at that origin. `cells` is the cube's actuals the points are checked
/// against. Sorted by key. What `forecast_coverage()` serves and
/// `forecast_points()` picks a point's `held_*` share from.
async fn tallies(
    shared: &Arc<Shared>,
    dataset: &str,
    cells: &HashMap<String, HashMap<i64, f64>>,
) -> Result<Vec<(TallyKey, Tally)>, SessionError> {
    let rctx = shared.read_context().await?;
    let returning: Vec<&str> = rctx
        .functions
        .iter()
        .filter(|f| {
            f.returns.as_deref() == Some(ASPECT)
                && f.scope_dataset.as_deref().is_none_or(|s| s == dataset)
        })
        .map(|f| f.name.as_str())
        .collect();
    // Every run, not the newest: the read context keeps one row per
    // function and subject, and coverage is read over the history. The
    // relation's cells in column order: dataset, function, subject, …,
    // the body at 5 and the clock at 6.
    let mut runs: Vec<(String, Value)> = shared
        .store
        .relation_rows("measurements")
        .await?
        .into_iter()
        .filter(|r| {
            r.first().and_then(Option::as_deref) == Some(dataset)
                && r.get(2).and_then(Option::as_deref) == Some(dataset)
                && r.get(1)
                    .and_then(Option::as_deref)
                    .is_some_and(|f| returning.contains(&f))
        })
        .filter_map(|r| {
            let body = serde_json::from_str::<Value>(r.get(5)?.as_deref()?).ok()?;
            Some((r.get(6)?.clone().unwrap_or_default(), body))
        })
        .collect();
    runs.sort_by(|a, b| b.0.cmp(&a.0));
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut tallies: HashMap<TallyKey, Tally> = HashMap::new();
    for (_, body) in &runs {
        for metric in body["metrics"].as_array().into_iter().flatten() {
            if metric["applicable"] != json!(true) {
                continue;
            }
            let name = metric["metric"].as_str().unwrap_or_default().to_string();
            let grain = metric["grain"].as_str().unwrap_or_default().to_string();
            let Some(origin_text) = metric["origin"].as_str() else {
                continue;
            };
            let (Some(origin), Some(resolution)) =
                (period::parse_date(origin_text), Resolution::parse(&grain))
            else {
                continue;
            };
            if !seen.insert((name.clone(), origin_text.to_string())) {
                continue;
            }
            // The run's points, and the origin's periods ahead as far
            // as the furthest of them: one pass of the calendar per
            // run.
            let points: Vec<(i64, Option<i64>, Quantiles, Option<String>)> = metric["points"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|point| {
                    Some((
                        point["step"].as_i64()?,
                        point["span"].as_i64(),
                        quantiles(point)?,
                        point["to_go"].as_str().map(str::to_string),
                    ))
                })
                .collect();
            let furthest = points
                .iter()
                .map(|(step, span, ..)| span.unwrap_or(*step).max(*step))
                .max()
                .and_then(|k| usize::try_from(k).ok())
                .unwrap_or(0);
            let ahead = period::ahead(origin, resolution, furthest)
                .await
                .map_err(period::refused)?;
            for (step, span, q, to_go) in points {
                let Some(actual) = actual_of(cells.get(&name), &ahead, step, span) else {
                    continue;
                };
                let where_ = landed(actual, q);
                for who in [name.as_str(), ""] {
                    let tally = tallies
                        .entry((who.to_string(), grain.clone(), step, span, to_go.clone()))
                        .or_default();
                    tally.points += 1;
                    tally.inside_80 += i64::from(where_ == "inside");
                    tally.inside_90 += i64::from(matches!(where_, "low" | "inside" | "high"));
                    tally.origins.insert(origin_text.to_string());
                }
            }
        }
    }
    let mut keyed: Vec<_> = tallies.into_iter().collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(keyed)
}

/// How often a band held at a grain and step, for a cell outside the
/// forecast record — the scenario's estimates: the metric's record, the
/// dataset's, else the service's share as the newest forecast run
/// recorded it for that grain and step. Built once per read.
pub(crate) struct HeldTable {
    record: HashMap<TallyKey, Tally>,
    service: HashMap<(String, i64), Held>,
}

impl HeldTable {
    pub(crate) fn at(
        &self,
        metric: &str,
        grain: &str,
        step: i64,
    ) -> (Option<f64>, Option<f64>, Option<&'static str>) {
        let key = |who: &str| (who.to_string(), grain.to_string(), step, None, None);
        let service = self
            .service
            .get(&(grain.to_string(), step))
            .copied()
            .unwrap_or_default();
        held(
            self.record.get(&key(metric)),
            self.record.get(&key("")),
            service,
        )
    }
}

pub(crate) async fn held_table(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<HeldTable, SessionError> {
    let cells = actuals(shared).await?;
    let record: HashMap<TallyKey, Tally> = tallies(shared, dataset, &cells)
        .await?
        .into_iter()
        .collect();
    let rctx = shared.read_context().await?;
    let mut service = HashMap::new();
    if let Some(verdict) = crate::cube::judged_bodies(&rctx, dataset, ASPECT).remove(dataset) {
        for metric in verdict.body["metrics"].as_array().into_iter().flatten() {
            let Some(grain) = metric["grain"].as_str() else {
                continue;
            };
            for point in metric["points"].as_array().into_iter().flatten() {
                if !point["span"].is_null() {
                    continue;
                }
                if let Some(step) = point["step"].as_i64() {
                    service.insert(
                        (grain.to_string(), step),
                        (point["service_80"].as_f64(), point["service_90"].as_f64()),
                    );
                }
            }
        }
    }
    Ok(HeldTable { record, service })
}

/// `forecast_coverage()` — over every forecast the record holds for the
/// bound dataset, one row per metric, grain, step and span, and the
/// same with `metric = ''` for the dataset as a whole: the points whose
/// actual has landed, the share inside the 80% band (p10 to p90), the
/// share inside the 90% band (p05 to p95), and how many forecasts the
/// count comes from. One forecast per metric and origin counts, the
/// newest run at that origin. `forecast_points()` serves a point's
/// `held_*` share from these rows once one reaches [`RECORD_FLOOR`].
pub(crate) async fn forecast_coverage(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let cells = actuals(shared).await?;
    let mut out = Vec::new();
    for ((metric, grain, step, span, to_go), tally) in tallies(shared, &dataset, &cells).await? {
        let share = |n: i64| n as f64 / tally.points as f64;
        out.push(json!({
            "metric": metric, "grain": grain, "step": step, "span": span, "to_go": to_go,
            "points": tally.points,
            "inside_80": share(tally.inside_80), "inside_90": share(tally.inside_90),
            "forecasts": tally.origins.len() as i64,
        }));
    }
    rows_batch(
        out,
        vec![
            // `''` is the dataset as a whole, every metric together.
            Field::new("metric", DataType::Utf8, true),
            Field::new("grain", DataType::Utf8, true),
            Field::new("step", DataType::Int64, true),
            Field::new("span", DataType::Int64, true),
            Field::new("to_go", DataType::Utf8, true),
            // Points whose actual has landed.
            Field::new("points", DataType::Int64, true),
            // Shares of those points inside p10..p90 and p05..p95.
            Field::new("inside_80", DataType::Float64, true),
            Field::new("inside_90", DataType::Float64, true),
            // Distinct origins the points come from.
            Field::new("forecasts", DataType::Int64, true),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn spans(
        origin: &str,
        resolution: Resolution,
        horizon: usize,
    ) -> Vec<(usize, &'static str)> {
        let origin = period::parse_date(origin).unwrap();
        let ahead = period::ahead(origin, resolution, horizon + 1)
            .await
            .unwrap();
        let ends = span_ends(ahead[0], resolution).await.unwrap();
        spans_of(&ahead, horizon, &ends)
    }

    #[tokio::test]
    async fn spans_run_to_the_quarter_and_the_year_the_first_step_falls_in() {
        use Resolution::{Day, Month, Quarter, Week};
        // Origin June: July starts Q3, three months to its end and six
        // to the year's.
        assert_eq!(
            spans("2025-06-01", Month, 12).await,
            vec![(3, "quarter"), (6, "year")]
        );
        // Origin September: October starts Q4; the quarter and the
        // year end together, so one span, asked once under both names.
        assert_eq!(
            spans("2025-09-01", Month, 12).await,
            vec![(3, "quarter"), (3, "year")]
        );
        // Origin December: a whole quarter and a whole year ahead.
        assert_eq!(
            spans("2025-12-01", Month, 12).await,
            vec![(3, "quarter"), (12, "year")]
        );
        // Quarters: the year to go.
        assert_eq!(spans("2025-04-01", Quarter, 4).await, vec![(2, "year")]);
        // Days: the month to go — 30 days of June after May 31.
        assert_eq!(spans("2025-05-31", Day, 31).await, vec![(30, "month")]);
        // Weeks: the quarter to go; a week belongs to the quarter its
        // Monday falls in. 2025-06-30 is a Monday: July's first week
        // starts 2025-07-07 and the quarter's last on 2025-09-29.
        assert_eq!(spans("2025-06-30", Week, 13).await, vec![(13, "quarter")]);
        // A span the horizon cannot reach is not asked: the third
        // quarter of 2024 starts on a Monday and holds fourteen.
        assert_eq!(spans("2024-06-24", Week, 13).await, vec![]);
    }

    #[test]
    fn an_actual_lands_against_the_bands() {
        let q = [90.0, 92.0, 100.0, 108.0, 110.0];
        assert_eq!(landed(80.0, q), "below");
        assert_eq!(landed(91.0, q), "low");
        assert_eq!(landed(100.0, q), "inside");
        assert_eq!(landed(109.0, q), "high");
        assert_eq!(landed(120.0, q), "above");
    }

    #[test]
    fn the_share_held_comes_from_the_nearest_record_that_reaches_the_floor() {
        let tally = |points, inside_80, inside_90| Tally {
            points,
            inside_80,
            inside_90,
            origins: HashSet::new(),
        };
        let service = (Some(0.75), Some(0.87));
        assert_eq!(
            held(None, None, service),
            (Some(0.75), Some(0.87), Some("service"))
        );
        // Neither record reaches the floor: the service's share.
        assert_eq!(
            held(Some(&tally(29, 29, 29)), Some(&tally(29, 0, 0)), service),
            (Some(0.75), Some(0.87), Some("service"))
        );
        // The dataset's does, the metric's own does not.
        assert_eq!(
            held(Some(&tally(29, 29, 29)), Some(&tally(40, 32, 36)), service),
            (Some(0.8), Some(0.9), Some("dataset"))
        );
        // The metric's own comes first.
        assert_eq!(
            held(Some(&tally(30, 15, 30)), Some(&tally(40, 32, 36)), service),
            (Some(0.5), Some(1.0), Some("metric"))
        );
        assert_eq!(held(None, None, (None, None)), (None, None, None));
        assert_eq!(
            held(None, None, (Some(0.7), None)),
            (Some(0.7), None, Some("service"))
        );
    }

    #[tokio::test]
    async fn a_span_sums_its_periods_only_when_every_one_is_known() {
        let d = |s: &str| period::parse_date(s).unwrap();
        let mut cells = HashMap::new();
        cells.insert(d("2025-07-01"), 10.0);
        cells.insert(d("2025-08-01"), 20.0);
        let ahead = period::ahead(d("2025-06-01"), Resolution::Month, 3)
            .await
            .unwrap();
        assert_eq!(actual_of(Some(&cells), &ahead, 2, None), Some(20.0));
        assert_eq!(actual_of(Some(&cells), &ahead, 2, Some(2)), Some(30.0));
        assert_eq!(actual_of(Some(&cells), &ahead, 3, Some(3)), None);
        assert_eq!(actual_of(None, &ahead, 1, None), None);
    }
}
