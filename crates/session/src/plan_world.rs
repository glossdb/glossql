//! The plan world (fixture 27): a plan is its own QUERY aspect,
//! declared `x-kind: plan` with `x-plan-of` naming the metric it plans,
//! grounded by SQL over a landed table as a metric is. The cube builds
//! its head as it builds a metric's and serves its cells under the
//! metric it names, `metric_series(world => 'plan', plan => '<name>')`,
//! and in no other world: a plan is no metric of the actual world, no
//! series the walk or the forecast reads, no collision candidate. An
//! edition of a plan is an aspect of its own, so two editions stand
//! side by side and a comparison is a join of two plan reads.
//!
//! What this module holds is the tie: read from a declaration, checked
//! at `DECLARE`, the refusal a read by an unknown name gets, and the
//! read over every plan at once, `plan_points()` — the plan against
//! the actual and the forecast, period by period, what the planning
//! goal in the next read and the docket's plan tile read (#117). The
//! cells are the cube's (`crate::cube`).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use glossql_parser::{AspectDecl, AspectKind};
use serde_json::Value;

use crate::cube::Resolution;
use crate::reads::Shared;
use crate::session::SessionError;

/// The metric a plan aspect's declaration names, where the schema
/// declares `x-kind: plan`: `x-plan-of` as a string. None for any other
/// aspect, and for a plan schema that names no metric — which
/// [`check_declaration`] refuses before it is declared.
pub(crate) fn plan_of(schema: &Value) -> Option<&str> {
    if schema["x-kind"].as_str() != Some("plan") {
        return None;
    }
    schema["x-plan-of"].as_str()
}

/// A plan declares as a QUERY aspect tied to a declared metric: refused
/// before the declaration lands where `x-kind: plan` rides another kind,
/// names no metric, names an aspect nobody declared, names a FACT or a
/// MEASUREMENT aspect, or names another plan. `x-plan-of` on an aspect
/// that is not a plan is refused too: the tie means one thing.
pub(crate) async fn check_declaration(
    shared: &Arc<Shared>,
    d: &AspectDecl,
) -> Result<(), SessionError> {
    let name = d.name.value.as_str();
    let schema = &d.schema.value;
    let bad = |detail: String| {
        SessionError::BadSubject(format!("DECLARE ASPECT {name}: {detail} (fixture 27)"))
    };
    let is_plan = schema["x-kind"].as_str() == Some("plan");
    let of = schema.get("x-plan-of");
    if !is_plan {
        if of.is_some() {
            return Err(bad(
                "`x-plan-of` rides `x-kind: plan` — a plan is a QUERY aspect declared with both"
                    .into(),
            ));
        }
        return Ok(());
    }
    if d.kind != AspectKind::Query {
        return Err(bad(
            "a plan is a QUERY aspect — `AS QUERY ON DATASET`; its numbers are a landed table \
             read by SQL, as a metric's are"
                .into(),
        ));
    }
    let Some(metric) = of.and_then(Value::as_str) else {
        return Err(bad(
            "a plan names the metric it plans — `\"x-plan-of\": \"<metric>\"` beside \
             `\"x-kind\": \"plan\"`"
                .into(),
        ));
    };
    match shared.store.aspect(metric).await? {
        None => Err(bad(format!(
            "`{metric}` is not a declared aspect — a plan is tied to a declared QUERY aspect; \
             `DECLARE ASPECT {metric} … AS QUERY ON DATASET` first"
        ))),
        Some((_, kind, _)) if kind != "query" => Err(bad(format!(
            "`{metric}` is a {kind} aspect — a plan is tied to a QUERY aspect, a metric"
        ))),
        Some((metric_schema, _, _)) if metric_schema["x-kind"].as_str() == Some("plan") => {
            Err(bad(format!(
                "`{metric}` is itself a plan — a plan is tied to a metric, never to another plan"
            )))
        }
        Some(_) => Ok(()),
    }
}

/// The refusal for `plan => '<name>'` naming no plan the cube holds:
/// nothing declared under the name, an aspect that is not a plan, or
/// a plan with no serving grounding on the dataset in use.
pub(crate) async fn unknown(shared: &Arc<Shared>, name: &str) -> SessionError {
    let bad = |detail: String| {
        SessionError::BadSubject(format!("metric_series(plan => '{name}'): {detail}"))
    };
    match shared.store.aspect(name).await {
        Err(e) => SessionError::from(e),
        Ok(None) => bad(
            "no aspect of that name is declared — a plan is a QUERY aspect declared with \
             `x-kind: plan` and `x-plan-of: <metric>` (fixture 27)"
                .into(),
        ),
        Ok(Some((schema, kind, _))) if kind != "query" || plan_of(&schema).is_none() => {
            bad(format!(
                "`{name}` is a {kind} aspect without `x-kind: plan` — a plan is a QUERY aspect \
                 declared with it and `x-plan-of: <metric>` (fixture 27)"
            ))
        }
        Ok(Some(_)) => {
            let dataset = shared
                .dataset
                .read()
                .expect("state lock")
                .clone()
                .unwrap_or_else(|| "<dataset>".into());
            bad(format!(
                "nothing serves it on `{dataset}` — `GLOSS {name} ON {dataset} AS \
                 $${{\"sql\": \"…\"}}$$` grounds the plan; a contested slot is withheld"
            ))
        }
    }
}

// -- the read ----------------------------------------------------------

/// One recorded forecast point as the plan read uses it: a step's band
/// at the metric's own grain (`span` none), or a total's that runs to
/// a period's end (`span` set, `to_go` the period's grain). Periods as
/// nanoseconds since the epoch, the bucket's start.
struct Band {
    origin: i64,
    span: Option<i64>,
    to_go: Option<String>,
    period: i64,
    p10: f64,
    p50: f64,
    p90: f64,
    held_80: Option<f64>,
    held_from: Option<String>,
}

/// The newest forecast's points per metric, from `forecast_points()`.
async fn bands(shared: &Arc<Shared>) -> Result<HashMap<String, Vec<Band>>, SessionError> {
    let batch = crate::search::forecast_points(shared).await?;
    let text = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
    };
    let float = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
    };
    let int = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    };
    let mut out: HashMap<String, Vec<Band>> = HashMap::new();
    let (Some(metric), Some(origin), Some(period), Some(to_go), Some(held_from)) = (
        text("metric"),
        text("origin"),
        text("period"),
        text("to_go"),
        text("held_from"),
    ) else {
        return Ok(out);
    };
    let (Some(applicable), Some(step), Some(span)) = (
        batch
            .column_by_name("applicable")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>()),
        int("step"),
        int("span"),
    ) else {
        return Ok(out);
    };
    let (Some(p10), Some(p50), Some(p90), Some(held_80)) =
        (float("p10"), float("p50"), float("p90"), float("held_80"))
    else {
        return Ok(out);
    };
    let date = |a: &StringArray, i: usize| {
        (!a.is_null(i))
            .then(|| crate::search::parse_date(a.value(i)))
            .flatten()
            .and_then(crate::search::nanos_of)
    };
    for i in 0..batch.num_rows() {
        if applicable.is_null(i) || !applicable.value(i) || step.is_null(i) {
            continue;
        }
        let (Some(origin), Some(period)) = (date(origin, i), date(period, i)) else {
            continue;
        };
        if p10.is_null(i) || p50.is_null(i) || p90.is_null(i) {
            continue;
        }
        out.entry(metric.value(i).to_string())
            .or_default()
            .push(Band {
                origin,
                span: (!span.is_null(i)).then(|| span.value(i)),
                to_go: (!to_go.is_null(i)).then(|| to_go.value(i).to_string()),
                period,
                p10: p10.value(i),
                p50: p50.value(i),
                p90: p90.value(i),
                held_80: (!held_80.is_null(i)).then(|| held_80.value(i)),
                held_from: (!held_from.is_null(i)).then(|| held_from.value(i).to_string()),
            });
    }
    Ok(out)
}

/// A period as text at its grain: `2026` a year, `2026-Q3` a quarter,
/// `2026-07` a month, `2026-07-13` a week or a day, `2026-07-13T09:00`
/// an hour or a minute. What a key names a plan period by.
fn label(period: i64, grain: Resolution) -> String {
    use chrono::Datelike;
    let at = chrono::DateTime::from_timestamp_nanos(period).naive_utc();
    match grain {
        Resolution::Year => at.format("%Y").to_string(),
        Resolution::Quarter => format!("{}-Q{}", at.year(), (at.month() - 1) / 3 + 1),
        Resolution::Month => at.format("%Y-%m").to_string(),
        Resolution::Week | Resolution::Day => at.format("%Y-%m-%d").to_string(),
        Resolution::Hour | Resolution::Minute => at.format("%Y-%m-%dT%H:%M").to_string(),
    }
}

/// `plan_points()` — every served plan against the actual and the
/// forecast, one row per plan and period at the plan's own resolution
/// and, where that is finer, at the year — a budget booked by the
/// month means something at the year, and the planning goal judges
/// the year rows: `plan`, `metric`, `grain`, `period`, `label`,
/// `value` (the plan's cell), `actual` (the actual world's cell at
/// that grain where the period has begun — for the current period,
/// the actual to date),
/// `variance` (actual minus plan, on a complete period), `low`,
/// `expected`, `high` (the newest forecast's 80% band and median for
/// the period: the step's at the metric's own resolution; at a coarser
/// grain, for a flow the actual to date plus the total that runs to the
/// period's end, for a stock the band of the last period of the
/// metric's own grain inside it — a year-end headcount against
/// December's band — where the horizon reaches it), `stands`
/// (`landed`: the period is complete; `inside`, `below`, `above`: where
/// the plan lies against the band; `open`: begun, no band covers the
/// rest; `ahead`: not begun, no band), `held_80` and
/// `held_from` (how often such a band held, and whose count), `origin`
/// (the forecast's, where one was read). A plan finer than its metric
/// compares to nothing. Computed at read from the cube and the
/// forecast record, nothing recorded.
pub(crate) async fn plan_points(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    use crate::cube::next_period;
    let series = crate::cube::plan_series(shared).await?;
    let bands = if series.is_empty() {
        HashMap::new()
    } else {
        bands(shared).await?
    };
    let mut plan_col = Vec::new();
    let mut metric_col = Vec::new();
    let mut grain_col = Vec::new();
    let mut period_col = Vec::new();
    let mut label_col = Vec::new();
    let mut value_col = Vec::new();
    let mut actual_col: Vec<Option<f64>> = Vec::new();
    let mut variance_col: Vec<Option<f64>> = Vec::new();
    let mut low_col: Vec<Option<f64>> = Vec::new();
    let mut expected_col: Vec<Option<f64>> = Vec::new();
    let mut high_col: Vec<Option<f64>> = Vec::new();
    let mut stands_col = Vec::new();
    let mut held_80_col: Vec<Option<f64>> = Vec::new();
    let mut held_from_col: Vec<Option<String>> = Vec::new();
    let mut origin_col: Vec<Option<String>> = Vec::new();
    for s in &series {
        let metric_bands = bands.get(&s.metric);
        // The origin's bucket end: a plan period ending at or before
        // it is complete, its actual stands.
        let origin_end = s
            .origin
            .zip(s.metric_resolution)
            .and_then(|(o, r)| next_period(o, r));
        for g in &s.grains {
            let actual: HashMap<i64, f64> = g.actual.iter().copied().collect();
            let resolution = g.resolution;
            for &(period, value) in &g.cells {
                let end = next_period(period, resolution);
                let begun = s.origin.is_some_and(|o| period <= o);
                let complete = end.zip(origin_end).is_some_and(|(e, oe)| e <= oe);
                let a = if begun {
                    actual.get(&period).copied()
                } else {
                    None
                };
                let (mut low, mut expected, mut high) = (None, None, None);
                let (mut held_80, mut held_from, mut origin) = (None, None, None);
                let mut variance = None;
                let stands = if complete {
                    variance = a.map(|a| a - value);
                    "landed"
                } else {
                    // The band covering the period: a step's at the metric's
                    // own grain; at a coarser grain the total running to the
                    // period's end, on top of the actual to date.
                    let stock = s.metric_behavior.as_deref() == Some("stock");
                    let band = metric_bands.and_then(|bs| {
                        if Some(resolution) == s.metric_resolution {
                            bs.iter()
                                .find(|b| b.span.is_none() && b.period == period)
                                .map(|b| (b, 0.0))
                        } else if stock {
                            // A level: the period's last bucket at the
                            // metric's own grain, where the forecast
                            // reaches it; nothing to add.
                            let mut last = period;
                            if let Some(mres) = s.metric_resolution {
                                while let Some(n) = next_period(last, mres) {
                                    if end.is_some_and(|e| n < e) {
                                        last = n;
                                    } else {
                                        break;
                                    }
                                }
                            }
                            bs.iter()
                                .find(|b| b.span.is_none() && b.period == last)
                                .map(|b| (b, 0.0))
                        } else if begun {
                            a.and_then(|to_date| {
                                bs.iter()
                                    .find(|b| {
                                        b.span.is_some()
                                            && b.to_go.as_deref() == Some(resolution.as_str())
                                            && b.period >= period
                                            && end.is_some_and(|e| b.period < e)
                                    })
                                    .map(|b| (b, to_date))
                            })
                        } else {
                            None
                        }
                    });
                    match band {
                        Some((b, base)) => {
                            low = Some(base + b.p10);
                            expected = Some(base + b.p50);
                            high = Some(base + b.p90);
                            held_80 = b.held_80;
                            held_from = b.held_from.clone();
                            origin = Some(crate::search::iso(crate::search::date_of(b.origin)));
                            if value < base + b.p10 {
                                "below"
                            } else if value > base + b.p90 {
                                "above"
                            } else {
                                "inside"
                            }
                        }
                        None if begun => "open",
                        None => "ahead",
                    }
                };
                plan_col.push(s.plan.clone());
                metric_col.push(s.metric.clone());
                grain_col.push(resolution.as_str().to_string());
                period_col.push(period);
                label_col.push(label(period, resolution));
                value_col.push(value);
                actual_col.push(a);
                variance_col.push(variance);
                low_col.push(low);
                expected_col.push(expected);
                high_col.push(high);
                stands_col.push(stands.to_string());
                held_80_col.push(held_80);
                held_from_col.push(held_from);
                origin_col.push(origin);
            }
        }
    }
    let utf8 = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
    let float = |name: &str, nullable: bool| Field::new(name, DataType::Float64, nullable);
    let schema = Arc::new(Schema::new(vec![
        utf8("plan", false),
        utf8("metric", false),
        utf8("grain", false),
        Field::new(
            "period",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        utf8("label", false),
        float("value", false),
        float("actual", true),
        float("variance", true),
        float("low", true),
        float("expected", true),
        float("high", true),
        utf8("stands", false),
        float("held_80", true),
        utf8("held_from", true),
        utf8("origin", true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(plan_col)),
        Arc::new(StringArray::from(metric_col)),
        Arc::new(StringArray::from(grain_col)),
        Arc::new(TimestampNanosecondArray::from_iter_values(period_col)),
        Arc::new(StringArray::from(label_col)),
        Arc::new(Float64Array::from(value_col)),
        Arc::new(Float64Array::from(actual_col)),
        Arc::new(Float64Array::from(variance_col)),
        Arc::new(Float64Array::from(low_col)),
        Arc::new(Float64Array::from(expected_col)),
        Arc::new(Float64Array::from(high_col)),
        Arc::new(StringArray::from(stands_col)),
        Arc::new(Float64Array::from(held_80_col)),
        Arc::new(StringArray::from(held_from_col)),
        Arc::new(StringArray::from(origin_col)),
    ];
    RecordBatch::try_new(schema, columns).map_err(SessionError::from)
}
