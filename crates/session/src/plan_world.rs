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
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use glossql_parser::{AspectDecl, AspectKind};
use serde_json::Value;

use crate::period::{self, Resolution};
use crate::shared::{SessionError, Shared};

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

/// The grain a plan's rows are booked against, where its declaration
/// says so: `x-covers` as a grain word, `year` or `quarter` — the
/// adopted budget on the January row covers the year, and the plan
/// reads at that grain. None where the declaration carries no word: a
/// row covers its own period. None too where the aspect is no plan or
/// the word is no grain, which [`check_declaration`] refuses.
pub(crate) fn covers(schema: &Value) -> Option<Resolution> {
    if schema["x-kind"].as_str() != Some("plan") {
        return None;
    }
    schema["x-covers"].as_str().and_then(Resolution::parse)
}

/// The grains a plan's rows may be booked against.
const COVERS: [Resolution; 2] = [Resolution::Quarter, Resolution::Year];

/// A plan declares as a QUERY aspect tied to a declared metric: refused
/// before the declaration lands where `x-kind: plan` rides another kind,
/// names no metric, names an aspect nobody declared, names a FACT or a
/// MEASUREMENT aspect, or names another plan. `x-plan-of` on an aspect
/// that is not a plan is refused too: the tie means one thing. So is
/// `x-covers` on one, and on a plan a word that is not `year` or
/// `quarter`.
pub(crate) async fn check_declaration(
    shared: &Arc<Shared>,
    d: &AspectDecl,
) -> Result<(), SessionError> {
    let name = d.name.value.as_str();
    let schema = &d.schema.value;
    let bad = |detail: String| {
        SessionError::shape(
            name,
            format!("DECLARE ASPECT {name}: {detail} (fixture 27)"),
        )
    };
    let is_plan = schema["x-kind"].as_str() == Some("plan");
    let of = schema.get("x-plan-of");
    let covers_word = schema.get("x-covers");
    if !is_plan {
        if of.is_some() {
            return Err(bad(
                "`x-plan-of` rides `x-kind: plan` — a plan is a QUERY aspect declared with both"
                    .into(),
            ));
        }
        if covers_word.is_some() {
            return Err(bad(
                "`x-covers` rides `x-kind: plan` — it names the grain a plan's rows are booked \
                 against"
                    .into(),
            ));
        }
        return Ok(());
    }
    if let Some(word) = covers_word
        && !word
            .as_str()
            .and_then(Resolution::parse)
            .is_some_and(|r| COVERS.contains(&r))
    {
        return Err(bad(format!(
            "`x-covers` names the grain a row's value is booked against, `year` or `quarter` — \
             {word} is neither; leave it out where a row covers its own period"
        )));
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
        SessionError::unknown(name, format!("metric_series(plan => '{name}'): {detail}"))
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
            let dataset = shared.dataset().unwrap_or_else(|| "<dataset>".into());
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
/// a period's end (`span` set, `to_go` the period's grain). Periods in
/// the period unit, the bucket's start.
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
            .then(|| period::parse_date(a.value(i)))
            .flatten()
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
    let Some(at) = period::civil(period) else {
        return period.to_string();
    };
    match grain {
        Resolution::Year => at.format("%Y").to_string(),
        Resolution::Quarter => format!("{}-Q{}", at.year(), (at.month() - 1) / 3 + 1),
        Resolution::Month => at.format("%Y-%m").to_string(),
        Resolution::Week | Resolution::Day => at.format("%Y-%m-%d").to_string(),
        Resolution::Hour | Resolution::Minute => at.format("%Y-%m-%dT%H:%M").to_string(),
    }
}

/// How much of a year the rows of a plan finer than the year cover —
/// what its year row is judged on: the rows the year holds against
/// the buckets the year has at the plan's own grain, the first row's
/// period and the last row's end, and the actual over those rows — a
/// flow's sum, a stock's latest level, none for a ratio, whose halves
/// the rows do not carry.
struct Coverage {
    covered: i64,
    of: i64,
    first: i64,
    last_end: i64,
    actual: Option<f64>,
}

impl Coverage {
    /// The rows cover the year whole: every bucket holds one.
    fn whole(&self) -> bool {
        self.covered >= self.of
    }
}

/// The coverage of every year the rows fall in, keyed by the year's
/// period: one pass of the calendar for each row's year and end, one
/// for the buckets a year has at the grain.
async fn coverage(
    rows: &[(i64, f64, Option<f64>)],
    grain: Resolution,
    behavior: Option<&str>,
) -> Result<HashMap<i64, Coverage>, SessionError> {
    let periods: Vec<i64> = rows.iter().map(|(p, ..)| *p).collect();
    let step = period::interval(grain, 1);
    let calendar = period::over(
        &periods,
        &[period::bucket("p", Resolution::Year), format!("p + {step}")],
    )
    .await
    .map_err(period::refused)?;
    let mut out: HashMap<i64, Coverage> = HashMap::new();
    for (i, &(p, _, actual)) in rows.iter().enumerate() {
        let at = |column: usize| {
            calendar
                .get(column)
                .and_then(|c| c.get(i).copied())
                .flatten()
        };
        let (Some(year), Some(end)) = (at(0), at(1)) else {
            continue;
        };
        let c = out.entry(year).or_insert(Coverage {
            covered: 0,
            of: 0,
            first: p,
            last_end: end,
            actual: None,
        });
        c.covered += 1;
        c.first = c.first.min(p);
        c.last_end = c.last_end.max(end);
        // The rows are in period order, so a stock's last actual is
        // its latest level.
        if let Some(a) = actual {
            c.actual = match behavior {
                Some("flow") => Some(c.actual.unwrap_or(0.0) + a),
                Some("stock") => Some(a),
                _ => None,
            };
        }
    }
    let years: Vec<i64> = out.keys().copied().collect();
    let buckets = buckets_in_year(&years, grain)
        .await
        .map_err(period::refused)?;
    for (year, n) in years.iter().zip(buckets) {
        if let Some(c) = out.get_mut(year) {
            c.of = n.unwrap_or(c.covered);
        }
    }
    Ok(out)
}

/// The buckets at a grain inside each year — twelve months, four
/// quarters, the year's Mondays or its days — asked of the calendar.
/// A week belongs to the year its Monday falls in, as the cube buckets
/// it, so a year's weeks start at its first Monday.
async fn buckets_in_year(
    years: &[i64],
    grain: Resolution,
) -> Result<Vec<Option<i64>>, DataFusionError> {
    if years.is_empty() {
        return Ok(Vec::new());
    }
    let first = match grain {
        Resolution::Week => period::bucket("p + INTERVAL '6 days'", Resolution::Week),
        _ => "p".to_string(),
    };
    let step = period::interval(grain, 1);
    let sql = format!(
        "SELECT CAST(cardinality(generate_series({first}, \
         p + INTERVAL '12 months' - INTERVAL '1 microsecond', {step})) AS BIGINT) AS n FROM p"
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![period::field("p", false)])),
        vec![Arc::new(TimestampMicrosecondArray::from_iter_values(
            years.iter().copied(),
        ))],
    )?;
    let columns = period::eval(batch, &sql).await?;
    let Some(n) = columns
        .first()
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    else {
        return Err(DataFusionError::Execution(
            "the calendar answered no bucket count".into(),
        ));
    };
    Ok((0..n.len())
        .map(|i| (!n.is_null(i)).then(|| n.value(i)))
        .collect())
}

/// What one plan row is judged to be: the actual beside the plan's
/// number, the variance, the band it was held against, where it
/// stands, and the forecast the band came from.
struct Verdict {
    actual: Option<f64>,
    variance: Option<f64>,
    band: Option<(f64, f64, f64)>,
    stands: &'static str,
    held_80: Option<f64>,
    held_from: Option<String>,
    origin: Option<String>,
}

impl Verdict {
    fn new(actual: Option<f64>, stands: &'static str) -> Verdict {
        Verdict {
            actual,
            variance: None,
            band: None,
            stands,
            held_80: None,
            held_from: None,
            origin: None,
        }
    }
}

/// One period's rows as the judgment reads them: the series' facts
/// and the period's own calendar.
struct At<'a> {
    series: &'a crate::cube::PlanSeries,
    bands: Option<&'a Vec<Band>>,
    /// The end of the origin's bucket at the metric's grain: a period
    /// ending at or before it is complete.
    origin_end: Option<i64>,
    resolution: Resolution,
    period: i64,
    /// The period's end, and for a stock at a coarser grain the bucket
    /// at the metric's own grain that holds the period's last instant.
    end: Option<i64>,
    last_inside: Option<i64>,
}

/// A row whose plan covers part of the year: the plan's number is over
/// the periods its rows cover, so the actual is too, and no band is
/// read — the forecast totals a quarter or a year to go, never part of
/// one. Landed with its variance once the covered periods are
/// complete; partial while they have begun; ahead before.
fn judge_part(at: &At<'_>, c: &Coverage, value: f64) -> Verdict {
    let begun = at.series.origin.is_some_and(|o| c.first <= o);
    let actual = if begun { c.actual } else { None };
    if at.origin_end.is_some_and(|oe| c.last_end <= oe) {
        let mut v = Verdict::new(actual, "landed");
        v.variance = actual.map(|a| a - value);
        return v;
    }
    Verdict::new(actual, if begun { "partial" } else { "ahead" })
}

/// A row whose plan covers its period whole: landed with its variance
/// once complete, else held against the band that covers it — a step's
/// at the metric's own grain; at a coarser grain, for a stock the band
/// of the period's last bucket at the metric's grain, for a flow that
/// has begun the actual to date plus the total running to the period's
/// end. Open where it has begun and no band covers the rest, ahead
/// where it has not.
fn judge_whole(at: &At<'_>, value: f64, actual: Option<f64>) -> Verdict {
    let begun = at.series.origin.is_some_and(|o| at.period <= o);
    let complete = at.end.zip(at.origin_end).is_some_and(|(e, oe)| e <= oe);
    let actual = if begun { actual } else { None };
    if complete {
        let mut v = Verdict::new(actual, "landed");
        v.variance = actual.map(|a| a - value);
        return v;
    }
    let stock = at.series.metric_behavior.as_deref() == Some("stock");
    let band = at.bands.and_then(|bs| {
        if Some(at.resolution) == at.series.metric_resolution {
            bs.iter()
                .find(|b| b.span.is_none() && b.period == at.period)
                .map(|b| (b, 0.0))
        } else if stock {
            at.last_inside.and_then(|last| {
                bs.iter()
                    .find(|b| b.span.is_none() && b.period == last)
                    .map(|b| (b, 0.0))
            })
        } else if begun {
            actual.and_then(|to_date| {
                bs.iter()
                    .find(|b| {
                        b.span.is_some()
                            && b.to_go.as_deref() == Some(at.resolution.as_str())
                            && b.period >= at.period
                            && at.end.is_some_and(|e| b.period < e)
                    })
                    .map(|b| (b, to_date))
            })
        } else {
            None
        }
    });
    let Some((b, base)) = band else {
        return Verdict::new(actual, if begun { "open" } else { "ahead" });
    };
    let (low, high) = (base + b.p10, base + b.p90);
    let stands = if value < low {
        "below"
    } else if value > high {
        "above"
    } else {
        "inside"
    };
    Verdict {
        actual,
        variance: None,
        band: Some((low, base + b.p50, high)),
        stands,
        held_80: b.held_80,
        held_from: b.held_from.clone(),
        origin: Some(period::date(b.origin)),
    }
}

/// `plan_points()` — every served plan against the actual and the
/// forecast, one row per plan and period at the plan's own grain — its
/// rows' resolution, or the grain they are booked against where the
/// declaration says so (`x-covers`) — and, where that is finer, at the
/// year — a budget booked by the month means something at the year,
/// and the planning goal judges the year rows: `plan`, `metric`,
/// `grain`, `period`, `label`, `value` (the plan's cell), `actual` (the
/// actual world's cell at that grain where the period has begun — for
/// the current period, the actual to date), `variance` (actual minus
/// plan, on a complete period), `low`, `expected`, `high` (the newest
/// forecast's 80% band and median for the period: the step's at the
/// metric's own resolution; at a coarser grain, for a flow the actual
/// to date plus the total that runs to the period's end, for a stock
/// the band of the last period of the metric's own grain inside it — a
/// year-end headcount against December's band — where the horizon
/// reaches it), `stands` (`landed`: the periods the plan covers are
/// complete; `inside`, `below`, `above`: where the plan lies against
/// the band; `partial`: begun, and the plan's rows cover part of the
/// period, so no band is read; `open`: begun, no band covers the rest;
/// `ahead`: not begun, no band), `covers` (on the year row of a plan
/// whose rows cover part of the year, how much they cover — `9 of 12
/// months`; NULL where they cover it whole), `held_80` and `held_from`
/// (how often such a band held, and whose count), `origin` (the
/// forecast's, where one was read). Where the rows cover part of the
/// year, the plan's number and the actual are over the periods the
/// rows cover. A plan finer than its metric compares to nothing.
/// Computed at read from the cube and the forecast record, nothing
/// recorded.
pub(crate) async fn plan_points(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
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
    let mut covers_col: Vec<Option<String>> = Vec::new();
    let mut held_80_col: Vec<Option<f64>> = Vec::new();
    let mut held_from_col: Vec<Option<String>> = Vec::new();
    let mut origin_col: Vec<Option<String>> = Vec::new();
    for s in &series {
        let metric_bands = bands.get(&s.metric);
        // The origin's bucket end: a plan period ending at or before
        // it is complete, its actual stands.
        let origin_end = match s.origin.zip(s.metric_resolution) {
            Some((o, r)) => Some(period::next(o, r).await.map_err(period::refused)?),
            None => None,
        };
        // A plan finer than the year: how much of each year its rows
        // cover, read off the rows at its own grain.
        let (own_grain, covering) = match s.grains.as_slice() {
            [own, _] => (
                Some(own.resolution),
                coverage(&own.rows, own.resolution, s.metric_behavior.as_deref()).await?,
            ),
            _ => (None, HashMap::new()),
        };
        for g in &s.grains {
            let resolution = g.resolution;
            // Each plan period's end and, for a stock at a coarser
            // grain, the bucket at the metric's own grain that holds
            // the period's last instant: one pass of the calendar per
            // grain.
            let periods: Vec<i64> = g.rows.iter().map(|(p, ..)| *p).collect();
            let step = period::interval(resolution, 1);
            let mut exprs = vec![format!("p + {step}")];
            if let Some(mres) = s.metric_resolution {
                exprs.push(period::bucket(
                    &format!("p + {step} - INTERVAL '1 microsecond'"),
                    mres,
                ));
            }
            let calendar = period::over(&periods, &exprs)
                .await
                .map_err(period::refused)?;
            for (i, &(period, value, actual)) in g.rows.iter().enumerate() {
                let at = At {
                    series: s,
                    bands: metric_bands,
                    origin_end,
                    resolution,
                    period,
                    end: calendar.first().and_then(|c| c.get(i).copied()).flatten(),
                    last_inside: calendar.get(1).and_then(|c| c.get(i).copied()).flatten(),
                };
                let part = (resolution == Resolution::Year)
                    .then(|| covering.get(&period))
                    .flatten()
                    .filter(|c| !c.whole());
                let v = match part {
                    Some(c) => judge_part(&at, c, value),
                    None => judge_whole(&at, value, actual),
                };
                plan_col.push(s.plan.clone());
                metric_col.push(s.metric.clone());
                grain_col.push(resolution.as_str().to_string());
                period_col.push(period);
                label_col.push(label(period, resolution));
                value_col.push(value);
                actual_col.push(v.actual);
                variance_col.push(v.variance);
                low_col.push(v.band.map(|b| b.0));
                expected_col.push(v.band.map(|b| b.1));
                high_col.push(v.band.map(|b| b.2));
                stands_col.push(v.stands.to_string());
                covers_col
                    .push(part.zip(own_grain).map(|(c, grain)| {
                        format!("{} of {} {}s", c.covered, c.of, grain.as_str())
                    }));
                held_80_col.push(v.held_80);
                held_from_col.push(v.held_from);
                origin_col.push(v.origin);
            }
        }
    }
    let utf8 = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
    let float = |name: &str, nullable: bool| Field::new(name, DataType::Float64, nullable);
    let schema = Arc::new(Schema::new(vec![
        utf8("plan", false),
        utf8("metric", false),
        utf8("grain", false),
        period::field("period", false),
        utf8("label", false),
        float("value", false),
        float("actual", true),
        float("variance", true),
        float("low", true),
        float("expected", true),
        float("high", true),
        utf8("stands", false),
        utf8("covers", true),
        float("held_80", true),
        utf8("held_from", true),
        utf8("origin", true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(plan_col)),
        Arc::new(StringArray::from(metric_col)),
        Arc::new(StringArray::from(grain_col)),
        Arc::new(TimestampMicrosecondArray::from_iter_values(period_col)),
        Arc::new(StringArray::from(label_col)),
        Arc::new(Float64Array::from(value_col)),
        Arc::new(Float64Array::from(actual_col)),
        Arc::new(Float64Array::from(variance_col)),
        Arc::new(Float64Array::from(low_col)),
        Arc::new(Float64Array::from(expected_col)),
        Arc::new(Float64Array::from(high_col)),
        Arc::new(StringArray::from(stands_col)),
        Arc::new(StringArray::from(covers_col)),
        Arc::new(Float64Array::from(held_80_col)),
        Arc::new(StringArray::from(held_from_col)),
        Arc::new(StringArray::from(origin_col)),
    ];
    RecordBatch::try_new(schema, columns).map_err(SessionError::from)
}
