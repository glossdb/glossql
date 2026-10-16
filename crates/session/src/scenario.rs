//! The scenario world (fixture 19): a declared scenario read in two
//! passes. Pass one, the arithmetic: where a metric's grounding scans a
//! declared lever's table, the grounding is replayed with the
//! scenario's factors per period over the rows it holds, through the
//! plan rewrite (`crate::replay`) — exact, and only as far as the data
//! reaches. Pass two, the estimate: for the periods ahead of the
//! metric's last actual, up to the last period the scenario names, the
//! what-if point (`WHATIF`) estimates every metric from its own series
//! and the levers', every lever an input column, as planned and under
//! the scenario; the model's 80% band rides with how often such a band
//! held at that grain and step, by the forecast record's rule, and the
//! sign and its basis where the service gives no band. Several levers
//! are read as one change, and the answer's `parts` say what each
//! lever alone does, so the parts add up to the joint estimate; an
//! interaction between levers is not read. Nothing is recorded and
//! nothing is cached: a scenario is read, and the service is called at
//! each read.
//!
//! `metric_series(world => 'scenario', scenario => '<name>')` serves
//! the cells; `scenario_points(scenario => '<name>')` the detail per
//! cell: the pass, the band, the share and its source, the sign, the
//! basis; `scenario_levers(scenario => '<name>')` each lever's part per
//! metric. The gloss-time check ([`check`]) refuses a scenario naming
//! a column without a current `lever` gloss, before it lands.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use chrono::{Days, Months, NaiveDate};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, Value as SQLValue,
};
use glossql_glossary::Scope;
use serde_json::{Value, json};

use crate::cube::{Resolution, World};
use crate::reads::Shared;
use crate::replay::{Factor, Override};
use crate::search::{date_of, iso, nanos_of, step_date, whole_series};
use crate::session::{SessionError, WHATIF};
use crate::subject::qi;

/// The levels asked of the what-if: the 80% band around the median.
const LEVELS: [f64; 3] = [0.10, 0.50, 0.90];
/// Periods ahead a scenario may name past the last actual — the
/// service's own ceiling on a horizon.
const MAX_AHEAD: u32 = 1024;

/// One lever the scenario moves: the declared column and its factor
/// per period, with the basis the author gave.
#[derive(Debug)]
pub(crate) struct Lever {
    /// `table.column`, as declared.
    pub column: String,
    pub table: String,
    pub name: String,
    /// The factor per period, keyed by the period as written.
    pub factor: BTreeMap<String, f64>,
    pub basis: String,
}

#[derive(Debug)]
pub(crate) struct Scenario {
    pub name: String,
    pub levers: Vec<Lever>,
}

/// A period as a scenario writes it: `YYYY-MM` a calendar month,
/// `YYYY-MM-DD` a day. The half-open range of dates it covers.
pub(crate) fn period_bounds(key: &str) -> Option<(NaiveDate, NaiveDate)> {
    if key.len() == 10
        && let Ok(d) = NaiveDate::parse_from_str(key, "%Y-%m-%d")
    {
        return Some((d, d.checked_add_days(Days::new(1))?));
    }
    if key.len() == 7
        && let Ok(d) = NaiveDate::parse_from_str(&format!("{key}-01"), "%Y-%m-%d")
    {
        return Some((d, d.checked_add_months(Months::new(1))?));
    }
    None
}

/// The scenario's body, checked for shape: `levers`, each with `column`
/// (`table.column`), `factor` (an object of period to number) and
/// `basis`. The message names what is wrong.
pub(crate) fn parse(name: &str, body: &Value) -> Result<Scenario, String> {
    let levers = body
        .get("levers")
        .and_then(Value::as_array)
        .ok_or("the body carries no `levers` list")?;
    if levers.is_empty() {
        return Err("`levers` is empty — a scenario moves at least one declared lever".into());
    }
    let mut out = Vec::new();
    for (i, lever) in levers.iter().enumerate() {
        let column = lever.get("column").and_then(Value::as_str).ok_or_else(|| {
            format!("lever {i}: no `column` — a declared lever as `table.column`")
        })?;
        let (table, col) = match column.split_once('.') {
            Some((t, c)) if !t.is_empty() && !c.is_empty() && !c.contains('.') => (t, c),
            _ => return Err(format!("lever {i}: `{column}` is not `table.column`")),
        };
        let factors = lever
            .get("factor")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                format!(
                    "lever `{column}`: no `factor` — an object of period to number, \
                     {{\"2027-01\": 1.15}}"
                )
            })?;
        if factors.is_empty() {
            return Err(format!("lever `{column}`: `factor` names no period"));
        }
        let mut factor = BTreeMap::new();
        for (period, f) in factors {
            if period_bounds(period).is_none() {
                return Err(format!(
                    "lever `{column}`: `{period}` is not a period — `YYYY-MM` a month, \
                     `YYYY-MM-DD` a day"
                ));
            }
            let f = f
                .as_f64()
                .filter(|f| f.is_finite() && *f >= 0.0)
                .ok_or_else(|| {
                    format!(
                        "lever `{column}`: the factor at {period} is not a number of zero or more"
                    )
                })?;
            factor.insert(period.clone(), f);
        }
        let basis = lever
            .get("basis")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("lever `{column}`: no `basis` — why these factors"))?;
        out.push(Lever {
            column: column.to_string(),
            table: table.to_string(),
            name: col.to_string(),
            factor,
            basis: basis.to_string(),
        });
    }
    Ok(Scenario {
        name: name.to_string(),
        levers: out,
    })
}

/// The gloss-time check: the body is a scenario's, and every column it
/// names carries a current `lever` gloss of `true` in the dataset.
pub(crate) async fn check(
    shared: &Arc<Shared>,
    dataset: &str,
    aspect: &str,
    body: &Value,
) -> Result<(), SessionError> {
    let bad =
        |detail: String| SessionError::BadSubject(format!("GLOSS {aspect} ON {dataset}: {detail}"));
    let scenario = parse(aspect, body).map_err(bad)?;
    let rctx = shared.read_context().await?;
    let levers = crate::search::current_fact_values(&rctx, dataset, "lever").await?;
    for lever in &scenario.levers {
        let declared = levers
            .get(&lever.column)
            .is_some_and(|(v, _)| v["value"].as_bool() == Some(true));
        if !declared {
            return Err(bad(format!(
                "`{0}` is not a declared lever — `GLOSS lever ON {0} AS $${{\"value\": true, \
                 \"grounds\": \"who sets it\"}}$$` declares it",
                lever.column
            )));
        }
    }
    Ok(())
}

/// The scenario as its current gloss on the dataset says, by name.
pub(crate) async fn load(
    shared: &Arc<Shared>,
    dataset: &str,
    name: &str,
) -> Result<Scenario, SessionError> {
    let bad = |detail: String| SessionError::BadSubject(format!("scenario '{name}': {detail}"));
    let Some((schema, kind, _)) = shared.store.aspect(name).await? else {
        return Err(bad(
            "no aspect of that name is declared — a scenario is a FACT aspect declared with \
             `x-kind: scenario` (fixture 19)"
                .into(),
        ));
    };
    if kind != "fact" || schema["x-kind"].as_str() != Some("scenario") {
        return Err(bad(format!(
            "`{name}` is a {kind} aspect without `x-kind: scenario` — a scenario is a FACT aspect \
             declared with it (fixture 19)"
        )));
    }
    let rctx = shared.read_context().await?;
    let scope = Scope::Subject(dataset.to_string());
    let verdicts = crate::reads::verdicts(&rctx, dataset, &scope, Some(name)).await?;
    let current =
        glossql_glossary::Store::collapsed_read(dataset, &scope, Some(name), &rctx, &verdicts)
            .into_iter()
            .find(|r| r.subject == dataset && r.aspect == name)
            .ok_or_else(|| {
                bad(format!(
                    "nothing is glossed on `{dataset}` — `GLOSS {name} ON {dataset} AS \
                     $${{\"levers\": [...]}}$$`"
                ))
            })?;
    if current.state != "current" {
        return Err(bad(format!(
            "the gloss on `{dataset}` is {}",
            current.state
        )));
    }
    let body: Value = current
        .value
        .as_deref()
        .and_then(|v| serde_json::from_str(v).ok())
        .ok_or_else(|| bad("the body is not JSON".into()))?;
    parse(name, &body).map_err(bad)
}

/// The scenario's levers as the plan rewrite takes them.
fn overrides(scenario: &Scenario) -> Vec<Override> {
    scenario
        .levers
        .iter()
        .map(|l| Override {
            table: l.table.clone(),
            column: l.name.clone(),
            factors: l
                .factor
                .iter()
                .filter_map(|(period, f)| {
                    let (from, until) = period_bounds(period)?;
                    Some(Factor {
                        from: iso(from),
                        until: Some(iso(until)),
                        factor: *f,
                    })
                })
                .collect(),
        })
        .collect()
}

/// The lever's factor at a date: the period that holds it, else one.
fn factor_at(lever: &Lever, at: NaiveDate) -> f64 {
    lever
        .factor
        .iter()
        .filter_map(|(period, f)| {
            let (from, until) = period_bounds(period)?;
            (from <= at && at < until).then_some(*f)
        })
        .next_back()
        .unwrap_or(1.0)
}

/// One cell of the scenario world with its detail: `replayed` (pass
/// one, the arithmetic) or `estimated` (pass two, the what-if) — an
/// estimated cell without a value says in `basis` why the estimate did
/// not come.
pub(crate) struct Point {
    pub metric: String,
    pub grain: &'static str,
    pub period: i64,
    pub step: Option<i64>,
    pub pass: &'static str,
    pub value: Option<f64>,
    pub num: Option<f64>,
    pub den: Option<f64>,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub held_80: Option<f64>,
    pub held_90: Option<f64>,
    pub held_from: Option<&'static str>,
    pub sign: Option<String>,
    pub basis: Option<String>,
    pub served_by: Option<String>,
}

/// One metric's question for pass two: its series and the periods
/// ahead the scenario reaches.
struct Ask {
    metric: String,
    grain: &'static str,
    resolution: Resolution,
    series: Vec<(NaiveDate, f64)>,
    ahead: Vec<NaiveDate>,
}

/// What one lever alone does to one metric over the periods ahead, by
/// the service's reading of the history: the mean shift with its 90%
/// interval, from the same regression as the joint estimate, so the
/// parts add up to it. A part the service did not give carries why in
/// `basis`.
pub(crate) struct Part {
    pub metric: String,
    pub lever: String,
    pub shift: Option<f64>,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub basis: Option<String>,
    pub served_by: Option<String>,
}

/// The scenario world as read: the cells with their detail, and each
/// lever's part per estimated metric.
pub(crate) struct Read {
    pub points: Vec<Point>,
    pub parts: Vec<Part>,
}

/// The two passes over the bound dataset for the named scenario, the
/// cells alone.
pub(crate) async fn points(shared: &Arc<Shared>, name: &str) -> Result<Vec<Point>, SessionError> {
    Ok(read(shared, name).await?.points)
}

/// The two passes over the bound dataset for the named scenario.
pub(crate) async fn read(shared: &Arc<Shared>, name: &str) -> Result<Read, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let scenario = load(shared, &dataset, name).await?;
    let frames = crate::cube::scenario_frames(shared).await?;
    let actual = crate::cube::metric_series_batch(shared, None, World::Actual, None).await?;
    let ctx = shared.session_ctx();
    let overrides = overrides(&scenario);
    // The first date the scenario touches and the one past the last.
    let mut first: Option<NaiveDate> = None;
    let mut last: Option<NaiveDate> = None;
    for (from, until) in scenario
        .levers
        .iter()
        .flat_map(|l| l.factor.keys())
        .filter_map(|k| period_bounds(k))
    {
        first = Some(first.map_or(from, |f| f.min(from)));
        last = Some(last.map_or(until, |l| l.max(until)));
    }
    let (Some(first), Some(last)) = (first, last) else {
        return Ok(Read {
            points: Vec::new(),
            parts: Vec::new(),
        });
    };
    let tables: HashSet<&str> = scenario.levers.iter().map(|l| l.table.as_str()).collect();
    // What a replayed cell rests on: the arithmetic, and the author's
    // word on each lever.
    let replayed_basis = format!(
        "replayed through the grounding with the scenario's factors — {}",
        scenario
            .levers
            .iter()
            .map(|l| format!("{}: {}", l.column, l.basis))
            .collect::<Vec<_>>()
            .join("; ")
    );
    let mut out: Vec<Point> = Vec::new();
    let mut asks: Vec<Ask> = Vec::new();
    for frame in &frames {
        let grain = frame.resolution.as_str();
        // Pass one: the arithmetic, where a lever's table is scanned.
        if frame.scans.iter().any(|t| tables.contains(t.as_str())) {
            let sql =
                crate::cube::total_sql(&frame.sql, &frame.tcol, frame.verb, frame.resolution, None);
            let rows = crate::cube::replayed_series(shared, &ctx, &sql, &overrides).await?;
            let Some(first_ns) = nanos_of(first) else {
                continue;
            };
            let since = crate::cube::bucket_start(first_ns, frame.resolution);
            for (period, _, value, num, den) in rows {
                if period < since {
                    continue;
                }
                out.push(Point {
                    metric: frame.metric.clone(),
                    grain,
                    period,
                    step: None,
                    pass: "replayed",
                    value: Some(value),
                    num,
                    den,
                    low: None,
                    high: None,
                    held_80: None,
                    held_90: None,
                    held_from: None,
                    sign: None,
                    basis: Some(replayed_basis.clone()),
                    served_by: None,
                });
            }
        }
        // Pass two: the periods ahead of the metric's last actual, up to
        // the last period the scenario names.
        let series: Vec<(NaiveDate, f64)> = whole_series(&actual.partitions, &frame.metric)
            .into_iter()
            .map(|(p, v)| (date_of(p), v))
            .collect();
        let Some(&(origin, _)) = series.last() else {
            continue;
        };
        let mut ahead = Vec::new();
        for k in 1..=MAX_AHEAD {
            match step_date(origin, grain, k) {
                Some(d) if d < last => ahead.push(d),
                _ => break,
            }
        }
        if ahead.is_empty() {
            continue;
        }
        asks.push(Ask {
            metric: frame.metric.clone(),
            grain,
            resolution: frame.resolution,
            series,
            ahead,
        });
    }
    let (estimated, mut parts) = estimate(shared, &ctx, &dataset, &scenario, asks).await?;
    out.extend(estimated);
    out.sort_by(|a, b| (&a.metric, a.period, a.pass).cmp(&(&b.metric, b.period, b.pass)));
    parts.sort_by(|a, b| (&a.metric, &a.lever).cmp(&(&b.metric, &b.lever)));
    Ok(Read { points: out, parts })
}

/// Pass two: one table per grain to the what-if point, every asked
/// metric a member, every lever an input column, as planned (its last
/// value held) and under the scenario (that value times the lever's
/// factor for the period). Several levers are read as one change; the
/// answer's `parts` say what each does alone. Where the estimate cannot
/// come — no service, a lever with no series, the service's refusal —
/// every cell and every part says why.
async fn estimate(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    scenario: &Scenario,
    asks: Vec<Ask>,
) -> Result<(Vec<Point>, Vec<Part>), SessionError> {
    let mut out = Vec::new();
    let mut parts = Vec::new();
    if asks.is_empty() {
        return Ok((out, parts));
    }
    let runtime = shared.runtime();
    let held = crate::search::held_table(shared, dataset).await?;
    let mut by_grain: BTreeMap<&'static str, Vec<usize>> = BTreeMap::new();
    for (i, a) in asks.iter().enumerate() {
        by_grain.entry(a.grain).or_default().push(i);
    }
    // Every asked cell of a grain, valueless, with one reason — and
    // every lever's part the same way.
    let unanswered = |idx: &[usize], why: &str| -> (Vec<Point>, Vec<Part>) {
        let mut rows = Vec::new();
        let mut blanks = Vec::new();
        for &i in idx {
            let a = &asks[i];
            for (k, d) in a.ahead.iter().enumerate() {
                let Some(period) = nanos_of(*d) else { continue };
                rows.push(Point {
                    metric: a.metric.clone(),
                    grain: a.grain,
                    period,
                    step: Some(k as i64 + 1),
                    pass: "estimated",
                    value: None,
                    num: None,
                    den: None,
                    low: None,
                    high: None,
                    held_80: None,
                    held_90: None,
                    held_from: None,
                    sign: None,
                    basis: Some(why.to_string()),
                    served_by: None,
                });
            }
            for lever in &scenario.levers {
                blanks.push(Part {
                    metric: a.metric.clone(),
                    lever: lever.column.clone(),
                    shift: None,
                    low: None,
                    high: None,
                    basis: Some(why.to_string()),
                    served_by: None,
                });
            }
        }
        (rows, blanks)
    };
    let refuse = |idx: &[usize], why: &str, out: &mut Vec<Point>, parts: &mut Vec<Part>| {
        let (rows, blanks) = unanswered(idx, why);
        out.extend(rows);
        parts.extend(blanks);
    };
    for (grain, idx) in &by_grain {
        if !runtime.serves(WHATIF) {
            refuse(
                idx,
                &crate::session::no_model(
                    &format!(
                        "metric_series(world => 'scenario', scenario => '{}')",
                        scenario.name
                    ),
                    WHATIF,
                )
                .to_string(),
                &mut out,
                &mut parts,
            );
            continue;
        }
        let resolution = asks[idx[0]].resolution;
        // Every lever's series at the grain, placed in time by its table.
        let mut inputs: Vec<HashMap<NaiveDate, f64>> = Vec::with_capacity(scenario.levers.len());
        let mut failed = None;
        for lever in &scenario.levers {
            match lever_series(shared, ctx, lever, resolution).await {
                Ok(input) => inputs.push(input.into_iter().collect()),
                Err(why) => {
                    failed = Some(format!("no estimate: `{}`: {why}", lever.column));
                    break;
                }
            }
        }
        if let Some(why) = failed {
            refuse(idx, &why, &mut out, &mut parts);
            continue;
        }
        // One table: the members' histories with every lever beside
        // them, then each period ahead twice.
        let (mut members, mut periods, mut status, mut values) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut columns: Vec<Vec<Option<f64>>> = vec![Vec::new(); scenario.levers.len()];
        // Per metric, each lever's value held as planned: its last
        // value at or before the metric's origin.
        let mut planned_of: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        for &i in idx {
            let a = &asks[i];
            let origin = a.series.last().map(|(d, _)| *d);
            let planned: Vec<Option<f64>> = inputs
                .iter()
                .map(|input| {
                    let mut dated: Vec<(&NaiveDate, &f64)> = input
                        .iter()
                        .filter(|(d, _)| origin.is_some_and(|o| **d <= o))
                        .collect();
                    dated.sort_by_key(|(d, _)| **d);
                    dated.last().map(|(_, v)| **v)
                })
                .collect();
            planned_of.insert(a.metric.clone(), planned.clone());
            for (d, v) in &a.series {
                members.push(a.metric.as_str());
                periods.push(nanos_of(*d));
                status.push("actual");
                values.push(Some(*v));
                for (l, input) in inputs.iter().enumerate() {
                    columns[l].push(input.get(d).copied());
                }
            }
            for d in &a.ahead {
                for s in ["plan", "scenario"] {
                    members.push(a.metric.as_str());
                    periods.push(nanos_of(*d));
                    status.push(s);
                    values.push(None);
                    for (l, lever) in scenario.levers.iter().enumerate() {
                        let factor = if s == "plan" {
                            1.0
                        } else {
                            factor_at(lever, *d)
                        };
                        columns[l].push(planned[l].map(|p| p * factor));
                    }
                }
            }
        }
        let mut fields = vec![
            Field::new("member", DataType::Utf8, false),
            Field::new(
                "period",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("status", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ];
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(members)),
            Arc::new(TimestampNanosecondArray::from(periods)),
            Arc::new(StringArray::from(status)),
            Arc::new(Float64Array::from(values)),
        ];
        for (lever, column) in scenario.levers.iter().zip(columns) {
            fields.push(Field::new(&lever.column, DataType::Float64, true));
            arrays.push(Arc::new(Float64Array::from(column)));
        }
        let table = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(SessionError::from)?;
        let spec = json!({"targets": ["value"], "levels": LEVELS});
        let answered = match runtime.table(WHATIF, table, spec).await {
            Ok(answered) => answered,
            Err(e) => {
                refuse(
                    idx,
                    &format!("not served: the what-if service refused — {e}"),
                    &mut out,
                    &mut parts,
                );
                continue;
            }
        };
        let served_by = format!(
            "{} {}",
            answered.answer["service"].as_str().unwrap_or("?"),
            answered.answer["version"].as_str().unwrap_or("?")
        );
        let errors = crate::search::walk_errors(&answered.answer);
        let folded = fold(&answered.table)
            .map_err(|e| SessionError::Runtime(format!("the what-if service answered {e}")))?;
        let given = answered.answer["parts"].as_array();
        for &i in idx {
            let a = &asks[i];
            let said = folded.get(&a.metric);
            // The levers with no value to hold as planned, by name.
            let unheld: Vec<&str> = planned_of
                .get(&a.metric)
                .map(|p| {
                    scenario
                        .levers
                        .iter()
                        .zip(p)
                        .filter(|(_, v)| v.is_none())
                        .map(|(l, _)| l.column.as_str())
                        .collect()
                })
                .unwrap_or_default();
            // Why a cell or a part has no number: the service's word on
            // the member first, then its errors, then a lever with no
            // value to hold, then the default.
            let reason = |default: &str| -> Option<String> {
                said.and_then(|s| s.basis.clone())
                    .or_else(|| errors.get(&a.metric).map(|errs| errs.join("; ")))
                    .or_else(|| {
                        (!unheld.is_empty()).then(|| {
                            format!(
                                "no estimate: `{}` has no value at or before the last actual to \
                                 hold as planned",
                                unheld.join("`, `")
                            )
                        })
                    })
                    .or_else(|| Some(default.to_string()))
            };
            for (k, d) in a.ahead.iter().enumerate() {
                let Some(period) = nanos_of(*d) else { continue };
                let step = k as i64 + 1;
                let point = said.and_then(|s| s.points.get(d));
                let (held_80, held_90, held_from) = held.at(&a.metric, grain, step);
                let basis = match point {
                    Some(_) => said.and_then(|s| s.basis.clone()),
                    None => reason("the service answered no estimate for this period"),
                };
                out.push(Point {
                    metric: a.metric.clone(),
                    grain,
                    period,
                    step: Some(step),
                    pass: "estimated",
                    value: point.and_then(|p| p.median),
                    num: None,
                    den: None,
                    low: point.and_then(|p| p.low),
                    high: point.and_then(|p| p.high),
                    held_80: point.and(held_80),
                    held_90: point.and(held_90),
                    held_from: point.and(held_from),
                    sign: said.and_then(|s| s.sign.clone()),
                    basis,
                    served_by: Some(served_by.clone()),
                });
            }
            // Each lever's part, as the service gave it for this member
            // and the `value` column; a part not given carries why.
            for lever in &scenario.levers {
                let part = given.into_iter().flatten().find(|p| {
                    p["member"].as_str() == Some(a.metric.as_str())
                        && p["column"].as_str().is_none_or(|c| c == "value")
                        && p["input"].as_str() == Some(lever.column.as_str())
                });
                parts.push(Part {
                    metric: a.metric.clone(),
                    lever: lever.column.clone(),
                    shift: part.and_then(|p| p["history_shift"].as_f64()),
                    low: part.and_then(|p| p["history_low"].as_f64()),
                    high: part.and_then(|p| p["history_high"].as_f64()),
                    basis: match part {
                        Some(_) => None,
                        None => reason("the service answered no part for this lever"),
                    },
                    served_by: Some(served_by.clone()),
                });
            }
        }
    }
    Ok((out, parts))
}

/// The lever's series at a resolution: the mean of the column per
/// period of its table, placed by the table's first date column.
async fn lever_series(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    lever: &Lever,
    resolution: Resolution,
) -> Result<Vec<(NaiveDate, f64)>, String> {
    let probe =
        crate::plan::build_plan(shared, ctx, &format!("SELECT * FROM {}", qi(&lever.table)))
            .await
            .map_err(|e| e.to_string())?;
    let Some(tcol) = crate::plan::date_column(probe.schema().fields()) else {
        return Err(format!(
            "`{}` has no date column to place the lever's values in time",
            lever.table
        ));
    };
    let sql = format!(
        "SELECT {p} AS period, avg(CAST({c} AS DOUBLE)) AS value FROM {t} \
         WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 1",
        p = crate::cube::period_expr(&qi(&tcol), resolution),
        c = qi(&lever.name),
        t = qi(&lever.table),
    );
    let rows = crate::cube::replayed_series(shared, ctx, &sql, &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|(p, _, v, _, _)| (date_of(p), v))
        .collect())
}

/// What the what-if said of one member: its scenario rows folded to a
/// point per period, and the sign and basis it gave the member.
#[derive(Default)]
struct Said {
    points: HashMap<NaiveDate, Estimate>,
    sign: Option<String>,
    basis: Option<String>,
}

#[derive(Default)]
struct Estimate {
    low: Option<f64>,
    median: Option<f64>,
    high: Option<f64>,
}

/// The answer's rows by member: the `scenario` rows of the `value`
/// column, each level into its place, and the sign and basis per member.
fn fold(answer: &RecordBatch) -> Result<HashMap<String, Said>, String> {
    let column = |name: &str| {
        answer
            .column_by_name(name)
            .ok_or_else(|| format!("no `{name}` column"))
    };
    let floats = |name: &str| -> Result<Float64Array, String> {
        let c = cast(column(name)?, &DataType::Float64).map_err(|e| e.to_string())?;
        c.as_any()
            .downcast_ref::<Float64Array>()
            .cloned()
            .ok_or_else(|| format!("`{name}` did not read as a number"))
    };
    let text = |name: &str| -> Result<Option<ArrayRef>, String> {
        match answer.column_by_name(name) {
            Some(c) => Ok(Some(Arc::clone(c))),
            None => Ok(None),
        }
    };
    let member = column("member")?;
    let period = column("period")?;
    let status = column("status")?;
    let level = floats("level")?;
    let value = floats("value")?;
    let target = text("column")?;
    let sign = text("sign")?;
    let basis = text("basis")?;
    let said_at = |col: &Option<ArrayRef>, i: usize| -> Option<String> {
        let c = col.as_ref()?;
        if c.is_null(i) {
            return None;
        }
        array_value_to_string(c, i).ok().filter(|s| !s.is_empty())
    };
    let mut out: HashMap<String, Said> = HashMap::new();
    for i in 0..answer.num_rows() {
        let m = array_value_to_string(member, i).map_err(|e| e.to_string())?;
        if let Some(t) = said_at(&target, i)
            && t != "value"
        {
            continue;
        }
        let said = out.entry(m).or_default();
        if said.sign.is_none() {
            said.sign = said_at(&sign, i);
        }
        if said.basis.is_none() {
            said.basis = said_at(&basis, i);
        }
        if array_value_to_string(status, i).map_err(|e| e.to_string())? != "scenario" {
            continue;
        }
        if level.is_null(i) || value.is_null(i) || period.is_null(i) {
            continue;
        }
        let when = array_value_to_string(period, i).map_err(|e| e.to_string())?;
        let Some(date) = when
            .get(..10)
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        else {
            continue;
        };
        let point = said.points.entry(date).or_default();
        let l = level.value(i);
        let v = value.value(i);
        if (l - LEVELS[0]).abs() < 1e-9 {
            point.low = Some(v);
        } else if (l - LEVELS[1]).abs() < 1e-9 {
            point.median = Some(v);
        } else if (l - LEVELS[2]).abs() < 1e-9 {
            point.high = Some(v);
        }
    }
    Ok(out)
}

/// The scenario world's cells for `metric_series(world => 'scenario',
/// scenario => …)`: per metric, every cell with a value — the replayed
/// ones and the estimated medians — as `(period, value, num, den)`.
pub(crate) type WorldCells = HashMap<String, Vec<(i64, f64, Option<f64>, Option<f64>)>>;

pub(crate) async fn world_cells(
    shared: &Arc<Shared>,
    name: &str,
) -> Result<WorldCells, SessionError> {
    let mut out: WorldCells = HashMap::new();
    for p in points(shared, name).await? {
        if let Some(v) = p.value {
            out.entry(p.metric)
                .or_default()
                .push((p.period, v, p.num, p.den));
        }
    }
    Ok(out)
}

/// `scenario_points(scenario => '<name>')` — the scenario world cell by
/// cell: `scenario`, `metric`, `grain`, `period`, `step` (ahead of the
/// last actual; null on a replayed cell), `pass` (`replayed` or
/// `estimated`), `value`, `num`, `den`, `low` and `high` (the model's
/// 80% band on an estimate), `held_80`, `held_90` and `held_from` (how
/// often such a band held at that grain and step, and whose count:
/// `metric`, `dataset` or `service`), `sign` and `basis` (the service's
/// word on the estimate, or why there is none), `served_by`.
pub(crate) async fn scenario_points(
    shared: &Arc<Shared>,
    name: &str,
) -> Result<RecordBatch, SessionError> {
    let rows: Vec<Value> = points(shared, name)
        .await?
        .into_iter()
        .map(|p| {
            json!({
                "scenario": name, "metric": p.metric, "grain": p.grain,
                "period": period_text(p.period), "step": p.step, "pass": p.pass,
                "value": p.value, "num": p.num, "den": p.den,
                "low": p.low, "high": p.high,
                "held_80": p.held_80, "held_90": p.held_90, "held_from": p.held_from,
                "sign": p.sign, "basis": p.basis, "served_by": p.served_by,
            })
        })
        .collect();
    let utf8 = |n: &str, nullable: bool| Field::new(n, DataType::Utf8, nullable);
    let f64n = |n: &str| Field::new(n, DataType::Float64, true);
    let fields = vec![
        utf8("scenario", false),
        utf8("metric", false),
        utf8("grain", false),
        utf8("period", false),
        Field::new("step", DataType::Int64, true),
        utf8("pass", false),
        f64n("value"),
        f64n("num"),
        f64n("den"),
        f64n("low"),
        f64n("high"),
        f64n("held_80"),
        f64n("held_90"),
        utf8("held_from", true),
        utf8("sign", true),
        utf8("basis", true),
        utf8("served_by", true),
    ];
    crate::search::rows_batch(rows, fields)
}

/// `scenario_levers(scenario => '<name>')` — each lever's part of the
/// scenario world's estimate, one row per estimated metric and lever:
/// `scenario`, `metric`, `lever` (the declared `table.column`), `shift`
/// (what the lever alone does to the metric over the periods ahead, by
/// the service's reading of the history, the mean), `low` and `high`
/// (its 90% interval), `basis` (why there is no part, where there is
/// none), `served_by`. The parts add up to the joint estimate; a
/// scenario of one lever serves its one part per metric.
pub(crate) async fn scenario_levers(
    shared: &Arc<Shared>,
    name: &str,
) -> Result<RecordBatch, SessionError> {
    let rows: Vec<Value> = read(shared, name)
        .await?
        .parts
        .into_iter()
        .map(|p| {
            json!({
                "scenario": name, "metric": p.metric, "lever": p.lever,
                "shift": p.shift, "low": p.low, "high": p.high,
                "basis": p.basis, "served_by": p.served_by,
            })
        })
        .collect();
    let utf8 = |n: &str, nullable: bool| Field::new(n, DataType::Utf8, nullable);
    let f64n = |n: &str| Field::new(n, DataType::Float64, true);
    let fields = vec![
        utf8("scenario", false),
        utf8("metric", false),
        utf8("lever", false),
        f64n("shift"),
        f64n("low"),
        f64n("high"),
        utf8("basis", true),
        utf8("served_by", true),
    ];
    crate::search::rows_batch(rows, fields)
}

/// A period as text: the date where the bucket starts at midnight, the
/// full timestamp where it does not.
fn period_text(period: i64) -> String {
    let at = chrono::DateTime::from_timestamp_nanos(period).naive_utc();
    if at.time() == chrono::NaiveTime::MIN {
        iso(at.date())
    } else {
        at.format("%Y-%m-%dT%H:%M:%S").to_string()
    }
}

/// The one argument of `scenario_points` and `scenario_levers`, and of
/// `metric_series` in the scenario world: `scenario => '<name>'`.
pub(crate) fn name_arg(door: &str, args: &[FunctionArg]) -> Result<String, SessionError> {
    let refuse = |what: String| SessionError::BadSubject(format!("{door}({what})"));
    let meant =
        || refuse("…): the one argument is the scenario by name — `scenario => '<name>'`".into());
    let [arg] = args else {
        return Err(meant());
    };
    let (name, value) = match arg {
        FunctionArg::ExprNamed {
            name: SQLExpr::Identifier(n),
            arg: FunctionArgExpr::Expr(v),
            ..
        }
        | FunctionArg::Named {
            name: n,
            arg: FunctionArgExpr::Expr(v),
            ..
        } => (n.value.to_ascii_lowercase(), v),
        _ => return Err(meant()),
    };
    if name != "scenario" {
        return Err(meant());
    }
    match value {
        SQLExpr::Value(v) => match &v.value {
            SQLValue::SingleQuotedString(s) => Ok(s.clone()),
            SQLValue::Placeholder(p) => Err(refuse(format!(
                "scenario => {p}): the scenario is unbound — a frame binds it from its URL, a \
                 statement spells it: scenario => '<name>'"
            ))),
            other => Err(refuse(format!(
                "scenario => {other}): the scenario is a quoted name"
            ))),
        },
        other => Err(refuse(format!(
            "scenario => {other}): the scenario is a quoted name"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_period_is_a_month_or_a_day() {
        let (from, until) = period_bounds("2027-01").expect("a month");
        assert_eq!(
            (iso(from), iso(until)),
            ("2027-01-01".into(), "2027-02-01".into())
        );
        let (from, until) = period_bounds("2027-02-28").expect("a day");
        assert_eq!(
            (iso(from), iso(until)),
            ("2027-02-28".into(), "2027-03-01".into())
        );
        for bad in ["2027", "2027-13", "2027-1", "soon", "2027-02-30"] {
            assert!(period_bounds(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn the_body_is_checked_for_shape_with_the_fault_named() {
        let ok = json!({"levers": [{"column": "sales.price", "factor": {"2027-01": 1.15, "2027-02": 1.15}, "basis": "the declared lever"}]});
        let s = parse("price_hike", &ok).expect("parses");
        assert_eq!(s.levers[0].table, "sales");
        assert_eq!(s.levers[0].name, "price");
        assert_eq!(s.levers[0].factor.len(), 2);
        let faults = [
            (json!({}), "no `levers`"),
            (json!({"levers": []}), "is empty"),
            (
                json!({"levers": [{"factor": {"2027-01": 1.0}, "basis": "x"}]}),
                "no `column`",
            ),
            (
                json!({"levers": [{"column": "price", "factor": {"2027-01": 1.0}, "basis": "x"}]}),
                "not `table.column`",
            ),
            (
                json!({"levers": [{"column": "sales.price", "basis": "x"}]}),
                "no `factor`",
            ),
            (
                json!({"levers": [{"column": "sales.price", "factor": {}, "basis": "x"}]}),
                "names no period",
            ),
            (
                json!({"levers": [{"column": "sales.price", "factor": {"2027-1": 1.0}, "basis": "x"}]}),
                "is not a period",
            ),
            (
                json!({"levers": [{"column": "sales.price", "factor": {"2027-01": -1.0}, "basis": "x"}]}),
                "not a number of zero or more",
            ),
            (
                json!({"levers": [{"column": "sales.price", "factor": {"2027-01": 1.0}}]}),
                "no `basis`",
            ),
        ];
        for (body, said) in faults {
            let e = parse("s", &body).expect_err("refused");
            assert!(e.contains(said), "{body}: {e}");
        }
    }

    #[test]
    fn the_factor_at_a_date_is_its_periods_or_one() {
        let s = parse(
            "s",
            &json!({"levers": [{"column": "t.c", "factor": {"2027-01": 1.1, "2027-03-15": 2.0}, "basis": "x"}]}),
        )
        .expect("parses");
        let at = |d: &str| {
            factor_at(
                &s.levers[0],
                NaiveDate::parse_from_str(d, "%Y-%m-%d").unwrap(),
            )
        };
        assert_eq!(at("2027-01-01"), 1.1);
        assert_eq!(at("2027-01-31"), 1.1);
        assert_eq!(at("2027-02-01"), 1.0);
        assert_eq!(at("2027-03-15"), 2.0);
        assert_eq!(at("2027-03-16"), 1.0);
    }
}
