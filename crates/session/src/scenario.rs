//! The scenario world (fixture 19): a declared scenario read in two
//! passes. Pass one, the arithmetic: where a metric's grounding scans a
//! declared lever's table, the grounding is replayed with the
//! scenario's factors per period over the rows it holds, through the
//! plan rewrite (`crate::replay`) — exact, and only as far as the data
//! reaches. A lever's rows are placed in time by its table's own date
//! column, else by a declared edge from the table to one that carries
//! a date — a fact table keyed into a date dimension — the key against
//! the keys whose dates fall in each period ([`Placement`]); a lever
//! placed neither way is refused when the scenario is glossed. Pass
//! two, the estimate: for the periods ahead of the
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

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, Value as SQLValue,
};
use glossql_glossary::Scope;
use serde_json::{Value, json};

use crate::cube::World;
use crate::period::{self, Resolution};
use crate::replay::{Factor, KeyAnchor, Override};
use crate::search::whole_series;
use crate::shared::{SessionError, Shared, WHATIF};
use crate::subject::qi;

/// The levels asked of the what-if: the 80% band around the median.
const LEVELS: [f64; 3] = [0.10, 0.50, 0.90];
/// Periods ahead a scenario may name past the last actual — the
/// service's own ceiling on a horizon.
const MAX_AHEAD: usize = 1024;

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

/// Where a lever's rows stand in time: by the table's own date column;
/// by a declared edge from the table to one that carries a date, the
/// key column against the dated table's key; or nowhere, with the
/// reason — such a scenario is refused when it is glossed, and a
/// standing one serves its cells without a value and with the reason.
#[derive(Debug, Clone)]
pub(crate) enum Placement {
    Date(String),
    Edge {
        key: String,
        table: String,
        dim_key: String,
        date: String,
    },
    Nowhere(String),
}

/// One lever's placement, from the table's schema and the dataset's
/// declared edges. The table and the dated one are planned by their
/// qualified names, so the check holds from any channel.
async fn place(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    pointers: &[crate::behavior::Pointer],
    lever: &Lever,
) -> Placement {
    let whole = |table: &str| format!("SELECT * FROM {}.{}", qi(dataset), qi(table));
    let fields = match crate::plan::build_plan(shared, ctx, &whole(&lever.table)).await {
        Ok(plan) => plan.schema().fields().clone(),
        Err(e) => return Placement::Nowhere(format!("`{}` does not plan: {e}", lever.table)),
    };
    if let Some(column) = crate::plan::date_column(&fields) {
        return Placement::Date(column);
    }
    let mut edges: Vec<&crate::behavior::Pointer> = pointers
        .iter()
        .filter(|p| p.src_t == lever.table && p.src_cols.len() == 1 && p.dst_cols.len() == 1)
        .collect();
    edges.sort_by(|a, b| (&a.dst_t, &a.src_cols[0]).cmp(&(&b.dst_t, &b.src_cols[0])));
    for edge in edges {
        let Ok(dim) = crate::plan::build_plan(shared, ctx, &whole(&edge.dst_t)).await else {
            continue;
        };
        if let Some(date) = crate::plan::date_column(dim.schema().fields()) {
            return Placement::Edge {
                key: edge.src_cols[0].clone(),
                table: edge.dst_t.clone(),
                dim_key: edge.dst_cols[0].clone(),
                date,
            };
        }
    }
    Placement::Nowhere(format!(
        "`{t}` carries no date column, and no declared edge reaches a table that does — \
         `DECLARE RELATIONSHIP {t}.<key> -> <dates>.<key>` places its rows in time",
        t = lever.table
    ))
}

/// Every lever's placement, by its `table.column`.
async fn placements(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    scenario: &Scenario,
) -> Result<HashMap<String, Placement>, SessionError> {
    let rows = shared.store.relation_rows("relationships").await?;
    let pointers = crate::behavior::declared_pointers(&rows, dataset);
    let mut out = HashMap::new();
    for lever in &scenario.levers {
        out.insert(
            lever.column.clone(),
            place(shared, ctx, dataset, &pointers, lever).await,
        );
    }
    Ok(out)
}

/// A period as a scenario writes it: `YYYY-MM` a calendar month,
/// `YYYY-MM-DD` a day. Its start and its length.
pub(crate) fn period_key(key: &str) -> Option<(i64, Resolution)> {
    match key.len() {
        10 => period::parse_date(key).map(|p| (p, Resolution::Day)),
        7 => period::parse_date(&format!("{key}-01")).map(|p| (p, Resolution::Month)),
        _ => None,
    }
}

/// The half-open range each period key covers, from the calendar: one
/// pass per length. A key that is no period has no entry.
pub(crate) async fn period_bounds(
    keys: &[&str],
) -> Result<HashMap<String, (i64, i64)>, SessionError> {
    let mut out = HashMap::new();
    for resolution in [Resolution::Day, Resolution::Month] {
        let named: Vec<(&str, i64)> = keys
            .iter()
            .filter_map(|k| {
                let (from, r) = period_key(k)?;
                (r == resolution).then_some((*k, from))
            })
            .collect();
        let froms: Vec<i64> = named.iter().map(|(_, from)| *from).collect();
        let untils = period::over(
            &froms,
            &[format!("p + {}", period::interval(resolution, 1))],
        )
        .await
        .map_err(period::refused)?;
        for ((key, from), until) in named.iter().zip(&untils[0]) {
            if let Some(until) = until {
                out.insert((*key).to_string(), (*from, *until));
            }
        }
    }
    Ok(out)
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
            if period_key(period).is_none() {
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
    // Every lever's rows can be placed in time, or the scenario is
    // refused with the edge to declare.
    let ctx = shared.session_ctx();
    let mut placed: Vec<(String, Placement)> = placements(shared, &ctx, dataset, &scenario)
        .await?
        .into_iter()
        .collect();
    placed.sort_by(|a, b| a.0.cmp(&b.0));
    for (column, placement) in placed {
        if let Placement::Nowhere(why) = placement {
            return Err(bad(format!("`{column}`: {why}")));
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

/// The scenario's levers as the plan rewrite takes them, each placed in
/// time: a dated table by its date column, a table dated through an
/// edge by its key against the dated table's keys per factor period,
/// read here. The levers placed nowhere come back by table with the
/// reason, and the frames that scan such a table are not replayed.
async fn overrides(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    scenario: &Scenario,
    placements: &HashMap<String, Placement>,
    bounds: &HashMap<String, (i64, i64)>,
) -> Result<(Vec<Override>, HashMap<String, String>), SessionError> {
    let mut out = Vec::new();
    let mut unplaced = HashMap::new();
    for l in &scenario.levers {
        let placement = placements.get(&l.column);
        let mut factors = Vec::new();
        let mut keys: Vec<Vec<ScalarValue>> = Vec::new();
        for (period, f) in &l.factor {
            let Some(&(from, until)) = bounds.get(period) else {
                continue;
            };
            if let Some(Placement::Edge {
                table,
                dim_key,
                date,
                ..
            }) = placement
            {
                // The keys whose dates fall in the period.
                let sql = format!(
                    "SELECT DISTINCT {k} AS key FROM {ds}.{t} \
                     WHERE {d} >= TIMESTAMP '{from}' AND {d} < TIMESTAMP '{until}'",
                    k = qi(dim_key),
                    ds = qi(dataset),
                    t = qi(table),
                    d = qi(date),
                    from = period::date(from),
                    until = period::date(until),
                );
                let plan = crate::plan::build_plan(shared, ctx, &sql).await?;
                let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
                let mut found = Vec::new();
                for b in &batches {
                    if let Some(col) = b.column_by_name("key") {
                        for i in 0..b.num_rows() {
                            if !col.is_null(i) {
                                found.push(ScalarValue::try_from_array(col, i)?);
                            }
                        }
                    }
                }
                keys.push(found);
            }
            factors.push(Factor {
                from: period::date(from),
                until: Some(period::date(until)),
                factor: *f,
            });
        }
        match placement {
            Some(Placement::Nowhere(why)) => {
                unplaced.insert(l.table.clone(), why.clone());
            }
            Some(Placement::Edge { key, .. }) => out.push(Override {
                table: l.table.clone(),
                column: l.name.clone(),
                factors,
                anchor: Some(KeyAnchor {
                    column: key.clone(),
                    keys,
                }),
            }),
            _ => out.push(Override {
                table: l.table.clone(),
                column: l.name.clone(),
                factors,
                anchor: None,
            }),
        }
    }
    Ok((out, unplaced))
}

/// The lever's factor at a period: the period that holds it, else one.
fn factor_at(lever: &Lever, bounds: &HashMap<String, (i64, i64)>, at: i64) -> f64 {
    lever
        .factor
        .iter()
        .filter_map(|(period, f)| {
            let (from, until) = bounds.get(period)?;
            (*from <= at && at < *until).then_some(*f)
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
    series: Vec<(i64, f64)>,
    ahead: Vec<i64>,
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
    let placements = placements(shared, &ctx, &dataset, &scenario).await?;
    let keys: Vec<&str> = scenario
        .levers
        .iter()
        .flat_map(|l| l.factor.keys())
        .map(String::as_str)
        .collect();
    let bounds = period_bounds(&keys).await?;
    let (overrides, unplaced) =
        overrides(shared, &ctx, &dataset, &scenario, &placements, &bounds).await?;
    // The first period the scenario touches and the one past the last.
    let first = bounds.values().map(|(from, _)| *from).min();
    let last = bounds.values().map(|(_, until)| *until).max();
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
        // Pass one: the arithmetic, where a lever's table is scanned. A
        // frame that scans a table placed nowhere, or a replay the
        // rewrite refuses, serves its cells without a value and with
        // the reason: a replay that changed nothing is no replay.
        if frame.scans.iter().any(|t| tables.contains(t.as_str())) {
            let since = period::start(first, frame.resolution)
                .await
                .map_err(period::refused)?;
            let blank = |why: String, out: &mut Vec<Point>| {
                for (period, _) in whole_series(&actual.partitions, &frame.metric) {
                    if period < since {
                        continue;
                    }
                    out.push(Point {
                        metric: frame.metric.clone(),
                        grain,
                        period,
                        step: None,
                        pass: "replayed",
                        value: None,
                        num: None,
                        den: None,
                        low: None,
                        high: None,
                        held_80: None,
                        held_90: None,
                        held_from: None,
                        sign: None,
                        basis: Some(why.clone()),
                        served_by: None,
                    });
                }
            };
            let sql =
                crate::cube::total_sql(&frame.sql, &frame.tcol, frame.verb, frame.resolution, None);
            let rows = match frame.scans.iter().find_map(|t| unplaced.get(t)) {
                Some(why) => Err(format!("not replayed: {why}")),
                None => crate::cube::replayed_series(shared, &ctx, &sql, &overrides)
                    .await
                    .map_err(|e| format!("not replayed: {e}")),
            };
            let rows = match rows {
                Ok(rows) => rows,
                Err(why) => {
                    blank(why, &mut out);
                    Vec::new()
                }
            };
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
        let series = whole_series(&actual.partitions, &frame.metric);
        let Some(&(origin, _)) = series.last() else {
            continue;
        };
        let ahead: Vec<i64> = period::ahead_until(origin, frame.resolution, last)
            .await
            .map_err(period::refused)?
            .into_iter()
            .filter(|d| *d < last)
            .take(MAX_AHEAD)
            .collect();
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
    let (estimated, mut parts) = estimate(
        shared,
        &ctx,
        &dataset,
        &scenario,
        &placements,
        &bounds,
        asks,
    )
    .await?;
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
    placements: &HashMap<String, Placement>,
    bounds: &HashMap<String, (i64, i64)>,
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
    for (&grain, idx) in &by_grain {
        let asked: Vec<&Ask> = idx.iter().map(|&i| &asks[i]).collect();
        if !runtime.serves(WHATIF) {
            let why = crate::shared::no_model(
                &format!(
                    "metric_series(world => 'scenario', scenario => '{}')",
                    scenario.name
                ),
                WHATIF,
            )
            .to_string();
            unanswered(&asked, scenario, &why, &mut out, &mut parts);
            continue;
        }
        let resolution = asked[0].resolution;
        let inputs =
            match lever_inputs(shared, ctx, dataset, scenario, placements, resolution).await {
                Ok(inputs) => inputs,
                Err(why) => {
                    unanswered(&asked, scenario, &why, &mut out, &mut parts);
                    continue;
                }
            };
        let (table, planned_of) = ask_table(&asked, scenario, bounds, &inputs)?;
        let spec = json!({"targets": ["value"], "levels": LEVELS});
        let answered = match runtime.table(WHATIF, table, spec).await {
            Ok(answered) => answered,
            Err(e) => {
                let why = format!("not served: the what-if service refused — {e}");
                unanswered(&asked, scenario, &why, &mut out, &mut parts);
                continue;
            }
        };
        let answer = Answer::read(&answered)?;
        for a in asked {
            let planned = planned_of.get(&a.metric).map_or(&[][..], Vec::as_slice);
            answer.points_of(a, grain, scenario, planned, &held, &mut out, &mut parts);
        }
    }
    Ok((out, parts))
}

/// Every asked cell of the metrics, valueless, with one reason — and
/// every lever's part the same way.
fn unanswered(
    asked: &[&Ask],
    scenario: &Scenario,
    why: &str,
    out: &mut Vec<Point>,
    parts: &mut Vec<Part>,
) {
    for a in asked {
        for (k, &period) in a.ahead.iter().enumerate() {
            out.push(Point {
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
            parts.push(Part {
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
}

/// Every lever's series at the grain, placed in time by its table; a
/// lever with no series is the reason no estimate comes.
async fn lever_inputs(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    scenario: &Scenario,
    placements: &HashMap<String, Placement>,
    resolution: Resolution,
) -> Result<Vec<HashMap<i64, f64>>, String> {
    let mut inputs = Vec::with_capacity(scenario.levers.len());
    for lever in &scenario.levers {
        let placement = placements
            .get(&lever.column)
            .cloned()
            .unwrap_or_else(|| Placement::Nowhere("the lever is not placed".into()));
        match lever_series(shared, ctx, dataset, lever, &placement, resolution).await {
            Ok(input) => inputs.push(input.into_iter().collect()),
            Err(why) => return Err(format!("no estimate: `{}`: {why}", lever.column)),
        }
    }
    Ok(inputs)
}

/// One table: the members' histories with every lever beside them,
/// then each period ahead twice — as planned and under the scenario.
/// Beside it, per metric, each lever's value held as planned: its
/// last value at or before the metric's origin.
#[expect(
    clippy::type_complexity,
    reason = "the table and the planned values it was built from"
)]
fn ask_table(
    asked: &[&Ask],
    scenario: &Scenario,
    bounds: &HashMap<String, (i64, i64)>,
    inputs: &[HashMap<i64, f64>],
) -> Result<(RecordBatch, HashMap<String, Vec<Option<f64>>>), SessionError> {
    let (mut members, mut periods, mut status, mut values) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut columns: Vec<Vec<Option<f64>>> = vec![Vec::new(); scenario.levers.len()];
    let mut planned_of: HashMap<String, Vec<Option<f64>>> = HashMap::new();
    for a in asked {
        let origin = a.series.last().map(|(d, _)| *d);
        let planned: Vec<Option<f64>> = inputs
            .iter()
            .map(|input| {
                let mut dated: Vec<(&i64, &f64)> = input
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
            periods.push(Some(*d));
            status.push("actual");
            values.push(Some(*v));
            for (l, input) in inputs.iter().enumerate() {
                columns[l].push(input.get(d).copied());
            }
        }
        for d in &a.ahead {
            for s in ["plan", "scenario"] {
                members.push(a.metric.as_str());
                periods.push(Some(*d));
                status.push(s);
                values.push(None);
                for (l, lever) in scenario.levers.iter().enumerate() {
                    let factor = if s == "plan" {
                        1.0
                    } else {
                        factor_at(lever, bounds, *d)
                    };
                    columns[l].push(planned[l].map(|p| p * factor));
                }
            }
        }
    }
    let mut fields = vec![
        Field::new("member", DataType::Utf8, false),
        period::field("period", true),
        Field::new("status", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(members)),
        Arc::new(TimestampMicrosecondArray::from(periods)),
        Arc::new(StringArray::from(status)),
        Arc::new(Float64Array::from(values)),
    ];
    for (lever, column) in scenario.levers.iter().zip(columns) {
        fields.push(Field::new(&lever.column, DataType::Float64, true));
        arrays.push(Arc::new(Float64Array::from(column)));
    }
    let table =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(SessionError::from)?;
    Ok((table, planned_of))
}

/// The what-if's answer as the cells read it: who served, what each
/// member's rows folded to, the service's errors by member, and the
/// parts it gave.
struct Answer<'a> {
    served_by: String,
    folded: HashMap<String, Said>,
    errors: HashMap<String, Vec<String>>,
    given: Option<&'a Vec<Value>>,
}

impl<'a> Answer<'a> {
    fn read(answered: &'a crate::shared::Answered) -> Result<Self, SessionError> {
        Ok(Answer {
            served_by: format!(
                "{} {}",
                answered.answer["service"].as_str().unwrap_or("?"),
                answered.answer["version"].as_str().unwrap_or("?")
            ),
            folded: fold(&answered.table)
                .map_err(|e| SessionError::Runtime(format!("the what-if service answered {e}")))?,
            errors: crate::search::walk_errors(&answered.answer),
            given: answered.answer["parts"].as_array(),
        })
    }

    /// One asked metric's cells per period ahead and each lever's
    /// part, as the service gave them for the member and the `value`
    /// column. Why a cell or a part has no number: the service's word
    /// on the member first, then its errors, then a lever with no
    /// value to hold as planned, then the default.
    #[expect(
        clippy::too_many_arguments,
        reason = "the ask, its answer and the two rows it fills"
    )]
    fn points_of(
        &self,
        a: &Ask,
        grain: &'static str,
        scenario: &Scenario,
        planned: &[Option<f64>],
        held: &crate::search::HeldTable,
        out: &mut Vec<Point>,
        parts: &mut Vec<Part>,
    ) {
        let said = self.folded.get(&a.metric);
        let unheld: Vec<&str> = scenario
            .levers
            .iter()
            .zip(planned)
            .filter(|(_, v)| v.is_none())
            .map(|(l, _)| l.column.as_str())
            .collect();
        let reason = |default: &str| -> Option<String> {
            said.and_then(|s| s.basis.clone())
                .or_else(|| self.errors.get(&a.metric).map(|errs| errs.join("; ")))
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
        for (k, &period) in a.ahead.iter().enumerate() {
            let step = k as i64 + 1;
            let point = said.and_then(|s| s.points.get(&period));
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
                served_by: Some(self.served_by.clone()),
            });
        }
        for lever in &scenario.levers {
            let part = self.given.into_iter().flatten().find(|p| {
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
                served_by: Some(self.served_by.clone()),
            });
        }
    }
}

/// The lever's series at a resolution: the mean of the column per
/// period of its table, placed in time as the lever is — by the
/// table's own date column, or through the declared edge to a dated
/// table.
async fn lever_series(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    dataset: &str,
    lever: &Lever,
    placement: &Placement,
    resolution: Resolution,
) -> Result<Vec<(i64, f64)>, String> {
    let sql = match placement {
        Placement::Date(tcol) => format!(
            "SELECT {p} AS period, avg(CAST({c} AS DOUBLE)) AS value FROM {ds}.{t} \
             WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 1",
            p = period::bucket(&qi(tcol), resolution),
            c = qi(&lever.name),
            ds = qi(dataset),
            t = qi(&lever.table),
        ),
        Placement::Edge {
            key,
            table,
            dim_key,
            date,
        } => format!(
            "SELECT {p} AS period, avg(CAST(l.{c} AS DOUBLE)) AS value \
             FROM {ds}.{t} l JOIN {ds}.{dim} d ON l.{k} = d.{dk} \
             WHERE l.{c} IS NOT NULL GROUP BY 1 ORDER BY 1",
            p = period::bucket(&format!("d.{}", qi(date)), resolution),
            c = qi(&lever.name),
            ds = qi(dataset),
            t = qi(&lever.table),
            dim = qi(table),
            k = qi(key),
            dk = qi(dim_key),
        ),
        Placement::Nowhere(why) => return Err(why.clone()),
    };
    let rows = crate::cube::replayed_series(shared, ctx, &sql, &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows.into_iter().map(|(p, _, v, _, _)| (p, v)).collect())
}

/// What the what-if said of one member: its scenario rows folded to a
/// point per period, and the sign and basis it gave the member.
#[derive(Default)]
struct Said {
    points: HashMap<i64, Estimate>,
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
    // The service's own unit for the period; the cells' is the
    // calendar's.
    let period = cast(column("period")?, &period::data_type()).map_err(|e| e.to_string())?;
    let period =
        period::column(&period).ok_or_else(|| "`period` did not read as a period".to_string())?;
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
        let point = said.points.entry(period.value(i)).or_default();
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
                "period": period::short(p.period), "step": p.step, "pass": p.pass,
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

    #[tokio::test]
    async fn a_period_is_a_month_or_a_day() {
        let bounds = period_bounds(&["2027-01", "2027-02-28", "2027-12"])
            .await
            .expect("the calendar answers");
        let dates = |key: &str| {
            let (from, until) = bounds[key];
            (period::date(from), period::date(until))
        };
        assert_eq!(dates("2027-01"), ("2027-01-01".into(), "2027-02-01".into()));
        assert_eq!(
            dates("2027-02-28"),
            ("2027-02-28".into(), "2027-03-01".into())
        );
        assert_eq!(dates("2027-12"), ("2027-12-01".into(), "2028-01-01".into()));
        for bad in ["2027", "2027-13", "2027-1", "soon", "2027-02-30"] {
            assert!(period_key(bad).is_none(), "{bad}");
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

    #[tokio::test]
    async fn the_factor_at_a_date_is_its_periods_or_one() {
        let s = parse(
            "s",
            &json!({"levers": [{"column": "t.c", "factor": {"2027-01": 1.1, "2027-03-15": 2.0}, "basis": "x"}]}),
        )
        .expect("parses");
        let bounds = period_bounds(&["2027-01", "2027-03-15"])
            .await
            .expect("the calendar answers");
        let at = |d: &str| factor_at(&s.levers[0], &bounds, period::parse_date(d).unwrap());
        assert_eq!(at("2027-01-01"), 1.1);
        assert_eq!(at("2027-01-31"), 1.1);
        assert_eq!(at("2027-02-01"), 1.0);
        assert_eq!(at("2027-03-15"), 2.0);
        assert_eq!(at("2027-03-16"), 1.0);
    }
}
