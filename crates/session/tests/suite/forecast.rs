//! The forecast at an origin: the door behind `metric_forecast`, against
//! a stub service that answers the periods ahead. What the door sends
//! — each metric as a whole at its own grain, the history and the
//! periods ahead, the totals a flow asks for — and what it serves: one
//! row per metric and point, the inapplicable ones with their reason.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    Array, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Answered, FORECAST, FunctionRuntime, Outcome, Session};
use serde_json::{Value, json};

/// A forecast service that carries the last actual forward: every
/// step's median is the member's last value, the bands at ±8% and
/// ±10% of it; a total is the sum of its steps' medians with the same
/// relative bands. Its answer says how often its bands held: the 80%
/// band 0.70 plus a hundredth per step, the 90% band 0.85 plus the
/// same, a span's 80% band 0.60 plus a hundredth per period. It
/// records every specification it is handed.
#[derive(Debug, Default)]
struct Flat {
    specs: Mutex<Vec<Value>>,
}

#[glossql_session::async_trait]
impl FunctionRuntime for Flat {
    fn serves(&self, point: &str) -> bool {
        point == FORECAST
    }

    async fn table(
        &self,
        point: &str,
        table: RecordBatch,
        spec: Value,
    ) -> Result<Answered, String> {
        assert_eq!(point, FORECAST);
        assert!(
            spec.get("origins").is_none(),
            "the forecast asks no origins: {spec}"
        );
        self.specs.lock().unwrap().push(spec.clone());
        let levels: Vec<f64> = spec["levels"]
            .as_array()
            .expect("levels")
            .iter()
            .map(|l| l.as_f64().expect("a level"))
            .collect();
        let totals: Vec<i64> = spec["totals"]
            .as_array()
            .map(|t| t.iter().map(|s| s.as_i64().expect("a span")).collect())
            .unwrap_or_default();
        let member = table.column_by_name("member").expect("member");
        let period = table.column_by_name("period").expect("period");
        let value = table
            .column_by_name("value")
            .expect("value")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("floats");
        let status = table
            .column_by_name("status")
            .expect("status")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text");
        // Per member, in the table's order: the last actual and its
        // period, then the periods ahead.
        struct Series {
            name: String,
            last: Option<(String, f64)>,
            ahead: Vec<String>,
        }
        let mut series: Vec<Series> = Vec::new();
        for i in 0..table.num_rows() {
            let m = array_value_to_string(member, i).expect("text");
            let p = array_value_to_string(period, i).expect("text");
            if series.last().is_none_or(|s| s.name != m) {
                series.push(Series {
                    name: m.clone(),
                    last: None,
                    ahead: Vec::new(),
                });
            }
            let s = series.last_mut().expect("a series");
            match status.value(i) {
                "actual" => {
                    assert!(!value.is_null(i), "an actual row carries a value");
                    s.last = Some((p, value.value(i)));
                }
                "forecast" => {
                    assert!(value.is_null(i), "a period ahead carries no value");
                    s.ahead.push(p);
                }
                other => panic!("status {other}"),
            }
        }
        let (mut m_col, mut o_col, mut step_col, mut p_col, mut span_col, mut l_col, mut v_col) = (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        for s in &series {
            let (origin, last) = s.last.clone().expect("history");
            let mut push = |step: i64, span: Option<i64>, median: f64| {
                for l in &levels {
                    m_col.push(s.name.clone());
                    o_col.push(origin.clone());
                    step_col.push(step);
                    p_col.push(s.ahead[(step - 1) as usize].clone());
                    span_col.push(span);
                    l_col.push(*l);
                    v_col.push(median * (1.0 + (l - 0.5) * 0.2));
                }
            };
            for step in 1..=s.ahead.len() as i64 {
                push(step, None, last);
            }
            for span in &totals {
                push(*span, Some(*span), last * *span as f64);
            }
        }
        let n = m_col.len();
        let table = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("member", DataType::Utf8, false),
                Field::new("origin", DataType::Utf8, true),
                Field::new("step", DataType::Int64, false),
                Field::new("period", DataType::Utf8, true),
                Field::new("span", DataType::Int64, true),
                Field::new("column", DataType::Utf8, false),
                Field::new("level", DataType::Float64, false),
                Field::new("value", DataType::Float64, true),
                Field::new("actual", DataType::Float64, true),
                Field::new("pit", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(m_col)),
                Arc::new(StringArray::from(o_col)),
                Arc::new(Int64Array::from(step_col)),
                Arc::new(StringArray::from(p_col)),
                Arc::new(Int64Array::from(span_col)),
                Arc::new(StringArray::from(vec!["value"; n])),
                Arc::new(Float64Array::from(l_col)),
                Arc::new(Float64Array::from(v_col)),
                Arc::new(Float64Array::from(vec![None::<f64>; n])),
                Arc::new(Float64Array::from(vec![None::<f64>; n])),
            ],
        )
        .expect("the answer table");
        let horizon = spec["horizon"].as_i64().expect("horizon");
        let mut coverage = Vec::new();
        for step in 1..=horizon {
            coverage.push(json!({"step": step, "span": null, "low": 0.1, "high": 0.9,
                                 "share": (70 + step) as f64 / 100.0, "points": 600}));
            coverage.push(
                json!({"step": step, "span": null, "low": 0.05, "high": 0.95,
                                 "share": (85 + step) as f64 / 100.0, "points": 600}),
            );
        }
        for span in &totals {
            coverage.push(json!({"step": span, "span": span, "low": 0.1, "high": 0.9,
                                 "share": (60 + *span) as f64 / 100.0, "points": 450}));
        }
        Ok(Answered {
            table,
            answer: json!({"service": "flat", "version": "test", "errors": [], "coverage": coverage}),
        })
    }
}

/// Day offsets since the epoch for a civil date.
fn day(year: i32, month: u32, day: u32) -> i32 {
    let d = chrono::NaiveDate::from_ymd_opt(year, month, day).expect("a civil date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
    (d - epoch).num_days() as i32
}

/// The temporal verdict the cube admits a series on, judged by a stub
/// function that names one cadence — one aspect has one producer, so
/// the cadence is the fixture's.
fn judged(cadence: &str) -> String {
    format!(
        r##"
DECLARE ASPECT temporal_profile WITH $${{"type": "object", "required": ["applicable"],
  "properties": {{"applicable": {{"type": "boolean"}}}}}}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION judge_time FOR GLOBAL AS
  $$SELECT true AS applicable, '{cadence}' AS granularity, named_struct('ratio', 1.0) AS completeness$$
  RETURNS temporal_profile;
"##
    )
}

/// A session over a lake in a scratch dir, the dataset in use, the
/// forecast service stubbed.
async fn session(stub: Arc<Flat>, setup: &str) -> Session {
    let dir = tempfile::tempdir().expect("a scratch dir");
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .expect("a lake");
    let store = Store::open(lake).await.expect("a store");
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .expect("a session")
    .with_runtime(stub);
    session.execute(setup).await.expect("the setup lands");
    std::mem::forget(dir);
    session
}

async fn register(session: &Session, name: &str, schema: Arc<Schema>, batch: RecordBatch) {
    session
        .register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .await
        .expect("the table registers");
}

async fn rows(session: &Session, sql: &str) -> String {
    let outcomes = session.execute(sql).await.expect("the read serves");
    let Some(Outcome::Rows { batches, .. }) = outcomes.into_iter().next_back() else {
        panic!("no rows");
    };
    pretty_format_batches(&batches)
        .expect("printable")
        .to_string()
}

fn dated_values(column: &str, dates: Vec<i32>, values: Vec<f64>) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(column, DataType::Date32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .expect("a batch");
    (schema, batch)
}

/// Eighteen months of a rising flow, 2024-01 to 2025-06, and a race
/// table whose win rate is a ratio.
async fn monthly_session(stub: Arc<Flat>) -> Session {
    let setup = format!(
        r##"
DECLARE DATASET fin SET (purpose: 'the forecast at an origin');
USE fin;
DECLARE ASPECT cube WITH $${{"type": "object", "properties": {{
  "resolution": {{"default": "month"}},
  "windows": {{"type": "object", "properties": {{"month": {{"default": "48 months"}}}}}}}}}}$$
AS FACT ON DATASET;
{judged}
DECLARE ASPECT revenue WITH $${{"title": "Revenue"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT win_rate WITH $${{"title": "Win rate"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT dated WITH $${{"title": "Dates alone"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT recent WITH $${{"title": "Recent revenue"}}$$ AS QUERY ON DATASET;
GLOSS revenue ON fin AS $${{"sql": "SELECT sale_date, amount AS value FROM sales"}}$$;
GLOSS win_rate ON fin AS $${{"sql": "SELECT race_date, wins / starts AS value, wins AS num, starts AS den FROM races"}}$$;
GLOSS dated ON fin AS $${{"sql": "SELECT sale_date FROM sales"}}$$;
GLOSS recent ON fin AS $${{"sql": "SELECT sale_date, amount AS value FROM sales WHERE sale_date >= DATE '2025-03-01'"}}$$;
"##,
        judged = judged("month")
    );
    let session = session(stub, &setup).await;
    let months: Vec<(i32, u32)> = (0..18)
        .map(|i| (2024 + i / 12, (i % 12 + 1) as u32))
        .collect();
    let (schema, batch) = dated_values(
        "sale_date",
        months.iter().map(|(y, m)| day(*y, *m, 1)).collect(),
        (0..18).map(|i| 100.0 + 10.0 * i as f64).collect(),
    );
    register(&session, "sales", schema, batch).await;
    let races = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, false),
        Field::new("wins", DataType::Float64, false),
        Field::new("starts", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&races),
        vec![
            Arc::new(Date32Array::from(
                months
                    .iter()
                    .map(|(y, m)| day(*y, *m, 15))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![1.0; 18])),
            Arc::new(Float64Array::from(vec![20.0; 18])),
        ],
    )
    .expect("a batch");
    register(&session, "races", races, batch).await;
    session
        .execute(
            "SELECT judge_time() FROM sales.sale_date; SELECT judge_time() FROM races.race_date;",
        )
        .await
        .expect("the cadence is judged");
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_door_forecasts_each_metric_as_a_whole_at_its_grain() {
    let stub = Arc::new(Flat::default());
    let session = monthly_session(Arc::clone(&stub)).await;
    let served = rows(
        &session,
        "SELECT metric, applicable, reason, grain, origin, served_by, step, span, to_go, period, p50 \
         FROM metric_forecast_ahead('fin') ORDER BY seq, point_seq;",
    )
    .await;

    // The flow: twelve steps from the origin, the last month with an
    // actual, then the quarter and the year to go — July starts the
    // third quarter, three months to its end and six to the year's.
    let revenue: Vec<&str> = served.lines().filter(|l| l.contains("revenue")).collect();
    assert_eq!(revenue.len(), 14, "{served}");
    assert!(
        revenue[0].contains("2025-06-01") && revenue[0].contains("2025-07-01"),
        "the origin and the first period ahead: {}",
        revenue[0]
    );
    assert!(revenue[11].contains("2026-06-01"), "{}", revenue[11]);
    assert!(
        revenue[12].contains("quarter") && revenue[12].contains("2025-09-01"),
        "{}",
        revenue[12]
    );
    assert!(
        revenue[13].contains("year") && revenue[13].contains("2025-12-01"),
        "{}",
        revenue[13]
    );
    // The stub carries the last value forward: 270 per month, 810 the
    // quarter, 1620 the year.
    assert!(revenue[0].contains("270.0"), "{}", revenue[0]);
    assert!(revenue[12].contains("810.0"), "{}", revenue[12]);
    assert!(revenue[13].contains("1620.0"), "{}", revenue[13]);
    for line in &revenue {
        assert!(
            line.contains("month") && line.contains("flat test"),
            "{line}"
        );
    }
    // The ratio: twelve steps and no total — a total of a ratio is not
    // a sum.
    let win_rate: Vec<&str> = served.lines().filter(|l| l.contains("win_rate")).collect();
    assert_eq!(win_rate.len(), 12, "{served}");
    assert!(win_rate.iter().all(|l| !l.contains("quarter")), "{served}");
    // No series to forecast: the cube's reason stands.
    let dated: Vec<&str> = served.lines().filter(|l| l.contains("dated")).collect();
    assert_eq!(dated.len(), 1, "{served}");
    assert!(
        dated[0].contains("false") && dated[0].contains("no value column"),
        "{}",
        dated[0]
    );
    // Too short a history: inapplicable before any call.
    let recent: Vec<&str> = served.lines().filter(|l| l.contains("recent")).collect();
    assert_eq!(recent.len(), 1, "{served}");
    assert!(
        recent[0].contains("4 months of history: a forecast needs 6"),
        "{}",
        recent[0]
    );

    // Two requests: the flow with its totals, the ratio without — one
    // specification per request, so metrics asking for different
    // spans go separately.
    let specs = stub.specs.lock().unwrap().clone();
    assert_eq!(specs.len(), 2, "{specs:?}");
    for spec in &specs {
        assert_eq!(spec["targets"], json!(["value"]));
        assert_eq!(spec["horizon"], json!(12));
        assert_eq!(spec["grain"], json!("month"));
        assert_eq!(spec["levels"], json!([0.05, 0.1, 0.5, 0.9, 0.95]));
    }
    let totals: Vec<&Value> = specs.iter().map(|s| &s["totals"]).collect();
    assert!(totals.contains(&&json!([3, 6])), "{specs:?}");
    assert!(totals.contains(&&Value::Null), "{specs:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_week_metric_walks_thirteen_steps_to_the_quarter() {
    let stub = Arc::new(Flat::default());
    let setup = format!(
        r##"
DECLARE DATASET fin SET (purpose: 'weekly');
USE fin;
DECLARE ASPECT cube WITH $${{"type": "object", "properties": {{
  "resolution": {{"default": "week"}},
  "windows": {{"type": "object", "properties": {{"week": {{"default": "3 years"}}}}}}}}}}$$
AS FACT ON DATASET;
{judged}
DECLARE ASPECT visits WITH $${{"title": "Visits"}}$$ AS QUERY ON DATASET;
GLOSS visits ON fin AS $${{"sql": "SELECT visit_date, amount AS value FROM visit_log"}}$$;
"##,
        judged = judged("week")
    );
    let session = session(Arc::clone(&stub), &setup).await;
    // Twenty Mondays from 2025-01-06: the last is 2025-05-19, the first
    // week ahead starts 2025-05-26, and six Mondays fall before the
    // quarter ends on 2025-06-30.
    let monday = day(2025, 1, 6);
    let (schema, batch) = dated_values(
        "visit_date",
        (0..20).map(|i| monday + 7 * i).collect(),
        (0..20).map(|i| 50.0 + i as f64).collect(),
    );
    register(&session, "visit_log", schema, batch).await;
    session
        .execute("SELECT judge_time() FROM visit_log.visit_date;")
        .await
        .expect("the cadence is judged");
    let served = rows(
        &session,
        "SELECT grain, origin, step, span, to_go, period FROM metric_forecast_ahead('fin') \
         ORDER BY point_seq;",
    )
    .await;
    let lines: Vec<&str> = served.lines().filter(|l| l.contains("week")).collect();
    assert_eq!(lines.len(), 14, "{served}");
    assert!(
        lines[0].contains("2025-05-19") && lines[0].contains("2025-05-26"),
        "{}",
        lines[0]
    );
    assert!(lines[12].contains("2025-08-18"), "{}", lines[12]);
    assert!(
        lines[13].contains("quarter")
            && lines[13].contains("| 6 ")
            && lines[13].contains("2025-06-30"),
        "{}",
        lines[13]
    );
    let specs = stub.specs.lock().unwrap().clone();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0]["horizon"], json!(13));
    assert_eq!(specs[0]["grain"], json!("week"));
    assert_eq!(specs[0]["totals"], json!([6]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_door_reads_the_dataset_in_use() {
    let session = monthly_session(Arc::new(Flat::default())).await;
    let e = session
        .execute("SELECT * FROM metric_forecast_ahead('other');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("USE other first"), "{e}");
}

/// The kit's measurement, declared as the bootstrap does: the aspect at
/// dataset grain and the function that folds the door's rows into one
/// body per run.
fn forecast_kit() -> &'static str {
    r##"
DECLARE ASPECT metric_forecast WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {
    "applicable": {"type": "boolean"},
    "metrics": {"type": "array", "items": {"type": "object",
      "properties": {
        "metric": {"type": "string"}, "applicable": {"type": "boolean"},
        "reason": {"type": "string"},
        "grain": {"type": "string"}, "origin": {"type": "string"},
        "served_by": {"type": "string"},
        "points": {"type": "array", "items": {"type": "object",
          "properties": {
            "step": {"type": "integer"},
            "span": {"type": ["integer", "null"]},
            "to_go": {"type": ["string", "null"]},
            "period": {"type": "string"},
            "p05": {"type": "number"}, "p10": {"type": "number"},
            "p50": {"type": "number"}, "p90": {"type": "number"},
            "p95": {"type": "number"},
            "service_80": {"type": ["number", "null"]},
            "service_90": {"type": ["number", "null"]}}}}}}}
  }
}$$ AS MEASUREMENT ON DATASET;
DECLARE FUNCTION metric_forecast FOR GLOBAL AS $$
WITH m AS (
  SELECT seq, metric, applicable, reason, grain, origin, served_by,
         CASE WHEN applicable THEN coalesce(array_agg(named_struct(
           'step', step, 'span', span, 'to_go', to_go, 'period', period,
           'p05', p05, 'p10', p10, 'p50', p50, 'p90', p90, 'p95', p95,
           'service_80', service_80, 'service_90', service_90
         ) ORDER BY point_seq) FILTER (WHERE period IS NOT NULL), []) END AS points
  FROM metric_forecast_ahead($subject)
  GROUP BY seq, metric, applicable, reason, grain, origin, served_by
)
SELECT
  count(metric) > 0 AS applicable,
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'grain', grain, 'origin', origin, 'served_by', served_by,
    'points', points
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics
FROM m
$$ RETURNS metric_forecast;
"##
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_forecast_world_serves_the_newest_run_as_cells() {
    let session = monthly_session(Arc::new(Flat::default())).await;
    session
        .execute(forecast_kit())
        .await
        .expect("the measurement is declared");

    // Nothing forecast yet: the forecast world is empty, and the
    // default read is the actual world alone.
    let ahead = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(world => 'forecast');",
    )
    .await;
    assert!(ahead.contains("| 0 "), "{ahead}");
    let worlds = rows(&session, "SELECT DISTINCT world FROM metric_series();").await;
    assert!(
        worlds.contains("actual") && !worlds.contains("forecast"),
        "{worlds}"
    );

    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .expect("the forecast lands");

    // The flow and the ratio: twelve steps each as cells, the median
    // for the value, the metric's own verb, no halves — and no total.
    let cells = rows(
        &session,
        "SELECT metric, world, dimension, member, period, value, num, den, behavior \
         FROM metric_series(world => 'forecast') ORDER BY metric, period;",
    )
    .await;
    let revenue: Vec<&str> = cells.lines().filter(|l| l.contains("revenue")).collect();
    assert_eq!(revenue.len(), 12, "{cells}");
    assert!(
        revenue[0].contains("2025-07-01") && revenue[0].contains("270.0"),
        "{}",
        revenue[0]
    );
    assert!(revenue[11].contains("2026-06-01"), "{}", revenue[11]);
    for line in &revenue {
        assert!(
            line.contains("| forecast |") && line.contains("| 270.0 |     |     | flow "),
            "{line}"
        );
    }
    let win_rate: Vec<&str> = cells.lines().filter(|l| l.contains("win_rate")).collect();
    assert_eq!(win_rate.len(), 12, "{cells}");
    assert!(win_rate.iter().all(|l| l.contains("| ratio ")), "{cells}");

    // The default read is unchanged: no forecast row, and the two
    // worlds' periods are disjoint, so a join on metric and period is
    // empty until the actuals land.
    let actual = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series() WHERE world <> 'actual';",
    )
    .await;
    assert!(actual.contains("| 0 "), "{actual}");
    let joined = rows(
        &session,
        "SELECT count(*) AS n, max(a.period) < min(f.period) AS ahead \
         FROM metric_series() a \
         JOIN metric_series(world => 'forecast') f \
           ON a.metric = f.metric AND a.period = f.period;",
    )
    .await;
    assert!(joined.contains("| 0 "), "{joined}");
    let order = rows(
        &session,
        "SELECT max(a.period) < min(f.period) AS ahead \
         FROM metric_series() a, metric_series(world => 'forecast') f \
         WHERE a.metric = 'revenue' AND f.metric = 'revenue';",
    )
    .await;
    assert!(order.contains("true"), "{order}");

    // The forecast stands at the metric's own grain: asked at the
    // grain, the same cells; at another, none.
    let month = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(world => 'forecast', grain => 'month');",
    )
    .await;
    assert!(month.contains("| 24 "), "{month}");
    let quarter = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(grain => 'quarter', world => 'forecast');",
    )
    .await;
    assert!(quarter.contains("| 0 "), "{quarter}");

    // The world not served yet refuses by name; a word that is no
    // world says what one is.
    let plan = session
        .execute("SELECT * FROM metric_series(world => 'plan');")
        .await
        .unwrap_err();
    assert!(
        plan.to_string()
            .contains("the plan world is not served yet")
            && plan.to_string().contains("plan grounding"),
        "{plan}"
    );
    let weather = session
        .execute("SELECT * FROM metric_series(world => 'weather');")
        .await
        .unwrap_err();
    assert!(
        weather
            .to_string()
            .contains("a world is actual, forecast or scenario"),
        "{weather}"
    );
}

/// A range is served with how often such a range held. The service's
/// held-out share per band lands with each point; `forecast_points()`
/// serves it as `held_80` and `held_90` while the record is short, and
/// `held_from` says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_point_says_how_often_its_band_held() {
    let session = monthly_session(Arc::new(Flat::default())).await;
    session
        .execute(forecast_kit())
        .await
        .expect("the measurement is declared");

    // The door's rows carry the service's shares per step and span:
    // the 80% band's and the 90% band's at every step, the 80% band's
    // alone for a total, which the stub measured for one band only.
    let door = rows(
        &session,
        "SELECT step, span, service_80 AS s80, service_90 AS s90 \
         FROM metric_forecast_ahead('fin') WHERE metric = 'revenue' ORDER BY point_seq;",
    )
    .await;
    let lines: Vec<&str> = door.lines().filter(|l| l.contains("| 0.")).collect();
    assert_eq!(lines.len(), 14, "{door}");
    assert!(
        lines[0].contains("| 1    |      | 0.71 | 0.86 |"),
        "{}",
        lines[0]
    );
    assert!(
        lines[11].contains("| 12   |      | 0.82 | 0.97 |"),
        "{}",
        lines[11]
    );
    assert!(
        lines[12].contains("| 3    | 3    | 0.63 |      |"),
        "{}",
        lines[12]
    );

    // Landed, the shares are read back on every point: no point of the
    // record has an actual yet, so every share is the service's.
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .expect("the forecast lands");
    let points = rows(
        &session,
        "SELECT metric, step, span, service_80 AS s80, held_80 AS h80, held_90 AS h90, \
                held_from AS src \
         FROM forecast_points() WHERE applicable ORDER BY seq, point_seq;",
    )
    .await;
    let revenue: Vec<&str> = points.lines().filter(|l| l.contains("revenue")).collect();
    assert_eq!(revenue.len(), 14, "{points}");
    assert!(
        revenue[0].contains("| 1    |      | 0.71 | 0.71 | 0.86 | service |"),
        "{}",
        revenue[0]
    );
    assert!(
        revenue[13].contains("| 6    | 6    | 0.66 | 0.66 |      | service |"),
        "{}",
        revenue[13]
    );
    let sources = rows(
        &session,
        "SELECT held_from AS src, count(*) AS n FROM forecast_points() WHERE applicable GROUP BY held_from;",
    )
    .await;
    assert!(sources.contains("| service | 26 |"), "{sources}");
    let short = rows(
        &session,
        "SELECT count(*) AS n FROM forecast_points() WHERE applicable AND held_80 IS NULL;",
    )
    .await;
    assert!(short.contains("| 0 "), "{short}");
}

/// A flow's empty period is a zero cell — a sum over no rows — from
/// its first period to the data's edge, the latest date any judged
/// time column holds, so a forecast for it is checked against zero. A
/// stock's and a ratio's empty periods stay gaps. A landing that moves
/// the edge rebuilds the flow's head, and its points land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flows_empty_periods_are_zero_cells_to_the_datas_edge() {
    let stub = Arc::new(Flat::default());
    let setup = format!(
        r##"
DECLARE DATASET fin SET (purpose: 'zero cells');
USE fin;
DECLARE ASPECT cube WITH $${{"type": "object", "properties": {{
  "resolution": {{"default": "month"}},
  "windows": {{"type": "object", "properties": {{"month": {{"default": "48 months"}}}}}}}}}}$$
AS FACT ON DATASET;
{judged}
DECLARE ASPECT revenue WITH $${{"title": "Revenue"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT win_rate WITH $${{"title": "Win rate"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT balance WITH $${{"title": "Balance"}}$$ AS QUERY ON DATASET;
DECLARE ASPECT signups WITH $${{"title": "Signups"}}$$ AS QUERY ON DATASET;
GLOSS revenue ON fin AS $${{"sql": "SELECT sale_date, amount AS value FROM sales"}}$$;
GLOSS win_rate ON fin AS $${{"sql": "SELECT race_date, wins / starts AS value, wins AS num, starts AS den FROM races"}}$$;
GLOSS balance ON fin AS $${{"sql": "SELECT as_of, amount AS value FROM balances", "behavior": "stock"}}$$;
GLOSS signups ON fin AS $${{"sql": "SELECT signed_on, amount AS value FROM registrations"}}$$;
{kit}
"##,
        judged = judged("month"),
        kit = forecast_kit()
    );
    let session = session(Arc::clone(&stub), &setup).await;
    // Eighteen months, 2024-01 to 2025-06, the second quarter of 2024
    // missing from every table; signups stop after 2024-09.
    let months: Vec<(i32, u32)> = (0..18)
        .map(|i| (2024 + i / 12, (i % 12 + 1) as u32))
        .filter(|&(y, m)| !(y == 2024 && (4..=6).contains(&m)))
        .collect();
    let dated = |column: &str, months: &[(i32, u32)]| {
        dated_values(
            column,
            months.iter().map(|(y, m)| day(*y, *m, 1)).collect(),
            months.iter().map(|_| 10.0).collect(),
        )
    };
    let (schema, batch) = dated("sale_date", &months);
    register(&session, "sales", schema, batch).await;
    let (schema, batch) = dated("as_of", &months);
    register(&session, "balances", schema, batch).await;
    let early: Vec<(i32, u32)> = months
        .iter()
        .copied()
        .filter(|&(y, m)| y == 2024 && m <= 9)
        .collect();
    let (schema, batch) = dated("signed_on", &early);
    register(&session, "registrations", schema, batch).await;
    let races = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, false),
        Field::new("wins", DataType::Float64, false),
        Field::new("starts", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&races),
        vec![
            Arc::new(Date32Array::from(
                months
                    .iter()
                    .map(|(y, m)| day(*y, *m, 15))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![1.0; months.len()])),
            Arc::new(Float64Array::from(vec![20.0; months.len()])),
        ],
    )
    .expect("a batch");
    register(&session, "races", races, batch).await;
    session
        .execute(
            "SELECT judge_time() FROM sales.sale_date; SELECT judge_time() FROM races.race_date; \
             SELECT judge_time() FROM balances.as_of; SELECT judge_time() FROM registrations.signed_on;",
        )
        .await
        .expect("the cadences are judged");

    let line = |table: &str, key: &str| -> String {
        table
            .lines()
            .find(|l| l.contains(key))
            .unwrap_or_else(|| panic!("no {key} in {table}"))
            .to_string()
    };

    // The flow: eighteen cells, the missing quarter's months at zero,
    // and that quarter's own fold a zero cell.
    let revenue = rows(
        &session,
        "SELECT period, value FROM metric_series() \
         WHERE metric = 'revenue' AND dimension = '' ORDER BY period;",
    )
    .await;
    let cells: Vec<&str> = revenue.lines().filter(|l| l.contains("202")).collect();
    assert_eq!(cells.len(), 18, "{revenue}");
    for month in ["2024-04-01", "2024-05-01", "2024-06-01"] {
        assert!(line(&revenue, month).contains("| 0.0 "), "{revenue}");
    }
    assert!(
        line(&revenue, "2024-07-01").contains("| 10.0 "),
        "{revenue}"
    );
    let quarter = rows(
        &session,
        "SELECT period, value FROM metric_series(grain => 'quarter') \
         WHERE metric = 'revenue' AND dimension = '' ORDER BY period;",
    )
    .await;
    assert!(line(&quarter, "2024-04-01").contains("| 0.0 "), "{quarter}");
    assert!(
        line(&quarter, "2024-07-01").contains("| 30.0 "),
        "{quarter}"
    );

    // The ratio and the stock keep the gap: fifteen cells each, none
    // in the missing quarter. The flow that stops in September carries
    // zeros to the data's edge, June 2025.
    let shape = rows(
        &session,
        "SELECT metric, count(*) AS n, \
                count(*) FILTER (WHERE period >= TIMESTAMP '2024-04-01' \
                                   AND period < TIMESTAMP '2024-07-01') AS in_gap, \
                count(*) FILTER (WHERE value = 0) AS zeros, \
                max(period) AS last \
         FROM metric_series() WHERE dimension = '' GROUP BY metric ORDER BY metric;",
    )
    .await;
    let balance = line(&shape, "balance");
    assert!(
        balance.contains("| 15 ") && balance.contains("| 0 ") && balance.contains("2025-06-01"),
        "{shape}"
    );
    let win_rate = line(&shape, "win_rate");
    assert!(
        win_rate.contains("| 15 ") && win_rate.contains("| 0 ") && win_rate.contains("2025-06-01"),
        "{shape}"
    );
    let signups = line(&shape, "signups");
    assert!(
        signups.contains("| 18 ")
            && signups.contains("| 3 ")
            && signups.contains("| 12 ")
            && signups.contains("2025-06-01"),
        "{shape}"
    );

    // The forecast starts from the edge, where the flow's last cell is
    // a zero.
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .expect("the forecast lands");
    let origin = rows(
        &session,
        "SELECT origin, p50 FROM forecast_points() WHERE metric = 'signups' AND step = 1;",
    )
    .await;
    assert!(
        origin.contains("2025-06-01") && origin.contains("| 0.0 "),
        "{origin}"
    );

    // Three more months of data on another table move the edge: the
    // stopped flow's head is built over, its zeros reach September,
    // and the points for July to September land on zero.
    let late: Vec<(i32, u32)> = [(2025, 7), (2025, 8), (2025, 9)].to_vec();
    let (schema, batch) = dated("shipped_on", &late);
    register(&session, "shipments", schema, batch).await;
    session
        .execute("SELECT judge_time() FROM shipments.shipped_on;")
        .await
        .expect("the cadence is judged");
    let moved = rows(
        &session,
        "SELECT count(*) AS n, max(period) AS last FROM metric_series() \
         WHERE metric = 'signups' AND dimension = '';",
    )
    .await;
    assert!(
        moved.contains("| 21 ") && moved.contains("2025-09-01"),
        "{moved}"
    );
    let landed = rows(
        &session,
        "SELECT step, actual, landed FROM forecast_points() \
         WHERE metric = 'signups' AND span IS NULL ORDER BY step;",
    )
    .await;
    for step in ["| 1 ", "| 2 ", "| 3 "] {
        let l = line(&landed, step);
        assert!(l.contains("| 0.0 ") && l.contains("inside"), "{landed}");
    }
    assert!(!line(&landed, "| 4 ").contains("inside"), "{landed}");
}
