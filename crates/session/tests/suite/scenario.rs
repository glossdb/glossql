//! The scenario world (fixture 19): a lever declared on its column, a
//! scenario declared as a factor per period on it, and the world read in
//! two passes against a stub what-if service — the arithmetic through
//! the grounding where the lever's table is scanned, the estimate
//! ahead of the last actual, every cell saying which. What the door
//! sends: each metric's series with the lever's beside it, the periods
//! ahead twice, as planned and under the scenario.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    Array, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Answered, FunctionRuntime, Outcome, Session, WHATIF};
use serde_json::{Value, json};

/// A what-if service that moves the target with the inputs: under the
/// scenario a member's median is its last actual times one plus the sum
/// over the inputs of the scenario input over the planned one less one
/// — one input, the ratio itself — the 80% band at half and one and a
/// half of it; the sign is `+` and the basis says the history showed
/// it. The answer object names the inputs changed and, per member and
/// input, the part each does alone: the last actual times the ratio
/// less one, read on the first period ahead, its interval at half and
/// one and a half. It records every table it is handed as rows.
#[derive(Debug)]
struct WhatIf {
    serve: bool,
    sent: Mutex<Vec<(Vec<Value>, Value)>>,
}

impl WhatIf {
    fn new(serve: bool) -> Arc<Self> {
        Arc::new(WhatIf {
            serve,
            sent: Mutex::new(Vec::new()),
        })
    }
}

#[glossql_session::async_trait]
impl FunctionRuntime for WhatIf {
    fn serves(&self, point: &str) -> bool {
        self.serve && point == WHATIF
    }

    async fn table(
        &self,
        point: &str,
        table: RecordBatch,
        spec: Value,
    ) -> Result<Answered, String> {
        assert_eq!(point, WHATIF);
        assert_eq!(spec["targets"], json!(["value"]));
        assert_eq!(spec["levels"], json!([0.1, 0.5, 0.9]));
        let names: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            names.len() >= 5,
            "member, period, status, value and the inputs: {names:?}"
        );
        let mut rows = Vec::new();
        for i in 0..table.num_rows() {
            let mut row = serde_json::Map::new();
            for (name, col) in names.iter().zip(table.columns()) {
                let v = if col.is_null(i) {
                    Value::Null
                } else if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
                    json!(f.value(i))
                } else {
                    json!(array_value_to_string(col, i).unwrap())
                };
                row.insert(name.clone(), v);
            }
            rows.push(Value::Object(row));
        }
        self.sent.lock().unwrap().push((rows.clone(), spec));
        let inputs: Vec<&String> = names[4..].iter().collect();
        // Per member: the last actual, the planned inputs, then each
        // scenario row answered at the three levels, and the parts
        // from the first scenario row.
        let member = table.column_by_name("member").unwrap();
        let period = table
            .column_by_name("period")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let (mut m_col, mut p_col, mut s_col, mut l_col, mut v_col) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut parts: Vec<Value> = Vec::new();
        let mut last: Option<(String, f64, Vec<Option<f64>>)> = None;
        for (i, row) in rows.iter().enumerate() {
            let m = array_value_to_string(member, i).unwrap();
            match row["status"].as_str().unwrap() {
                "actual" => last = Some((m, row["value"].as_f64().unwrap(), Vec::new())),
                "plan" => {
                    if let Some(l) = last.as_mut() {
                        l.2 = inputs.iter().map(|c| row[c.as_str()].as_f64()).collect();
                    }
                }
                "scenario" => {
                    let (ref name, actual, ref planned) = *last.as_ref().unwrap();
                    assert_eq!(*name, m);
                    let ratios: Vec<f64> = inputs
                        .iter()
                        .zip(planned)
                        .map(|(c, p)| row[c.as_str()].as_f64().unwrap() / p.unwrap())
                        .collect();
                    let median = actual * (1.0 + ratios.iter().map(|r| r - 1.0).sum::<f64>());
                    if !parts.iter().any(|p| p["member"] == m) {
                        for (c, r) in inputs.iter().zip(&ratios) {
                            let shift = actual * (r - 1.0);
                            parts.push(json!({
                                "member": m, "column": "value", "input": c,
                                "history_shift": shift,
                                "history_low": 0.5 * shift, "history_high": 1.5 * shift,
                            }));
                        }
                    }
                    for (level, factor) in [(0.1, 0.5), (0.5, 1.0), (0.9, 1.5)] {
                        m_col.push(m.clone());
                        p_col.push(period.value(i));
                        s_col.push("scenario");
                        l_col.push(level);
                        v_col.push(median * factor);
                    }
                }
                other => panic!("status {other}"),
            }
        }
        let n = m_col.len();
        let answer = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("member", DataType::Utf8, false),
                Field::new(
                    "period",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("column", DataType::Utf8, false),
                Field::new("status", DataType::Utf8, false),
                Field::new("level", DataType::Float64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("sign", DataType::Utf8, true),
                Field::new("basis", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(m_col)),
                Arc::new(TimestampNanosecondArray::from(p_col)),
                Arc::new(StringArray::from(vec!["value"; n])),
                Arc::new(StringArray::from(s_col)),
                Arc::new(Float64Array::from(l_col)),
                Arc::new(Float64Array::from(v_col)),
                Arc::new(StringArray::from(vec!["+"; n])),
                Arc::new(StringArray::from(vec![
                    "the history shows the input moving the column";
                    n
                ])),
            ],
        )
        .unwrap();
        Ok(Answered {
            table: answer,
            answer: json!({"service": "stub", "version": "test", "errors": [],
                           "changed": inputs, "parts": parts}),
        })
    }
}

fn day(y: i32, m: u32, d: u32) -> i32 {
    let d = chrono::NaiveDate::from_ymd_opt(y, m, d).expect("a date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
    (d - epoch).num_days() as i32
}

/// The kit's `lever` aspect as shipped, and the temporal verdict the
/// cube admits a series on, judged by a stub function.
fn setup() -> &'static str {
    r##"
DECLARE DATASET fin SET (purpose: 'the scenario world');
USE fin;
DECLARE ASPECT cube WITH $${"type": "object", "properties": {
  "resolution": {"default": "month"},
  "windows": {"type": "object", "properties": {"month": {"default": "48 months"}}}}}$$
AS FACT ON DATASET;
DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION judge_time FOR GLOBAL AS
  $$SELECT true AS applicable, 'month' AS granularity, named_struct('ratio', 1.0) AS completeness$$
  RETURNS temporal_profile;
DECLARE ASPECT lever WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "boolean"}, "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN;
DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
DECLARE ASPECT win_rate WITH $${"title": "Win rate"}$$ AS QUERY ON DATASET;
GLOSS revenue ON fin AS $${"sql": "SELECT sale_date, units * price AS value FROM sales"}$$;
GLOSS win_rate ON fin AS $${"sql": "SELECT race_date, wins / starts AS value, wins AS num, starts AS den FROM races"}$$;
DECLARE ASPECT price_up WITH $${
  "title": "Price up from April", "x-kind": "scenario",
  "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}
}$$ AS FACT ON DATASET;
DECLARE ASPECT two WITH $${
  "title": "Two levers", "x-kind": "scenario",
  "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}
}$$ AS FACT ON DATASET;
"##
}

/// Eighteen months, 2024-01 to 2025-06: sales of ten units at a price
/// rising by one a month from ten — revenue 100, 110, … 270 — and
/// races won half the time.
async fn session(stub: Arc<WhatIf>) -> Session {
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
    session.execute(setup()).await.expect("the setup lands");
    std::mem::forget(dir);
    let months: Vec<(i32, u32)> = (0..18)
        .map(|i| (2024 + i / 12, (i % 12 + 1) as u32))
        .collect();
    let sales = Arc::new(Schema::new(vec![
        Field::new("sale_date", DataType::Date32, false),
        Field::new("units", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&sales),
        vec![
            Arc::new(Date32Array::from(
                months
                    .iter()
                    .map(|(y, m)| day(*y, *m, 1))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![10.0; 18])),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 10.0 + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "sales", sales, batch).await;
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
            Arc::new(Float64Array::from(vec![5.0; 18])),
            Arc::new(Float64Array::from(vec![10.0; 18])),
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

async fn refused(session: &Session, sql: &str) -> String {
    session.execute(sql).await.expect_err("refused").to_string()
}

/// The price lever, declared; a scenario of one and a half from April
/// to August and double in September.
const DECLARE_PRICE_UP: &str = r##"
GLOSS lever ON sales.price AS $${"value": true, "grounds": "the list price, set by sales"}$$;
GLOSS price_up ON fin AS $${"levers": [{"column": "sales.price",
  "factor": {"2025-04": 1.5, "2025-05": 1.5, "2025-06": 1.5, "2025-07": 1.5, "2025-08": 1.5, "2025-09": 2.0},
  "basis": "the planned price list"}]}$$;
"##;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scenario_names_declared_levers_or_is_refused() {
    let session = session(WhatIf::new(true)).await;
    // No lever glossed yet: the scenario is refused by name, before it
    // lands.
    let e = refused(
        &session,
        r##"GLOSS price_up ON fin AS $${"levers": [{"column": "sales.price", "factor": {"2025-07": 1.5}, "basis": "x"}]}$$;"##,
    )
    .await;
    assert!(
        e.contains("`sales.price` is not a declared lever")
            && e.contains("GLOSS lever ON sales.price"),
        "{e}"
    );
    // A body that is not a scenario's says what is wrong.
    for (body, said) in [
        (r#"{"overrides": []}"#, "no `levers`"),
        (
            r#"{"levers": [{"column": "sales.price", "factor": {"2025-7": 1.5}, "basis": "x"}]}"#,
            "`2025-7` is not a period",
        ),
        (
            r#"{"levers": [{"column": "price", "factor": {"2025-07": 1.5}, "basis": "x"}]}"#,
            "not `table.column`",
        ),
    ] {
        let e = refused(&session, &format!("GLOSS price_up ON fin AS $${body}$$;")).await;
        assert!(e.contains(said), "{body}: {e}");
    }
    // A lever examined and found none is no lever.
    session
        .execute(r##"GLOSS lever ON sales.units AS $${"value": false, "grounds": "demand, not set"}$$;"##)
        .await
        .expect("glossed");
    let e = refused(
        &session,
        r##"GLOSS price_up ON fin AS $${"levers": [{"column": "sales.units", "factor": {"2025-07": 1.5}, "basis": "x"}]}$$;"##,
    )
    .await;
    assert!(e.contains("`sales.units` is not a declared lever"), "{e}");
    // Declared, the scenario lands.
    session
        .execute(DECLARE_PRICE_UP)
        .await
        .expect("the scenario lands");
    let glossed = rows(
        &session,
        "SELECT aspect, subject FROM GLOSSARY(fin) WHERE aspect IN ('lever', 'price_up') ORDER BY aspect, subject;",
    )
    .await;
    assert!(
        glossed.contains("price_up") && glossed.contains("sales.price"),
        "{glossed}"
    );
    // The read refuses a world without its scenario, a scenario without
    // its world, and a name nobody declared.
    let e = refused(
        &session,
        "SELECT * FROM metric_series(world => 'scenario');",
    )
    .await;
    assert!(e.contains("a scenario is read by name"), "{e}");
    let e = refused(
        &session,
        "SELECT * FROM metric_series(scenario => 'price_up');",
    )
    .await;
    assert!(e.contains("read in the scenario world"), "{e}");
    let e = refused(
        &session,
        "SELECT * FROM metric_series(world => 'scenario', scenario => 'nobody');",
    )
    .await;
    assert!(e.contains("no aspect of that name"), "{e}");
    let e = refused(
        &session,
        "SELECT * FROM scenario_points(scenario => 'revenue');",
    )
    .await;
    assert!(e.contains("without `x-kind: scenario`"), "{e}");
    let e = refused(&session, "SELECT * FROM scenario_points('price_up');").await;
    assert!(e.contains("scenario => '<name>'"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_world_is_replayed_where_the_lever_reaches_and_estimated_ahead() {
    let stub = WhatIf::new(true);
    let session = session(Arc::clone(&stub)).await;
    session
        .execute(DECLARE_PRICE_UP)
        .await
        .expect("the scenario lands");

    // The cells: revenue replayed from April — price one and a half
    // from April on, 250 → 375, 260 → 390, 270 → 405 — and estimated
    // July to September from the last actual, 270: one and a half, one
    // and a half, double. The win rate, which no lever reaches, is
    // estimated alone: 0.5 → 0.75, 0.75, 1.0. Nothing before April.
    let cells = rows(
        &session,
        "SELECT metric, world, period, value, num, den, behavior \
         FROM metric_series(world => 'scenario', scenario => 'price_up') ORDER BY metric, period;",
    )
    .await;
    let lines: Vec<&str> = cells.lines().filter(|l| l.contains("scenario")).collect();
    assert_eq!(lines.len(), 9, "{cells}");
    for (i, (period, value)) in [
        ("2025-04-01", "375.0"),
        ("2025-05-01", "390.0"),
        ("2025-06-01", "405.0"),
        ("2025-07-01", "405.0"),
        ("2025-08-01", "405.0"),
        ("2025-09-01", "540.0"),
    ]
    .iter()
    .enumerate()
    {
        assert!(
            lines[i].contains("revenue") && lines[i].contains(period) && lines[i].contains(value),
            "{period} {value}: {}",
            lines[i]
        );
        assert!(lines[i].contains("flow"), "{}", lines[i]);
    }
    for (i, (period, value)) in [
        ("2025-07-01", "0.75"),
        ("2025-08-01", "0.75"),
        ("2025-09-01", "1.0"),
    ]
    .iter()
    .enumerate()
    {
        let line = lines[6 + i];
        assert!(
            line.contains("win_rate") && line.contains(period) && line.contains(value),
            "{period} {value}: {line}"
        );
        assert!(line.contains("ratio"), "{line}");
    }
    assert!(!cells.contains("2025-03-01"), "{cells}");
    // The replayed ratio has no halves of its own: the estimate is a
    // median, not a division.
    assert!(
        lines[6].contains("|     |     | ratio") || lines[6].matches("| ").count() >= 6,
        "{}",
        lines[6]
    );

    // Another grain serves no scenario rows; the actual world is as it
    // was.
    let other = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(grain => 'quarter', world => 'scenario', scenario => 'price_up');",
    )
    .await;
    assert!(other.contains("| 0 "), "{other}");
    let actual = rows(
        &session,
        "SELECT value FROM metric_series() WHERE metric = 'revenue' AND period = TIMESTAMP '2025-06-01';",
    )
    .await;
    assert!(actual.contains("270.0"), "{actual}");

    // Cell by cell: the pass, the band on an estimate, the sign and
    // the basis; no forecast was ever recorded, so no share is known.
    let points = rows(
        &session,
        "SELECT metric, period, step, pass, value, low, high, held_80, held_from, sign, basis, served_by \
         FROM scenario_points(scenario => 'price_up') ORDER BY metric, period;",
    )
    .await;
    let lines: Vec<&str> = points.lines().filter(|l| l.contains("202")).collect();
    assert_eq!(lines.len(), 9, "{points}");
    assert!(
        lines[0].contains("| 2025-04-01 |      | replayed  | 375.0")
            && lines[0].contains("the planned price list"),
        "{}",
        lines[0]
    );
    assert!(
        lines[3].contains("| 2025-07-01 | 1    | estimated | 405.0 | 202.5 | 607.5 |")
            && lines[3].contains("| +    |")
            && lines[3].contains("the history shows the input moving the column")
            && lines[3].contains("| stub test |"),
        "{}",
        lines[3]
    );
    assert!(
        lines[5].contains("| 2025-09-01 | 3    | estimated | 540.0 | 270.0 | 810.0 |"),
        "{}",
        lines[5]
    );
    assert!(
        lines[6].contains("win_rate") && lines[6].contains("| 1    | estimated | 0.75 "),
        "{}",
        lines[6]
    );
    let known = rows(
        &session,
        "SELECT count(held_80) AS n, count(held_from) AS m FROM scenario_points(scenario => 'price_up');",
    )
    .await;
    assert!(known.contains("| 0 | 0 |"), "{known}");
    // Each lever's part: one lever, one part per estimated metric — the
    // stub's shift is the last actual times the first period's ratio
    // less one, 270 at one and a half, 0.5 for the win rate.
    let levers = rows(
        &session,
        "SELECT metric, lever, shift, low, high, basis, served_by \
         FROM scenario_levers(scenario => 'price_up') ORDER BY metric;",
    )
    .await;
    let lines: Vec<&str> = levers
        .lines()
        .filter(|l| l.contains("sales.price"))
        .collect();
    assert_eq!(lines.len(), 2, "{levers}");
    assert!(
        lines[0].contains("| revenue  | sales.price | 135.0 | 67.5  | 202.5 |")
            && lines[0].contains("| stub test |"),
        "{levers}"
    );
    assert!(
        lines[1].contains("| win_rate | sales.price | 0.25 "),
        "{levers}"
    );

    // What the service was handed: one table at the month grain, both
    // metrics as members, the lever beside each series as its monthly
    // mean, the three periods ahead twice — as planned, the last price
    // held at 27, and under the scenario, 40.5, 40.5 and 54.
    // A call per read that served a cell — the cells, the points, the
    // count, the levers; the quarter grain served none and asked none.
    let sent = stub.sent.lock().unwrap().clone();
    let tables: Vec<&(Vec<Value>, Value)> = sent.iter().collect();
    assert_eq!(tables.len(), 4, "{}", tables.len());
    let (table, spec) = tables[0];
    assert_eq!(
        spec,
        &json!({"targets": ["value"], "levels": [0.1, 0.5, 0.9]})
    );
    assert_eq!(table.len(), 2 * (18 + 3 + 3), "{}", table.len());
    let revenue: Vec<&Value> = table.iter().filter(|r| r["member"] == "revenue").collect();
    assert_eq!(revenue[0]["status"], "actual");
    assert_eq!(revenue[0]["value"], json!(100.0));
    assert_eq!(revenue[0]["sales.price"], json!(10.0));
    assert_eq!(revenue[17]["sales.price"], json!(27.0));
    assert_eq!(revenue[18]["status"], "plan");
    assert!(revenue[18]["value"].is_null());
    assert_eq!(revenue[18]["sales.price"], json!(27.0));
    assert_eq!(revenue[19]["status"], "scenario");
    assert_eq!(revenue[19]["sales.price"], json!(40.5));
    assert_eq!(revenue[23]["sales.price"], json!(54.0));
    assert!(
        revenue[19]["period"]
            .as_str()
            .unwrap()
            .starts_with("2025-07-01")
    );
    let win: Vec<&Value> = table.iter().filter(|r| r["member"] == "win_rate").collect();
    assert_eq!(win.len(), 24);
    assert_eq!(
        win[0]["sales.price"],
        json!(10.0),
        "the lever rides every member's history"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn several_levers_are_replayed_and_estimated_as_one_change_and_no_service_says_so() {
    let stub = WhatIf::new(true);
    let session = session(Arc::clone(&stub)).await;
    session
        .execute(
            r##"
GLOSS lever ON sales.price AS $${"value": true, "grounds": "the list price"}$$;
GLOSS lever ON races.starts AS $${"value": true, "grounds": "entries, set by the stable"}$$;
GLOSS two ON fin AS $${"levers": [
  {"column": "sales.price", "factor": {"2025-06": 2.0, "2025-07": 2.0}, "basis": "a"},
  {"column": "races.starts", "factor": {"2025-06": 2.0, "2025-07": 2.0}, "basis": "b"}]}$$;
"##,
        )
        .await
        .expect("the scenario lands");
    let points = rows(
        &session,
        "SELECT metric, period, pass, value, den, basis FROM scenario_points(scenario => 'two') \
         ORDER BY metric, period;",
    )
    .await;
    let lines: Vec<&str> = points.lines().filter(|l| l.contains("2025")).collect();
    assert_eq!(lines.len(), 4, "{points}");
    // Both replayed in June — price doubled, starts doubled — and both
    // estimated in July as one change of two levers: the stub adds the
    // two ratios' excess, 270 to 810 and 0.5 to 1.5.
    assert!(
        lines[0].contains("| 2025-06-01 | replayed  | 540.0"),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].contains("| 2025-07-01 | estimated | 810.0"),
        "{}",
        lines[1]
    );
    assert!(
        lines[2].contains("| 2025-06-01 | replayed  | 0.25") && lines[2].contains("| 20.0"),
        "{}",
        lines[2]
    );
    assert!(
        lines[3].contains("| 2025-07-01 | estimated | 1.5"),
        "{}",
        lines[3]
    );
    // The parts: per metric and lever, what each does alone, adding up
    // to the joint estimate's shift — 270 and 270 for revenue, 0.5 and
    // 0.5 for the win rate.
    let levers = rows(
        &session,
        "SELECT metric, lever, shift, low, high, served_by \
         FROM scenario_levers(scenario => 'two') ORDER BY metric, lever;",
    )
    .await;
    let lines: Vec<&str> = levers
        .lines()
        .filter(|l| l.contains("| stub test"))
        .collect();
    assert_eq!(lines.len(), 4, "{levers}");
    assert!(
        lines[0].contains("| revenue  | races.starts | 270.0 | 135.0 | 405.0 |"),
        "{levers}"
    );
    assert!(
        lines[1].contains("| revenue  | sales.price  | 270.0 |"),
        "{levers}"
    );
    assert!(
        lines[2].contains("| win_rate | races.starts | 0.5 "),
        "{levers}"
    );
    assert!(
        lines[3].contains("| win_rate | sales.price  | 0.5 "),
        "{levers}"
    );
    // What the service was handed: both levers as input columns, each
    // held as planned — the price at 27, the starts at 10 — and doubled
    // by its own factor under the scenario.
    let sent = stub.sent.lock().unwrap().clone();
    let (table, _) = sent.last().expect("a call");
    let july: Vec<&Value> = table
        .iter()
        .filter(|r| r["member"] == "revenue" && r["status"] == "scenario")
        .collect();
    assert_eq!(july.len(), 1, "{july:?}");
    assert_eq!(july[0]["sales.price"], json!(54.0), "{}", july[0]);
    assert_eq!(july[0]["races.starts"], json!(20.0), "{}", july[0]);
    let cells = rows(
        &session,
        "SELECT count(*) AS n FROM metric_series(world => 'scenario', scenario => 'two');",
    )
    .await;
    assert!(cells.contains("| 4 "), "{cells}");

    // Without a what-if service the arithmetic is served, the estimate
    // says what to set, and so does every lever's part.
    let session = session_without_service().await;
    let levers = rows(
        &session,
        "SELECT metric, lever, shift, basis FROM scenario_levers(scenario => 'price_up') ORDER BY metric;",
    )
    .await;
    let lines: Vec<&str> = levers
        .lines()
        .filter(|l| l.contains("sales.price"))
        .collect();
    assert_eq!(lines.len(), 2, "{levers}");
    assert!(
        lines[0].contains("| revenue  | sales.price |       |")
            && lines[0].contains("GLOSSQL_WHATIF_URL"),
        "{levers}"
    );
    let points = rows(
        &session,
        "SELECT period, pass, value, basis FROM scenario_points(scenario => 'price_up') \
         WHERE metric = 'revenue' ORDER BY period;",
    )
    .await;
    let lines: Vec<&str> = points.lines().filter(|l| l.contains("2025")).collect();
    assert_eq!(lines.len(), 6, "{points}");
    assert!(lines[2].contains("| replayed  | 405.0"), "{}", lines[2]);
    assert!(
        lines[3].contains("| estimated |") && lines[3].contains("GLOSSQL_WHATIF_URL"),
        "{}",
        lines[3]
    );
}

async fn session_without_service() -> Session {
    let session = session(WhatIf::new(false)).await;
    session
        .execute(DECLARE_PRICE_UP)
        .await
        .expect("the scenario lands");
    session
}

/// A fact table dated through a key into a date dimension — every star
/// schema: the lever's rows are placed in time by the declared edge,
/// the replay doubles June through the keys whose dates fall in it,
/// and the estimate reads the lever's series through the same edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lever_dated_through_a_declared_edge_is_replayed_and_estimated() {
    let stub = WhatIf::new(true);
    let session = session(Arc::clone(&stub)).await;
    let months: Vec<(i32, u32)> = (0..18)
        .map(|i| (2024 + i / 12, (i % 12 + 1) as u32))
        .collect();
    let orders = Arc::new(Schema::new(vec![
        Field::new("date_sk", DataType::Int64, false),
        Field::new("units", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&orders),
        vec![
            Arc::new(Int64Array::from((1..=18).collect::<Vec<i64>>())),
            Arc::new(Float64Array::from(vec![10.0; 18])),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 10.0 + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "orders", orders, batch).await;
    let days = Arc::new(Schema::new(vec![
        Field::new("date_sk", DataType::Int64, false),
        Field::new("day", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&days),
        vec![
            Arc::new(Int64Array::from((1..=18).collect::<Vec<i64>>())),
            Arc::new(Date32Array::from(
                months
                    .iter()
                    .map(|(y, m)| day(*y, *m, 1))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("a batch");
    register(&session, "days", days, batch).await;
    session
        .execute(
            r##"DECLARE RELATIONSHIP orders.date_sk -> days.date_sk;
SELECT judge_time() FROM days.day;
DECLARE ASPECT keyed WITH $${"title": "Keyed revenue"}$$ AS QUERY ON DATASET;
GLOSS keyed ON fin AS $${"sql": "SELECT d.day, o.units * o.price AS value FROM orders o JOIN days d ON d.date_sk = o.date_sk"}$$;
GLOSS lever ON orders.price AS $${"value": true, "grounds": "the list price"}$$;
DECLARE ASPECT keyed_up WITH $${
  "title": "Keyed price up", "x-kind": "scenario",
  "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}
}$$ AS FACT ON DATASET;
GLOSS keyed_up ON fin AS $${"levers": [{"column": "orders.price",
  "factor": {"2025-06": 2.0, "2025-07": 2.0}, "basis": "the price list"}]}$$;"##,
        )
        .await
        .expect("the edge, the grounding, the lever and the scenario land");
    // June replayed: the price doubled on the rows whose key dates in
    // June, 270 to 540; July estimated from the lever's series read
    // through the edge, 27 held and 54 under the scenario.
    let points = rows(
        &session,
        "SELECT period, pass, value, low, high, basis FROM scenario_points(scenario => 'keyed_up') \
         WHERE metric = 'keyed' ORDER BY period;",
    )
    .await;
    let lines: Vec<&str> = points.lines().filter(|l| l.contains("2025")).collect();
    assert_eq!(lines.len(), 2, "{points}");
    assert!(
        lines[0].contains("| 2025-06-01 | replayed  | 540.0 |")
            && lines[0].contains("replayed through the grounding"),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].contains("| 2025-07-01 | estimated | 540.0 | 270.0 | 810.0 |"),
        "{}",
        lines[1]
    );
    let sent = stub.sent.lock().unwrap().clone();
    let (table, _) = sent.last().expect("a call");
    let july: Vec<&Value> = table
        .iter()
        .filter(|r| r["member"] == "keyed" && r["status"] == "scenario")
        .collect();
    assert_eq!(july.len(), 1, "{july:?}");
    assert_eq!(july[0]["orders.price"], json!(54.0), "{}", july[0]);
}

/// A lever on a table with no date column and no declared edge to a
/// dated table can be placed nowhere in time: the scenario is refused
/// when it is glossed, with the edge to declare as the road out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lever_placed_nowhere_in_time_is_refused() {
    let session = session(WhatIf::new(true)).await;
    let raw = Arc::new(Schema::new(vec![
        Field::new("units", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&raw),
        vec![
            Arc::new(Float64Array::from(vec![10.0; 3])),
            Arc::new(Float64Array::from(vec![1.0; 3])),
        ],
    )
    .expect("a batch");
    register(&session, "raw", raw, batch).await;
    session
        .execute(
            r##"GLOSS lever ON raw.price AS $${"value": true, "grounds": "set by hand"}$$;
DECLARE ASPECT raw_up WITH $${
  "title": "Raw price up", "x-kind": "scenario",
  "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}
}$$ AS FACT ON DATASET;"##,
        )
        .await
        .expect("the lever and the aspect land");
    let e = refused(
        &session,
        r##"GLOSS raw_up ON fin AS $${"levers": [{"column": "raw.price", "factor": {"2025-07": 1.5}, "basis": "x"}]}$$;"##,
    )
    .await;
    assert!(
        e.contains("GLOSS raw_up ON fin")
            && e.contains("`raw.price`: `raw` carries no date column, and no declared edge reaches a table that does")
            && e.contains("DECLARE RELATIONSHIP raw.<key> -> <dates>.<key>"),
        "{e}"
    );
    let glossed = rows(
        &session,
        "SELECT count(*) AS n FROM GLOSSARY(fin) WHERE aspect = 'raw_up';",
    )
    .await;
    assert!(glossed.contains("| 0 "), "{glossed}");
}
