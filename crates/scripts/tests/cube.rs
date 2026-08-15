//! The shipped metric_cube.rhai over a query-routing fake door: two
//! grounded metrics (a flow with served dimension columns and a
//! disclosed rival, a marked stock with none), asserting dimension
//! admission and ranking, the month window, the rival series, and the
//! stock verb. No model, no weights — the cube is plain SQL policy.

use std::path::Path;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use glossql_glossary::FunctionRow;
use glossql_scripts::RhaiRuntime;
use glossql_session::{FunctionRuntime, SqlDoor};
use serde_json::{Value, json};

const MONTHS: usize = 30;

fn period(i: usize) -> String {
    format!("20{:02}-{:02}-01T00:00:00", 24 + i / 12, 1 + i % 12)
}

/// Routes every query the cube sends and keeps what it saw.
struct CubeDoor(Mutex<Vec<String>>);

fn utf8_f64(periods: Vec<String>, values: Vec<f64>) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("period", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    vec![
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(periods)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap(),
    ]
}

impl SqlDoor for CubeDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        self.0.lock().unwrap().push(query.to_string());
        if query.contains("FROM glossary") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("subject", DataType::Utf8, false),
                Field::new("aspect", DataType::Utf8, false),
                Field::new("actor_kind", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
            ]));
            let revenue = json!({
                "sql": "SELECT date, value, region, channel, note FROM lines",
                "assumptions": [
                    {"dimension": "definition",
                     "assumption": "revenue = invoiced less credit notes",
                     "alternative": "all invoiced",
                     "alternative_sql": "SELECT date, value FROM alt",
                     "confidence": 0.7}
                ]
            });
            let inventory = json!({
                "sql": "SELECT date, value FROM levels",
                "behavior": "stock"
            });
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["fin", "fin"])),
                        Arc::new(StringArray::from(vec!["revenue", "inventory"])),
                        Arc::new(StringArray::from(vec!["agent", "agent"])),
                        Arc::new(StringArray::from(vec![
                            revenue.to_string(),
                            inventory.to_string(),
                        ])),
                    ],
                )
                .unwrap(),
            ]);
        }
        if query.contains("count(DISTINCT") {
            let n: i64 = if query.contains("\"region\"") {
                3
            } else if query.contains("\"channel\"") {
                5
            } else {
                40 // note — past the member cap, rejected
            };
            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
            return Ok(vec![
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))]).unwrap(),
            ]);
        }
        if query.contains("AS member") {
            // a flow member series: every member, every month
            let members: &[&str] = if query.contains("\"region\"") {
                &["r1", "r2", "r3"]
            } else {
                &["c1", "c2", "c3", "c4", "c5"]
            };
            let (mut ps, mut ms, mut vs) = (Vec::new(), Vec::new(), Vec::new());
            for i in 0..MONTHS {
                for m in members {
                    ps.push(period(i));
                    ms.push(m.to_string());
                    vs.push(10.0 + i as f64);
                }
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("period", DataType::Utf8, false),
                Field::new("member", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]));
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(ps)),
                        Arc::new(StringArray::from(ms)),
                        Arc::new(Float64Array::from(vs)),
                    ],
                )
                .unwrap(),
            ]);
        }
        if query.contains("date_trunc") {
            // total series: the stock walks 18 rising levels, the flow
            // and its rival 30 months apiece
            if query.contains("FROM levels") {
                let periods = (0..18).map(period).collect();
                let values = (0..18).map(|i| 1000.0 + 5.0 * i as f64).collect();
                return Ok(utf8_f64(periods, values));
            }
            let base = if query.contains("FROM alt") { 90.0 } else { 100.0 };
            let periods = (0..MONTHS).map(period).collect();
            let values = (0..MONTHS).map(|i| base + 3.0 * i as f64).collect();
            return Ok(utf8_f64(periods, values));
        }
        // the probes: one row of each extract, date-typed time axis
        let mut fields = vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ];
        let mut cols: Vec<datafusion::arrow::array::ArrayRef> = vec![
            Arc::new(datafusion::arrow::array::Date32Array::from(vec![19000])),
            Arc::new(Float64Array::from(vec![1.0])),
        ];
        if query.contains("FROM lines") {
            for name in ["region", "channel", "note"] {
                fields.push(Field::new(name, DataType::Utf8, false));
                cols.push(Arc::new(StringArray::from(vec!["x"])));
            }
        }
        Ok(vec![
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap(),
        ])
    }
}

fn invoke(dir: &Path, door: Arc<CubeDoor>) -> Value {
    let rt = RhaiRuntime::new(dir);
    rt.invoke(
        &FunctionRow {
            name: "metric_cube".into(),
            scope_dataset: None,
            script: glossql_scripts::library::script("metric_cube.rhai")
                .expect("shipped")
                .into(),
            accepts: vec![],
            returns: None,
        },
        "fin",
        &json!({}),
        door,
    )
    .unwrap()
}

fn rows_of<'v>(metric: &'v Value, dimension: &str) -> Vec<&'v Value> {
    metric["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r[0] == dimension)
        .collect()
}

#[test]
fn the_cube_slices_windows_and_carries_the_rival() {
    let dir = tempfile::tempdir().unwrap();
    let door = Arc::new(CubeDoor(Mutex::new(Vec::new())));
    let out = invoke(dir.path(), door.clone());

    assert_eq!(out["applicable"], json!(true));
    assert_eq!(out["caps"]["months"], json!(24));
    let metrics = out["metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 2);

    let revenue = metrics.iter().find(|m| m["metric"] == "revenue").unwrap();
    // admission by member count, fewest first; the 40-member column is out
    assert_eq!(revenue["dims"], json!(["region", "channel"]));
    assert_eq!(revenue["behavior"], json!("flow"));
    assert_eq!(revenue["alternative"], json!("all invoiced"));

    // the window: 30 generated months serve as the last 24
    let total = rows_of(revenue, "");
    assert_eq!(total.len(), 24);
    assert_eq!(total[0][2], json!("2024-07"));
    assert_eq!(rows_of(revenue, "region").len(), 3 * 24);
    assert_eq!(rows_of(revenue, "channel").len(), 5 * 24);
    let rival = rows_of(revenue, "alternative");
    assert_eq!(rival.len(), 24);
    assert_eq!(rival[0][1], json!("all invoiced"));

    // the stock: no served dimensions, the sum-at-latest-date verb
    let inventory = metrics.iter().find(|m| m["metric"] == "inventory").unwrap();
    assert_eq!(inventory["behavior"], json!("stock"));
    assert_eq!(inventory["dims"], json!([]));
    assert_eq!(rows_of(inventory, "").len(), 18);
    let seen = door.0.lock().unwrap();
    assert!(
        seen.iter()
            .any(|q| q.contains("FROM levels")
                && q.contains("rank()")
                && q.contains("sum(value)")),
        "the stock series must sum the month's latest snapshot, never take one arbitrary row"
    );
    assert!(
        seen.iter()
            .any(|q| q.contains("FROM lines") && q.contains("sum(value)")),
        "the flow series must sum per month"
    );
}

/// Routes the glossary read, executes everything else on a real
/// DataFusion context — the stock verb's semantics, not its SQL shape.
struct LiveDoor {
    rt: tokio::runtime::Runtime,
    ctx: datafusion::prelude::SessionContext,
}

impl SqlDoor for LiveDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        if query.contains("FROM glossary") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("subject", DataType::Utf8, false),
                Field::new("aspect", DataType::Utf8, false),
                Field::new("actor_kind", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
            ]));
            let inventory = json!({
                "sql": "SELECT date, value, product FROM levels",
                "behavior": "stock"
            });
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["fin"])),
                        Arc::new(StringArray::from(vec!["inventory"])),
                        Arc::new(StringArray::from(vec!["agent"])),
                        Arc::new(StringArray::from(vec![inventory.to_string()])),
                    ],
                )
                .unwrap(),
            ]);
        }
        self.rt.block_on(async {
            self.ctx
                .sql(query)
                .await
                .map_err(|e| e.to_string())?
                .collect()
                .await
                .map_err(|e| e.to_string())
        })
    }
}

#[test]
fn the_stock_total_sums_the_months_latest_snapshot() {
    // Two products, snapshotted mid-month and again at month end:
    // the mid-month rows are superseded observations, the month-end
    // rows are the standing stock. January also proves the old
    // defect: one arbitrary month-end row (100 or 200) is wrong,
    // and summing everything (630) is wrong — the total is 300.
    let days = vec![
        ("2024-01-15", 10.0, "p1"),
        ("2024-01-15", 20.0, "p2"),
        ("2024-01-31", 100.0, "p1"),
        ("2024-01-31", 200.0, "p2"),
        ("2024-02-28", 110.0, "p1"),
        ("2024-02-28", 210.0, "p2"),
    ];
    // Date32 days since epoch; 19723 = 2024-01-01, offsets pre-March.
    let epoch = |d: &str| {
        let parts: Vec<i32> = d.split('-').map(|p| p.parse().unwrap()).collect();
        19723 + [0, 31][(parts[1] - 1) as usize] + parts[2] - 1
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("product", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(datafusion::arrow::array::Date32Array::from(
                days.iter().map(|(d, _, _)| epoch(d)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                days.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                days.iter().map(|(_, _, p)| *p).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_batch("levels", batch).unwrap();
    let door = Arc::new(LiveDoor {
        rt: tokio::runtime::Runtime::new().unwrap(),
        ctx,
    });

    let dir = tempfile::tempdir().unwrap();
    let rt = RhaiRuntime::new(dir.path());
    let out = rt
        .invoke(
            &FunctionRow {
                name: "metric_cube".into(),
                scope_dataset: None,
                script: glossql_scripts::library::script("metric_cube.rhai")
                .expect("shipped")
                .into(),
                accepts: vec![],
                returns: None,
            },
            "fin",
            &json!({}),
            door,
        )
        .unwrap();

    let metrics = out["metrics"].as_array().unwrap();
    let inventory = metrics.iter().find(|m| m["metric"] == "inventory").unwrap();
    assert_eq!(inventory["behavior"], json!("stock"));
    // product (2 members) is a served dimension on the real context
    assert_eq!(inventory["dims"], json!(["product"]));

    // the total: the month-end snapshot summed across products
    let total = rows_of(inventory, "");
    let by_period: Vec<(&str, f64)> = total
        .iter()
        .map(|r| (r[2].as_str().unwrap(), r[3].as_f64().unwrap()))
        .collect();
    assert_eq!(by_period, vec![("2024-01", 300.0), ("2024-02", 320.0)]);

    // the member series: each product's own latest observation
    let members = rows_of(inventory, "product");
    let got: Vec<(&str, &str, f64)> = members
        .iter()
        .map(|r| {
            (
                r[1].as_str().unwrap(),
                r[2].as_str().unwrap(),
                r[3].as_f64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("p1", "2024-01", 100.0),
            ("p2", "2024-01", 200.0),
            ("p1", "2024-02", 110.0),
            ("p2", "2024-02", 210.0),
        ]
    );
}

/// A live door whose only grounding is a ratio serving both halves.
struct RatioDoor {
    rt: tokio::runtime::Runtime,
    ctx: datafusion::prelude::SessionContext,
}

impl SqlDoor for RatioDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        if query.contains("FROM glossary") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("subject", DataType::Utf8, false),
                Field::new("aspect", DataType::Utf8, false),
                Field::new("actor_kind", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
            ]));
            let dso = json!({
                "sql": "SELECT date, value, num, den, segment, region FROM cells"
            });
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["fin"])),
                        Arc::new(StringArray::from(vec!["dso"])),
                        Arc::new(StringArray::from(vec!["agent"])),
                        Arc::new(StringArray::from(vec![dso.to_string()])),
                    ],
                )
                .unwrap(),
            ]);
        }
        let df = self
            .rt
            .block_on(self.ctx.sql(query))
            .map_err(|e| e.to_string())?;
        self.rt.block_on(df.collect()).map_err(|e| e.to_string())
    }
}

#[test]
fn a_ratio_totals_by_dividing_the_summed_halves_never_by_adding_ratios() {
    // The defect this exists for (found 2026-08-15, in a run): a ratio
    // is neither stock nor flow, so it took the flow path and every
    // sliced ratio reported the SUM of its members. DSO for one month
    // came back 928.3 days against a true 75.6 — the grounding served
    // segment x region, so twelve member ratios were added together.
    //
    // Four cells a month, chosen so the right answer and the old wrong
    // one cannot be confused: summing the per-row ratios gives 0.65,
    // dividing the summed halves gives 1000/6000.
    let cells = vec![
        ("2024-01-31", 100.0, 1000.0, "A", "EMEA"),
        ("2024-01-31", 200.0, 1000.0, "A", "APAC"),
        ("2024-01-31", 300.0, 2000.0, "B", "EMEA"),
        ("2024-01-31", 400.0, 2000.0, "B", "APAC"),
        ("2024-02-29", 110.0, 1100.0, "A", "EMEA"),
        ("2024-02-29", 210.0, 1100.0, "A", "APAC"),
        ("2024-02-29", 310.0, 2100.0, "B", "EMEA"),
        ("2024-02-29", 410.0, 2100.0, "B", "APAC"),
    ];
    let epoch = |d: &str| {
        let parts: Vec<i32> = d.split('-').map(|p| p.parse().unwrap()).collect();
        19723 + [0, 31][(parts[1] - 1) as usize] + parts[2] - 1
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("num", DataType::Float64, false),
        Field::new("den", DataType::Float64, false),
        Field::new("segment", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(datafusion::arrow::array::Date32Array::from(
                cells.iter().map(|c| epoch(c.0)).collect::<Vec<_>>(),
            )),
            // `value` is each row's own ratio — what the old code summed.
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.1 / c.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                cells.iter().map(|c| c.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                cells.iter().map(|c| c.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                cells.iter().map(|c| c.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_batch("cells", batch).unwrap();
    let door = Arc::new(RatioDoor {
        rt: tokio::runtime::Runtime::new().unwrap(),
        ctx,
    });

    let dir = tempfile::tempdir().unwrap();
    let rt = RhaiRuntime::new(dir.path());
    let out = rt
        .invoke(
            &FunctionRow {
                name: "metric_cube".into(),
                scope_dataset: None,
                script: glossql_scripts::library::script("metric_cube.rhai")
                    .expect("shipped")
                    .into(),
                accepts: vec![],
                returns: None,
            },
            "fin",
            &json!({}),
            door,
        )
        .unwrap();

    let metrics = out["metrics"].as_array().unwrap();
    let dso = metrics.iter().find(|m| m["metric"] == "dso").unwrap();
    assert_eq!(dso["behavior"], json!("ratio"), "{dso}");

    // The halves are measures, not axes: num/den must never be offered
    // as dimensions to slice along.
    let dims: Vec<&str> = dso["dims"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d.as_str().unwrap())
        .collect();
    assert!(dims.contains(&"segment") && dims.contains(&"region"), "{dims:?}");
    assert!(!dims.contains(&"num") && !dims.contains(&"den"), "{dims:?}");

    let near = |got: f64, want: f64, what: &str| {
        assert!((got - want).abs() < 1e-9, "{what}: got {got}, want {want}");
    };
    let at = |rows: &[&Value], member: &str, period: &str| -> f64 {
        rows.iter()
            .find(|r| r[1] == member && r[2] == period)
            .unwrap_or_else(|| panic!("no {member}/{period}"))[3]
            .as_f64()
            .unwrap()
    };

    // The total: sum(num)/sum(den), never the 0.65 that adding the four
    // per-row ratios would give.
    let total = rows_of(dso, "");
    near(at(&total, "", "2024-01"), 1000.0 / 6000.0, "january total");
    near(at(&total, "", "2024-02"), 1040.0 / 6400.0, "february total");

    // Each member likewise divides its own summed halves — segment A is
    // 300/2000, not the 0.3 its two region rows add up to.
    let by_segment = rows_of(dso, "segment");
    near(at(&by_segment, "A", "2024-01"), 300.0 / 2000.0, "segment A");
    near(at(&by_segment, "B", "2024-01"), 700.0 / 4000.0, "segment B");
    let by_region = rows_of(dso, "region");
    near(at(&by_region, "EMEA", "2024-01"), 400.0 / 3000.0, "region EMEA");
    near(at(&by_region, "APAC", "2024-01"), 600.0 / 3000.0, "region APAC");
}
