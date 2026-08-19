//! The shipped metric_cube body over a real session — since stage 5 the
//! slicing is the metric_cube_slices door and the body is SQL: two
//! grounded metrics (a flow with served dimension columns and a
//! disclosed rival, a marked stock with none), asserting dimension
//! admission and ranking, the month window, the rival series, and the
//! stock and ratio verbs. No model, no weights — the cube is plain SQL
//! policy.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Outcome, Session};
use serde_json::{Value, json};

/// Date32 day offsets for each month's first day from 2024-01 (leap)
/// through 2026-06; 19723 = 2024-01-01.
const MONTH_STARTS: [i32; 30] = [
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, // 2024
    366, 397, 425, 456, 486, 517, 547, 578, 609, 639, 670, 700, // 2025
    731, 762, 790, 821, 851, 882, // 2026
];

async fn cube_session(
    dir: &std::path::Path,
    tables: Vec<(&str, RecordBatch)>,
    glosses: &[&str],
) -> Session {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open(lake).await.unwrap();
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "t".into(),
        },
    )
    .unwrap();
    session
        .execute("DECLARE DATASET fin SET (purpose: 'the cube'); USE fin;")
        .await
        .unwrap();
    for (name, batch) in tables {
        let schema = batch.schema();
        session
            .register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .await
            .unwrap();
    }
    // A measurement is never hand-glossed (SPEC.md §5.2) — the judged
    // verdicts the cube reads land through extractions. The judge
    // functions serve fixed verdicts so the tests exercise the cube's
    // read policy, not the shipped profilers.
    let declarations = glossql_scripts::library::splice(
        r#"DECLARE ASPECT temporal_profile WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
           DECLARE ASPECT dimension_relevance WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
           DECLARE ASPECT metric_cube WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"},
                            "metrics": {"type": "array"}}}$$ AS MEASUREMENT ON DATASET;
           DECLARE FUNCTION judge_time FOR GLOBAL AS
             $$SELECT true AS applicable,
                      CASE WHEN $subject LIKE '%.signed' THEN 'irregular'
                           ELSE 'month' END AS granularity,
                      CASE WHEN $subject LIKE '%.signed' THEN NULL
                           WHEN $subject LIKE '%.booked' THEN named_struct('ratio', 0.5)
                           ELSE named_struct('ratio', 1.0) END AS completeness$$
             RETURNS temporal_profile;
           DECLARE FUNCTION judge_axis FOR GLOBAL AS
             $$SELECT true AS applicable,
                      CASE WHEN $subject LIKE '%.note' THEN 0.9
                           ELSE 0.7 END AS relevance$$
             RETURNS dimension_relevance;
           DECLARE FUNCTION metric_cube FOR GLOBAL AS $$metric_cube.sql$$
             RETURNS metric_cube;"#,
    )
    .expect("shipped body splices");
    session.execute(&declarations).await.unwrap();
    for g in glosses {
        session.execute(g).await.unwrap();
    }
    session.execute("SELECT metric_cube() FROM fin;").await.unwrap();
    session
}

async fn cube(session: &Session) -> Value {
    let outcomes = session
        .execute("SELECT value FROM GLOSSARY(fin::metric_cube) WHERE state = 'current';")
        .await
        .unwrap();
    let Some(Outcome::Rows(batches)) = outcomes.last() else {
        panic!("a value row")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let text =
        datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap();
    serde_json::from_str(&text).unwrap()
}

fn rows_of<'v>(metric: &'v Value, dimension: &str) -> Vec<&'v Value> {
    metric["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["dimension"] == dimension)
        .collect()
}

/// One read, rendered as text — for asserts over served relations.
async fn grid(session: &Session, sql: &str) -> String {
    let outcomes = session.execute(sql).await.unwrap();
    let Some(Outcome::Rows(batches)) = outcomes.last() else {
        panic!("rows")
    };
    datafusion::arrow::util::pretty::pretty_format_batches(batches)
        .unwrap()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cube_slices_windows_and_carries_the_rival() {
    let dir = tempfile::tempdir().unwrap();
    // 30 months of a flow: 15 rows per month (3 regions × 5 channels),
    // with a 40-distinct note column that exceeds the named-member cap
    // and must come back bucketed — top members plus 'other'.
    let (mut dates, mut values, mut regions, mut channels, mut notes) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (i, start) in MONTH_STARTS.iter().enumerate() {
        let mut k = 0;
        for r in ["r1", "r2", "r3"] {
            for c in ["c1", "c2", "c3", "c4", "c5"] {
                dates.push(19723 + start);
                values.push(1.0);
                regions.push(r);
                channels.push(c);
                notes.push(format!("n{}", (i * 15 + k) % 40));
                k += 1;
            }
        }
    }
    let lines = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(channels)),
            Arc::new(StringArray::from(notes)),
        ],
    )
    .unwrap();
    let alt = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Date32Array::from(
                MONTH_STARTS.iter().map(|s| 19723 + s).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![90.0; 30])),
        ],
    )
    .unwrap();
    let levels = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Date32Array::from(
                MONTH_STARTS[..18].iter().map(|s| 19723 + s).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 1000.0 + 5.0 * i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let session = cube_session(
        dir.path(),
        vec![("lines", lines), ("alt", alt), ("levels", levels)],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${
                 "sql": "SELECT date, value, region, channel, note FROM lines",
                 "assumptions": [
                     {"dimension": "definition",
                      "assumption": "revenue = invoiced less credit notes",
                      "alternative": "all invoiced",
                      "alternative_sql": "SELECT date, value FROM alt",
                      "confidence": 0.7}
                 ]}$$;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
            // The judged axes land LAST: a measurement is keyed at the
            // statement pin, and every declaration moves it — a judge
            // run before the metric glosses would read as drift. The
            // rival's `alt` table deliberately carries no verdict — a
            // rival is authored, never admission-validated, and still
            // serves. The wide note column takes the higher relevance
            // so ordering is by judgment, never by member count.
            "SELECT judge_time() FROM lines.date;",
            "SELECT judge_time() FROM levels.date;",
            "SELECT judge_axis() FROM lines.note;",
            "SELECT judge_axis() FROM lines.region;",
            "SELECT judge_axis() FROM lines.channel;",
        ],
    )
    .await;
    let out = cube(&session).await;

    assert_eq!(out["applicable"], json!(true));
    let metrics = out["metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 2);

    let revenue = metrics.iter().find(|m| m["metric"] == "revenue").unwrap();
    // admission by judged verdict: the 40-member note column leads on
    // relevance (and enters bucketed), the 0.7 tie breaks by fewest
    // members
    assert_eq!(revenue["dims"], json!(["note", "region", "channel"]));
    assert_eq!(revenue["bucketed"], json!(["note"]));
    assert_eq!(revenue["behavior"], json!("flow"));
    assert_eq!(revenue["alternative"], json!("all invoiced"));

    // the window: all 30 generated months fit under the 48 cap
    let total = rows_of(revenue, "");
    assert_eq!(total.len(), 30);
    assert_eq!(total[0]["period"], json!("2024-01"));
    assert_eq!(rows_of(revenue, "region").len(), 3 * 30);
    assert_eq!(rows_of(revenue, "channel").len(), 5 * 30);
    let rival = rows_of(revenue, "alternative");
    assert_eq!(rival.len(), 30);
    assert_eq!(rival[0]["member"], json!("all invoiced"));

    // the bucketed dimension: at most 24 members counting 'other', and
    // bucketing loses nothing — each month's note slices still sum to
    // the month's 15 rows
    let notes = rows_of(revenue, "note");
    let members: std::collections::HashSet<&str> = notes
        .iter()
        .map(|r| r["member"].as_str().unwrap())
        .collect();
    assert!(members.len() <= 24, "{} members", members.len());
    assert!(members.contains("other"));
    let mut by_month: std::collections::HashMap<&str, f64> = Default::default();
    for r in &notes {
        *by_month.entry(r["period"].as_str().unwrap()).or_default() +=
            r["value"].as_f64().unwrap();
    }
    assert_eq!(by_month.len(), 30);
    for (period, sum) in by_month {
        assert!((sum - 15.0).abs() < 1e-9, "{period}: {sum}");
    }

    // the stock: no served dimensions, its own 18-month window
    let inventory = metrics.iter().find(|m| m["metric"] == "inventory").unwrap();
    assert_eq!(inventory["behavior"], json!("stock"));
    assert_eq!(inventory["dims"], json!([]));
    assert_eq!(rows_of(inventory, "").len(), 18);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stock_total_sums_the_months_latest_snapshot() {
    // Two products, snapshotted mid-month and again at month end:
    // the mid-month rows are superseded observations, the month-end
    // rows are the standing stock. January also proves the old
    // defect: one arbitrary month-end row (100 or 200) is wrong,
    // and summing everything (630) is wrong — the total is 300.
    let days = [
        ("2024-01-15", 10.0, "p1"),
        ("2024-01-15", 20.0, "p2"),
        ("2024-01-31", 100.0, "p1"),
        ("2024-01-31", 200.0, "p2"),
        ("2024-02-28", 110.0, "p1"),
        ("2024-02-28", 210.0, "p2"),
    ];
    let epoch = |d: &str| {
        let parts: Vec<i32> = d.split('-').map(|p| p.parse().unwrap()).collect();
        19723 + [0, 31][(parts[1] - 1) as usize] + parts[2] - 1
    };
    let levels = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("product", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Date32Array::from(
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

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("levels", levels)],
        &[
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value, product FROM levels", "behavior": "stock"}$$;"#,
            "SELECT judge_time() FROM levels.date;",
            "SELECT judge_axis() FROM levels.product;",
        ],
    )
    .await;
    let out = cube(&session).await;

    let metrics = out["metrics"].as_array().unwrap();
    let inventory = metrics.iter().find(|m| m["metric"] == "inventory").unwrap();
    assert_eq!(inventory["behavior"], json!("stock"));
    // product (2 members) is a served dimension on the real context
    assert_eq!(inventory["dims"], json!(["product"]));

    // the total: the month-end snapshot summed across products
    let total = rows_of(inventory, "");
    let by_period: Vec<(&str, f64)> = total
        .iter()
        .map(|r| (r["period"].as_str().unwrap(), r["value"].as_f64().unwrap()))
        .collect();
    assert_eq!(by_period, vec![("2024-01", 300.0), ("2024-02", 320.0)]);

    // the member series: each product's own latest observation
    let members = rows_of(inventory, "product");
    let got: Vec<(&str, &str, f64)> = members
        .iter()
        .map(|r| {
            (
                r["member"].as_str().unwrap(),
                r["period"].as_str().unwrap(),
                r["value"].as_f64().unwrap(),
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

#[tokio::test(flavor = "multi_thread")]
async fn a_ratio_totals_by_dividing_the_summed_halves_never_by_adding_ratios() {
    // The defect this exists for: a ratio
    // is neither stock nor flow, so it took the flow path and every
    // sliced ratio reported the SUM of its members. DSO for one month
    // came back 928.3 days against a true 75.6 — the grounding served
    // segment x region, so twelve member ratios were added together.
    //
    // Four cells a month, chosen so the right answer and the old wrong
    // one cannot be confused: summing the per-row ratios gives 0.65,
    // dividing the summed halves gives 1000/6000.
    let cells = [
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
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("num", DataType::Float64, false),
            Field::new("den", DataType::Float64, false),
            Field::new("segment", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Date32Array::from(
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

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("cells", batch)],
        &[
            r#"DECLARE ASPECT dso WITH $${"title": "DSO"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS dso ON fin AS $${"sql": "SELECT date, value, num, den, segment, region FROM cells"}$$;"#,
            "SELECT judge_time() FROM cells.date;",
            "SELECT judge_axis() FROM cells.segment;",
            "SELECT judge_axis() FROM cells.region;",
        ],
    )
    .await;
    let out = cube(&session).await;

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
            .find(|r| r["member"] == member && r["period"] == period)
            .unwrap_or_else(|| panic!("no {member}/{period}"))["value"]
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

    // Every ratio cell carries its summed halves — the client's
    // coarser windows re-derive the division from them, never from
    // the monthly ratio values.
    let jan = total
        .iter()
        .find(|r| r["period"] == "2024-01")
        .unwrap();
    near(jan["num"].as_f64().unwrap(), 1000.0, "january num");
    near(jan["den"].as_f64().unwrap(), 6000.0, "january den");
    let seg_a = by_segment
        .iter()
        .find(|r| r["member"] == "A" && r["period"] == "2024-01")
        .unwrap();
    near(seg_a["num"].as_f64().unwrap(), 300.0, "segment A num");
    near(seg_a["den"].as_f64().unwrap(), 2000.0, "segment A den");

    // The flattened read serves the halves and the verb beside every
    // value.
    let series = grid(
        &session,
        "SELECT period, value, num, den, behavior FROM metric_series() \
         WHERE metric = 'dso' AND dimension = '' ORDER BY period;",
    )
    .await;
    assert!(series.contains("ratio") && series.contains("6000.0"), "{series}");

    // The day drill computes at read from the grounding, same judged
    // time axis, same verb, halves included.
    let days = grid(
        &session,
        "SELECT period, value, num, den, behavior FROM metric_days('dso') ORDER BY period;",
    )
    .await;
    assert!(days.contains("2024-01-31") && days.contains("2024-02-29"), "{days}");
    assert!(
        days.contains("ratio") && days.contains("1000.0") && days.contains("6000.0"),
        "{days}"
    );

    // A frame's named param binds into the door argument before the
    // pre-pass — the app's day drill rides `metric_days($metric)`.
    let mut values: std::collections::HashMap<String, datafusion::common::ScalarValue> =
        Default::default();
    values.insert(
        "metric".into(),
        datafusion::common::ScalarValue::Utf8(Some("dso".into())),
    );
    let query = session
        .query_stream_with_params(
            "SELECT count(*) AS n FROM metric_days($metric)",
            Some(datafusion::common::ParamValues::from(values)),
        )
        .await
        .unwrap();
    let batches: Vec<_> = futures::StreamExt::collect::<Vec<_>>(query.stream)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let n = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(n.contains("| 2 "), "{n}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_axes_come_from_judged_verdicts_never_from_the_datas_shape() {
    // The glos onboarding shape: the served frame leads with an
    // attribute date (all contracts signed in one month), a sparser
    // date follows, the event date comes last among the three; a
    // low-cardinality column rides along with no verdict landed.
    // Schema order would anchor the series on `signed` (one period
    // instead of thirty) and cardinality would admit `channel`; the
    // judged verdicts pick `date` — irregular never anchors, and among
    // named cadences the higher completeness wins — and `region`
    // alone.
    let (mut signed, mut booked, mut dates, mut values, mut regions, mut channels) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for (i, start) in MONTH_STARTS.iter().enumerate() {
        for r in ["r1", "r2", "r3"] {
            signed.push(19723 + (i as i32 % 28));
            booked.push(19723 + (i as i32 % 28));
            dates.push(19723 + start);
            values.push(1.0);
            regions.push(r);
            channels.push(["c1", "c2", "c3", "c4", "c5"][i % 5]);
        }
    }
    let lines = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("signed", DataType::Date32, false),
            Field::new("booked", DataType::Date32, false),
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("channel", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Date32Array::from(signed)),
            Arc::new(Date32Array::from(booked)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(channels)),
        ],
    )
    .unwrap();
    let bare = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Date32Array::from(vec![19723])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let session = cube_session(
        dir.path(),
        vec![("lines", lines), ("bare", bare)],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT raw WITH $${"title": "Raw"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT signed, booked, date, value, region, channel FROM lines"}$$;"#,
            r#"GLOSS raw ON fin AS $${"sql": "SELECT date, value FROM bare"}$$;"#,
            // The attribute date carries a verdict too — irregular, no
            // completeness — proving a landed verdict without a named
            // cadence never anchors, not merely an absent one.
            "SELECT judge_time() FROM lines.signed;",
            "SELECT judge_time() FROM lines.booked;",
            "SELECT judge_time() FROM lines.date;",
            "SELECT judge_axis() FROM lines.region;",
        ],
    )
    .await;
    let out = cube(&session).await;

    let metrics = out["metrics"].as_array().unwrap();
    let revenue = metrics.iter().find(|m| m["metric"] == "revenue").unwrap();
    assert_eq!(revenue["applicable"], json!(true), "{revenue}");
    // the judged axis, not the first temporal column: thirty monthly
    // periods, not one
    let total = rows_of(revenue, "");
    assert_eq!(total.len(), 30, "{revenue}");
    assert_eq!(total[0]["period"], json!("2024-01"));
    assert_eq!(total[29]["period"], json!("2026-06"));
    // the judged dimension alone — the unjudged column is a gap, not a
    // candidate
    assert_eq!(revenue["dims"], json!(["region"]));

    // a frame whose date column carries no verdict abstains with the
    // road out
    let raw = metrics.iter().find(|m| m["metric"] == "raw").unwrap();
    assert_eq!(raw["applicable"], json!(false), "{raw}");
    assert!(
        raw["reason"]
            .as_str()
            .unwrap()
            .contains("no judged time column"),
        "{raw}"
    );

    // a flow cell carries no halves — the keys exist only where a
    // division has to be re-derivable
    assert!(total[0].get("num").is_none_or(Value::is_null), "{:?}", total[0]);

    // the day drill anchors on the same judged axis (thirty event
    // days, three rows summing per day) and refuses the unjudged
    // frame with the same road out
    let days = grid(
        &session,
        "SELECT count(*) AS n, sum(value) AS v FROM metric_days('revenue');",
    )
    .await;
    assert!(days.contains("| 30 ") && days.contains("90.0"), "{days}");
    let e = session
        .execute("SELECT * FROM metric_days('raw');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no judged time column"), "{e}");
    let e = session
        .execute("SELECT * FROM metric_days('nope');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no current QUERY grounding"), "{e}");
}
