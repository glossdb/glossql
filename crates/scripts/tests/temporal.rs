//! The temporal reference script against a real DataFusion context — the
//! v0.3 semantics it ports (analysis/temporal/detection.py), scenario by
//! scenario: cadence from the median distinct-instant gap, calendar-bucket
//! completeness, significant gaps with severity, and the abstentions.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use glossql_glossary::FunctionRow;
use glossql_scripts::RhaiRuntime;
use glossql_session::{FunctionRuntime, SqlDoor};
use serde_json::{Value, json};

/// A door straight onto a SessionContext, blocking on its own runtime.
struct CtxDoor {
    ctx: SessionContext,
    rt: tokio::runtime::Runtime,
}

impl CtxDoor {
    fn new() -> Self {
        CtxDoor {
            ctx: SessionContext::new(),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    fn run(&self, sql: &str) {
        self.rt
            .block_on(async { self.ctx.sql(sql).await.unwrap().collect().await })
            .unwrap();
    }
}

impl SqlDoor for CtxDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        // The door contract: an empty result still ships one empty batch
        // carrying the schema, so LIMIT 0 types columns without scanning.
        self.rt
            .block_on(async {
                let df = self.ctx.sql(query).await?;
                let schema = Arc::new(df.schema().as_arrow().clone());
                let mut batches = df.collect().await?;
                if batches.is_empty() {
                    batches.push(RecordBatch::new_empty(schema));
                }
                Ok(batches)
            })
            .map_err(|e: datafusion::error::DataFusionError| e.to_string())
    }
}

fn temporal(door: CtxDoor, subject: &str) -> Value {
    let rt = RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"));
    let function = FunctionRow {
        name: "temporal".into(),
        scope_dataset: None,
        script: "functions/temporal.rhai".into(),
        accepts: vec![],
        returns: Some("temporal_profile".into()),
    };
    rt.invoke(&function, subject, &Value::Null, Arc::new(door))
        .unwrap()
}

#[test]
fn daily_with_a_hole_finds_cadence_completeness_and_the_gap() {
    let door = CtxDoor::new();
    // Jan 1–5 and Jan 11–15: day cadence with a six-day stretch between
    // observations, i.e. five missing days.
    door.run(
        "CREATE TABLE events AS SELECT * FROM (VALUES \
         (DATE '2024-01-01'), (DATE '2024-01-02'), (DATE '2024-01-03'), \
         (DATE '2024-01-04'), (DATE '2024-01-05'), (DATE '2024-01-11'), \
         (DATE '2024-01-12'), (DATE '2024-01-13'), (DATE '2024-01-14'), \
         (DATE '2024-01-15')) AS t(d)",
    );
    let out = temporal(door, "events.d");

    assert_eq!(out["applicable"], json!(true));
    assert_eq!(out["min"], json!("2024-01-01"));
    assert_eq!(out["max"], json!("2024-01-15"));
    assert_eq!(out["span_days"], json!(14.0));
    assert_eq!(out["granularity"], json!("day"));
    // variation = (518400 - 86400) / 86400 = 5 → 1.0 - 0.5 = the floor.
    assert_eq!(out["confidence"], json!(0.5));

    // Ten day-buckets present of the fifteen the window holds.
    assert_eq!(out["completeness"]["actual"], json!(10));
    assert_eq!(out["completeness"]["expected"], json!(15));
    let ratio = out["completeness"]["ratio"].as_f64().unwrap();
    assert!((ratio - 10.0 / 15.0).abs() < 1e-12, "{ratio}");

    // One stretch beyond twice the median: 6× → moderate, 5 missing days,
    // bounded by the observations around the hole.
    assert_eq!(out["gaps"]["count"], json!(1));
    assert_eq!(out["gaps"]["largest_days"], json!(6.0));
    let gap = &out["gaps"]["sample"][0];
    assert_eq!(gap["severity"], json!("moderate"));
    assert_eq!(gap["missing_periods"], json!(5));
    assert_eq!(gap["days"], json!(6.0));
    assert!(
        gap["start"].as_str().unwrap().starts_with("2024-01-05"),
        "{gap}"
    );
    assert!(
        gap["end"].as_str().unwrap().starts_with("2024-01-11"),
        "{gap}"
    );
}

#[test]
fn monthly_cadence_counts_calendar_buckets_across_the_year_boundary() {
    let door = CtxDoor::new();
    // Month starts across a year boundary, one duplicated instant — the
    // family reads DISTINCT instants, so the duplicate must not count.
    door.run(
        "CREATE TABLE events AS SELECT * FROM (VALUES \
         (DATE '2023-11-01'), (DATE '2023-12-01'), (DATE '2023-12-01'), \
         (DATE '2024-01-01'), (DATE '2024-02-01')) AS t(d)",
    );
    let out = temporal(door, "events.d");

    assert_eq!(out["granularity"], json!("month"));
    let confidence = out["confidence"].as_f64().unwrap();
    assert!(confidence > 0.99, "near-uniform month steps: {confidence}");
    // Calendar arithmetic, not nominal seconds: (2024-2023)*12 + (2-11) + 1.
    assert_eq!(out["completeness"]["expected"], json!(4));
    assert_eq!(out["completeness"]["actual"], json!(4));
    assert_eq!(out["completeness"]["ratio"], json!(1.0));
    assert_eq!(out["gaps"]["count"], json!(0));
}

#[test]
fn irregular_cadence_still_reports_gaps_but_no_completeness() {
    let door = CtxDoor::new();
    // Steps of 1, 10, and 45 days: median 10d matches no named grain, yet
    // the 45-day stretch is still 4.5× the median — a reportable gap.
    door.run(
        "CREATE TABLE events AS SELECT * FROM (VALUES \
         (DATE '2024-01-01'), (DATE '2024-01-02'), (DATE '2024-01-12'), \
         (DATE '2024-02-26')) AS t(d)",
    );
    let out = temporal(door, "events.d");

    assert_eq!(out["granularity"], json!("irregular"));
    assert_eq!(out["confidence"], json!(0.3));
    assert!(
        out.get("completeness").is_none(),
        "no grain, no buckets to count: {out}"
    );
    assert_eq!(out["gaps"]["count"], json!(1));
    let gap = &out["gaps"]["sample"][0];
    assert_eq!(gap["severity"], json!("minor"));
    assert_eq!(gap["missing_periods"], json!(3));
}

#[test]
fn a_single_instant_and_a_non_temporal_column_abstain_their_own_ways() {
    let door = CtxDoor::new();
    // One distinct instant, repeated: no gap exists, so cadence is unknown
    // — not zero, not stale, not complete.
    door.run(
        "CREATE TABLE events AS SELECT * FROM (VALUES \
         (DATE '2024-03-31'), (DATE '2024-03-31'), (DATE '2024-03-31')) AS t(d)",
    );
    let out = temporal(door, "events.d");
    assert_eq!(out["applicable"], json!(true));
    assert_eq!(out["granularity"], json!("unknown"));
    assert_eq!(out["confidence"], json!(0.0));
    assert_eq!(out["span_days"], json!(0.0));
    assert_eq!(out["gaps"]["count"], json!(0));
    assert!(out.get("completeness").is_none(), "{out}");

    // A column that is not a point in time abstains — and the reason
    // names the type, so a date landed as text reads as a typing gap
    // rather than a dead end (the SQLite run, 2026-08-07).
    let door = CtxDoor::new();
    door.run("CREATE TABLE events AS SELECT * FROM (VALUES (1.5), (2.5)) AS t(d)");
    let out = temporal(door, "events.d");
    assert_eq!(out["applicable"], json!(false));
    let reason = out["reason"].as_str().unwrap();
    assert!(
        reason.contains("Float64") && reason.contains("typing in the recipe"),
        "{reason}"
    );

    // An all-NULL temporal column abstains too — nothing bounds a window.
    let door = CtxDoor::new();
    door.run(
        "CREATE TABLE events AS SELECT CAST(NULL AS DATE) AS d FROM (VALUES (1), (2)) AS t(x)",
    );
    let out = temporal(door, "events.d");
    assert_eq!(out["applicable"], json!(false));
    assert_eq!(
        out["reason"],
        json!("no non-null values — nothing bounds a window")
    );
}

#[test]
fn timestamps_at_hour_grain_ride_the_fixed_grain_path() {
    let door = CtxDoor::new();
    door.run(
        "CREATE TABLE events AS SELECT * FROM (VALUES \
         (TIMESTAMP '2024-01-01 08:00:00'), (TIMESTAMP '2024-01-01 09:00:00'), \
         (TIMESTAMP '2024-01-01 10:00:00'), (TIMESTAMP '2024-01-01 12:00:00')) AS t(ts)",
    );
    let out = temporal(door, "events.ts");

    assert_eq!(out["granularity"], json!("hour"));
    // Four hour-buckets present of the five between 08:00 and 12:00.
    assert_eq!(out["completeness"]["expected"], json!(5));
    assert_eq!(out["completeness"]["actual"], json!(4));
    // A 2-hour step is exactly the 2× threshold, not beyond it — no gap.
    assert_eq!(out["gaps"]["count"], json!(0));
}
