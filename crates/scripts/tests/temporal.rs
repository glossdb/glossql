//! The temporal reference measurement against a real session — the v0.3
//! semantics it ports (analysis/temporal/detection.py), scenario by
//! scenario: cadence from the median distinct-instant gap, calendar-bucket
//! completeness, significant gaps with severity, and the abstentions.
//! Since stage 5 the body is SQL and the engine is the runtime, so each
//! scenario runs the whole extraction path: declare, extract, read back.

use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Outcome, Session};
use serde_json::{Value, json};

/// One scenario: a lake of its own, `events` built from the VALUES
/// clause, the shipped body declared, one extraction — the landed body.
async fn temporal(values_sql: &str, subject: &str) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
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
        .execute("DECLARE DATASET fin SET (purpose: 'temporal scenarios'); USE fin;")
        .await
        .unwrap();

    let ctx = SessionContext::new();
    let df = ctx.sql(values_sql).await.unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.unwrap();
    session
        .register_table(
            "events",
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
        )
        .await
        .unwrap();

    let declarations = glossql_scripts::library::splice(
        r#"DECLARE ASPECT temporal_profile WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT;
           DECLARE FUNCTION temporal FOR GLOBAL AS $$temporal.sql$$
             RETURNS temporal_profile;"#,
    )
    .expect("shipped body splices");
    session.execute(&declarations).await.unwrap();

    let outcomes = session
        .execute(&format!("SELECT temporal() FROM {subject};"))
        .await
        .unwrap();
    let Some(Outcome::Rows(batches)) = outcomes.last() else {
        panic!("extraction serves rows")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let body = batch
        .column(batch.schema().index_of("body").unwrap())
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    serde_json::from_str(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn daily_with_a_hole_finds_cadence_completeness_and_the_gap() {
    // Jan 1–5 and Jan 11–15: day cadence with a six-day stretch between
    // observations, i.e. five missing days.
    let out = temporal(
        "SELECT * FROM (VALUES \
         (DATE '2024-01-01'), (DATE '2024-01-02'), (DATE '2024-01-03'), \
         (DATE '2024-01-04'), (DATE '2024-01-05'), (DATE '2024-01-11'), \
         (DATE '2024-01-12'), (DATE '2024-01-13'), (DATE '2024-01-14'), \
         (DATE '2024-01-15')) AS t(d)",
        "events.d",
    )
    .await;

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

#[tokio::test(flavor = "multi_thread")]
async fn monthly_cadence_counts_calendar_buckets_across_the_year_boundary() {
    // Month starts across a year boundary, one duplicated instant — the
    // family reads DISTINCT instants, so the duplicate must not count.
    let out = temporal(
        "SELECT * FROM (VALUES \
         (DATE '2023-11-01'), (DATE '2023-12-01'), (DATE '2023-12-01'), \
         (DATE '2024-01-01'), (DATE '2024-02-01')) AS t(d)",
        "events.d",
    )
    .await;

    assert_eq!(out["granularity"], json!("month"));
    let confidence = out["confidence"].as_f64().unwrap();
    assert!(confidence > 0.99, "near-uniform month steps: {confidence}");
    // Calendar arithmetic, not nominal seconds: (2024-2023)*12 + (2-11) + 1.
    assert_eq!(out["completeness"]["expected"], json!(4));
    assert_eq!(out["completeness"]["actual"], json!(4));
    assert_eq!(out["completeness"]["ratio"], json!(1.0));
    assert_eq!(out["gaps"]["count"], json!(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn irregular_cadence_still_reports_gaps_but_no_completeness() {
    // Steps of 1, 10, and 45 days: median 10d matches no named grain, yet
    // the 45-day stretch is still 4.5× the median — a reportable gap.
    let out = temporal(
        "SELECT * FROM (VALUES \
         (DATE '2024-01-01'), (DATE '2024-01-02'), (DATE '2024-01-12'), \
         (DATE '2024-02-26')) AS t(d)",
        "events.d",
    )
    .await;

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

#[tokio::test(flavor = "multi_thread")]
async fn a_single_instant_and_a_non_temporal_column_abstain_their_own_ways() {
    // One distinct instant, repeated: no gap exists, so cadence is unknown
    // — not zero, not stale, not complete.
    let out = temporal(
        "SELECT * FROM (VALUES \
         (DATE '2024-03-31'), (DATE '2024-03-31'), (DATE '2024-03-31')) AS t(d)",
        "events.d",
    )
    .await;
    assert_eq!(out["applicable"], json!(true));
    assert_eq!(out["granularity"], json!("unknown"));
    assert_eq!(out["confidence"], json!(0.0));
    assert_eq!(out["span_days"], json!(0.0));
    assert_eq!(out["gaps"]["count"], json!(0));
    assert!(out.get("completeness").is_none(), "{out}");

    // A column that is not a point in time abstains — and the reason
    // names the type, so a date landed as text reads as a typing gap
    // rather than a dead end.
    let out = temporal("SELECT * FROM (VALUES (1.5), (2.5)) AS t(d)", "events.d").await;
    assert_eq!(out["applicable"], json!(false));
    let reason = out["reason"].as_str().unwrap();
    assert!(
        reason.contains("Float64") && reason.contains("typing in the recipe"),
        "{reason}"
    );

    // An all-NULL temporal column abstains too — nothing bounds a window.
    let out = temporal(
        "SELECT CAST(NULL AS DATE) AS d FROM (VALUES (1), (2)) AS t(x)",
        "events.d",
    )
    .await;
    assert_eq!(out["applicable"], json!(false));
    assert_eq!(
        out["reason"],
        json!("no non-null values — nothing bounds a window")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamps_at_hour_grain_ride_the_fixed_grain_path() {
    let out = temporal(
        "SELECT * FROM (VALUES \
         (TIMESTAMP '2024-01-01 08:00:00'), (TIMESTAMP '2024-01-01 09:00:00'), \
         (TIMESTAMP '2024-01-01 10:00:00'), (TIMESTAMP '2024-01-01 12:00:00')) AS t(ts)",
        "events.ts",
    )
    .await;

    assert_eq!(out["granularity"], json!("hour"));
    // Four hour-buckets present of the five between 08:00 and 12:00.
    assert_eq!(out["completeness"]["expected"], json!(5));
    assert_eq!(out["completeness"]["actual"], json!(4));
    // A 2-hour step is exactly the 2× threshold, not beyond it — no gap.
    assert_eq!(out["gaps"]["count"], json!(0));
}
