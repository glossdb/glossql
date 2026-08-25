//! The grounding-collision guard: two concepts grounding to the same
//! extract make every ratio between them compute 1.0, silently. The
//! measurement buckets current groundings by canonical SQL and reports
//! shared buckets; judging synonym-vs-error stays with the agent. No
//! lake — groundings are dataset glosses, the store alone carries them.

use std::sync::Arc;

use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_session::{Outcome, Session};

fn session(store: &Store) -> Session {
    Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(KernelRuntime::new(env!("CARGO_MANIFEST_DIR"))))
}

fn one(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let batch = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

const SETUP: &str = r#"
DECLARE DATASET fin SET (purpose: 'test');
USE fin;
DECLARE ASPECT grounding_collisions WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "groundings": {"type": "integer"},
                 "collisions": {"type": "array"}}
}$$ AS MEASUREMENT ON DATASET;
DECLARE FUNCTION detect_grounding_collisions FOR fin AS $$grounding_collisions.sql$$
  RETURNS grounding_collisions;
DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
DECLARE ASPECT turnover WITH $${"title": "Turnover"}$$ AS QUERY ON DATASET;
DECLARE ASPECT costs WITH $${"title": "Costs"}$$ AS QUERY ON DATASET;
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concepts_sharing_an_extract_collide_and_spelling_does_not_hide_it() {
    let (_dir, store) = scratch_store().await;
    let s = session(&store);
    for stmt in SETUP.split(';').filter(|s| !s.trim().is_empty()) {
        // The marker survives the split (a body might not — SQL can carry
        // semicolons in prose), so the shipped text goes in afterwards, by the
        // same substitution the door makes at boot.
        let stmt = glossql_scripts::library::splice(&format!("{stmt};")).expect("shipped");
        s.execute(&stmt).await.unwrap();
    }
    // Two concepts, one extract — spelled differently: extra whitespace
    // and keyword case collapse under canonicalization. The third is its
    // own extract.
    s.execute(
        r#"GLOSS revenue ON fin AS $${"sql": "SELECT l.credit - l.debit AS value FROM journal_lines l WHERE l.kind = 'rev'"}$$;"#,
    )
    .await
    .unwrap();
    s.execute(
        r#"GLOSS turnover ON fin AS $${"sql": "select   l.credit - l.debit AS value from journal_lines l where l.kind = 'rev'"}$$;"#,
    )
    .await
    .unwrap();
    s.execute(
        r#"GLOSS costs ON fin AS $${"sql": "SELECT l.debit - l.credit AS value FROM journal_lines l WHERE l.kind = 'cost'"}$$;"#,
    )
    .await
    .unwrap();

    s.execute("SELECT detect_grounding_collisions() FROM fin;")
        .await
        .unwrap();
    let value = s
        .execute("SELECT value FROM GLOSSARY(fin::grounding_collisions);")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&one(&value)).unwrap();
    assert_eq!(v["applicable"], true);
    assert_eq!(v["groundings"], 3);
    assert_eq!(v["collisions"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(
        v["collisions"][0]["aspects"],
        serde_json::json!(["revenue", "turnover"])
    );

    // A gloss write stales the measurement through the `glossary` edge:
    // turnover re-grounds to its own extract, the re-read recomputes, and
    // the collision is gone — no stale bucket served from cache.
    s.execute(
        r#"GLOSS turnover ON fin AS $${"sql": "SELECT l.credit AS value FROM journal_lines l WHERE l.kind = 'rev'"}$$;"#,
    )
    .await
    .unwrap();
    s.execute("SELECT detect_grounding_collisions() FROM fin;")
        .await
        .unwrap();
    let value = s
        .execute("SELECT value FROM GLOSSARY(fin::grounding_collisions);")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&one(&value)).unwrap();
    assert_eq!(v["collisions"].as_array().unwrap().len(), 0, "{v}");
    assert_eq!(v["groundings"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_served_series_collide_even_when_the_sql_differs() {
    // The miss this guards: revenue and ar_open_items served the same
    // monthly totals in every month from different SQL, and the
    // canonical buckets saw nothing. The series pass fingerprints the
    // served numbers; the third grounding's filtered series stays in
    // its own bucket.
    use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    let (_dir, store) = scratch_store().await;
    let s = session(&store);
    for stmt in SETUP.split(';').filter(|s| !s.trim().is_empty()) {
        let stmt = glossql_scripts::library::splice(&format!("{stmt};")).expect("shipped");
        s.execute(&stmt).await.unwrap();
    }
    s.execute(r#"DECLARE ASPECT open_items WITH $${"title": "Open items"}$$ AS QUERY ON DATASET;"#)
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![19737, 19742, 19763, 19787])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 400.0])),
        ],
    )
    .unwrap();
    s.register_table(
        "lines",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .await
    .unwrap();

    s.execute(r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#)
        .await
        .unwrap();
    s.execute(
        r#"GLOSS open_items ON fin AS $${"sql": "SELECT date, value FROM lines WHERE value IS NOT NULL"}$$;"#,
    )
    .await
    .unwrap();
    s.execute(
        r#"GLOSS costs ON fin AS $${"sql": "SELECT date, value FROM lines WHERE value > 250"}$$;"#,
    )
    .await
    .unwrap();

    s.execute("SELECT detect_grounding_collisions() FROM fin;")
        .await
        .unwrap();
    let value = s
        .execute("SELECT value FROM GLOSSARY(fin::grounding_collisions);")
        .await
        .unwrap();
    let out: serde_json::Value = serde_json::from_str(&one(&value)).unwrap();

    assert_eq!(out["applicable"], serde_json::json!(true), "{out}");
    let collisions = out["collisions"].as_array().unwrap();
    assert_eq!(collisions.len(), 1, "{out}");
    assert_eq!(collisions[0]["kind"], "served_series", "{out}");
    assert_eq!(
        collisions[0]["aspects"],
        serde_json::json!(["open_items", "revenue"]),
        "{out}"
    );
    assert_eq!(collisions[0]["months"], serde_json::json!(3), "{out}");
}

/// A store over its own throwaway lake; hold the dir for the test's life.
async fn scratch_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    (dir, store)
}
