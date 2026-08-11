//! The grounding-collision guard: two concepts grounding to the same
//! extract make every ratio between them compute 1.0, silently. The
//! measurement buckets current groundings by canonical SQL and reports
//! shared buckets; judging synonym-vs-error stays with the agent. No
//! lake — groundings are dataset glosses, the store alone carries them.

use std::sync::Arc;

use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
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
    .with_runtime(Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"))))
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
DECLARE FUNCTION detect_grounding_collisions FOR fin FROM 'functions/grounding_collisions.rhai'
  ACCEPTS (glossary)
  RETURNS grounding_collisions;
DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
DECLARE ASPECT turnover WITH $${"title": "Turnover"}$$ AS QUERY ON DATASET;
DECLARE ASPECT costs WITH $${"title": "Costs"}$$ AS QUERY ON DATASET;
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concepts_sharing_an_extract_collide_and_spelling_does_not_hide_it() {
    let store = Store::open_memory().await.unwrap();
    let s = session(&store);
    for stmt in SETUP.split(';').filter(|s| !s.trim().is_empty()) {
        s.execute(&format!("{stmt};")).await.unwrap();
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
