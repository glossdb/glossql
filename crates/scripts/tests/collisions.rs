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
DECLARE FUNCTION detect_grounding_collisions FOR fin AS $$grounding_collisions.rhai$$
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
        // The marker survives the split (a body would not — rhai is full
        // of semicolons), so the shipped text goes in afterwards, by the
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

/// Real execution for the served-series pass: routes the glossary read
/// to canned slots and everything else to DataFusion.
struct SeriesDoor {
    rt: tokio::runtime::Runtime,
    ctx: datafusion::prelude::SessionContext,
}

impl glossql_session::SqlDoor for SeriesDoor {
    fn sql(
        &self,
        query: &str,
    ) -> Result<Vec<datafusion::arrow::array::RecordBatch>, String> {
        use datafusion::arrow::array::{RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        if query.contains("FROM glossary") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("subject", DataType::Utf8, false),
                Field::new("aspect", DataType::Utf8, false),
                Field::new("actor_kind", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
            ]));
            let bodies = [
                r#"{"sql": "SELECT date, value FROM lines"}"#,
                r#"{"sql": "SELECT date, value FROM lines WHERE value IS NOT NULL"}"#,
                r#"{"sql": "SELECT date, value FROM lines WHERE value > 250"}"#,
            ];
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["fin", "fin", "fin"])),
                        Arc::new(StringArray::from(vec![
                            "revenue",
                            "open_items",
                            "costs",
                        ])),
                        Arc::new(StringArray::from(vec!["agent", "agent", "agent"])),
                        Arc::new(StringArray::from(bodies.to_vec())),
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
fn identical_served_series_collide_even_when_the_sql_differs() {
    // The 2026-08-14 miss: revenue and ar_open_items served the same
    // monthly totals in every month from different SQL, and the
    // canonical buckets saw nothing. The series pass fingerprints the
    // served numbers; the third grounding's filtered series stays in
    // its own bucket.
    use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![19737, 19742, 19763, 19787])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 400.0])),
        ],
    )
    .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_batch("lines", batch).unwrap();

    let runtime = RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"));
    let out = glossql_session::FunctionRuntime::invoke(
        &runtime,
        &glossql_glossary::FunctionRow {
            name: "detect_grounding_collisions".into(),
            scope_dataset: None,
            script: glossql_scripts::library::script("grounding_collisions.rhai")
                .expect("shipped")
                .into(),
            accepts: vec![],
            returns: None,
        },
        "fin",
        &serde_json::json!({}),
        Arc::new(SeriesDoor { rt, ctx }),
    )
    .unwrap();

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
