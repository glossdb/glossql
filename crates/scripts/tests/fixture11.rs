//! The acceptance test: fixture 11 end-to-end with real scripts, under the
//! authored-typing ruling (2026-08-04). The agent probes the source through
//! the statement door; the recipe carries the casts and the column choices;
//! the landed table IS the typed table. The measurement plane runs on it
//! (profile, outliers chained through ACCEPTS); semantic glosses and the
//! detector adjudicate as before; the collapsed read discloses state.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

async fn parquet_fixture(root: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("order_date", DataType::Utf8, true),
        Field::new("legacy_code", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "12.50", "8.00", "99.90", "7.25", "n/a",
            ])),
            Arc::new(StringArray::from(vec![
                "15.01.2024",
                "16.01.2024",
                "17.01.2024",
                "18.01.2024",
                "19.01.2024",
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; 5])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .write_parquet(
            &root.join("orders").display().to_string(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
}

fn runtime() -> Arc<RhaiRuntime> {
    Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR")))
}

fn session_for(kind: ActorKind, id: &str, store: &Store) -> Session {
    Session::new(
        store.clone(),
        Actor {
            kind,
            id: id.into(),
        },
    )
    .unwrap()
    
    .with_runtime(runtime())
}

fn one(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1, "expected one row");
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// The corpus block verbatim (fixture 11): shape-descriptive schemas on the
// aspects — the one live contract — RETURNS as aspect references, and the
// only witness the one with judgment in it. `slot_entropy` has no RETURNS:
// that shape *is* the detector role.
const DECLARATIONS: &str = r#"
DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow"]}}
}$$ AS FACT;
DECLARE ASPECT column_profile WITH $${
  "type": "object",
  "required": ["total", "null_ratio", "distinct"],
  "properties": {
    "total": {"type": "integer"}, "null_ratio": {"type": "number"},
    "distinct": {"type": "integer"}, "cardinality_ratio": {"type": "number"},
    "min": {}, "max": {}, "top_values": {"type": "array"},
    "lengths": {"type": "object"},
    "numeric": {
      "type": "object",
      "required": ["mean", "mad", "percentiles"],
      "properties": {"mean": {}, "stddev": {}, "mad": {},
                     "percentiles": {"type": "object"}}
    }
  }
}$$ AS MEASUREMENT;
DECLARE ASPECT outlier_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "iqr": {"type": "object"}, "zscore": {"type": "object"}}
}$$ AS MEASUREMENT;
DECLARE ASPECT temporal_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "granularity": {"type": "string"},
                 "confidence": {"type": "number"},
                 "completeness": {"type": "object"}, "gaps": {"type": "object"}}
}$$ AS MEASUREMENT;

DECLARE FUNCTION profile FOR GLOBAL AS $$profile.sql$$
  RETURNS column_profile;
DECLARE FUNCTION outliers FOR GLOBAL AS $$outliers.sql$$
  RETURNS outlier_profile;
DECLARE FUNCTION temporal FOR GLOBAL AS $$temporal.rhai$$
  RETURNS temporal_profile;
DECLARE FUNCTION slot_entropy FOR GLOBAL AS $$slot_entropy.rhai$$;

DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN)
  DETECTOR slot_entropy THRESHOLD 0.7;
"#;

#[tokio::test(flavor = "multi_thread")]
async fn fixture_11_with_real_scripts() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;

    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open_scratch(lake.clone()).await.unwrap();
    let agent = session_for(ActorKind::Agent, "agent-1", &store);

    agent
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'working-capital analysis');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');",
            erp_root.display()
        ))
        .await
        .unwrap();
    let declarations =
        glossql_scripts::library::splice(DECLARATIONS).expect("shipped scripts splice");
    agent.execute(&declarations).await.unwrap();

    // The probe: the agent looks at the source before writing the recipe —
    // trial casts through the same statement door, landing nothing.
    let parsed = agent
        .execute(
            "PROBE erp_export AS $$SELECT count(try_cast(\"amount\" AS DOUBLE)) \
             FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(one(&parsed), "4", "one amount does not read as a number");
    let dates = agent
        .execute(
            "PROBE erp_export AS $$SELECT count(try_to_date(\"order_date\", '%d.%m.%Y')) \
             FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(one(&dates), "5", "the EU date pattern parses every row");
    let legacy = agent
        .execute(
            "PROBE erp_export AS $$SELECT count(\"legacy_code\") \
             FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(one(&legacy), "0", "all null — the author will leave it out");

    // LIMIT 0 rehearses the identity: the probe's empty result carries
    // exactly the schema the recipe will land.
    let shape = agent
        .execute(
            "PROBE erp_export AS $$\
               SELECT order_id, \
                      try_cast(amount AS DOUBLE) AS amount, \
                      try_to_date(order_date, '%d.%m.%Y') AS order_date \
               FROM read_parquet('orders/*.parquet') LIMIT 0$$;",
        )
        .await
        .unwrap();
    let Outcome::Rows(batches) = shape.last().unwrap() else {
        panic!("expected Rows");
    };
    let schema = batches[0].schema();
    assert_eq!(schema.field(1).data_type().to_string(), "Float64");
    assert_eq!(schema.field(2).data_type().to_string(), "Date32");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

    // Typing is authored: the recipe carries the casts, keeps every row
    // (a bad amount is a NULL cell), and leaves the all-null column out.
    agent
        .execute(
            "DECLARE RECIPE orders ON fin FROM erp_export AS $$\
               SELECT order_id, \
                      try_cast(amount AS DOUBLE) AS amount, \
                      try_to_date(order_date, '%d.%m.%Y') AS order_date \
               FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();

    // The landed table is the typed table.
    let total = agent
        .execute("SELECT sum(amount) FROM orders;")
        .await
        .unwrap();
    assert_eq!(one(&total), "127.65");
    let earliest = agent
        .execute("SELECT min(order_date) FROM orders;")
        .await
        .unwrap();
    assert_eq!(
        one(&earliest),
        "2024-01-15",
        "the recipe's expr parsed EU dates"
    );
    let gone = agent
        .execute("SELECT legacy_code FROM orders;")
        .await
        .unwrap_err();
    assert!(gone.to_string().contains("legacy_code"), "{gone}");
    let dropped = agent
        .execute("SELECT dropped_rows_count FROM imports WHERE table_name = 'orders';")
        .await
        .unwrap();
    assert_eq!(one(&dropped), "0", "this author kept every row");

    // Fixture 11's "outliers before profile" sequence, after §7e: the
    // SQL body composes its dependency inline — the same `profile`
    // aggregate the profile measurement lands — so the first ask
    // computes and lands. The missing_aspects abstention this sequence
    // used to produce is gone with the stored intermediate it named.
    let early = agent
        .execute("SELECT outliers() FROM orders.amount;")
        .await
        .unwrap();
    let Some(Outcome::Rows(batches)) = early.last() else {
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
    assert!(body.contains("\"applicable\":true"), "{body}");

    // The profile measurement still lands its own full body, and the
    // outlier read serves the landed row at the pin.
    agent
        .execute("SELECT profile() FROM orders.amount;")
        .await
        .unwrap();
    let outlier = agent
        .execute("SELECT value FROM GLOSSARY(orders.amount::outlier_profile);")
        .await
        .unwrap();
    assert!(
        one(&outlier).contains("\"applicable\":true"),
        "{}",
        one(&outlier)
    );
    assert!(
        one(&outlier).contains("\"count\":1"),
        "99.90 is beyond both fences: {}",
        one(&outlier)
    );

    // The temporal family is one ordinary function on the landed column:
    // five consecutive days read as day cadence, complete and gapless.
    agent
        .execute("SELECT temporal() FROM orders.order_date;")
        .await
        .unwrap();
    let tprof = agent
        .execute("SELECT value FROM GLOSSARY(orders.order_date::temporal_profile);")
        .await
        .unwrap();
    let v = one(&tprof);
    assert!(v.contains("\"granularity\":\"day\""), "{v}");
    assert!(v.contains("\"ratio\":1.0"), "{v}");
    assert!(v.contains("\"gaps\":{\"count\":0}"), "{v}");
    // A numeric column abstains instead of erroring.
    agent
        .execute("SELECT temporal() FROM orders.amount;")
        .await
        .unwrap();
    let na = agent
        .execute("SELECT value FROM GLOSSARY(orders.amount::temporal_profile);")
        .await
        .unwrap();
    assert!(one(&na).contains("\"applicable\":false"), "{}", one(&na));

    // Same pin, same answer: a repeated extraction serves the landed
    // measurement — nothing recomputes, nothing sweeps. Any input moving
    // makes a new pin, and that is the whole invalidation story.
    agent
        .execute("SELECT profile() FROM orders.amount;")
        .await
        .unwrap();
    let landed = agent
        .execute(
            "SELECT count(*) FROM measurements \
             WHERE function = 'profile' AND subject = 'orders.amount';",
        )
        .await
        .unwrap();
    assert_eq!(one(&landed), "1", "a hit at the pin lands nothing new");

    // Semantic glosses and the detector at read: one slot green, a human
    // disagreement turns it red and the collapsed value withholds.
    agent
        .execute(r#"GLOSS behavior ON orders.amount AS $${"value": "flow"}$$;"#)
        .await
        .unwrap();
    let band = agent
        .execute("SELECT band FROM ATTEST(orders.amount::behavior);")
        .await
        .unwrap();
    assert_eq!(one(&band), "green");

    let human = session_for(ActorKind::Human, "philipp", &store);
    human.execute("USE fin;").await.unwrap();
    human
        .execute(r#"GLOSS behavior ON orders.amount AS $${"value": "stock"}$$;"#)
        .await
        .unwrap();
    let band = human
        .execute("SELECT band, score FROM ATTEST(orders.amount::behavior);")
        .await
        .unwrap();
    assert_eq!(one(&band), "red");
    let contested = human
        .execute("SELECT value, state FROM GLOSSARY(orders.amount::behavior);")
        .await
        .unwrap();
    let row = match contested.last().unwrap() {
        Outcome::Rows(batches) => {
            let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            (
                b.column(0).is_null(0),
                datafusion::arrow::util::display::array_value_to_string(b.column(1), 0).unwrap(),
            )
        }
        other => panic!("expected Rows, got {other:?}"),
    };
    assert!(row.0, "a contested value is withheld");
    assert_eq!(row.1, "contested");

    // Disclosure: witnessed aspects nobody spoke to appear as rows.
    let unassessed = human
        .execute("SELECT count(*) FROM GLOSSARY(orders) WHERE state = 'unassessed';")
        .await
        .unwrap();
    assert_ne!(
        one(&unassessed),
        "0",
        "absence is a visible row, not an omission"
    );

    // A typing correction is a recipe correction — supersede-and-reland
    // (ruled 2026-08-06): the changed recipe drops the old landing and
    // lands fresh. DROP TABLE stays refused while the table holds data.
    let outcomes = human
        .execute(
            "DECLARE RECIPE orders ON fin FROM erp_export AS $$SELECT order_id FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert!(
        format!("{outcomes:?}").contains("superseded and re-landed"),
        "{outcomes:?}"
    );
    let err = human.execute("DROP TABLE orders;").await.unwrap_err();
    assert!(err.to_string().contains("holds data"), "{err}");
}
