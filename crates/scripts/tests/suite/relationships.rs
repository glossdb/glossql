//! Fixture 12's spine with the real detector: candidate → verified →
//! declared. `detect_relationships` measures at dataset grain and is
//! deliberately generous — the true edge arrives with its orphan
//! evidence, and a coincidental key/key overlap arrives beside it
//! (high recall is the contract; precision is the judge's). Declaring
//! the survivor lands in the `relationships` relation; the reject
//! stays visible in the measurement.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
const RELATIONSHIPS: &str = include_str!("../../functions/relationships.sql");

async fn write_table(root: &std::path::Path, name: &str, batch: RecordBatch) {
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .write_parquet(
            &root.join(name).display().to_string(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
}

async fn parquet_fixture(root: &std::path::Path) {
    // customers.id is a clean key; the names repeat so text stays out
    // of the key-like pool.
    let customers = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "customers",
        RecordBatch::try_new(
            customers,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["ann", "ann", "bob", "bob", "cat"])),
            ],
        )
        .unwrap(),
    )
    .await;
    // orders.customer_id is the true edge with one orphan (9);
    // orders.order_id is the decoy — a second unique integer sequence
    // that overlaps customers.id perfectly without meaning it.
    let orders = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("customer_id", DataType::Int64, true),
    ]));
    write_table(
        root,
        "orders",
        RecordBatch::try_new(
            orders,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![1, 2, 2, 3, 9])),
            ],
        )
        .unwrap(),
    )
    .await;
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

#[tokio::test(flavor = "multi_thread")]
async fn candidates_are_generous_and_declaration_records_the_survivor() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    parquet_fixture(&root).await;

    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake.clone()).await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(KernelRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'relationship judging');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT relationship_candidates WITH $${{\n\
               \"type\": \"object\",\n\
               \"properties\": {{\"candidates\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON DATASET;\n\
             DECLARE FUNCTION detect_relationships FOR GLOBAL \
             AS $${RELATIONSHIPS}$$ RETURNS relationship_candidates;\n\
             DECLARE RECIPE customers ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('customers/*.parquet')$$;\n\
             DECLARE RECIPE orders ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('orders/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();

    let landed = one(&session
        .execute("SELECT count(*) FROM imports;")
        .await
        .unwrap());
    assert_eq!(landed, "2", "both recipes landed and recorded");

    session
        .execute("SELECT detect_relationships() FROM fin;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(fin::relationship_candidates) WHERE state = 'current';",
        )
        .await
        .unwrap());

    // The true edge, with its evidence: 3 of 4 distinct customer ids
    // resolve, one orphan.
    assert!(value.contains(r#""from":"orders.customer_id""#), "{value}");
    assert!(value.contains(r#""to":"customers.id""#), "{value}");
    assert!(value.contains(r#""cardinality":"many-to-one""#), "{value}");
    assert!(value.contains(r#""overlap":0.75"#), "{value}");
    assert!(value.contains(r#""orphans":1"#), "{value}");
    // High recall keeps the coincidence: two parallel unique sequences
    // overlap perfectly. Removing it is the judge's job, not the
    // script's.
    assert!(value.contains(r#""from":"orders.order_id""#), "{value}");
    assert!(value.contains(r#""cardinality":"one-to-one""#), "{value}");

    // The ranking is the read order — orphan evidence first, so the
    // true edge leads and the too-clean decoy follows — and the
    // summary rides the body for extraction to serve, the full list
    // reading back whole.
    let body: serde_json::Value = serde_json::from_str(&value).unwrap();
    assert_eq!(body["candidates"][0]["from"], "orders.customer_id", "{value}");
    assert_eq!(body["summary"]["candidates"], 4, "{value}");
    assert_eq!(
        body["summary"]["top"][0]["from"], "orders.customer_id",
        "{value}"
    );

    // The judge declares the survivor; the declaration reads back from
    // the relationships relation.
    session
        .execute("DECLARE RELATIONSHIP orders.customer_id -> customers.id;")
        .await
        .unwrap();
    let declared = one(&session
        .execute("SELECT count(*) FROM relationships;")
        .await
        .unwrap());
    assert_eq!(declared, "1");
    let right = one(&session
        .execute("SELECT right_path FROM relationships;")
        .await
        .unwrap());
    assert_eq!(right, "customers.id");

    // The declaration moved the pin, but a declared edge is not an
    // input of this door — candidates come from the data alone — so
    // the measurement still stands (the currency rule: what it READ),
    // and it still carries the reject: not declared, and not erased.
    let standing = one(&session
        .execute(
            "SELECT count(*) FROM GLOSSARY(fin::relationship_candidates) \
             WHERE state = 'current';",
        )
        .await
        .unwrap());
    assert_eq!(standing, "1", "declaring an edge is not this door's staleness");
    session
        .execute("SELECT detect_relationships() FROM fin;")
        .await
        .unwrap();
    let after = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(fin::relationship_candidates) WHERE state = 'current';",
        )
        .await
        .unwrap());
    assert!(after.contains(r#""from":"orders.order_id""#), "{after}");
}

/// The multi-tenant fixture: party names repeat across businesses, so
/// `name` is no key alone — only (businessID, name) identifies a row.
/// booksql's shape: every FK is (businessID, X) -> target(businessID, Y).
async fn tenant_fixture(root: &std::path::Path) {
    let parties = Arc::new(Schema::new(vec![
        Field::new("business_id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "parties",
        RecordBatch::try_new(
            parties,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2])),
                Arc::new(StringArray::from(vec!["ann", "bob", "cat", "ann", "bob"])),
            ],
        )
        .unwrap(),
    )
    .await;
    let txns = Arc::new(Schema::new(vec![
        Field::new("business_id", DataType::Int64, true),
        Field::new("party", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "txns",
        RecordBatch::try_new(
            txns,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2])),
                Arc::new(StringArray::from(vec!["ann", "ann", "bob", "ann", "bob"])),
            ],
        )
        .unwrap(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_key_is_rescued_as_a_composite_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    tenant_fixture(&root).await;

    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake.clone()).await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(KernelRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'composite rescue');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT relationship_candidates WITH $${{\n\
               \"type\": \"object\",\n\
               \"properties\": {{\"candidates\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON DATASET;\n\
             DECLARE FUNCTION detect_relationships FOR GLOBAL \
             AS $${RELATIONSHIPS}$$ RETURNS relationship_candidates;\n\
             DECLARE RECIPE parties ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('parties/*.parquet')$$;\n\
             DECLARE RECIPE txns ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('txns/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();

    session
        .execute("SELECT detect_relationships() FROM fin;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(fin::relationship_candidates) WHERE state = 'current';",
        )
        .await
        .unwrap());

    // No column is a key alone here — the composite pass is the only
    // producer: the anchor pair plus the scoping leg, data-decided.
    assert!(value.contains(r#""from":"txns.party""#), "{value}");
    assert!(value.contains(r#""to":"parties.name""#), "{value}");
    assert!(
        value.contains(r#""key_columns":[{"from":"txns.business_id","to":"parties.business_id"}]"#),
        "{value}"
    );
    assert!(value.contains(r#""cardinality":"many-to-one""#), "{value}");
    // The reverse direction is refused by the data: (party, business_id)
    // does not identify a txn row.
    assert!(!value.contains(r#""from":"parties."#), "{value}");

    // The ruling (fixture 14): the tuple is the key — the
    // survivor declares directly, no derived-column cure. The declaration
    // reads back, and the grounds glossed on the pair path surface in the
    // anchor table's sweep.
    session
        .execute(
            "DECLARE RELATIONSHIP txns.(business_id, party) -> parties.(business_id, name);\n\
             DECLARE ASPECT meaning WITH $${\"type\": \"object\", \
             \"properties\": {\"value\": {\"type\": \"string\"}}}$$ AS FACT ON RELATIONSHIP;\n\
             GLOSS meaning ON txns.(business_id, party) -> parties.(business_id, name) AS \
             $${\"value\": \"party names repeat across businesses; the scope leg carries the tenant\"}$$;",
        )
        .await
        .unwrap();
    let right = one(&session
        .execute("SELECT right_path FROM relationships;")
        .await
        .unwrap());
    assert_eq!(right, "parties.(business_id, name)");
    let swept = one(&session
        .execute(
            "SELECT subject FROM GLOSSARY(txns) \
                 WHERE aspect = 'meaning' AND state = 'current';",
        )
        .await
        .unwrap());
    assert_eq!(
        swept,
        "txns.(business_id, party) -> parties.(business_id, name)"
    );
}
