//! Fixture 12's spine with the real detector: candidate → verified →
//! declared. `detect_relationships` measures at dataset grain and is
//! deliberately generous — the true edge arrives with its orphan
//! evidence, and a coincidental key/key overlap arrives beside it
//! (high recall is the contract; precision is the judge's). Declaring
//! the survivor lands in the `relationships` relation; the reject
//! stays visible in the measurement.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Int64Array, RecordBatch, StringArray};
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

    // The ranking is the read order — a reference repeats, so the
    // true edge leads and the one-to-one decoy follows — and the
    // summary rides the body for extraction to serve, the full list
    // reading back whole.
    let body: serde_json::Value = serde_json::from_str(&value).unwrap();
    assert_eq!(
        body["candidates"][0]["from"], "orders.customer_id",
        "{value}"
    );
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
    assert_eq!(
        standing, "1",
        "declaring an edge is not this door's staleness"
    );
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

/// `SELECT detect_relationships() FROM fin` names its dataset in the
/// FROM: a session that has `USE`d nothing — a door's first call —
/// measures `fin`, not the empty binding, and lands what it counted.
#[tokio::test(flavor = "multi_thread")]
async fn the_detector_reads_the_named_dataset_before_any_use() {
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
    let agent = |id: &str| {
        Session::new(
            store.clone(),
            Actor {
                kind: ActorKind::Agent,
                id: id.into(),
            },
        )
        .unwrap()
        .with_runtime(Arc::new(KernelRuntime::new(env!("CARGO_MANIFEST_DIR"))))
    };
    let landing = agent("agent-1");
    landing
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

    let unbound = agent("agent-2");
    assert_eq!(unbound.dataset(), None);
    unbound
        .execute("SELECT detect_relationships() FROM fin;")
        .await
        .unwrap();
    let value = one(&landing
        .execute(
            "SELECT value FROM GLOSSARY(fin::relationship_candidates) WHERE state = 'current';",
        )
        .await
        .unwrap());
    let body: serde_json::Value = serde_json::from_str(&value).unwrap();
    assert_eq!(body["summary"]["candidates"], 4, "{value}");
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

/// Lands each table as its own recipe on a fresh workspace, runs
/// `detect_relationships`, and returns the body the door served.
async fn ranked(tables: Vec<(&str, RecordBatch)>) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    let mut recipes = String::new();
    for (name, batch) in tables {
        write_table(&root, name, batch).await;
        recipes.push_str(&format!(
            "DECLARE RECIPE {name} ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('{name}/*.parquet')$$;\n"
        ));
    }
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
             {recipes}",
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
    serde_json::from_str(&value).unwrap()
}

fn ints(name: &str, values: Vec<i64>) -> (Arc<Schema>, Arc<Int64Array>) {
    (
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)])),
        Arc::new(Int64Array::from(values)),
    )
}

fn edge(body: &serde_json::Value, i: usize) -> (String, String) {
    let c = &body["candidates"][i];
    (
        c["from"].as_str().unwrap().to_string(),
        c["to"].as_str().unwrap().to_string(),
    )
}

/// A small dense integer column — a priority of 1, 2, 3 — is contained
/// in every id range at overlap 1.0 and reaches almost none of it. On
/// equal overlap, key coverage puts the reference first; the decoy
/// stays in the list, and an edge with orphans ranks below it, for the
/// judge to settle against the data.
#[tokio::test(flavor = "multi_thread")]
async fn a_dense_code_inside_an_id_range_ranks_below_the_reference() {
    let (cs, cid) = ints("id", (1..=20).collect());
    let customers = RecordBatch::try_new(cs, vec![cid]).unwrap();
    let orders = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("priority", DataType::Int64, true),
    ]));
    let mut customer_id: Vec<i64> = (1..=15).collect();
    customer_id.extend([3, 7, 11, 11, 12]);
    let priority: Vec<i64> = (0..20).map(|i| i % 3 + 1).collect();
    let orders = RecordBatch::try_new(
        orders,
        vec![
            Arc::new(Int64Array::from(customer_id)),
            Arc::new(Int64Array::from(priority)),
        ],
    )
    .unwrap();
    let body = ranked(vec![("customers", customers), ("orders", orders)]).await;

    assert_eq!(body["summary"]["candidates"], 2, "{body}");
    assert_eq!(
        edge(&body, 0),
        ("orders.customer_id".into(), "customers.id".into()),
        "{body}"
    );
    assert_eq!(
        edge(&body, 1),
        ("orders.priority".into(), "customers.id".into()),
        "{body}"
    );
    // Both resolve fully; the reference reaches three quarters of the
    // key and the code reaches 3 of 20.
    assert_eq!(body["candidates"][1]["overlap"], 1.0, "{body}");
    assert_eq!(body["candidates"][0]["overlap"], 1.0, "{body}");
    assert_eq!(body["candidates"][0]["matched"], 15, "{body}");
    assert_eq!(body["candidates"][1]["matched"], 3, "{body}");
    assert_eq!(body["candidates"][0]["to_unique"], true, "{body}");
    assert_eq!(body["candidates"][0]["to_temporal"], false, "{body}");
}

/// A date copied from the parent onto every child row contains the
/// parent's dates perfectly; the parent's key is what the child refers
/// to. A temporal target ranks below a clean key.
#[tokio::test(flavor = "multi_thread")]
async fn a_copied_date_column_ranks_below_the_key() {
    let races = Arc::new(Schema::new(vec![
        Field::new("race_id", DataType::Int64, true),
        Field::new("race_date", DataType::Date32, true),
    ]));
    let day = |d: i32| 20_000 + 7 * d;
    let races = RecordBatch::try_new(
        races,
        vec![
            Arc::new(Int64Array::from((1..=8).collect::<Vec<i64>>())),
            Arc::new(Date32Array::from((1..=8).map(day).collect::<Vec<i32>>())),
        ],
    )
    .unwrap();
    let results = Arc::new(Schema::new(vec![
        Field::new("race_id", DataType::Int64, true),
        Field::new("result_date", DataType::Date32, true),
    ]));
    // Races 1..7 with results, one orphan race 99 whose date is race
    // 8's: the copied date resolves fully, the key does not.
    let race_id: Vec<i64> = vec![1, 1, 2, 2, 3, 4, 5, 6, 7, 7, 99, 99];
    let result_date: Vec<i32> = race_id
        .iter()
        .map(|r| if *r == 99 { day(8) } else { day(*r as i32) })
        .collect();
    let results = RecordBatch::try_new(
        results,
        vec![
            Arc::new(Int64Array::from(race_id)),
            Arc::new(Date32Array::from(result_date)),
        ],
    )
    .unwrap();
    let body = ranked(vec![("races", races), ("results", results)]).await;

    assert_eq!(body["summary"]["candidates"], 2, "{body}");
    assert_eq!(
        edge(&body, 0),
        ("results.race_id".into(), "races.race_id".into()),
        "{body}"
    );
    assert_eq!(
        edge(&body, 1),
        ("results.result_date".into(), "races.race_date".into()),
        "{body}"
    );
    assert_eq!(body["candidates"][1]["overlap"], 1.0, "{body}");
    assert_eq!(body["candidates"][1]["to_unique"], true, "{body}");
    assert_eq!(body["candidates"][1]["to_temporal"], true, "{body}");
    assert_eq!(body["candidates"][0]["overlap"], 0.875, "{body}");
}

/// Two child tables on one parent key contain each other's references
/// perfectly, and the sibling's column is near-unique enough to be
/// key-like without being a key. A target that is not exactly unique
/// in its table ranks below the parent's key.
#[tokio::test(flavor = "multi_thread")]
async fn a_sibling_on_a_shared_parent_key_ranks_below_the_parent() {
    let (es, eid) = ints("emp_no", (1..=12).collect());
    let employees = RecordBatch::try_new(es, vec![eid]).unwrap();
    let mut dept: Vec<i64> = (1..=9).collect();
    dept.push(9);
    let (ds, did) = ints("emp_no", dept);
    let dept_emp = RecordBatch::try_new(ds, vec![did]).unwrap();
    let mut held: Vec<i64> = (1..=9).collect();
    held.extend([2, 5, 7]);
    let (ts, tid) = ints("emp_no", held);
    let titles = RecordBatch::try_new(ts, vec![tid]).unwrap();
    let body = ranked(vec![
        ("employees", employees),
        ("dept_emp", dept_emp),
        ("titles", titles),
    ])
    .await;

    let edges: Vec<(String, String)> = (0..body["candidates"].as_array().unwrap().len())
        .map(|i| edge(&body, i))
        .collect();
    // Both true edges lead, in either order; the sibling pair follows.
    assert_eq!(edges[0].1, "employees.emp_no", "{body}");
    assert_eq!(edges[1].1, "employees.emp_no", "{body}");
    assert!(
        edges.contains(&("titles.emp_no".into(), "dept_emp.emp_no".into())),
        "the sibling pair stays in the list: {body}"
    );
    let sibling = body["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["to"] == "dept_emp.emp_no" && c["from"] == "titles.emp_no")
        .unwrap();
    // Its statistics beat the true edge on coverage and tie on overlap.
    assert_eq!(sibling["overlap"], 1.0, "{body}");
    assert_eq!(sibling["matched"], 9, "{body}");
    assert_eq!(sibling["to_distinct"], 9, "{body}");
    assert_eq!(sibling["to_unique"], false, "{body}");
    assert_eq!(body["candidates"][0]["to_unique"], true, "{body}");
}
