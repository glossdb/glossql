//! Extraction composes through GLOSSARY, never through a subquery.
//!
//! `SELECT … FROM (SELECT fn() FROM t.c)` cannot serve: extraction is
//! the compute act (it lands a measurement gloss), and a read stays a
//! read — so the pre-pass refuses the shape with the road out instead
//! of the engine's bare "table not found". The full-support fork is
//! parked as a test case for the cache design (the register). The
//! fixture is behavior_evidence.rs's, trimmed to one measure column.

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_session::Session;

const BEHAVIOR_EVIDENCE: &str = include_str!("../../functions/behavior_evidence.sql");

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

const MONTHS: [&str; 6] = [
    "2025-01-01",
    "2025-02-01",
    "2025-03-01",
    "2025-04-01",
    "2025-05-01",
    "2025-06-01",
];
const ENTITIES: [&str; 3] = ["a", "b", "c"];
const MOVEMENTS: [[f64; 6]; 3] = [
    [10.0, -5.0, 20.0, 3.0, -2.0, 7.0],
    [100.0, 50.0, -30.0, 20.0, 10.0, -60.0],
    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
];

async fn fixture(root: &std::path::Path) {
    let ledgers = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    write_table(
        root,
        "ledgers",
        RecordBatch::try_new(
            ledgers,
            vec![Arc::new(StringArray::from(ENTITIES.to_vec()))],
        )
        .unwrap(),
    )
    .await;

    let mut p_entity = Vec::new();
    let mut p_period = Vec::new();
    let mut p_balance = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        let mut running = 0.0;
        for (month, mv) in MONTHS.iter().zip(moves) {
            running += mv;
            p_entity.push(*e);
            p_period.push(*month);
            p_balance.push(running);
        }
    }
    let positions = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("balance", DataType::Float64, true),
    ]));
    write_table(
        root,
        "positions",
        RecordBatch::try_new(
            positions,
            vec![
                Arc::new(StringArray::from(p_entity)),
                Arc::new(StringArray::from(p_period)),
                Arc::new(Float64Array::from(p_balance)),
            ],
        )
        .unwrap(),
    )
    .await;

    let mut m_entity = Vec::new();
    let mut m_date = Vec::new();
    let mut m_amount = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        for (month, mv) in MONTHS.iter().zip(moves) {
            m_entity.push(*e);
            m_date.push(month.to_string());
            m_amount.push(mv);
        }
    }
    let moves = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    write_table(
        root,
        "moves",
        RecordBatch::try_new(
            moves,
            vec![
                Arc::new(StringArray::from(m_entity)),
                Arc::new(StringArray::from(m_date)),
                Arc::new(Float64Array::from(m_amount)),
            ],
        )
        .unwrap(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subquery_extraction_is_refused_with_the_road_out() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;

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
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(KernelRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'subquery repro');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT behavior_evidence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                                \"anchors\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION behavior_evidence FOR GLOBAL \
             AS $${BEHAVIOR_EVIDENCE}$$ \
 RETURNS behavior_evidence;\n\
             DECLARE RECIPE ledgers ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
             DECLARE RECIPE positions ON fin FROM erp_export AS \
             $$SELECT entity, CAST(period AS DATE) AS period, balance \
             FROM read_parquet('positions/*.parquet')$$;\n\
             DECLARE RECIPE moves ON fin FROM erp_export AS \
             $$SELECT entity, CAST(d AS DATE) AS d, amount \
             FROM read_parquet('moves/*.parquet')$$;\n\
             DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
            root.display()
        ))
        .await
        .unwrap();

    // The plain extraction serves — it is its own statement.
    session
        .execute("SELECT behavior_evidence() FROM positions.balance;")
        .await
        .expect("the plain extraction serves");

    // Inside a subquery the same spelling is refused with guidance,
    // not the engine's "table not found".
    let err = session
        .execute("SELECT * FROM (SELECT behavior_evidence() FROM positions.balance);")
        .await
        .expect_err("a subquery extraction is refused")
        .to_string();
    assert!(
        err.contains("is a subject, not a table") && err.contains("GLOSSARY(positions.balance::"),
        "the refusal names the subject and the road out: {err}"
    );

    // The road out the message names actually works: the measurement
    // the extraction landed composes in any subquery.
    session
        .execute(
            "SELECT * FROM (SELECT value FROM \
             GLOSSARY(positions.balance::behavior_evidence) \
             WHERE state = 'current');",
        )
        .await
        .expect("the glossary read composes");
}
