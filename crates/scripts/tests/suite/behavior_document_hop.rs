//! The event-side document hop: an event table carrying no entity key
//! — only a document key, with the document master holding the entity
//! — still anchors the discriminator. The order-to-cash shape whose
//! report-era runs abstained on every anchor: the borrow-when-starving
//! rule reaches the master with one m:1 join, and the HAVING probe
//! still gates the alignment before either side is scanned wide.
//!
//! Shape: positions (measure, entity-keyed) — moves_doc (event,
//! document-keyed) -> docs (document master, carries entity) ->
//! ledgers. The running balance reconciles to the movements exactly,
//! so the anchor must verdict stock.

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_session::{Outcome, Session};

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

    // The measure: a running balance per entity.
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

    // The document master: one document per (entity, month).
    let mut d_id = Vec::new();
    let mut d_entity = Vec::new();
    for e in ENTITIES {
        for month in MONTHS {
            d_id.push(format!("{e}-{}", &month[..7]));
            d_entity.push(e);
        }
    }
    let docs = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("entity", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "docs",
        RecordBatch::try_new(
            docs,
            vec![
                Arc::new(StringArray::from(d_id)),
                Arc::new(StringArray::from(d_entity)),
            ],
        )
        .unwrap(),
    )
    .await;

    // The event table: document-keyed only — no entity column.
    let mut m_doc = Vec::new();
    let mut m_date = Vec::new();
    let mut m_amount = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        for (month, mv) in MONTHS.iter().zip(moves) {
            m_doc.push(format!("{e}-{}", &month[..7]));
            m_date.push(month.to_string());
            m_amount.push(mv);
        }
    }
    let moves_doc = Arc::new(Schema::new(vec![
        Field::new("doc", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    write_table(
        root,
        "moves_doc",
        RecordBatch::try_new(
            moves_doc,
            vec![
                Arc::new(StringArray::from(m_doc)),
                Arc::new(StringArray::from(m_date)),
                Arc::new(Float64Array::from(m_amount)),
            ],
        )
        .unwrap(),
    )
    .await;
}

fn one(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_keyed_event_table_still_anchors_the_stock() {
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
            "DECLARE DATASET fin SET (purpose: 'document-key repro');\n\
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
             DECLARE RECIPE docs ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('docs/*.parquet')$$;\n\
             DECLARE RECIPE positions ON fin FROM erp_export AS \
             $$SELECT entity, CAST(period AS DATE) AS period, balance \
             FROM read_parquet('positions/*.parquet')$$;\n\
             DECLARE RECIPE moves_doc ON fin FROM erp_export AS \
             $$SELECT doc, CAST(d AS DATE) AS d, amount \
             FROM read_parquet('moves_doc/*.parquet')$$;\n\
             DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP moves_doc.doc -> docs.id;\n\
             DECLARE RELATIONSHIP docs.entity -> ledgers.id;",
            root.display()
        ))
        .await
        .unwrap();

    session
        .execute("SELECT behavior_evidence() FROM positions.balance;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(positions.balance::behavior_evidence) \
             WHERE state = 'current';",
        )
        .await
        .unwrap());
    let evidence: serde_json::Value = serde_json::from_str(&value).unwrap();

    // The event table is reachable only through the document hop; a
    // working discriminator anchors on it and reads the running
    // balance as a stock.
    assert_eq!(evidence["applicable"], true, "{evidence}");
    let anchor = evidence["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["event"] == "moves_doc")
        .cloned()
        .unwrap_or_else(|| panic!("no moves_doc anchor in {evidence}"));
    assert_eq!(anchor["verdict"], "stock", "{anchor}");
    assert_eq!(evidence["summary"]["verdict"], "stock", "{evidence}");
}
