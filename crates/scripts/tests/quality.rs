//! The quality pair from the 2026-08 evaluation (tfmeval): the
//! derivation a table's lineage carries, and what a declared join
//! asserts. Both are recall-oriented measurements — the judge reads
//! them; neither issues a verdict.

use std::sync::Arc;

use datafusion::arrow::array::{Date32Array, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
const COHERENCE: &str = include_str!("../functions/coherence.rhai");
/// The shipped body, so the declaration carries what runs.
const DERIVATIONS: &str = include_str!("../functions/derivations.rhai");

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

fn one(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

async fn session_over(dir: &std::path::Path) -> Session {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open_scratch(lake.clone()).await.unwrap();
    Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"))))
}

/// `line_amount = units * unit_price` with two silently scaled rows —
/// the confusable pair's artifact half — and `gross = net + tax` exact.
/// The measurement proposes both identities and counts the violations;
/// it never says which rows are wrong or why.
#[tokio::test(flavor = "multi_thread")]
async fn derivations_surface_with_their_violations() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();

    let n = 40;
    let units: Vec<i64> = (0..n).map(|i| (i % 5) + 1).collect();
    let price: Vec<f64> = (0..n).map(|i| 10.0 + i as f64).collect();
    let line: Vec<f64> = (0..n)
        .map(|i| {
            let exact = units[i as usize] as f64 * price[i as usize];
            // Rows 0 and 1 carry the undeclared scaling.
            if i < 2 { exact * 1.15 } else { exact }
        })
        .collect();
    let net: Vec<f64> = (0..n).map(|i| 1000.0 + 13.0 * i as f64).collect();
    let tax: Vec<f64> = (0..n).map(|i| 77.0 + 3.0 * i as f64).collect();
    let gross: Vec<f64> = (0..n).map(|i| net[i as usize] + tax[i as usize]).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("units", DataType::Int64, true),
        Field::new("unit_price", DataType::Float64, true),
        Field::new("line_amount", DataType::Float64, true),
        Field::new("net", DataType::Float64, true),
        Field::new("tax", DataType::Float64, true),
        Field::new("gross", DataType::Float64, true),
    ]));
    write_table(
        &root,
        "lines",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(units)),
                Arc::new(Float64Array::from(price)),
                Arc::new(Float64Array::from(line)),
                Arc::new(Float64Array::from(net)),
                Arc::new(Float64Array::from(tax)),
                Arc::new(Float64Array::from(gross)),
            ],
        )
        .unwrap(),
    )
    .await;

    let session = session_over(dir.path()).await;
    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'derivation check');\n\
             USE fin;\n\
             DECLARE SOURCE erp SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT derivation_candidates WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                               \"candidates\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON TABLE;\n\
             DECLARE FUNCTION detect_derivations FOR GLOBAL \
             AS $${DERIVATIONS}$$ RETURNS derivation_candidates;\n\
             DECLARE RECIPE lines ON fin FROM erp AS \
             $$SELECT * FROM read_parquet('lines/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();

    session
        .execute("SELECT detect_derivations() FROM lines;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(lines::derivation_candidates) WHERE state = 'current';",
        )
        .await
        .unwrap());
    let doc: serde_json::Value = serde_json::from_str(&value).unwrap();
    assert_eq!(doc["applicable"], true);
    let candidates = doc["candidates"].as_array().unwrap();

    let product = candidates
        .iter()
        .find(|c| c["target"] == "line_amount" && c["form"] == "product")
        .expect("the product identity surfaces despite its violations");
    assert_eq!(product["support"], 40);
    assert_eq!(product["violations"], 2);

    let sum = candidates
        .iter()
        .find(|c| c["target"] == "gross" && c["form"] == "sum")
        .expect("the exact sum identity surfaces");
    assert_eq!(sum["violations"], 0);
    let ops: Vec<&str> = sum["operands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(ops.contains(&"net") && ops.contains(&"tax"));
}

/// A declared join, measured: two payments point at invoices that do
/// not exist, three more are dated before the invoice they claim to
/// settle. The orphans are exact; the temporal read is evidence the
/// judge interprets against the pair's names.
#[tokio::test(flavor = "multi_thread")]
async fn coherence_reads_what_the_declared_join_asserts() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();

    // invoices 1..=18, dated day 100 + id.
    let inv_ids: Vec<i64> = (1..=18).collect();
    let inv_dates: Vec<i32> = (1..=18).map(|i| 100 + i as i32).collect();
    let invoices = Arc::new(Schema::new(vec![
        Field::new("invoice_id", DataType::Int64, true),
        Field::new("inv_date", DataType::Date32, true),
    ]));
    write_table(
        &root,
        "invoices",
        RecordBatch::try_new(
            invoices,
            vec![
                Arc::new(Int64Array::from(inv_ids)),
                Arc::new(Date32Array::from(inv_dates)),
            ],
        )
        .unwrap(),
    )
    .await;

    // 20 payments: ids 1..=18 resolve, 98 and 99 are orphans. Matched
    // payments land 30 days after their invoice, except payments 1..=3
    // which are dated BEFORE it — the wrong-pairing trace.
    let pay_inv: Vec<i64> = (1..=18).chain([98, 99]).collect();
    let pay_dates: Vec<i32> = (1..=18)
        .map(|i| {
            if i <= 3 {
                100 + i as i32 - 10
            } else {
                100 + i as i32 + 30
            }
        })
        .chain([500, 501])
        .collect();
    let payments = Arc::new(Schema::new(vec![
        Field::new("payment_id", DataType::Int64, true),
        Field::new("invoice_id", DataType::Int64, true),
        Field::new("pay_date", DataType::Date32, true),
    ]));
    write_table(
        &root,
        "payments",
        RecordBatch::try_new(
            payments,
            vec![
                Arc::new(Int64Array::from((1..=20).collect::<Vec<i64>>())),
                Arc::new(Int64Array::from(pay_inv)),
                Arc::new(Date32Array::from(pay_dates)),
            ],
        )
        .unwrap(),
    )
    .await;

    let session = session_over(dir.path()).await;
    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'coherence check');\n\
             USE fin;\n\
             DECLARE SOURCE erp SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT relationship_coherence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                               \"relationships\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON DATASET;\n\
             DECLARE FUNCTION relationship_coherence FOR GLOBAL \
             AS $${COHERENCE}$$ RETURNS relationship_coherence;\n\
             DECLARE RECIPE invoices ON fin FROM erp AS \
             $$SELECT * FROM read_parquet('invoices/*.parquet')$$;\n\
             DECLARE RECIPE payments ON fin FROM erp AS \
             $$SELECT * FROM read_parquet('payments/*.parquet')$$;\n\
             DECLARE RELATIONSHIP payments.invoice_id -> invoices.invoice_id;",
            root.display()
        ))
        .await
        .unwrap();

    session
        .execute("SELECT relationship_coherence() FROM fin;")
        .await
        .unwrap();
    let value = one(&session
        .execute("SELECT value FROM GLOSSARY(fin::relationship_coherence) WHERE state = 'current';")
        .await
        .unwrap());
    let doc: serde_json::Value = serde_json::from_str(&value).unwrap();
    assert_eq!(doc["applicable"], true);
    let rels = doc["relationships"].as_array().unwrap();
    assert_eq!(rels.len(), 1);
    let rel = &rels[0];
    assert_eq!(rel["filled"], 20);
    assert_eq!(rel["orphans"], 2);
    let temporal = rel["temporal"].as_array().unwrap();
    let pair = temporal
        .iter()
        .find(|t| t["child_column"] == "pay_date" && t["parent_column"] == "inv_date")
        .expect("the date pair is measured");
    assert_eq!(pair["joined"], 18);
    assert_eq!(pair["precedes"], 3);
}
