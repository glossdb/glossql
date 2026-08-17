//! The generator grades the relevance score (the standing rule — no
//! statistic ports without its oracle). Both expectations derive from
//! ground truth the score never sees: `invoices.status` scores the
//! Pielou evenness of the yaml's own monthly invoice_count totals
//! (paid 2554 / open 219 / cancelled 140 / overdue 56 / partial 31 →
//! 0.3682), and `bank_transactions.reconciled` scores the generator's
//! stated reconciliation rate 0.8951 (→ 0.4842) — the same deliberate
//! dirt the scorecard treats as a fidelity check, not a defect. Skips
//! when the sibling `../dataraum-testdata` checkout is absent.

use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
const DIMENSION_RELEVANCE: &str = include_str!("../functions/dimension_relevance.rhai");
/// The shipped body, so the declaration carries what runs.
const PROFILE: &str = include_str!("../functions/profile.rhai");

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

async fn relevance(session: &Session, subject: &str) -> serde_json::Value {
    session
        .execute(&format!(
            "SELECT profile() FROM {subject};\n\
             SELECT dimension_relevance() FROM {subject};"
        ))
        .await
        .unwrap();
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY({subject}::dimension_relevance) \
                 WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_generator_grades_the_relevance_score() {
    let data = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../dataraum-testdata/output/clean");
    let Ok(data) = data.canonicalize() else {
        eprintln!("skipping: sibling dataraum-testdata checkout not present");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open_scratch(lake.clone()).await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'dimensions oracle');\n\
             USE fin;\n\
             DECLARE SOURCE finance SET (type: csv, location: '{}');\n\
             DECLARE ASPECT column_profile WITH $${{\n\
               \"type\": \"object\", \"required\": [\"total\"],\n\
               \"properties\": {{\"total\": {{\"type\": \"integer\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE ASPECT dimension_relevance WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION profile FOR GLOBAL \
             AS $${PROFILE}$$ RETURNS column_profile;\n\
             DECLARE FUNCTION dimension_relevance FOR GLOBAL \
             AS $${DIMENSION_RELEVANCE}$$ \
             ACCEPTS (column_profile) RETURNS dimension_relevance;\n\
             DECLARE RECIPE invoices ON fin FROM finance AS \
             $$SELECT invoice_id, status FROM read_csv('invoices.csv')$$;\n\
             DECLARE RECIPE bank_transactions ON fin FROM finance AS \
             $$SELECT txn_id, TRY_CAST(reconciled AS BOOLEAN) AS reconciled \
             FROM read_csv('bank_transactions.csv')$$;",
            data.display()
        ))
        .await
        .unwrap();

    // Five statuses, no nulls: coverage 1, evenness computed
    // independently from the corpus's own status totals (corpus
    // 9dbcf6b6c6f3, seed 42, 12 months — re-pin when output/clean is
    // regenerated on a changed generator: the constants moved once
    // already, 2026-08-11, when the corpus gained families). The skew
    // is real (87% paid) and the score reports it — the number never
    // overrules the business judgment of interest.
    let status = relevance(&session, "invoices.status").await;
    assert_eq!(status["applicable"], true, "{status}");
    assert_eq!(status["groups"], 5, "{status}");
    assert!(
        (status["relevance"].as_f64().unwrap() - 0.3067).abs() < 0.01,
        "{status}"
    );
    assert!(
        (status["coverage"].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "{status}"
    );

    // The generator's stated reconciliation rate, read back through the
    // score: Pielou([0.9013, 0.0987]) ≈ 0.4650 on the same corpus.
    let reconciled = relevance(&session, "bank_transactions.reconciled").await;
    assert_eq!(reconciled["groups"], 2, "{reconciled}");
    assert!(
        (reconciled["relevance"].as_f64().unwrap() - 0.4650).abs() < 0.01,
        "{reconciled}"
    );

    // A document key is not an axis.
    let key = relevance(&session, "invoices.invoice_id").await;
    assert_eq!(key["applicable"], false, "{key}");
    assert!(
        key["reason"].as_str().unwrap().contains("near-key"),
        "{key}"
    );
}
