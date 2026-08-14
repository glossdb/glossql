//! The oracle test: the finance generator grades the discriminator (the
//! standing rule — no statistic ports without its oracle). The clean
//! strategy's `trial_balance` carries per-period turnover in columns
//! *named* balances — verified against the generator to the cent — so
//! the evidence must read them as FLOW despite the name; a name-based
//! judge would say stock. The stock arm is graded on a running balance
//! derived from the same data, and `net_amount` must reconcile against
//! the trial balance through the pair-difference convention (v0.3's
//! recorded reason for enumerating differences at all). Skips when the
//! sibling `../dataraum-testdata` checkout is absent.

use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

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

async fn evidence(session: &Session, subject: &str) -> serde_json::Value {
    session
        .execute(&format!("SELECT behavior_evidence() FROM {subject};"))
        .await
        .unwrap();
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY({subject}::behavior_evidence) \
                 WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

fn anchor<'a>(evidence: &'a serde_json::Value, event: &str) -> &'a serde_json::Value {
    evidence["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["event"] == event)
        .unwrap_or_else(|| panic!("no {event} anchor in {evidence}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_generator_grades_the_discriminator() {
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
    let store = Store::open_memory().await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_lake(lake)
    .with_runtime(Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'behavior oracle');\n\
             USE fin;\n\
             DECLARE SOURCE finance SET (type: csv, location: '{}');\n\
             DECLARE ASPECT behavior_evidence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                                \"anchors\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION behavior_evidence FOR GLOBAL \
             FROM 'functions/behavior_evidence.rhai' \
             ACCEPTS (relationships, imports) RETURNS behavior_evidence;\n\
             DECLARE RECIPE chart_of_accounts ON fin FROM finance AS \
             $$SELECT TRY_CAST(account_id AS BIGINT) AS account_id, name \
             FROM read_csv('chart_of_accounts.csv')$$;\n\
             DECLARE RECIPE journal_entries ON fin FROM finance AS \
             $$SELECT entry_id, try_to_date(\"date\", '%Y-%m-%d') AS \"date\" \
             FROM read_csv('journal_entries.csv')$$;\n\
             DECLARE RECIPE journal_lines ON fin FROM finance AS \
             $$SELECT line_id, entry_id, TRY_CAST(account_id AS BIGINT) AS account_id, \
             TRY_CAST(debit AS DOUBLE) AS debit, TRY_CAST(credit AS DOUBLE) AS credit, \
             TRY_CAST(net_amount AS DOUBLE) AS net_amount \
             FROM read_csv('journal_lines.csv')$$;\n\
             DECLARE RECIPE trial_balance ON fin FROM finance AS \
             $$SELECT TRY_CAST(account_id AS BIGINT) AS account_id, \
             try_to_date(period || '-01', '%Y-%m-%d') AS period, \
             TRY_CAST(debit_balance AS DOUBLE) AS debit_balance, \
             TRY_CAST(credit_balance AS DOUBLE) AS credit_balance \
             FROM read_csv('trial_balance.csv')$$;\n\
             DECLARE RECIPE account_balances ON fin FROM finance AS \
             $$SELECT TRY_CAST(account_id AS BIGINT) AS account_id, \
             try_to_date(period || '-01', '%Y-%m-%d') AS period, \
             SUM(TRY_CAST(debit_balance AS DOUBLE) - TRY_CAST(credit_balance AS DOUBLE)) \
             OVER (PARTITION BY account_id ORDER BY period) AS balance \
             FROM read_csv('trial_balance.csv')$$;\n\
             DECLARE RECIPE fx_rates ON fin FROM finance AS \
             $$SELECT from_ccy, to_ccy, try_to_date(\"date\", '%Y-%m-%d') AS \"date\", \
             TRY_CAST(rate AS DOUBLE) AS rate FROM read_csv('fx_rates.csv')$$;\n\
             DECLARE RELATIONSHIP journal_lines.account_id -> chart_of_accounts.account_id;\n\
             DECLARE RELATIONSHIP trial_balance.account_id -> chart_of_accounts.account_id;\n\
             DECLARE RELATIONSHIP account_balances.account_id -> chart_of_accounts.account_id;\n\
             DECLARE RELATIONSHIP journal_lines.entry_id -> journal_entries.entry_id;",
            data.display()
        ))
        .await
        .unwrap();

    // The name lie: `debit_balance` equals each month's line debits, not
    // a carried level — flow, through the single-column convention.
    let tb = evidence(&session, "trial_balance.debit_balance").await;
    assert_eq!(tb["applicable"], true, "{tb}");
    let a = anchor(&tb, "journal_lines");
    assert_eq!(a["verdict"], "flow", "{a}");
    assert_eq!(a["convention"], "debit", "{a}");
    assert!(a["voted"].as_i64().unwrap() >= 2, "{a}");
    assert!(a["agreement"].as_f64().unwrap() >= 0.8, "{a}");
    assert!(a["r_flow"].as_f64().unwrap() < 0.01, "{a}");

    // The true stock: the running balance's delta reconciles to the
    // ledger's period movement.
    let bal = evidence(&session, "account_balances.balance").await;
    let a = anchor(&bal, "journal_lines");
    assert_eq!(a["verdict"], "stock", "{a}");
    assert!(a["r_stock"].as_f64().unwrap() < 0.01, "{a}");

    // The pair-difference convention earning its place: net_amount
    // reconciles against the trial balance only as debit − credit —
    // neither single column fits.
    let net = evidence(&session, "journal_lines.net_amount").await;
    let a = anchor(&net, "trial_balance");
    assert_eq!(a["verdict"], "flow", "{a}");
    assert_eq!(a["convention"], "debit_balance - credit_balance", "{a}");

    // No declared edge touches fx_rates: the measurement abstains
    // whole, it does not improvise an anchor.
    let fx = evidence(&session, "fx_rates.rate").await;
    assert_eq!(fx["applicable"], false, "{fx}");
    assert!(
        fx["reason"]
            .as_str()
            .unwrap()
            .contains("no declared relationships"),
        "{fx}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn document_keyed_events_reconcile_at_month_grain() {
    // The 2026-08-14 starvation (the medium run): payment-shaped
    // tables key on the invoice document, and one row per document
    // never carries 4+ periods — the most obviously flow-shaped
    // columns in order-to-cash abstained on every anchor. Two moves
    // close it, both recall-first with the gates deciding: a
    // day-native alignment also serves a month variant, and a measure
    // table that lacks the dimension column borrows it through the
    // document edge (receipts → ar_invoices → customer). Truth by
    // construction: invoiced and received amounts are flows.
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
    let store = Store::open_memory().await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_lake(lake)
    .with_runtime(Arc::new(RhaiRuntime::new(env!("CARGO_MANIFEST_DIR"))));

    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'starvation oracle');\n\
             USE fin;\n\
             DECLARE SOURCE finance SET (type: csv, location: '{}');\n\
             DECLARE ASPECT behavior_evidence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                                \"anchors\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION behavior_evidence FOR GLOBAL \
             FROM 'functions/behavior_evidence.rhai' \
             ACCEPTS (relationships, imports) RETURNS behavior_evidence;\n\
             DECLARE RECIPE customers ON fin FROM finance AS \
             $$SELECT customer_id, name FROM read_csv('customers.csv')$$;\n\
             DECLARE RECIPE ar_invoices ON fin FROM finance AS \
             $$SELECT ar_invoice_id, customer_id, \
             try_to_date(invoice_date, '%Y-%m-%d') AS invoice_date, \
             TRY_CAST(amount AS DOUBLE) AS amount \
             FROM read_csv('ar_invoices.csv')$$;\n\
             DECLARE RECIPE receipts ON fin FROM finance AS \
             $$SELECT receipt_id, ar_invoice_id, customer_id, \
             try_to_date(receipt_date, '%Y-%m-%d') AS receipt_date, \
             TRY_CAST(amount AS DOUBLE) AS amount \
             FROM read_csv('receipts.csv')$$;\n\
             DECLARE RECIPE receipts_thin ON fin FROM finance AS \
             $$SELECT receipt_id, ar_invoice_id, \
             try_to_date(receipt_date, '%Y-%m-%d') AS receipt_date, \
             TRY_CAST(amount AS DOUBLE) AS amount \
             FROM read_csv('receipts.csv')$$;\n\
             DECLARE RELATIONSHIP receipts.ar_invoice_id -> ar_invoices.ar_invoice_id;\n\
             DECLARE RELATIONSHIP receipts_thin.ar_invoice_id -> ar_invoices.ar_invoice_id;\n\
             DECLARE RELATIONSHIP receipts.customer_id -> customers.customer_id;\n\
             DECLARE RELATIONSHIP ar_invoices.customer_id -> customers.customer_id;",
            data.display()
        ))
        .await
        .unwrap();

    let find = |ev: &serde_json::Value, event: &str, grain: &str| -> Option<serde_json::Value> {
        ev["anchors"].as_array().unwrap().iter().find(|a| {
            a["event"] == event && a["grain"] == grain && a["verdict"] != "abstain"
        }).cloned()
    };

    // The two-hop dimension alignment at the month variant settles the
    // invoiced amount against receipts per (customer, month).
    let inv = evidence(&session, "ar_invoices.amount").await;
    assert_eq!(inv["applicable"], true, "{inv}");
    let month = find(&inv, "receipts", "month")
        .unwrap_or_else(|| panic!("no non-abstaining month anchor on receipts: {inv}"));
    assert_eq!(month["verdict"], "flow", "{month}");

    // The thin receipts carry only the document reference: the entity
    // is borrowed through ar_invoices, and the month variant answers.
    let thin = evidence(&session, "receipts_thin.amount").await;
    assert_eq!(thin["applicable"], true, "{thin}");
    let borrowed = find(&thin, "ar_invoices", "month")
        .unwrap_or_else(|| panic!("no non-abstaining month anchor on ar_invoices: {thin}"));
    assert_eq!(borrowed["verdict"], "flow", "{borrowed}");
    assert!(
        borrowed["align"]
            .as_str()
            .unwrap()
            .contains("via ar_invoices.customer_id"),
        "{borrowed}"
    );
}
