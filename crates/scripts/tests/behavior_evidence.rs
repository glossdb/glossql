//! The stock/flow discriminator on data whose truth is by construction:
//! `balance` is a running sum of the movements (a stock — its delta
//! reconciles), `turnover` is the movement itself (a flow), and `noise`
//! ties to nothing — both residuals stay large and every entity
//! abstains, the wrong-anchor gate refusing to convert ignorance into a
//! verdict. Anchors ride declared edges only: the two tables meet at
//! the `ledgers` dimension.

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

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
    let mut p_turnover = Vec::new();
    let mut p_noise = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        let mut running = 0.0;
        for (month, mv) in MONTHS.iter().zip(moves) {
            running += mv;
            p_entity.push(*e);
            p_period.push(*month);
            p_balance.push(running);
            p_turnover.push(mv);
            p_noise.push(7.0);
        }
    }
    let positions = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("balance", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("noise", DataType::Float64, true),
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
                Arc::new(Float64Array::from(p_turnover)),
                Arc::new(Float64Array::from(p_noise)),
            ],
        )
        .unwrap(),
    )
    .await;

    // Each month's movement lands as two event rows so the aggregation
    // is real, not a copy.
    let mut m_entity = Vec::new();
    let mut m_date = Vec::new();
    let mut m_amount = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        for (month, mv) in MONTHS.iter().zip(moves) {
            let mid = format!("{}15", &month[..8]);
            m_entity.push(*e);
            m_date.push(month.to_string());
            m_amount.push(mv - 1.0);
            m_entity.push(*e);
            m_date.push(mid);
            m_amount.push(1.0);
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

async fn evidence(session: &Session, column: &str) -> serde_json::Value {
    session
        .execute(&format!(
            "SELECT behavior_evidence() FROM positions.{column};"
        ))
        .await
        .unwrap();
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY(positions.{column}::behavior_evidence) \
                 WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

fn moves_anchor(evidence: &serde_json::Value) -> serde_json::Value {
    evidence["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["event"] == "moves")
        .cloned()
        .unwrap_or_else(|| panic!("no moves anchor in {evidence}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_balance_is_a_stock_its_movement_a_flow_and_noise_abstains() {
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
            "DECLARE DATASET fin SET (purpose: 'behavior evidence');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT behavior_evidence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                                \"anchors\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION behavior_evidence FOR GLOBAL \
             FROM 'functions/behavior_evidence.rhai' \
             ACCEPTS (relationships, imports) RETURNS behavior_evidence;\n\
             DECLARE RECIPE ledgers ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
             DECLARE RECIPE positions ON fin FROM erp_export AS \
             $$SELECT entity, CAST(period AS DATE) AS period, balance, turnover, noise \
             FROM read_parquet('positions/*.parquet')$$;\n\
             DECLARE RECIPE moves ON fin FROM erp_export AS \
             $$SELECT entity, CAST(d AS DATE) AS d, amount \
             FROM read_parquet('moves/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();

    // Before any edge is declared there are no anchors — the
    // measurement abstains whole.
    let before = evidence(&session, "balance").await;
    assert_eq!(before["applicable"], false, "{before}");

    // Declaring the edges invalidates the cached abstention through the
    // `relationships` ACCEPTS edge (ruled 2026-08-05) — the next call
    // recomputes, no manual cache delete.
    session
        .execute(
            "DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
        )
        .await
        .unwrap();

    // The running balance: Δbalance reconciles to the period movement,
    // every entity votes stock.
    let balance = evidence(&session, "balance").await;
    assert_eq!(balance["applicable"], true, "{balance}");
    let anchor = moves_anchor(&balance);
    assert_eq!(anchor["verdict"], "stock", "{anchor}");
    assert_eq!(anchor["convention"], "amount", "{anchor}");
    assert_eq!(anchor["voted"], 3, "{anchor}");
    assert_eq!(anchor["agreement"], 1.0, "{anchor}");

    // The movement itself: y equals m, every entity votes flow.
    let turnover = evidence(&session, "turnover").await;
    let anchor = moves_anchor(&turnover);
    assert_eq!(anchor["verdict"], "flow", "{anchor}");
    assert_eq!(anchor["convention"], "amount", "{anchor}");

    // A column tied to nothing: both residuals stay large for every
    // entity — the wrong-anchor gate abstains, never guesses.
    let noise = evidence(&session, "noise").await;
    let anchor = moves_anchor(&noise);
    assert_eq!(anchor["verdict"], "abstain", "{anchor}");
    assert_eq!(
        anchor["reason"], "no entity series reconciled: wrong anchor, short series, or dead values",
        "{anchor}"
    );
}

/// A session over its own root with the behavior declarations landed —
/// the shared spelling of the two sign/tiebreak tests below.
async fn behavior_session(dir: &std::path::Path, recipes: &str) -> Session {
    let root = dir.join("lake/erp");
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
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
            "DECLARE DATASET fin SET (purpose: 'behavior evidence');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT behavior_evidence WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}},\n\
                                \"anchors\": {{\"type\": \"array\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE FUNCTION behavior_evidence FOR GLOBAL \
             FROM 'functions/behavior_evidence.rhai' \
             ACCEPTS (relationships, imports) RETURNS behavior_evidence;\n\
             {recipes}",
            root.display()
        ))
        .await
        .unwrap();
    session
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ledger_signed_entity_reads_in_the_mirror_count() {
    // Three entities store their balance as the running sum of the
    // movement; the fourth stores the NEGATED running sum — ledger-signed
    // by construction. It cannot vote under the original sign (its
    // residual is 2.0 exactly), but re-classified against the negated
    // anchor it fires — the mirror count carries that, and the judge
    // reads natural-vs-ledger-signed from it.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();

    let ledgers = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    write_table(
        &root,
        "ledgers",
        RecordBatch::try_new(
            ledgers,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c", "d"]))],
        )
        .unwrap(),
    )
    .await;

    let mirrored: [f64; 6] = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0];
    let mut p_entity = Vec::new();
    let mut p_period = Vec::new();
    let mut p_balance = Vec::new();
    let mut m_entity = Vec::new();
    let mut m_date = Vec::new();
    let mut m_amount = Vec::new();
    for (e, moves, sign) in [
        ("a", MOVEMENTS[0], 1.0),
        ("b", MOVEMENTS[1], 1.0),
        ("c", MOVEMENTS[2], 1.0),
        ("d", mirrored, -1.0),
    ] {
        let mut running = 0.0;
        for (month, mv) in MONTHS.iter().zip(moves) {
            running += mv;
            p_entity.push(e);
            p_period.push(*month);
            p_balance.push(sign * running);
            m_entity.push(e);
            m_date.push(*month);
            m_amount.push(mv);
        }
    }
    let positions = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("balance", DataType::Float64, true),
    ]));
    write_table(
        &root,
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
    let moves = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    write_table(
        &root,
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

    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE positions ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS period, balance \
         FROM read_parquet('positions/*.parquet')$$;\n\
         DECLARE RECIPE moves ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, amount \
         FROM read_parquet('moves/*.parquet')$$;",
    )
    .await;
    session
        .execute(
            "DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
        )
        .await
        .unwrap();

    let balance = evidence(&session, "balance").await;
    let anchor = moves_anchor(&balance);
    assert_eq!(anchor["verdict"], "stock", "{anchor}");
    assert_eq!(
        anchor["voted"], 3,
        "d abstains under the original sign: {anchor}"
    );
    assert_eq!(anchor["sign"]["primary"], 3, "{anchor}");
    assert_eq!(anchor["sign"]["mirror"], 1, "{anchor}");
    assert_eq!(anchor["sign"]["both"], 0, "{anchor}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exact_pair_difference_beats_a_loose_single_on_delta_bic() {
    // `net` is credit − debit exactly, and debit is small — so the bare
    // `credit` single also fires, loosely, with the same three voters.
    // Equal support used to hand the tie to the fewer-term convention
    // unconditionally; the ΔBIC>10 tiebreak keeps the pair, because its
    // fit is decisive, not merely simpler.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();

    let ledgers = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    write_table(
        &root,
        "ledgers",
        RecordBatch::try_new(
            ledgers,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap(),
    )
    .await;

    let base: [f64; 6] = [100.0, 120.0, 90.0, 110.0, 105.0, 95.0];
    let mut r_entity = Vec::new();
    let mut r_period = Vec::new();
    let mut r_net = Vec::new();
    let mut f_entity = Vec::new();
    let mut f_date = Vec::new();
    let mut f_credit = Vec::new();
    let mut f_debit = Vec::new();
    for (e, scale) in [("a", 1.0), ("b", 2.0), ("c", 0.5)] {
        for (month, v) in MONTHS.iter().zip(base) {
            let credit = v * scale;
            let debit = credit * 0.05;
            r_entity.push(e);
            r_period.push(*month);
            r_net.push(credit - debit);
            f_entity.push(e);
            f_date.push(*month);
            f_credit.push(credit);
            f_debit.push(debit);
        }
    }
    let reports = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("net", DataType::Float64, true),
    ]));
    write_table(
        &root,
        "reports",
        RecordBatch::try_new(
            reports,
            vec![
                Arc::new(StringArray::from(r_entity)),
                Arc::new(StringArray::from(r_period)),
                Arc::new(Float64Array::from(r_net)),
            ],
        )
        .unwrap(),
    )
    .await;
    let flows = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("credit", DataType::Float64, true),
        Field::new("debit", DataType::Float64, true),
    ]));
    write_table(
        &root,
        "flows",
        RecordBatch::try_new(
            flows,
            vec![
                Arc::new(StringArray::from(f_entity)),
                Arc::new(StringArray::from(f_date)),
                Arc::new(Float64Array::from(f_credit)),
                Arc::new(Float64Array::from(f_debit)),
            ],
        )
        .unwrap(),
    )
    .await;

    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE reports ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS period, net \
         FROM read_parquet('reports/*.parquet')$$;\n\
         DECLARE RECIPE flows ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, credit, debit \
         FROM read_parquet('flows/*.parquet')$$;",
    )
    .await;
    session
        .execute(
            "DECLARE RELATIONSHIP reports.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP flows.entity -> ledgers.id;",
        )
        .await
        .unwrap();

    session
        .execute("SELECT behavior_evidence() FROM reports.net;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(reports.net::behavior_evidence) \
                 WHERE state = 'current';",
        )
        .await
        .unwrap());
    let net: serde_json::Value = serde_json::from_str(&value).unwrap();
    let anchor = net["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["event"] == "flows")
        .cloned()
        .unwrap_or_else(|| panic!("no flows anchor in {net}"));
    assert_eq!(anchor["verdict"], "flow", "{anchor}");
    assert_eq!(anchor["convention"], "credit - debit", "{anchor}");
    assert_eq!(anchor["voted"], 3, "{anchor}");
    assert_eq!(anchor["sign"]["primary"], 3, "{anchor}");
}
