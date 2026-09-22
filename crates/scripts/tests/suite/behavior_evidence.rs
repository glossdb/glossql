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
use glossql_scripts::KernelRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
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
        Outcome::Rows { batches, .. } => {
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
    let store = Store::open(lake.clone()).await.unwrap();
    let session = Session::new(
        store.clone(),
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(KernelRuntime::native()));

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
             AS $${BEHAVIOR_EVIDENCE}$$ \
 RETURNS behavior_evidence;\n\
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

    // Declaring the edges moves the workspace pin, so the cached
    // abstention no longer serves — the next call recomputes, no manual
    // cache delete.
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

    // The summary is what extraction serves, and it has to carry the
    // whole verdict: run 4 read 102 anchors — 60KB of context — to
    // learn one word, and would have paid that per measure column.
    // The winner is the best-supported anchor; an all-abstain body
    // still says why, because the reason names the ladder rung.
    let summary = &balance["summary"];
    assert_eq!(summary["verdict"], "stock", "{summary}");
    assert_eq!(summary["convention"], "amount", "{summary}");
    assert_eq!(summary["voted"], 3, "{summary}");
    assert!(summary["support"].as_f64().unwrap() > 0.0, "{summary}");
    assert!(summary["anchors"].as_u64().unwrap() >= 1, "{summary}");
    assert_eq!(
        summary["verdict"],
        moves_anchor(&balance)["verdict"],
        "{summary}"
    );
    assert_eq!(turnover["summary"]["verdict"], "flow", "{turnover}");
    assert_eq!(noise["summary"]["verdict"], "abstain", "{noise}");
    assert_eq!(noise["summary"]["decided"], 0, "{noise}");
    assert!(
        noise["summary"]["reason"]
            .as_str()
            .unwrap()
            .contains("no entity series reconciled"),
        "an abstention must say why: {noise}"
    );
}

/// A session over its own root with the behavior declarations landed —
/// the shared spelling of the tests below.
async fn behavior_session(dir: &std::path::Path, recipes: &str) -> Session {
    let root = dir.join("lake/erp");
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
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
    .with_runtime(Arc::new(KernelRuntime::native()));
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
             AS $${BEHAVIOR_EVIDENCE}$$ \
 RETURNS behavior_evidence;\n\
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

/// Five drivers over three seasons of eight rounds: `season_wins` is
/// the running count of wins within a season, reset every year, and
/// nothing in `results` reconciles against it — positions and lap
/// counts, no wins column. The shape is the only evidence there is.
/// Wins are sparse: only `a` and `b` ever win — `c`, `d` and `e` sit
/// flat at zero all three seasons (and never finish, so they are
/// absent from `results`). The flat majority must not outvote the
/// movers.
async fn standings_fixture(root: &std::path::Path) {
    write_table(
        root,
        "drivers",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]))],
        )
        .unwrap(),
    )
    .await;
    let (mut s_driver, mut s_date, mut s_wins) = (Vec::new(), Vec::new(), Vec::new());
    let (mut r_driver, mut r_date, mut r_position, mut r_laps) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for year in 2022..=2024 {
        let mut wins = [0.0f64, 0.0];
        for round in 1..=8u32 {
            let date = format!("{year}-{:02}-15", round + 2);
            let winner = if round % 2 == 1 { 0 } else { 1 };
            wins[winner] += 1.0;
            for (i, d) in ["a", "b"].iter().enumerate() {
                s_driver.push(*d);
                s_date.push(date.clone());
                s_wins.push(wins[i]);
                r_driver.push(*d);
                r_date.push(date.clone());
                r_position.push(if i == winner { 1.0 } else { 2.0 });
                r_laps.push(50.0 + round as f64);
            }
            for d in ["c", "d", "e"] {
                s_driver.push(d);
                s_date.push(date.clone());
                s_wins.push(0.0);
            }
        }
    }
    write_table(
        root,
        "standings",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("driver", DataType::Utf8, true),
                Field::new("race_date", DataType::Utf8, true),
                Field::new("season_wins", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(s_driver)),
                Arc::new(StringArray::from(s_date)),
                Arc::new(Float64Array::from(s_wins)),
            ],
        )
        .unwrap(),
    )
    .await;
    write_table(
        root,
        "results",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("driver", DataType::Utf8, true),
                Field::new("race_date", DataType::Utf8, true),
                Field::new("position", DataType::Float64, true),
                Field::new("laps", DataType::Float64, true),
            ])),
            vec![
                Arc::new(StringArray::from(r_driver)),
                Arc::new(StringArray::from(r_date)),
                Arc::new(Float64Array::from(r_position)),
                Arc::new(Float64Array::from(r_laps)),
            ],
        )
        .unwrap(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cumulative_that_resets_yearly_is_a_stock_by_its_shape_inside_the_year() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    standings_fixture(&root).await;
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE drivers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('drivers/*.parquet')$$;\n\
         DECLARE RECIPE standings ON fin FROM erp_export AS \
         $$SELECT driver, CAST(race_date AS DATE) AS race_date, season_wins \
         FROM read_parquet('standings/*.parquet')$$;\n\
         DECLARE RECIPE results ON fin FROM erp_export AS \
         $$SELECT driver, CAST(race_date AS DATE) AS race_date, position, laps \
         FROM read_parquet('results/*.parquet')$$;\n\
         DECLARE RELATIONSHIP standings.driver -> drivers.id;\n\
         DECLARE RELATIONSHIP results.driver -> drivers.id;",
    )
    .await;

    session
        .execute("SELECT behavior_evidence() FROM standings.season_wins;")
        .await
        .unwrap();
    let value = one(&session
        .execute(
            "SELECT value FROM GLOSSARY(standings.season_wins::behavior_evidence) \
             WHERE state = 'current';",
        )
        .await
        .unwrap());
    let evidence: serde_json::Value = serde_json::from_str(&value).unwrap();
    let anchors = evidence["anchors"].as_array().unwrap();
    let monotone = |scope: &str| {
        anchors
            .iter()
            .find(|a| a["convention"] == "monotone" && a["scope"] == scope)
            .cloned()
            .unwrap_or_else(|| panic!("no monotone anchor at scope {scope} in {evidence}"))
    };

    // Across seasons the count falls back to zero every March: the raw
    // scope sees every entity decrease and abstains, saying so.
    let raw = monotone("none");
    assert_eq!(raw["verdict"], "abstain", "{raw}");
    assert!(
        raw["reason"]
            .as_str()
            .unwrap()
            .contains("decrease within the scope"),
        "{raw}"
    );

    // Inside a season it only ever rises. Fifteen driver-seasons carry
    // 4+ periods, but nine of them are flat at zero — only the six
    // that move vote, every one monotone, and the anchor votes stock
    // on the movers alone: the flat majority is not counter-evidence.
    let yearly = monotone("year");
    assert_eq!(yearly["verdict"], "stock", "{yearly}");
    assert_eq!(yearly["entities"], 15, "{yearly}");
    assert_eq!(yearly["voted"], 6, "{yearly}");
    assert_eq!(yearly["agreement"], 1.0, "{yearly}");

    // Nothing in `results` reconciles a win count, so the shape is what
    // the summary serves — and it names the scope the stock holds in.
    let summary = &evidence["summary"];
    assert_eq!(summary["verdict"], "stock", "{summary}");
    assert_eq!(summary["convention"], "monotone", "{summary}");
    assert_eq!(summary["scope"], "year", "{summary}");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_equal_support_anchors_elect_by_name_and_the_summary_says_so() {
    // Two event tables carry the same movements, so both anchors
    // reconcile the balance identically — equal support, equal vote,
    // the same convention. The election used to fall through exact
    // float equality to iteration order; now the name decides,
    // and the summary says which anchor won and by which rule.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;

    let mut c_entity = Vec::new();
    let mut c_date = Vec::new();
    let mut c_amount = Vec::new();
    for (e, moves) in ENTITIES.iter().zip(MOVEMENTS) {
        for (month, mv) in MONTHS.iter().zip(moves) {
            let mid = format!("{}15", &month[..8]);
            c_entity.push(*e);
            c_date.push(month.to_string());
            c_amount.push(mv - 1.0);
            c_entity.push(*e);
            c_date.push(mid);
            c_amount.push(1.0);
        }
    }
    let credits = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    write_table(
        &root,
        "credits",
        RecordBatch::try_new(
            credits,
            vec![
                Arc::new(StringArray::from(c_entity)),
                Arc::new(StringArray::from(c_date)),
                Arc::new(Float64Array::from(c_amount)),
            ],
        )
        .unwrap(),
    )
    .await;

    // `moves` is declared first: an election left to iteration order
    // would keep it. The name rule elects `credits`.
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE positions ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS period, balance, turnover, noise \
         FROM read_parquet('positions/*.parquet')$$;\n\
         DECLARE RECIPE moves ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, amount \
         FROM read_parquet('moves/*.parquet')$$;\n\
         DECLARE RECIPE credits ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, amount \
         FROM read_parquet('credits/*.parquet')$$;\n\
         DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
         DECLARE RELATIONSHIP moves.entity -> ledgers.id;\n\
         DECLARE RELATIONSHIP credits.entity -> ledgers.id;",
    )
    .await;

    let balance = evidence(&session, "balance").await;
    assert_eq!(balance["applicable"], true, "{balance}");
    let anchor = |event: &str| {
        balance["anchors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["event"] == event)
            .cloned()
            .unwrap_or_else(|| panic!("no {event} anchor in {balance}"))
    };
    let (m, c) = (anchor("moves"), anchor("credits"));
    assert_eq!(m["verdict"], "stock", "{m}");
    assert_eq!(c["verdict"], "stock", "{c}");
    assert_eq!(m["support"], c["support"], "the tie is by construction");
    assert_eq!(m["voted"], c["voted"], "the tie is by construction");

    let summary = &balance["summary"];
    assert_eq!(summary["verdict"], "stock", "{summary}");
    assert_eq!(summary["event"], "credits", "{summary}");
    assert_eq!(summary["tiebreak"], "event-name", "{summary}");
}

/// A grounding write measures the column its value sums: the verdict
/// lands with the write, the fact row folds by it, and nobody had to
/// call the door.
#[tokio::test(flavor = "multi_thread")]
async fn the_grounding_write_measures_the_column_it_sums() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE positions ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS period, balance, turnover, noise \
         FROM read_parquet('positions/*.parquet')$$;\n\
         DECLARE RECIPE moves ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, amount \
         FROM read_parquet('moves/*.parquet')$$;\n\
         DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
         DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
    )
    .await;
    // The shipped cube aspect: the fact row is computed under it.
    let kit = glossql_scripts::library::KIT;
    let start = kit.find("DECLARE ASPECT cube").unwrap();
    let len = kit[start..].find("AS FACT ON DATASET;").unwrap() + "AS FACT ON DATASET;".len();
    session.execute(&kit[start..start + len]).await.unwrap();
    // A judged time axis, so the fact row is a series and carries its
    // verb; the judge is a stub, the door under test is the real one.
    session
        .execute(
            r#"DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
                 "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
               DECLARE FUNCTION judge_time FOR GLOBAL AS
                 $$SELECT true AS applicable, 'month' AS granularity,
                          named_struct('ratio', 1.0) AS completeness$$
                 RETURNS temporal_profile;
               SELECT judge_time() FROM positions.period;
               DECLARE ASPECT closing WITH $${"title": "Closing balance"}$$ AS QUERY;
               GLOSS closing ON fin AS $${"sql": "SELECT period AS date, sum(balance) AS value FROM positions GROUP BY period"}$$;"#,
        )
        .await
        .unwrap();
    let landed = one(&session
        .execute(
            "SELECT count(*) FROM GLOSSARY(positions.balance::behavior_evidence) \
             WHERE state = 'current';",
        )
        .await
        .unwrap());
    assert_eq!(landed, "1", "the write landed the verdict");
    let verb = one(&session
        .execute(
            "SELECT behavior || ':' || behavior_basis FROM metric_axes() WHERE metric = 'closing';",
        )
        .await
        .unwrap());
    assert_eq!(verb, "stock:evidence");
}

/// An alignment whose two sides share entities but no period — the
/// event rows sit ten years off the measure's — joins to nothing. The
/// kernel reads an empty alignment and the anchor abstains; the door
/// answers, and the monotone read still speaks.
#[tokio::test(flavor = "multi_thread")]
async fn an_alignment_with_no_period_in_common_abstains_instead_of_refusing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE positions ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS period, balance, turnover, noise \
         FROM read_parquet('positions/*.parquet')$$;\n\
         DECLARE RECIPE moves ON fin FROM erp_export AS \
         $$SELECT entity, CAST(CAST(d AS DATE) + INTERVAL '10 years' AS DATE) AS d, amount \
         FROM read_parquet('moves/*.parquet')$$;\n\
         DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
         DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
    )
    .await;
    let balance = evidence(&session, "balance").await;
    assert_eq!(balance["applicable"], true, "{balance}");
    let anchor = moves_anchor(&balance);
    assert_eq!(anchor["verdict"], "abstain", "{anchor}");
    assert!(
        anchor["reason"]
            .as_str()
            .is_some_and(|r| r.contains("no entity series reconciled")),
        "{anchor}"
    );
}

/// The write's measurement reaches a column whose name needs quoting:
/// the verb's descent names the column as the table spells it, and
/// the door runs over it.
#[tokio::test(flavor = "multi_thread")]
async fn the_grounding_write_measures_a_quoted_column() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
         DECLARE RECIPE positions ON fin FROM erp_export AS \
         $$SELECT entity, CAST(period AS DATE) AS \"Period\", balance AS \"Balance\", \
         turnover, noise FROM read_parquet('positions/*.parquet')$$;\n\
         DECLARE RECIPE moves ON fin FROM erp_export AS \
         $$SELECT entity, CAST(d AS DATE) AS d, amount \
         FROM read_parquet('moves/*.parquet')$$;\n\
         DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
         DECLARE RELATIONSHIP moves.entity -> ledgers.id;",
    )
    .await;
    let kit = glossql_scripts::library::KIT;
    let start = kit.find("DECLARE ASPECT cube").unwrap();
    let len = kit[start..].find("AS FACT ON DATASET;").unwrap() + "AS FACT ON DATASET;".len();
    session.execute(&kit[start..start + len]).await.unwrap();
    session
        .execute(
            r##"DECLARE ASPECT temporal_profile WITH $${"type": "object", "required": ["applicable"],
                 "properties": {"applicable": {"type": "boolean"}}}$$ AS MEASUREMENT ON COLUMN;
               DECLARE FUNCTION judge_time FOR GLOBAL AS
                 $$SELECT true AS applicable, 'month' AS granularity,
                          named_struct('ratio', 1.0) AS completeness$$
                 RETURNS temporal_profile;
               SELECT judge_time() FROM positions."Period";
               DECLARE ASPECT closing WITH $${"title": "Closing balance"}$$ AS QUERY;
               GLOSS closing ON fin AS $${"sql": "SELECT \"Period\" AS date, sum(\"Balance\") AS value FROM positions GROUP BY \"Period\""}$$;"##,
        )
        .await
        .unwrap();
    let landed = one(&session
        .execute(
            "SELECT count(*) FROM GLOSSARY(positions.\"Balance\"::behavior_evidence) \
             WHERE state = 'current';",
        )
        .await
        .unwrap());
    assert_eq!(landed, "1", "the write landed the verdict");
    let verb = one(&session
        .execute(
            "SELECT behavior || ':' || behavior_basis FROM metric_axes() WHERE metric = 'closing';",
        )
        .await
        .unwrap());
    assert_eq!(verb, "stock:evidence");
}

/// An event table wider than one reconciliation takes — here `moves`
/// with 65 numeric columns beside the identifier — is not reconciled:
/// its anchor abstains and names the count, the door still answers,
/// and with every anchor abstaining the summary carries that reason.
#[tokio::test(flavor = "multi_thread")]
async fn an_event_table_wider_than_the_kernel_takes_abstains_with_the_count() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    fixture(&root).await;
    let wide: String = (1..=64).map(|i| format!(", amount AS a{i}")).collect();
    let session = behavior_session(
        dir.path(),
        &format!(
            "DECLARE RECIPE ledgers ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('ledgers/*.parquet')$$;\n\
             DECLARE RECIPE positions ON fin FROM erp_export AS \
             $$SELECT entity, CAST(period AS DATE) AS period, balance, turnover, noise \
             FROM read_parquet('positions/*.parquet')$$;\n\
             DECLARE RECIPE moves ON fin FROM erp_export AS \
             $$SELECT entity, CAST(d AS DATE) AS d, amount{wide} \
             FROM read_parquet('moves/*.parquet')$$;\n\
             DECLARE RELATIONSHIP positions.entity -> ledgers.id;\n\
             DECLARE RELATIONSHIP moves.entity -> ledgers.id;"
        ),
    )
    .await;
    let balance = evidence(&session, "balance").await;
    assert_eq!(balance["applicable"], true, "{balance}");
    let anchor = moves_anchor(&balance);
    assert_eq!(anchor["verdict"], "abstain", "{anchor}");
    assert_eq!(
        anchor["reason"], "65 movement terms on moves, above the 64 one reconciliation takes",
        "{anchor}"
    );
    assert_eq!(balance["summary"]["verdict"], "abstain", "{balance}");
    assert_eq!(
        balance["summary"]["reason"],
        "65 movement terms on moves, above the 64 one reconciliation takes",
        "{balance}"
    );
}

/// Truth by construction, positive-only: five warehouses over twelve
/// months, `movements` with `received` and `issued` drawn from
/// 100..900, `stock_levels.on_hand` the running sum of the month's
/// `received − issued` from 3000, `net_movement` the month's net,
/// `noise` drawn per row at the movements' size, `reorder_point` the
/// constant 500, and `mixed` the net for two warehouses and noise for
/// three. `move_id` is a unique integer, a key by shape.
async fn stockroom_fixture(root: &std::path::Path) {
    const WAREHOUSES: [&str; 5] = ["w1", "w2", "w3", "w4", "w5"];
    let mut seed: u64 = 20260909;
    let mut draw = move || -> f64 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        100.0 + ((seed >> 33) % 801) as f64
    };

    let warehouses = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    write_table(
        root,
        "warehouses",
        RecordBatch::try_new(
            warehouses,
            vec![Arc::new(StringArray::from(WAREHOUSES.to_vec()))],
        )
        .unwrap(),
    )
    .await;

    let (mut l_wh, mut l_period) = (Vec::new(), Vec::new());
    let (mut l_on_hand, mut l_net, mut l_noise, mut l_reorder, mut l_mixed) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let (mut m_id, mut m_wh, mut m_day, mut m_received, mut m_issued) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut next_id: i64 = 0;
    for (w, wh) in WAREHOUSES.iter().enumerate() {
        let mut on_hand = 3000.0;
        for month in 1..=12 {
            let (mut received, mut issued) = (0.0, 0.0);
            for day in ["01", "15"] {
                let (r, i) = (draw(), draw());
                received += r;
                issued += i;
                next_id += 1;
                m_id.push(next_id);
                m_wh.push(*wh);
                m_day.push(format!("2025-{month:02}-{day}"));
                m_received.push(r);
                m_issued.push(i);
            }
            let net = received - issued;
            on_hand += net;
            let noise = draw() + draw();
            l_wh.push(*wh);
            l_period.push(format!("2025-{month:02}-01"));
            l_on_hand.push(on_hand);
            l_net.push(net);
            l_noise.push(noise);
            l_reorder.push(500.0);
            l_mixed.push(if w < 2 { net } else { noise });
        }
    }
    let levels = Arc::new(Schema::new(vec![
        Field::new("warehouse", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("on_hand", DataType::Float64, true),
        Field::new("net_movement", DataType::Float64, true),
        Field::new("noise", DataType::Float64, true),
        Field::new("reorder_point", DataType::Float64, true),
        Field::new("mixed", DataType::Float64, true),
    ]));
    write_table(
        root,
        "stock_levels",
        RecordBatch::try_new(
            levels,
            vec![
                Arc::new(StringArray::from(l_wh)),
                Arc::new(StringArray::from(l_period)),
                Arc::new(Float64Array::from(l_on_hand)),
                Arc::new(Float64Array::from(l_net)),
                Arc::new(Float64Array::from(l_noise)),
                Arc::new(Float64Array::from(l_reorder)),
                Arc::new(Float64Array::from(l_mixed)),
            ],
        )
        .unwrap(),
    )
    .await;
    let movements = Arc::new(Schema::new(vec![
        Field::new("move_id", DataType::Int64, true),
        Field::new("warehouse", DataType::Utf8, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("received", DataType::Float64, true),
        Field::new("issued", DataType::Float64, true),
    ]));
    write_table(
        root,
        "movements",
        RecordBatch::try_new(
            movements,
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(m_id)),
                Arc::new(StringArray::from(m_wh)),
                Arc::new(StringArray::from(m_day)),
                Arc::new(Float64Array::from(m_received)),
                Arc::new(Float64Array::from(m_issued)),
            ],
        )
        .unwrap(),
    )
    .await;
}

async fn evidence_on(session: &Session, subject: &str) -> serde_json::Value {
    session
        .execute(&format!("SELECT behavior_evidence() FROM {subject};"))
        .await
        .unwrap();
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY({subject}::behavior_evidence) WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

/// The first anchor on an event table — the reconciliation at the
/// unscoped alignment, which is pushed before its monotone reading.
fn first_anchor<'a>(evidence: &'a serde_json::Value, event: &str) -> &'a serde_json::Value {
    evidence["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["event"] == event)
        .unwrap_or_else(|| panic!("no {event} anchor in {evidence}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn positive_only_movements_meet_the_null_model() {
    // The null model's case: every movement is
    // positive, so an unrelated column of the movements' size sits
    // near 0.4 on the flow residual and near 1.0 on the delta
    // residual. Under a gate of 0.5 that was a flow vote; under 0.05
    // it is an abstention. The reconciliations that exist stay exact,
    // the constant is a dead value, a key by shape leaves the pool,
    // and a fit carried by two warehouses of five stops at the
    // majority floor.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    stockroom_fixture(&root).await;
    let session = behavior_session(
        dir.path(),
        "DECLARE RECIPE warehouses ON fin FROM erp_export AS \
         $$SELECT * FROM read_parquet('warehouses/*.parquet')$$;\n\
         DECLARE RECIPE stock_levels ON fin FROM erp_export AS \
         $$SELECT warehouse, CAST(period AS DATE) AS period, on_hand, net_movement, \
         noise, reorder_point, mixed FROM read_parquet('stock_levels/*.parquet')$$;\n\
         DECLARE RECIPE movements ON fin FROM erp_export AS \
         $$SELECT move_id, warehouse, CAST(d AS DATE) AS d, received, issued \
         FROM read_parquet('movements/*.parquet')$$;\n\
         DECLARE RELATIONSHIP stock_levels.warehouse -> warehouses.id;\n\
         DECLARE RELATIONSHIP movements.warehouse -> warehouses.id;",
    )
    .await;

    // The level: its delta is the month's net, exactly.
    let on_hand = evidence_on(&session, "stock_levels.on_hand").await;
    let a = first_anchor(&on_hand, "movements");
    assert_eq!(a["verdict"], "stock", "{a}");
    assert_eq!(a["convention"], "received - issued", "{a}");
    assert_eq!(a["voted"], 5, "{a}");
    assert!(a["r_stock"].as_f64().unwrap() < 0.01, "{a}");
    assert!(
        a["identifier_columns"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("move_id")),
        "a unique integer is an identifier, not a movement: {a}"
    );
    assert_eq!(on_hand["summary"]["verdict"], "stock", "{on_hand}");

    // The net itself: a flow at the same convention.
    let net = evidence_on(&session, "stock_levels.net_movement").await;
    let a = first_anchor(&net, "movements");
    assert_eq!(a["verdict"], "flow", "{a}");
    assert_eq!(a["convention"], "received - issued", "{a}");
    assert!(a["r_flow"].as_f64().unwrap() < 0.01, "{a}");

    // Noise of the movements' size: no entity votes.
    let noise = evidence_on(&session, "stock_levels.noise").await;
    assert_eq!(noise["summary"]["verdict"], "abstain", "{noise}");
    assert_eq!(noise["summary"]["decided"], 0, "{noise}");
    let a = first_anchor(&noise, "movements");
    assert!(
        a["reason"]
            .as_str()
            .unwrap()
            .contains("no entity series reconciled"),
        "{a}"
    );

    // A constant: a dead value, whatever it is compared with.
    let constant = evidence_on(&session, "stock_levels.reorder_point").await;
    assert_eq!(constant["summary"]["verdict"], "abstain", "{constant}");
    assert_eq!(constant["summary"]["decided"], 0, "{constant}");

    // Two warehouses of five reconcile exactly; the kernel's own
    // floor (two voters, agreement 0.8) would call it a flow, the
    // majority floor holds it back and says with what counts.
    let mixed = evidence_on(&session, "stock_levels.mixed").await;
    assert_eq!(mixed["summary"]["verdict"], "abstain", "{mixed}");
    assert!(
        mixed["summary"]["reason"]
            .as_str()
            .unwrap()
            .contains("under the majority floor"),
        "the summary carries the most-voted anchor's reason: {mixed}"
    );
    let a = first_anchor(&mixed, "movements");
    assert_eq!(a["verdict"], "abstain", "{a}");
    let reason = a["reason"].as_str().unwrap();
    assert!(
        reason.contains("2 of 5 common entities voted flow") && reason.contains("majority"),
        "{reason}"
    );
    assert_eq!(
        a["alternatives"][0]["verdict"], "abstain",
        "the runner-up field reads the floor too: {a}"
    );
}
