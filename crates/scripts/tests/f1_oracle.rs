//! The RelBench rel-f1 oracle (declared-FK truth) — and the regression
//! record for the performance rework. Three claims: the
//! relationship sweep finishes on an id-heavy schema (34 Int64 columns;
//! it previously timed out) and its candidates cover all 13 declared
//! FKs; detect_hierarchies sees the one real nest (circuits.location →
//! country — the near-key gate used to strip it); behavior_evidence
//! reads driver standings points as a STOCK against per-race results at
//! day grain (74 seasons, ~850 drivers — it previously hit the op
//! backstop). Skips when `~/glossql-data/f1` is absent.

use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::KernelRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
const BEHAVIOR_EVIDENCE: &str = include_str!("../functions/behavior_evidence.sql");
/// The shipped body, so the declaration carries what runs.
const HIERARCHIES: &str = include_str!("../functions/hierarchies.sql");
/// The shipped body, so the declaration carries what runs.
const RELATIONSHIPS: &str = include_str!("../functions/relationships.sql");

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

async fn measurement(
    session: &Session,
    subject: &str,
    function: &str,
    aspect: &str,
) -> serde_json::Value {
    session
        .execute(&format!("SELECT {function}() FROM {subject};"))
        .await
        .unwrap();
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY({subject}::{aspect}) WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

const TABLES: [&str; 9] = [
    "circuits",
    "constructor_results",
    "constructor_standings",
    "constructors",
    "drivers",
    "qualifying",
    "races",
    "results",
    "standings",
];

/// RelBench's own FK metadata — the declared truth the candidates must
/// cover (recall is the contract; extras are the judge's to reject).
const DECLARED_FKS: [(&str, &str); 13] = [
    (
        "constructor_results.constructorId",
        "constructors.constructorId",
    ),
    ("constructor_results.raceId", "races.raceId"),
    (
        "constructor_standings.constructorId",
        "constructors.constructorId",
    ),
    ("constructor_standings.raceId", "races.raceId"),
    ("qualifying.constructorId", "constructors.constructorId"),
    ("qualifying.driverId", "drivers.driverId"),
    ("qualifying.raceId", "races.raceId"),
    ("races.circuitId", "circuits.circuitId"),
    ("results.constructorId", "constructors.constructorId"),
    ("results.driverId", "drivers.driverId"),
    ("results.raceId", "races.raceId"),
    ("standings.driverId", "drivers.driverId"),
    ("standings.raceId", "races.raceId"),
];

#[tokio::test(flavor = "multi_thread")]
#[ignore = "relbench scale — an evaluation, not a unit invariant; run via `cargo test -- --ignored`"]
async fn rel_f1_grades_the_planes_at_scale() {
    let Ok(home) = std::env::var("HOME") else {
        eprintln!("skipping: no HOME");
        return;
    };
    let data = std::path::Path::new(&home).join("glossql-data/f1");
    if !data.join("results.parquet").exists() {
        eprintln!("skipping: ~/glossql-data/f1 not present");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
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

    let mut setup = format!(
        "DECLARE DATASET f1 SET (purpose: 'relbench oracle');\n\
         USE f1;\n\
         DECLARE SOURCE relbench SET (type: parquet, location: '{}');\n\
         DECLARE ASPECT relationship_candidates WITH $${{\n\
           \"type\": \"object\",\n\
           \"properties\": {{\"candidates\": {{\"type\": \"array\"}}}}\n\
         }}$$ AS MEASUREMENT ON DATASET;\n\
         DECLARE ASPECT hierarchy_candidates WITH $${{\n\
           \"type\": \"object\", \"required\": [\"applicable\"],\n\
           \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}}}}\n\
         }}$$ AS MEASUREMENT ON TABLE;\n\
         DECLARE ASPECT behavior_evidence WITH $${{\n\
           \"type\": \"object\", \"required\": [\"applicable\"],\n\
           \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}}}}\n\
         }}$$ AS MEASUREMENT ON COLUMN;\n\
         DECLARE FUNCTION detect_relationships FOR GLOBAL \
         AS $${RELATIONSHIPS}$$ \
 RETURNS relationship_candidates;\n\
         DECLARE FUNCTION detect_hierarchies FOR GLOBAL \
         AS $${HIERARCHIES}$$ RETURNS hierarchy_candidates;\n\
         DECLARE FUNCTION behavior_evidence FOR GLOBAL \
         AS $${BEHAVIOR_EVIDENCE}$$ \
 RETURNS behavior_evidence;\n",
        data.display()
    );
    for t in TABLES {
        setup.push_str(&format!(
            "DECLARE RECIPE {t} ON f1 FROM relbench AS \
             $$SELECT * FROM read_parquet('{t}.parquet')$$;\n"
        ));
    }
    session.execute(&setup).await.unwrap();

    // The sweep that used to time out: 34 Int64 columns across 9
    // tables, batched through the door.
    let t0 = std::time::Instant::now();
    let rels = measurement(
        &session,
        "f1",
        "detect_relationships",
        "relationship_candidates",
    )
    .await;
    eprintln!("detect_relationships: {:?}", t0.elapsed());
    let candidates = rels["candidates"].as_array().unwrap();
    for (from, to) in DECLARED_FKS {
        assert!(
            candidates
                .iter()
                .any(|c| c["from"] == from && c["to"] == to),
            "declared FK {from} -> {to} missing from {} candidates",
            candidates.len()
        );
    }

    // The nest the near-key gate used to strip: location (75 distinct
    // of 77) → country.
    let circuits = measurement(
        &session,
        "circuits",
        "detect_hierarchies",
        "hierarchy_candidates",
    )
    .await;
    assert_eq!(circuits["applicable"], true, "{circuits}");
    let nest = circuits["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["from"] == "location" && c["to"] == "country")
        .unwrap_or_else(|| panic!("no location -> country candidate in {circuits}"));
    assert_eq!(nest["kind"], "edge", "{nest}");

    // The declared truth goes in; the discriminator then reads the
    // season-cumulative standings points as a stock against per-race
    // results points, aligned per driver at day grain.
    let mut edges = String::new();
    for (from, to) in DECLARED_FKS {
        edges.push_str(&format!("DECLARE RELATIONSHIP {from} -> {to};\n"));
    }
    session.execute(&edges).await.unwrap();

    let t1 = std::time::Instant::now();
    let points = measurement(
        &session,
        "standings.points",
        "behavior_evidence",
        "behavior_evidence",
    )
    .await;
    eprintln!("behavior_evidence(standings.points): {:?}", t1.elapsed());
    assert_eq!(points["applicable"], true, "{points}");

    // Season standings are a stock that RESETS: at raw driver grain the
    // wrong-anchor gate abstains (every season boundary injects a full
    // season's error — honest, and itself evidence), and the
    // year-scoped anchor reconciles exactly.
    let raw = points["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| {
            a["event"] == "results"
                && a["scope"] == "none"
                && a["align"].as_str().unwrap().contains("driverId")
        })
        .unwrap_or_else(|| panic!("no raw results/driverId anchor in {points}"));
    assert_eq!(raw["verdict"], "abstain", "{raw}");
    let seasonal = points["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| {
            a["event"] == "results"
                && a["scope"] == "year"
                && a["align"].as_str().unwrap().contains("driverId")
        })
        .unwrap_or_else(|| panic!("no year-scoped results/driverId anchor in {points}"));
    assert_eq!(seasonal["grain"], "day", "{seasonal}");
    assert_eq!(seasonal["verdict"], "stock", "{seasonal}");
    assert_eq!(seasonal["convention"], "points", "{seasonal}");
}
