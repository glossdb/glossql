//! The dimensions plane's two measurements on data whose truth is by
//! construction. Relevance: a known bucket distribution scores exactly
//! coverage × Pielou, gates abstain near-keys, and the missing-profile
//! abstention heals through the ACCEPTS edge once the profile lands.
//! Hierarchies: a strict zip → city → state nest arrives with g3 = 0
//! and λ = 1, the reverse directions are screened, a code↔label
//! bijection arrives as an alias candidate, and a 98%-dominant flag —
//! the vacuous-skew class — arrives with its λ signature visible for
//! the judge to kill.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_session::{Outcome, Session};

/// The shipped body, so the declaration carries what runs.
const DIMENSION_RELEVANCE: &str = include_str!("../functions/dimension_relevance.rhai");
/// The shipped body, so the declaration carries what runs.
const HIERARCHIES: &str = include_str!("../functions/hierarchies.rhai");
/// The shipped body, so the declaration carries what runs.
const PROFILE: &str = include_str!("../functions/profile.rhai");

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

async fn fixture(root: &std::path::Path) {
    // survey.segment: 50 a / 30 b / 20 c / 10 NULL — relevance is
    // coverage 100/110 × Pielou([.5, .3, .2]) ≈ 0.8520. survey.id is a
    // per-row key the near-key gate must refuse.
    let mut ids = Vec::new();
    let mut segments: Vec<Option<&str>> = Vec::new();
    for i in 0..110 {
        ids.push(format!("r{i:03}"));
        segments.push(match i {
            0..=49 => Some("a"),
            50..=79 => Some("b"),
            80..=99 => Some("c"),
            _ => None,
        });
    }
    let survey = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("segment", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "survey",
        RecordBatch::try_new(
            survey,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(segments)),
            ],
        )
        .unwrap(),
    )
    .await;

    // geo: 27 zips × 4 rows; each zip in one city, each city in one
    // state; city_code is a bijection of city; flag is 106 A / 2 B.
    let mut zip = Vec::new();
    let mut city = Vec::new();
    let mut city_code = Vec::new();
    let mut state = Vec::new();
    let mut flag = Vec::new();
    for zi in 0..27 {
        for row in 0..4 {
            zip.push(format!("z{zi:02}"));
            city.push(format!("city_{}", zi / 3));
            city_code.push(format!("C{}", zi / 3));
            state.push(format!("state_{}", zi / 9));
            flag.push(if row == 0 && zi < 2 { "B" } else { "A" });
        }
    }
    let geo = Arc::new(Schema::new(vec![
        Field::new("zip", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("city_code", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("flag", DataType::Utf8, true),
    ]));
    write_table(
        root,
        "geo",
        RecordBatch::try_new(
            geo,
            vec![
                Arc::new(StringArray::from(zip)),
                Arc::new(StringArray::from(city)),
                Arc::new(StringArray::from(city_code)),
                Arc::new(StringArray::from(state)),
                Arc::new(StringArray::from(flag)),
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

async fn measure(session: &Session, function: &str, subject: &str) -> serde_json::Value {
    session
        .execute(&format!("SELECT {function}() FROM {subject};"))
        .await
        .unwrap();
    let aspect = match function {
        "dimension_relevance" => "dimension_relevance",
        "detect_hierarchies" => "hierarchy_candidates",
        other => other,
    };
    let value = one(&session
        .execute(&format!(
            "SELECT value FROM GLOSSARY({subject}::{aspect}) WHERE state = 'current';"
        ))
        .await
        .unwrap());
    serde_json::from_str(&value).unwrap()
}

fn candidate<'a>(
    evidence: &'a serde_json::Value,
    from: &str,
    to: &str,
) -> Option<&'a serde_json::Value> {
    evidence["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["from"] == from && c["to"] == to)
}

#[tokio::test(flavor = "multi_thread")]
async fn relevance_scores_the_distribution_and_hierarchies_arrive_with_their_evidence() {
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
            "DECLARE DATASET fin SET (purpose: 'dimensions');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
             DECLARE ASPECT column_profile WITH $${{\n\
               \"type\": \"object\", \"required\": [\"total\"],\n\
               \"properties\": {{\"total\": {{\"type\": \"integer\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE ASPECT dimension_relevance WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}}}}\n\
             }}$$ AS MEASUREMENT ON COLUMN;\n\
             DECLARE ASPECT hierarchy_candidates WITH $${{\n\
               \"type\": \"object\", \"required\": [\"applicable\"],\n\
               \"properties\": {{\"applicable\": {{\"type\": \"boolean\"}}}}\n\
             }}$$ AS MEASUREMENT ON TABLE;\n\
             DECLARE FUNCTION profile FOR GLOBAL \
             AS $${PROFILE}$$ RETURNS column_profile;\n\
             DECLARE FUNCTION dimension_relevance FOR GLOBAL \
             AS $${DIMENSION_RELEVANCE}$$ \
             ACCEPTS (column_profile) RETURNS dimension_relevance;\n\
             DECLARE FUNCTION detect_hierarchies FOR GLOBAL \
             AS $${HIERARCHIES}$$ RETURNS hierarchy_candidates;\n\
             DECLARE RECIPE survey ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('survey/*.parquet')$$;\n\
             DECLARE RECIPE geo ON fin FROM erp_export AS \
             $$SELECT * FROM read_parquet('geo/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();

    // Without a profile the score abstains and names its input; the
    // profile's landing heals the cached abstention through ACCEPTS.
    let unhealed = measure(&session, "dimension_relevance", "survey.segment").await;
    assert_eq!(unhealed["applicable"], false, "{unhealed}");
    assert_eq!(
        unhealed["missing_aspects"][0], "column_profile",
        "{unhealed}"
    );

    session
        .execute("SELECT profile() FROM survey.segment;")
        .await
        .unwrap();
    let scored = measure(&session, "dimension_relevance", "survey.segment").await;
    assert_eq!(scored["applicable"], true, "{scored}");
    let relevance = scored["relevance"].as_f64().unwrap();
    assert!((relevance - 0.8520).abs() < 0.001, "{scored}");
    assert_eq!(scored["groups"], 3, "{scored}");
    // The score is exact (profile entropy scalar) — the truncated
    // lower-bound flag is retired.
    assert!(scored.get("truncated").is_none(), "{scored}");

    // A per-row key is not an axis.
    session
        .execute("SELECT profile() FROM survey.id;")
        .await
        .unwrap();
    let key = measure(&session, "dimension_relevance", "survey.id").await;
    assert_eq!(key["applicable"], false, "{key}");
    assert!(
        key["reason"].as_str().unwrap().contains("near-key"),
        "{key}"
    );

    // The nest arrives with exact evidence; reversals are screened.
    let geo = measure(&session, "detect_hierarchies", "geo").await;
    assert_eq!(geo["applicable"], true, "{geo}");
    let zip_city = candidate(&geo, "zip", "city").unwrap();
    assert_eq!(zip_city["kind"], "edge", "{zip_city}");
    assert_eq!(zip_city["g3"], 0.0, "{zip_city}");
    assert_eq!(zip_city["lambda"], 1.0, "{zip_city}");
    let city_state = candidate(&geo, "city", "state").unwrap();
    assert_eq!(city_state["g3"], 0.0, "{city_state}");
    assert!(candidate(&geo, "city", "zip").is_none(), "{geo}");
    assert!(candidate(&geo, "state", "city").is_none(), "{geo}");

    // The bijection is an alias candidate in both directions — whether
    // relabeling or coincidence is the judge's call, never the script's.
    assert_eq!(
        candidate(&geo, "city", "city_code").unwrap()["kind"],
        "alias"
    );
    assert_eq!(
        candidate(&geo, "city_code", "city").unwrap()["kind"],
        "alias"
    );

    // The vacuous-skew class survives the g3 screen — and carries the
    // λ signature the judge kills it by.
    let vacuous = candidate(&geo, "zip", "flag").unwrap();
    assert!(vacuous["g3"].as_f64().unwrap() <= 0.05, "{vacuous}");
    assert!(vacuous["lambda"].as_f64().unwrap() < 0.5, "{vacuous}");
}
