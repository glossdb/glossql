//! A fresh workspace receives the shipped system: reference scripts
//! under functions/, the measurement library and the KPI kit declared
//! — and the second boot changes nothing.

use std::sync::Arc;

use glossql_glossary::{Actor, ActorKind, Store};
use glossql_serverd::{Plane, bootstrap};
use glossql_session::{NoRuntime, Outcome};

fn human() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: glossql_serverd::BOOTSTRAP.into(),
    }
}

async fn count(plane: &Plane, sql: &str) -> String {
    let session = plane.channel(human(), None).await.unwrap();
    let outcomes = session.execute(sql).await.unwrap();
    let Some(Outcome::Rows(batches)) = outcomes.into_iter().next_back() else {
        panic!("`{sql}` produced no rows");
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_workspace_receives_the_shipped_system() {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));

    bootstrap(&plane, human()).await.unwrap();
    // Every boot calls it; the second changes nothing.
    bootstrap(&plane, human()).await.unwrap();

    // Nothing lands on disk (fixture 24): a body is data, so the
    // workspace keeps no `functions/` directory at all.
    assert!(!dir.path().join("functions").exists());

    // Every shipped body is in the table and is the script itself, not
    // a path to one — which is what makes the library readable as
    // examples over a door that has no filesystem.
    for (name, text) in glossql_scripts::library::SCRIPTS {
        let stored = count(
            &plane,
            &format!(
                "SELECT script FROM functions WHERE script LIKE '%{}%' LIMIT 1;",
                text.lines()
                    .find(|l| l.starts_with("//!"))
                    .unwrap_or("")
                    .replace('\'', "''")
            ),
        )
        .await;
        assert!(!stored.is_empty(), "{name} is not stored whole");
        assert!(!stored.ends_with(".sql"), "{name} stored as a path");
    }

    assert_eq!(count(&plane, "SELECT count(*) FROM functions;").await, "14");
    // 10 measurement contracts + the KPI kit's 11 semantic aspects
    // (the cube's floor and ladder among them) + the ruling channel +
    // the four app parts an agent authors a surface with.
    assert_eq!(count(&plane, "SELECT count(*) FROM aspects;").await, "27");
    // The functions without RETURNS are the detectors.
    assert_eq!(
        count(
            &plane,
            "SELECT count(*) FROM functions WHERE returns IS NULL;"
        )
        .await,
        "3"
    );
    // The kit's witnesses: the semantic six plus bands_w.
    assert_eq!(count(&plane, "SELECT count(*) FROM witnesses;").await, "7");
    // Owed questions stand on these from boot — nothing hand-declared.
    assert_eq!(
        count(
            &plane,
            "SELECT count(*) FROM witnesses WHERE aspect IN ('behavior', 'unit');"
        )
        .await,
        "2"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn column_evidence_is_owed_by_role() {
    // The library's column evidence conditions on the kit's `role`:
    // a role-less column owes only its profile and the role judgment,
    // and each role judgment opens exactly the evidence it makes
    // meaningful.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("ledger.csv"),
        "month,value\n2026-01-01,10.5\n2026-02-01,4.0\n",
    )
    .unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));
    bootstrap(&plane, human()).await.unwrap();

    let session = plane.channel(human(), None).await.unwrap();
    session
        .execute(&format!(
            "DECLARE DATASET perf SET (purpose: 'owed by role');\n\
             USE perf;\n\
             DECLARE SOURCE erp SET (type: csv, location: '{}');\n\
             DECLARE RECIPE ledger ON perf FROM erp AS \
             $$SELECT CAST(month AS DATE) AS month, CAST(value AS DOUBLE) AS value \
             FROM read_csv('ledger.csv')$$;",
            dir.path().display()
        ))
        .await
        .unwrap();

    let owed = |aspects: &'static str| {
        format!(
            "USE perf;\n\
             SELECT count(*) FROM GLOSSARY(ledger) \
             WHERE state = 'unassessed' AND aspect IN ({aspects});"
        )
    };
    let evidence =
        "'behavior_evidence', 'outlier_profile', 'temporal_profile', 'dimension_relevance'";

    // Role-less: the profile is owed on both columns, the evidence on
    // none.
    assert_eq!(count(&plane, &owed("'column_profile'")).await, "2");
    assert_eq!(count(&plane, &owed(evidence)).await, "0");

    // The measure judgment opens the measure evidence, on that column
    // alone.
    count(
        &plane,
        "USE perf;\nGLOSS role ON ledger.value AS $${\"value\": \"measure\"}$$;\n\
         SELECT count(*) FROM GLOSSARY(ledger.value) \
         WHERE state = 'unassessed' \
         AND aspect IN ('behavior_evidence', 'outlier_profile');",
    )
    .await;
    assert_eq!(
        count(
            &plane,
            "USE perf;\nSELECT count(*) FROM GLOSSARY(ledger.value) \
             WHERE state = 'unassessed' \
             AND aspect IN ('behavior_evidence', 'outlier_profile');"
        )
        .await,
        "2"
    );
    assert_eq!(
        count(&plane, &owed("'temporal_profile', 'dimension_relevance'")).await,
        "0"
    );

    // The timestamp judgment opens the temporal read on its column.
    assert_eq!(
        count(
            &plane,
            "USE perf;\nGLOSS role ON ledger.month AS $${\"value\": \"timestamp\"}$$;\n\
             SELECT count(*) FROM GLOSSARY(ledger.month) \
             WHERE state = 'unassessed' AND aspect = 'temporal_profile';"
        )
        .await,
        "1"
    );
}

/// The shipped system lands as one sequence: one append per relation
/// it touches, however many declarations it carries — the whole bill
/// on a catalog charging a round trip per commit.
#[tokio::test(flavor = "multi_thread")]
async fn the_shipped_system_lands_one_append_per_relation() {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));
    bootstrap(&plane, human()).await.unwrap();
    let mut appends = std::collections::HashMap::new();
    for l in store.lake().landings("glossql").await.unwrap() {
        *appends.entry(l.table).or_insert(0usize) += 1;
    }
    assert!(appends.len() >= 3, "{appends:?}");
    for (table, n) in &appends {
        assert_eq!(*n, 1, "`{table}` landed {n} times: {appends:?}");
    }
}
