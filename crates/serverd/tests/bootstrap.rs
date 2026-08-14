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
        id: glossql_serverd::HUMAN.into(),
    }
}

async fn count(plane: &Plane, sql: &str) -> String {
    let session = plane.session(human()).await.unwrap();
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
    let store = Store::open_memory().await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), None, Arc::new(NoRuntime)));

    bootstrap(&store, &plane, dir.path(), human())
        .await
        .unwrap();
    // Every boot calls it; the second changes nothing.
    bootstrap(&store, &plane, dir.path(), human())
        .await
        .unwrap();

    for name in [
        "profile.rhai",
        "outliers.rhai",
        "temporal.rhai",
        "relationships.rhai",
        "behavior_evidence.rhai",
        "dimension_relevance.rhai",
        "hierarchies.rhai",
        "grounding_collisions.rhai",
        "derivations.rhai",
        "coherence.rhai",
        "slot_entropy.rhai",
        "metric_bands.rhai",
        "metric_cube.rhai",
        "band_breach.rhai",
        "rate_tolerance.rhai",
    ] {
        assert!(dir.path().join("functions").join(name).exists(), "{name}");
    }

    assert_eq!(count(&plane, "SELECT count(*) FROM functions;").await, "15");
    // 11 measurement contracts + the KPI kit's 10 semantic aspects
    // + the ruling channel (2026-08-14) + the four app parts an agent
    // authors a surface with (2026-08-15).
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
