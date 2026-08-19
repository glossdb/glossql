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
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));

    bootstrap(&plane, human())
        .await
        .unwrap();
    // Every boot calls it; the second changes nothing.
    bootstrap(&plane, human())
        .await
        .unwrap();

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
                    .unwrap_or(&"")
                    .replace('\'', "''")
            ),
        )
        .await;
        assert!(!stored.is_empty(), "{name} is not stored whole");
        assert!(!stored.ends_with(".sql"), "{name} stored as a path");
    }

    assert_eq!(count(&plane, "SELECT count(*) FROM functions;").await, "15");
    // 11 measurement contracts + the KPI kit's 10 semantic aspects
    // + the ruling channel + the four app parts an agent
    // authors a surface with.
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
