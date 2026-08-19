//! The plane's channels: sessions keyed (actor, dataset), `USE` as
//! channel selection, one shared lake mount behind all of them. The
//! server-shaped claims live here — a channel's binding never moves,
//! and what one actor lands another actor reads without any remount.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{NoRuntime, Outcome, Plane};

fn agent(id: &str) -> Actor {
    Actor {
        kind: ActorKind::Agent,
        id: id.into(),
    }
}

async fn parquet_fixture(root: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "99.90"])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .write_parquet(
            &root.join("orders").display().to_string(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
}

async fn plane(dir: &std::path::Path) -> Plane {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open(lake).await.unwrap();
    Plane::new(store, Arc::new(NoRuntime))
}

fn single_value(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// `USE` moves the actor's pointer between channels; each channel's
/// binding is fixed at construction and survives the switch.
#[tokio::test(flavor = "multi_thread")]
async fn use_selects_a_channel_and_never_rebinds_one() {
    let dir = tempfile::tempdir().unwrap();
    let plane = plane(dir.path()).await;
    let actor = agent("agent-1");

    plane
        .execute(
            actor.clone(),
            "DECLARE DATASET fin SET (purpose: 'p');\n\
             DECLARE DATASET ops SET (purpose: 'p');\n\
             USE fin;",
        )
        .await
        .unwrap();
    let on_fin = plane.session(actor.clone()).await.unwrap();
    assert_eq!(on_fin.dataset().as_deref(), Some("fin"));

    plane.execute(actor.clone(), "USE ops;").await.unwrap();
    let on_ops = plane.session(actor.clone()).await.unwrap();
    assert_eq!(on_ops.dataset().as_deref(), Some("ops"));
    // The fin channel did not move — it is a different session.
    assert_eq!(on_fin.dataset().as_deref(), Some("fin"));

    // An unknown dataset refuses at the switch; the pointer stays.
    plane
        .execute(actor.clone(), "USE nope;")
        .await
        .expect_err("USE of an undeclared dataset must refuse");
    let still = plane.session(actor).await.unwrap();
    assert_eq!(still.dataset().as_deref(), Some("ops"));
}

/// What one actor lands, another reads bare through their own channel —
/// the shared mount is the server shape: no per-session load, no wait
/// on somebody else's `USE`.
#[tokio::test(flavor = "multi_thread")]
async fn a_landed_table_reads_across_channels() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;
    let plane = plane(dir.path()).await;

    plane
        .execute(
            agent("engineer"),
            &format!(
                "DECLARE DATASET fin SET (purpose: 'p');\n\
                 USE fin;\n\
                 DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
                 DECLARE RECIPE orders ON fin FROM erp_export AS $$\
                   SELECT order_id, try_cast(amount AS DOUBLE) AS amount \
                   FROM read_parquet('orders/*.parquet')$$;",
                erp_root.display()
            ),
        )
        .await
        .unwrap();

    // A different actor, a fresh channel: the bare name resolves through
    // the default schema — no aliases, no remount, no USE re-run.
    let read = plane
        .execute(agent("analyst"), "USE fin; SELECT count(*) FROM orders;")
        .await
        .unwrap();
    assert_eq!(single_value(&read), "3");

    // And the app-door shape — a channel asked for by dataset, no USE
    // statement anywhere.
    let app = plane.channel(agent("app:cash"), Some("fin")).await.unwrap();
    let qualified = app
        .execute("SELECT sum(amount) FROM fin.orders;")
        .await
        .unwrap();
    assert_eq!(single_value(&qualified), "120.4");
}

/// One channel's re-land must stale every channel's collapsed reads,
/// not only the writer's (a channel pinning its own
/// snapshot view forever makes serve-and-mark lie to concurrent readers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_land_stales_other_channels_reads_too() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;
    let plane = plane(dir.path()).await;

    plane
        .execute(
            agent("engineer"),
            &format!(
                "DECLARE DATASET fin SET (purpose: 'p');\n\
                 USE fin;\n\
                 DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
                 DECLARE RECIPE orders ON fin FROM erp_export AS $$\
                   SELECT order_id, try_cast(amount AS DOUBLE) AS amount \
                   FROM read_parquet('orders/*.parquet')$$;",
                erp_root.display()
            ),
        )
        .await
        .unwrap();

    // A second channel glosses the landed table and reads back current —
    // populating that channel's own snapshot view.
    plane
        .execute(
            agent("analyst"),
            r#"USE fin;
               DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;
               GLOSS note ON orders AS $${"value": "landed"}$$;"#,
        )
        .await
        .unwrap();
    // No USE on the reads below: `USE` clears the session's own cache,
    // which would mask exactly the defect this test pins. The actor's
    // channel pointer persists across calls.
    let before = plane
        .execute(
            agent("analyst"),
            "SELECT state FROM GLOSSARY(orders) WHERE aspect = 'note';",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&before), "current");

    // The first channel supersedes the recipe — a fresh landing, a new
    // snapshot. The analyst channel's next read must mark the gloss
    // stale.
    plane
        .execute(
            agent("engineer"),
            "USE fin;\n\
             DECLARE RECIPE orders ON fin FROM erp_export AS $$\
               SELECT order_id, try_cast(amount AS DOUBLE) AS amount \
               FROM read_parquet('orders/*.parquet') WHERE order_id > 0$$;",
        )
        .await
        .unwrap();
    let after = plane
        .execute(
            agent("analyst"),
            "SELECT state FROM GLOSSARY(orders) WHERE aspect = 'note';",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&after), "stale");
}
