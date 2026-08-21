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
use glossql_session::{MAX_CHANNELS, NoRuntime, Outcome, Plane};

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

/// `USE` moves the statements after it onto another channel; each
/// channel's binding is fixed at construction and survives the switch.
#[tokio::test(flavor = "multi_thread")]
async fn use_selects_a_channel_and_never_rebinds_one() {
    let dir = tempfile::tempdir().unwrap();
    let plane = plane(dir.path()).await;
    let actor = agent("agent-1");

    plane
        .execute(
            actor.clone(),
            None,
            "DECLARE DATASET fin SET (purpose: 'p');\n\
             DECLARE DATASET ops SET (purpose: 'p');",
        )
        .await
        .unwrap();
    let on_fin = plane.channel(actor.clone(), Some("fin")).await.unwrap();
    let on_ops = plane.channel(actor.clone(), Some("ops")).await.unwrap();
    assert_eq!(on_fin.dataset().as_deref(), Some("fin"));
    // Two channels, two bindings — a `USE` between them moves the
    // statements, never a session.
    assert_eq!(on_ops.dataset().as_deref(), Some("ops"));

    // An unknown dataset refuses at the switch, and what ran before it
    // stands.
    let refused = plane
        .execute(actor.clone(), Some("fin"), "SELECT 1; USE nope;")
        .await
        .expect_err("USE of an undeclared dataset must refuse");
    assert!(format!("{refused}").contains("nope"), "{refused}");
    assert_eq!(on_fin.dataset().as_deref(), Some("fin"));
}

/// Where a call arrives is where it lands, and a `USE` inside one
/// expires with it. Nothing on the plane remembers a caller's last
/// dataset: the URL says it every time, so a restart cannot lose it and
/// a second caller cannot move it.
#[tokio::test(flavor = "multi_thread")]
async fn a_use_does_not_outlive_its_call() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;
    let plane = plane(dir.path()).await;
    let actor = agent("agent-1");

    // `orders` exists in fin and nowhere else, so an unprefixed name is
    // the whole observation: it resolves exactly when the statement is
    // running on fin.
    plane
        .execute(
            actor.clone(),
            None,
            &format!(
                "DECLARE DATASET fin SET (purpose: 'p');\n\
                 DECLARE DATASET ops SET (purpose: 'p');\n\
                 USE fin;\n\
                 DECLARE SOURCE erp_export SET (type: parquet, location: '{}');\n\
                 DECLARE RECIPE orders ON fin FROM erp_export AS $$\
                   SELECT order_id FROM read_parquet('orders/*.parquet')$$;",
                erp_root.display()
            ),
        )
        .await
        .unwrap();

    // Inside one call the `USE` decides, whatever the call arrived on.
    let moved = plane
        .execute(
            actor.clone(),
            Some("ops"),
            "USE fin; SELECT count(*) FROM orders;",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&moved), "3");

    // The next call arrives on its own dataset with no memory of that
    // one — the whole reason nothing here is keyed by a connection.
    plane
        .execute(actor.clone(), Some("ops"), "SELECT count(*) FROM orders;")
        .await
        .expect_err("a `USE` from an earlier call must not still be in force");
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
            None,
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
        .execute(
            agent("analyst"),
            None,
            "USE fin; SELECT count(*) FROM orders;",
        )
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
            None,
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
            None,
            r#"USE fin;
               DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;
               GLOSS note ON orders AS $${"value": "landed"}$$;"#,
        )
        .await
        .unwrap();
    // No USE on the reads below: `USE` clears the session's own cache,
    // which would mask exactly the defect this test pins. The call
    // arrives already bound instead, which is how every door reaches
    // the same channel again.
    let before = plane
        .execute(
            agent("analyst"),
            Some("fin"),
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
            None,
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
            Some("fin"),
            "SELECT state FROM GLOSSARY(orders) WHERE aspect = 'note';",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&after), "stale");
}

/// The pool is bounded. Its key's actor half is not bounded by anything
/// the server controls — a door serving untokened calls takes the
/// client's own name for it — so an unbounded map is one loop away from
/// a process that grows until it dies. Eviction costs a cold DataFusion
/// context and nothing else: a channel is a cache in front of the store,
/// and every read re-derives.
#[tokio::test(flavor = "multi_thread")]
async fn the_channel_pool_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let plane = plane(dir.path()).await;
    for i in 0..MAX_CHANNELS + 200 {
        plane.channel(agent(&format!("a{i}")), None).await.unwrap();
    }
    assert!(
        plane.channel_count().await <= MAX_CHANNELS,
        "{} channels stand past a cap of {MAX_CHANNELS}",
        plane.channel_count().await
    );

    // And an evicted channel is not a lost one: asked for again, it is
    // rebuilt and serves.
    let again = plane.channel(agent("a0"), None).await.unwrap();
    assert_eq!(again.dataset(), None);
}
