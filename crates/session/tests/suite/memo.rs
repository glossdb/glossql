//! The `next` read served from memory: one run per version and pin,
//! and a statement that plans by name loads nothing its walk holds.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Outcome, Session};

async fn parquet_fixture(root: &std::path::Path, table: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .write_parquet(
            &root.join(table).display().to_string(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
}

/// A workspace with `fin` bound, a parquet source at `root`, and
/// `orders` landed from it.
async fn workspace() -> (tempfile::TempDir, Session, Lake) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&root).unwrap();
    parquet_fixture(&root, "orders").await;
    parquet_fixture(&root, "customers").await;
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake.clone()).await.unwrap();
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap();
    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'memo');\n\
             USE fin;\n\
             DECLARE SOURCE erp SET (type: parquet, location: '{}');\n\
             DECLARE RECIPE orders ON fin FROM erp AS $$SELECT id, label FROM read_parquet('orders/*.parquet')$$;",
            root.display()
        ))
        .await
        .unwrap();
    (dir, session, lake)
}

async fn run(session: &Session, sql: &str) -> Vec<Outcome> {
    session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

fn count(outcomes: &[Outcome]) -> usize {
    match outcomes.last().unwrap() {
        Outcome::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// `next` runs once for a version and pin, however many reads ask;
/// a write of the record moves the version, a landing moves the pin,
/// and each is one more run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_read_runs_once_per_version_and_pin() {
    let (_dir, session, _lake) = workspace().await;
    let cache = session.shipped_cache();
    let before = cache.runs();

    let first = run(&session, "SELECT * FROM next;").await;
    assert!(count(&first) > 0, "next has rows");
    run(&session, "SELECT * FROM next WHERE surface = 'structure';").await;
    assert_eq!(cache.runs() - before, 1, "two reads, one run");

    run(
        &session,
        r#"DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;
           GLOSS note ON orders AS $${"text": "landed from the erp export"}$$;"#,
    )
    .await;
    run(&session, "SELECT * FROM next;").await;
    assert_eq!(
        cache.runs() - before,
        2,
        "a write of the record is one more run"
    );
    run(&session, "SELECT * FROM next;").await;
    assert_eq!(cache.runs() - before, 2, "and the entry serves again");

    run(
        &session,
        "DECLARE RECIPE customers ON fin FROM erp AS $$SELECT id, label FROM read_parquet('customers/*.parquet')$$;",
    )
    .await;
    let after = run(&session, "SELECT * FROM next;").await;
    assert_eq!(
        cache.runs() - before,
        3,
        "a landing moves the pin: one more run"
    );
    assert!(count(&after) > 0);
}

/// A landing and its `next` in one call: the read after the landing
/// runs at the new pin, not from the entry the call before it left,
/// and the call after that serves what it ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_in_the_same_call_is_seen_by_the_read_after_it() {
    let (_dir, session, _lake) = workspace().await;
    let cache = session.shipped_cache();
    let before = cache.runs();
    run(&session, "SELECT * FROM next;").await;
    assert_eq!(cache.runs() - before, 1);
    run(
        &session,
        "DECLARE RECIPE customers ON fin FROM erp AS $$SELECT id, label FROM read_parquet('customers/*.parquet')$$;\n\
         SELECT * FROM next;",
    )
    .await;
    assert_eq!(
        cache.runs() - before,
        2,
        "the landing moved the pin inside the call, so next ran again"
    );
    run(&session, "SELECT * FROM next;").await;
    assert_eq!(cache.runs() - before, 2, "the next call serves that run");
}

/// A statement that walks the dataset and then plans by name under
/// that walk — a cube build plans one query per series — walks once
/// and takes every named table from that walk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_by_name_takes_the_walk() {
    let (_dir, session, lake) = workspace().await;
    run(
        &session,
        r#"DECLARE ASPECT cube WITH $${"type": "object", "properties": {
             "resolution": {"default": "day"},
             "windows": {"type": "object", "properties": {"month": {"default": "48 months"}}}}}$$
           AS FACT ON DATASET;
           DECLARE ASPECT orders_seen WITH $${"title": "Orders seen"}$$ AS QUERY ON DATASET;
           GLOSS orders_seen ON fin AS $${"sql": "SELECT id, 1.0 AS value FROM orders"}$$;"#,
    )
    .await;
    let walks = lake.walk_count();
    run(&session, "SELECT * FROM metric_axes();").await;
    assert_eq!(lake.walk_count() - walks, 1, "the statement walks once");
}
