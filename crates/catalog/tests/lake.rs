//! The two doors end to end: namespace → provider mount → CREATE via
//! `SchemaProvider::register_table` → append → snapshot id → read.
//!
//! Creating and reading go through iceberg-datafusion — the mounted
//! provider is what makes a table nameable in SQL at all. Writing does
//! not: it goes through [`Lake::append_batches`], because a landing's
//! facts ride the snapshot they describe and DataFusion's `INSERT INTO`
//! commits without them. This test takes the same two doors the server
//! takes, so the shapes it holds are the shapes in use.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]))
}

async fn mounted(lake: &Lake, ctx: &SessionContext, dataset: &str) -> Arc<dyn SchemaProvider> {
    let provider = lake.provider().await.unwrap();
    let schema = provider.schema(dataset).unwrap();
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema(dataset, Arc::clone(&schema))
        .unwrap();
    schema
}

#[tokio::test(flavor = "multi_thread")]
async fn create_and_read_through_the_provider_write_through_the_lake() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();

    assert!(
        lake.ensure_namespace("fin", Default::default())
            .await
            .unwrap()
    );
    assert!(
        !lake
            .ensure_namespace("fin", Default::default())
            .await
            .unwrap()
    );

    let ctx = SessionContext::new();
    let schema = mounted(&lake, &ctx, "fin").await;

    // CREATE: an empty shape through the provider — live, no rebuild.
    let empty = RecordBatch::new_empty(orders_schema());
    let shape = MemTable::try_new(orders_schema(), vec![vec![empty]]).unwrap();
    schema
        .register_table("orders".into(), Arc::new(shape))
        .unwrap();
    assert!(lake.table_exists("fin", "orders").await.unwrap());
    assert_eq!(lake.snapshot_id("fin", "orders").await.unwrap(), None);

    // WRITE: through the lake's own append, carrying a fact — which is
    // the reason this path exists rather than `INSERT INTO`.
    let batch = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "99.90"])),
        ],
    )
    .unwrap();
    lake.append_batches(
        "fin",
        "orders",
        std::slice::from_ref(&batch),
        HashMap::from([("glossql.source_rows".to_string(), "3".to_string())]),
    )
    .await
    .unwrap();

    let snapshot = lake.snapshot_id("fin", "orders").await.unwrap();
    assert!(snapshot.is_some(), "commit must produce a snapshot");
    let landings = lake.landings("fin").await.unwrap();
    let [landing] = landings.as_slice() else {
        panic!("one append, one landing, got {}", landings.len())
    };
    assert_eq!(
        landing.properties.get("glossql.source_rows"),
        Some(&"3".to_string()),
        "the fact rides the snapshot it describes — the reason this is \
         not `INSERT INTO`, whose commit carries no properties"
    );
    assert_eq!(landing.added_records, Some(3));

    // READ: fresh metadata per scan, no remount.
    let rows = ctx
        .sql("SELECT count(*) AS n FROM fin.orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        format!("{:?}", rows[0].column(0)),
        "PrimitiveArray<Int64>\n[\n  3,\n]"
    );

    // A second append moves the snapshot forward.
    lake.append_batches(
        "fin",
        "orders",
        std::slice::from_ref(&batch),
        HashMap::new(),
    )
    .await
    .unwrap();
    let later = lake.snapshot_id("fin", "orders").await.unwrap();
    assert!(later.is_some());
    assert_ne!(later, snapshot);

    // Reopening the workspace sees the same table.
    let reopened = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    assert_eq!(reopened.table_names("fin").await.unwrap(), vec!["orders"]);
    assert_eq!(reopened.snapshot_id("fin", "orders").await.unwrap(), later);
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_is_shared_until_a_namespace_lands() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    lake.ensure_namespace("fin", Default::default())
        .await
        .unwrap();

    // Every touch is the same mounted representation — an Arc clone,
    // never a rebuild — and clones of the Lake share it.
    let first = lake.provider().await.unwrap();
    let again = lake.provider().await.unwrap();
    assert!(Arc::ptr_eq(&first, &again));
    let through_clone = lake.clone().provider().await.unwrap();
    assert!(Arc::ptr_eq(&first, &through_clone));

    // A namespace create invalidates: the next touch rebuilds over the
    // current list, and the new dataset is visible.
    assert!(first.schema("ops").is_none());
    lake.ensure_namespace("ops", Default::default())
        .await
        .unwrap();
    let rebuilt = lake.provider().await.unwrap();
    assert!(!Arc::ptr_eq(&first, &rebuilt));
    assert!(rebuilt.schema("ops").is_some());
    assert!(rebuilt.schema("fin").is_some());
}
