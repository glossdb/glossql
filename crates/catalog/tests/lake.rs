//! The front door end to end: namespace → provider mount → CREATE via
//! `SchemaProvider::register_table` → `INSERT INTO` → snapshot id → read.

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
async fn front_door_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();

    assert!(lake.ensure_namespace("fin", Default::default()).await.unwrap());
    assert!(!lake.ensure_namespace("fin", Default::default()).await.unwrap());

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

    // WRITE: staged batches through DataFusion's INSERT path.
    let batch = RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "99.90"])),
        ],
    )
    .unwrap();
    let staged = MemTable::try_new(orders_schema(), vec![vec![batch]]).unwrap();
    ctx.register_table("__staged", Arc::new(staged)).unwrap();
    ctx.sql("INSERT INTO fin.orders SELECT * FROM __staged")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let snapshot = lake.snapshot_id("fin", "orders").await.unwrap();
    assert!(snapshot.is_some(), "commit must produce a snapshot");

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
    ctx.sql("INSERT INTO fin.orders SELECT * FROM __staged")
        .await
        .unwrap()
        .collect()
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
    lake.ensure_namespace("fin", Default::default()).await.unwrap();

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
    lake.ensure_namespace("ops", Default::default()).await.unwrap();
    let rebuilt = lake.provider().await.unwrap();
    assert!(!Arc::ptr_eq(&first, &rebuilt));
    assert!(rebuilt.schema("ops").is_some());
    assert!(rebuilt.schema("fin").is_some());
}
