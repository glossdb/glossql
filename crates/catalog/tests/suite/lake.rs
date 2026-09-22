//! The data plane end to end: a dataset, a landing, the pin, the
//! mount, a replace, an append, a drop — each through the same doors
//! the server takes, so the shapes held here are the shapes in use.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::CatalogProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use glossql_catalog::{Lake, Landing};

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]))
}

fn orders(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("{i}.50")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)),
    ))
}

async fn scratch() -> (tempfile::TempDir, Lake) {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    (dir, lake)
}

/// A landing of `batches` into `fin.orders`, as `landing` says.
async fn land(lake: &Lake, batches: Vec<RecordBatch>, landing: Landing) -> i64 {
    let schema = orders_schema();
    let written = lake
        .write(
            "fin",
            "orders",
            Arc::clone(&schema),
            stream(Arc::clone(&schema), batches),
        )
        .await
        .unwrap();
    lake.commit("fin", "orders", &schema, written, landing)
        .await
        .unwrap()
}

/// `count(*)` over `fin.orders` at the current pin.
async fn count(lake: &Lake) -> i64 {
    let ctx = SessionContext::new();
    lake.register(&ctx.runtime_env());
    let pinned = lake.pin_dataset("fin").await.unwrap();
    let orders = pinned
        .iter()
        .find(|p| p.name == "orders")
        .expect("orders pinned");
    ctx.register_table("orders", Arc::clone(&orders.provider))
        .unwrap();
    let batches = ctx
        .sql("SELECT count(*) FROM orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn parquet_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("warehouse/fin/orders"))
        .map(|d| {
            d.filter(|e| {
                e.as_ref()
                    .ok()
                    .is_some_and(|e| e.path().extension().is_some_and(|x| x == "parquet"))
            })
            .count()
        })
        .unwrap_or(0)
}

/// A landing is a table: pinned with its version and columns, readable
/// through the pin and through the mount, catalogued as one file.
#[tokio::test(flavor = "multi_thread")]
async fn a_landing_is_pinned_and_mounted() {
    let (dir, lake) = scratch().await;
    assert!(lake.ensure_dataset("fin").await.unwrap());
    assert!(!lake.ensure_dataset("fin").await.unwrap(), "already there");
    let version = land(&lake, vec![orders(&[1, 2])], Landing::Create).await;

    let pinned = lake.pin_dataset("fin").await.unwrap();
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0].name, "orders");
    assert_eq!(pinned[0].snapshot_id, Some(version));
    assert_eq!(pinned[0].columns, vec!["order_id", "amount"]);
    assert_eq!(lake.version("fin", "orders").await.unwrap(), Some(version));
    assert_eq!(count(&lake).await, 2);
    assert_eq!(parquet_files(dir.path()), 1);

    let ctx = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_information_schema(true),
    );
    lake.register(&ctx.runtime_env());
    let mount = lake.provider().await.unwrap();
    assert_eq!(mount.schema_names(), vec!["fin"]);
    let schema = mount.schema("fin").unwrap();
    assert_eq!(schema.table_names(), vec!["orders"]);
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("fin", schema)
        .unwrap();
    let rows = ctx
        .sql("SELECT order_id FROM fin.orders ORDER BY order_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let columns = ctx
        .sql("SELECT column_name FROM information_schema.columns WHERE table_schema = 'fin' ORDER BY ordinal_position")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(columns.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

/// A replace is one commit: the old file ends and is deleted, the new
/// one begins, the version moves — and a reader racing it sees the old
/// rows or the new, never an empty table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_is_one_commit_and_never_an_empty_table() {
    let (dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    let before = land(&lake, vec![orders(&[1, 2])], Landing::Create).await;

    let reads = async {
        let mut seen = Vec::new();
        for _ in 0..40 {
            seen.push(count(&lake).await);
        }
        seen
    };
    let replace = land(&lake, vec![orders(&[7, 8, 9])], Landing::Replace);
    let (seen, after) = tokio::join!(reads, replace);
    assert!(after > before, "the version moved");
    assert!(
        seen.iter().all(|n| *n == 2 || *n == 3),
        "every read saw the old rows or the new: {seen:?}"
    );
    assert_eq!(count(&lake).await, 3);
    assert_eq!(
        parquet_files(dir.path()),
        2,
        "the ended file waits out the grace"
    );
    lake.sweep(std::time::Duration::ZERO).await;
    assert_eq!(parquet_files(dir.path()), 1, "the ended file is gone");
    assert_eq!(lake.version("fin", "orders").await.unwrap(), Some(after));
}

/// An append adds a file beside the live ones.
#[tokio::test(flavor = "multi_thread")]
async fn an_append_joins_the_live_files() {
    let (dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    let first = land(&lake, vec![orders(&[1, 2])], Landing::Create).await;
    let second = land(&lake, vec![orders(&[3])], Landing::Append).await;
    assert!(second > first);
    assert_eq!(count(&lake).await, 3);
    assert_eq!(parquet_files(dir.path()), 2);
}

/// A drop ends the table and deletes its files; the mount and the pin
/// no longer hold it.
#[tokio::test(flavor = "multi_thread")]
async fn a_drop_ends_the_table_and_its_files() {
    let (dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    land(&lake, vec![orders(&[1, 2])], Landing::Create).await;
    let mounted_before = lake.provider().await.unwrap();
    lake.drop_table("fin", "orders").await.unwrap();
    assert!(!lake.table_exists("fin", "orders").await.unwrap());
    assert_eq!(lake.version("fin", "orders").await.unwrap(), None);
    assert!(lake.pin_dataset("fin").await.unwrap().is_empty());
    lake.sweep(std::time::Duration::ZERO).await;
    assert_eq!(parquet_files(dir.path()), 0);
    let mounted_after = lake.provider().await.unwrap();
    assert!(
        !Arc::ptr_eq(&mounted_before, &mounted_after),
        "the mount was rebuilt"
    );
    assert!(
        mounted_after
            .schema("fin")
            .unwrap()
            .table_names()
            .is_empty()
    );
    // The name is free again.
    land(&lake, vec![orders(&[5])], Landing::Create).await;
    assert_eq!(count(&lake).await, 1);
}

/// Named pins answer `None` for a name that is no table, so the caller
/// takes the whole walk and its hint.
#[tokio::test(flavor = "multi_thread")]
async fn named_pins_refuse_a_name_that_is_no_table() {
    let (_dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    land(&lake, vec![orders(&[1])], Landing::Create).await;
    let some = lake
        .pin_tables("fin", &["orders".to_string()])
        .await
        .unwrap();
    assert_eq!(some.map(|p| p.len()), Some(1));
    let none = lake
        .pin_tables("fin", &["orders".to_string(), "ordres".to_string()])
        .await
        .unwrap();
    assert!(none.is_none());
    assert_eq!(lake.walk_count(), 0, "a named pin is not a walk");
}

/// The mount is shared until a commit moves it.
#[tokio::test(flavor = "multi_thread")]
async fn the_mount_is_shared_until_a_commit() {
    let (_dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    let a = lake.provider().await.unwrap();
    let b = lake.provider().await.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    land(&lake, vec![orders(&[1])], Landing::Create).await;
    let c = lake.provider().await.unwrap();
    assert!(!Arc::ptr_eq(&a, &c));
    assert_eq!(c.schema("fin").unwrap().table_names(), vec!["orders"]);
}

/// Every landed type crosses the catalog rows and comes back as the
/// schema the scan reads with.
#[tokio::test(flavor = "multi_thread")]
async fn a_landed_schema_pins_back_as_it_landed() {
    let (_dir, lake) = scratch().await;
    lake.ensure_dataset("fin").await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("day", DataType::Date32, true),
        Field::new("price", DataType::Decimal128(12, 2), true),
        Field::new("ok", DataType::Boolean, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let written = lake
        .write(
            "fin",
            "typed",
            Arc::clone(&schema),
            stream(Arc::clone(&schema), vec![empty]),
        )
        .await
        .unwrap();
    lake.commit("fin", "typed", &schema, written, Landing::Create)
        .await
        .unwrap();
    let pinned = lake.pin_dataset("fin").await.unwrap();
    assert_eq!(pinned[0].provider.schema(), schema);
}
