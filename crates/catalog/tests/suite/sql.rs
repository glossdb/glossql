//! The data plane on a Postgres catalog — the deployment's shape —
//! behind `GLOSSQL_E2E_CATALOG_SQL`; the warehouse a directory unless
//! `GLOSSQL_E2E_WAREHOUSE` names a location.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::CatalogProvider;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use glossql_catalog::{Lake, Landing};

pub fn e2e_warehouse() -> String {
    std::env::var("GLOSSQL_E2E_WAREHOUSE")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join("glossql-e2e-sql")
                .join("warehouse")
                .display()
                .to_string()
        })
}

fn live_uri() -> Option<String> {
    let uri = std::env::var("GLOSSQL_E2E_CATALOG_SQL")
        .ok()
        .filter(|v| !v.is_empty());
    if uri.is_none() {
        eprintln!("skipping: GLOSSQL_E2E_CATALOG_SQL is not set");
    }
    uri
}

/// A dataset, a landing, a replace and a read on the live catalog, and
/// the table found by a second opening.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Postgres server: GLOSSQL_E2E_CATALOG_SQL"]
async fn live_sql_catalog_round_trip() {
    let Some(uri) = live_uri() else { return };
    let lake = Lake::open_sql(&uri, &e2e_warehouse())
        .await
        .expect("a live SQL catalog");
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock")
        .as_millis();
    let dataset = format!("e2e_{stamp}");
    assert!(lake.ensure_dataset(&dataset).await.expect("a dataset"));

    let orders = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let batch = |ids: &[i64]| {
        RecordBatch::try_new(
            Arc::clone(&orders),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vec!["12.50"; ids.len()])),
            ],
        )
        .expect("a batch")
    };
    let land = |rows: RecordBatch, landing: Landing| {
        let lake = lake.clone();
        let orders = Arc::clone(&orders);
        let dataset = dataset.clone();
        async move {
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&orders),
                futures::stream::iter(vec![Ok(rows)]),
            ));
            let written = lake
                .write(&dataset, "orders", Arc::clone(&orders), stream)
                .await
                .expect("a write");
            lake.commit(&dataset, "orders", &orders, written, landing)
                .await
                .expect("a commit")
        }
    };
    let first = land(batch(&[1, 2, 3]), Landing::Create).await;
    let second = land(batch(&[4]), Landing::Replace).await;
    assert!(second > first);

    let ctx = SessionContext::new();
    lake.register(&ctx.runtime_env());
    let provider = lake.provider().await.expect("a mount");
    let schema = provider.schema(&dataset).expect("the fresh dataset");
    ctx.catalog("datafusion")
        .expect("the default catalog")
        .register_schema(&dataset, schema)
        .expect("a mount");
    let rows = ctx
        .sql(&format!("SELECT count(*) AS n FROM {dataset}.orders"))
        .await
        .expect("a plan")
        .collect()
        .await
        .expect("a read");
    assert_eq!(
        format!("{:?}", rows[0].column(0)),
        "PrimitiveArray<Int64>\n[\n  1,\n]"
    );

    let again = Lake::open_sql(&uri, &e2e_warehouse())
        .await
        .expect("a second opening");
    assert_eq!(
        again.version(&dataset, "orders").await.expect("a lookup"),
        Some(second)
    );
    again.drop_table(&dataset, "orders").await.expect("a drop");
}

/// The record's null binding on the live dialect: a missing number
/// lands before a number in the same relation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Postgres server: GLOSSQL_E2E_CATALOG_SQL"]
async fn live_sql_record_lands_a_missing_number_before_a_number() {
    use glossql_catalog::{Db, Number, Record, RelationSpec};

    let Some(uri) = live_uri() else { return };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock")
        .as_millis();
    let name: &'static str = Box::leak(format!("e2e_witnesses_{stamp}").into_boxed_str());
    let spec = RelationSpec {
        name,
        columns: &["name", "aspect", "speakers", "detector", "threshold"],
        numbers: &[("threshold", Number::Real)],
    };
    let db = Db::connect(&uri).await.expect("the server");
    let record = Record::open(&db, std::slice::from_ref(&spec))
        .await
        .expect("the record on the server");
    let row = |name: &str, threshold: Option<&str>| -> Vec<Option<String>> {
        vec![
            Some(name.into()),
            Some("meaning".into()),
            None,
            Some("slot_entropy".into()),
            threshold.map(str::to_string),
        ]
    };
    record
        .append(name, vec![row("first", None)])
        .await
        .expect("a missing threshold lands");
    record
        .append(name, vec![row("second", Some("0.7"))])
        .await
        .expect("a threshold lands after a missing one, in the same relation");
    let rows = record.scan(name).await.expect("a read");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(4), None);
    assert_eq!(rows[1].get(4), Some("0.7"));
}
