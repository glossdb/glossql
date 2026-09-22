//! The SQL catalog on a server: the same three moves as the REST
//! round trip — create through the provider, write through the lake,
//! read through SQL — against the Postgres `GLOSSQL_E2E_CATALOG_SQL`
//! names, the warehouse on this machine or in the object store
//! `GLOSSQL_E2E_WAREHOUSE` names. Run by hand with a server standing:
//!
//!     GLOSSQL_E2E_CATALOG_SQL=postgres://glossql:glossql@127.0.0.1:5432/glossql \
//!       cargo test -p glossql-catalog live_sql -- --ignored
//!
//! The warehouse is a fixed directory under the OS temp dir, not a
//! tempdir: the server's catalog outlives a run and keeps pointing at
//! the files it was told about.

use std::collections::HashMap;
use std::sync::Arc;

use glossql_catalog::Lake;

/// The warehouse every SQL live test shares — the catalog remembers
/// it: the location `GLOSSQL_E2E_WAREHOUSE` names (an object store —
/// the emulator's `abfss://lake@devstoreaccount1.dfs.core.windows.net/
/// warehouse` with `AZURE_STORAGE_USE_EMULATOR=true`), else a fixed
/// directory under the OS temp dir.
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Postgres server: GLOSSQL_E2E_CATALOG_SQL"]
async fn live_sql_catalog_round_trip() {
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::CatalogProvider;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    let Some(uri) = std::env::var("GLOSSQL_E2E_CATALOG_SQL")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        eprintln!("skipping: GLOSSQL_E2E_CATALOG_SQL is not set");
        return;
    };
    let lake = Lake::open_sql(&uri, &e2e_warehouse())
        .await
        .expect("a live SQL catalog");

    // A fresh namespace per run, so re-runs never collide.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock")
        .as_millis();
    let dataset = format!("e2e_{stamp}");
    assert!(
        lake.ensure_namespace(&dataset, Default::default())
            .await
            .expect("a namespace")
    );

    let orders = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let ctx = SessionContext::new();
    let provider = lake.provider().await.expect("a provider");
    let schema = provider.schema(&dataset).expect("the fresh namespace");
    ctx.catalog("datafusion")
        .expect("the default catalog")
        .register_schema(&dataset, Arc::clone(&schema))
        .expect("a mount");

    let empty = RecordBatch::new_empty(Arc::clone(&orders));
    schema
        .register_table(
            "orders".into(),
            Arc::new(MemTable::try_new(Arc::clone(&orders), vec![vec![empty]]).expect("a shape")),
        )
        .expect("a create");
    let batch = RecordBatch::try_new(
        Arc::clone(&orders),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "99.90"])),
        ],
    )
    .expect("a batch");
    lake.append_batches(
        &dataset,
        "orders",
        std::slice::from_ref(&batch),
        HashMap::from([("glossql.source_rows".to_string(), "3".to_string())]),
    )
    .await
    .expect("a commit");

    let landings = lake.landings(&dataset).await.expect("landings");
    assert_eq!(landings.len(), 1);
    assert_eq!(
        landings[0].properties.get("glossql.source_rows"),
        Some(&"3".to_string()),
        "the fact rides the snapshot on this backend too"
    );
    let rows = ctx
        .sql(&format!("SELECT count(*) AS n FROM {dataset}.orders"))
        .await
        .expect("a plan")
        .collect()
        .await
        .expect("a read");
    assert_eq!(
        format!("{:?}", rows[0].column(0)),
        "PrimitiveArray<Int64>\n[\n  3,\n]"
    );

    // A second lake over the same server sees what the first committed:
    // the catalog is the server's, not the process's.
    let again = Lake::open_sql(&uri, &e2e_warehouse())
        .await
        .expect("a second opening");
    assert!(
        again
            .table_exists(&dataset, "orders")
            .await
            .expect("a lookup")
    );
}

/// The record on the same server: a row with a missing number lands
/// before a row with one, in one relation. Postgres types a prepared
/// statement's parameters at first use and keeps them, so the shape is
/// the one the typeless SQLite suite cannot see.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Postgres server: GLOSSQL_E2E_CATALOG_SQL"]
async fn live_sql_record_lands_a_missing_number_before_a_number() {
    use glossql_catalog::{Number, Record, RelationSpec};

    let Some(uri) = std::env::var("GLOSSQL_E2E_CATALOG_SQL")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        eprintln!("skipping: GLOSSQL_E2E_CATALOG_SQL is not set");
        return;
    };
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
    let record = Record::open(&uri, std::slice::from_ref(&spec))
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
