//! Recipes over file sources: the recipe's schema is the landed schema
//! (typing is authored), an uncast csv column stays byte-exact raw text,
//! paths cannot escape the source root.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_import::{SourceSpec, run_recipe};
use serde_json::json;

fn spec(kind: &str, root: &std::path::Path) -> SourceSpec {
    SourceSpec::from_settings(
        "src",
        &json!({"type": kind, "location": root.display().to_string()}),
    )
    .unwrap()
}

async fn write_parquet_fixture(dir: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new(
            "ordered_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000i64,
                1_700_000_100_000_000_000i64,
            ])),
            Arc::new(StringArray::from(vec!["12.50", "8.00"])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch).unwrap();
    ctx.table("t")
        .await
        .unwrap()
        .write_parquet(
            &dir.join("orders").display().to_string(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_recipe_keeps_types_and_folds_ns_to_us() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet_fixture(dir.path()).await;

    let landed = run_recipe(
        &spec("parquet", dir.path()),
        "SELECT * FROM read_parquet('orders/*.parquet')",
    )
    .await
    .unwrap();
    assert_eq!(landed.source_scans, vec![("orders/*.parquet".to_string(), 2)]);
    assert_eq!(landed.dropped_rows(2), Some(0), "SELECT * drops nothing");
    let (schema, batches) = (landed.schema, landed.batches);

    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "ns folds to µs — TimestampNs is a format-v3 type"
    );
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_typing_is_authored_uncast_stays_byte_exact() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("accounts.csv"),
        "account_no,balance\n00123,42.0\n00456,7.5\n",
    )
    .unwrap();

    // No casts authored: the read side is all-Utf8, so raw text lands
    // byte-exact — the author's default, not an import refold.
    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT * FROM read_csv('accounts.csv')",
    )
    .await
    .unwrap();
    assert!(
        landed
            .schema
            .fields()
            .iter()
            .all(|f| f.data_type() == &DataType::Utf8),
        "an uncast csv column is a string"
    );
    let col = landed.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        col.value(0),
        "00123",
        "no inferred typing — leading zeros survive"
    );

    // Authored casts land typed: the landed table is the typed table
    // (ruled 2026-08-04) — the schema the probe rehearsed, not a refold
    // (the force_utf8 that discarded these casts died 2026-08-05).
    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT account_no, try_cast(balance AS DOUBLE) AS balance \
         FROM read_csv('accounts.csv')",
    )
    .await
    .unwrap();
    assert_eq!(landed.schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        landed.schema.field(1).data_type(),
        &DataType::Float64,
        "the authored cast is the landed type"
    );
    let balances = landed.batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(balances.value(0), 42.0, "numbers aggregate as numbers");
}

#[tokio::test(flavor = "multi_thread")]
async fn recipe_paths_cannot_escape_the_source_root() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "x\n1\n").unwrap();

    let err = run_recipe(
        &spec("csv", dir.path()),
        "SELECT * FROM read_csv('../a.csv')",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("must stay under the source's location")
    );

    // A symlink under the root is the same escape by another spelling
    // (2026-08-06): the fence resolves the path before reading it.
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret.csv"), "x\n9\n").unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
    let err = run_recipe(
        &spec("csv", dir.path()),
        "SELECT * FROM read_csv('link/secret.csv')",
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("outside the source's location"),
        "{err}"
    );
}

// -- row accounting per scanned source (found 2026-08-12) ------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_provider_recipe_accounts_each_source() {
    // A join reads five source rows and lands three — "2 dropped" would
    // mislead: no row was dropped, the sum just crossed two files. The
    // scan keeps each provider's count under the path that named it.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("orders.csv"),
        "id,customer\n1,a\n2,b\n3,a\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("customers.csv"), "customer,region\na,x\nb,y\n").unwrap();

    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT o.id, c.region \
         FROM read_csv('orders.csv') o \
         JOIN read_csv('customers.csv') c ON o.customer = c.customer",
    )
    .await
    .unwrap();
    assert_eq!(
        landed.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        3
    );
    // The bug this whole shape exists to prevent: 3 + 2 = 5 scanned
    // against 3 landed reads as "2 dropped", and nothing was dropped.
    assert_eq!(
        landed.dropped_rows(3),
        None,
        "two scans: no single difference is true"
    );
    assert_eq!(
        landed.source_scans,
        vec![
            ("orders.csv".to_string(), 3),
            ("customers.csv".to_string(), 2)
        ]
    );
    assert_eq!(
        landed.row_summary(3),
        "3 rows landed; sources scanned: orders.csv 3 rows, customers.csv 2 rows"
    );

    // One provider keeps the difference: it really is the dropped count.
    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT id FROM read_csv('orders.csv') WHERE id > 1",
    )
    .await
    .unwrap();
    assert_eq!(landed.source_scans, vec![("orders.csv".to_string(), 3)]);
    assert_eq!(landed.row_summary(2), "2 rows landed, 1 dropped");
}

// -- cast accounting (cells, not rows — 2026-08-06) ------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_accounts_its_cast_nulled_cells() {
    // Five rows land, five rows counted — but two amount cells and one
    // date cell were values the casts nulled. The tokens come from the
    // data; there is no vocabulary to match them against.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("orders.csv"),
        "order_id,amount,order_date\n\
         1,12.50,03.01.2026\n\
         2,\\N,04.01.2026\n\
         3,8.00,not yet\n\
         4,\\N,05.01.2026\n\
         5,1.25,06.01.2026\n",
    )
    .unwrap();
    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT order_id, \
                try_cast(amount AS DOUBLE) AS amount, \
                try_to_date(order_date, '%d.%m.%Y') AS order_date \
         FROM read_csv('orders.csv')",
    )
    .await
    .unwrap();
    assert_eq!(
        landed.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        5
    );

    let glossql_import::CastAccounting::Checked(checks) = &landed.casts else {
        panic!("accounted: {:?}", landed.casts);
    };
    assert_eq!(checks.len(), 2, "{checks:?}");
    let amount = checks.iter().find(|c| c.column == "amount").unwrap();
    assert_eq!(amount.failed, 2);
    assert_eq!(amount.tokens, vec![("\\N".to_string(), 2)]);
    let date = checks.iter().find(|c| c.column == "order_date").unwrap();
    assert_eq!(date.failed, 1);
    assert_eq!(date.tokens, vec![("not yet".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_try_to_date_reads_a_column_of_mixed_formats() {
    // Run 4's payments column: three conventions interleaved, no ISO
    // row at all. Formats are tried in order and the first that parses
    // wins, so the recipe spells the value once instead of coalescing
    // three calls over three copies of it.
    //
    // The last row is the honest hard case — `05/06/2026` parses under
    // both slashed formats, and the one named first decides. Order is
    // the author's claim about the source, and the accounting counts a
    // cell as failed only when no format took it.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("pay.csv"),
        "id,paid\n\
         1,24/10/2026\n\
         2,02/24/2026\n\
         3,23-Mar-26\n\
         4,sometime\n\
         5,05/06/2026\n",
    )
    .unwrap();
    let landed = run_recipe(
        &spec("csv", dir.path()),
        "SELECT id, try_to_date(paid, '%d/%m/%Y', '%m/%d/%Y', '%d-%b-%y') AS paid \
         FROM read_csv('pay.csv')",
    )
    .await
    .unwrap();

    let dates: Vec<Option<i32>> = landed
        .batches
        .iter()
        .flat_map(|b| {
            use datafusion::arrow::array::Array;
            let col = b
                .column_by_name("paid")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Date32Array>()
                .unwrap();
            (0..col.len())
                .map(|i| (!col.is_null(i)).then(|| col.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    let day = |y: i32, m: u32, d: u32| {
        (chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32
    };
    assert_eq!(
        dates,
        vec![
            Some(day(2026, 10, 24)), // only day-first can read it
            Some(day(2026, 2, 24)),  // only month-first can
            Some(day(2026, 3, 23)),  // the named month
            None,                    // nothing parses it
            Some(day(2026, 6, 5)),   // ambiguous: day-first was named first
        ]
    );

    let glossql_import::CastAccounting::Checked(checks) = &landed.casts else {
        panic!("accounted: {:?}", landed.casts);
    };
    let paid = checks.iter().find(|c| c.column == "paid").unwrap();
    assert_eq!(paid.failed, 1, "only the unparseable cell counts as failed");
    assert_eq!(paid.tokens, vec![("sometime".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accounting_discloses_what_it_cannot_account() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("t.csv"), "a,b\n1,2\n1,3\n").unwrap();
    let s = spec("csv", dir.path());

    // An aggregating recipe has no per-row cast to account.
    let landed = run_recipe(
        &s,
        "SELECT a, count(*) AS n FROM read_csv('t.csv') GROUP BY a",
    )
    .await
    .unwrap();
    assert!(
        matches!(&landed.casts, glossql_import::CastAccounting::Unchecked(note) if note.contains("GROUP BY")),
        "{:?}",
        landed.casts
    );
    // The same shape reading answers the row counts: two rows collapsed
    // to one dropped nothing, and saying so is the whole point of
    // keeping the reading apart from the cast verdict (2026-08-15).
    assert!(!landed.row_preserving);
    assert_eq!(landed.dropped_rows(1), None, "a GROUP BY drops no rows");

    // No casts: the account is complete and empty.
    let landed = run_recipe(&s, "SELECT * FROM read_csv('t.csv')")
        .await
        .unwrap();
    assert!(
        matches!(&landed.casts, glossql_import::CastAccounting::Checked(c) if c.is_empty()),
        "{:?}",
        landed.casts
    );
}

// -- relational sources (the ADBC executor) --------------------------------

fn relational_spec(driver: &str, uri: &str) -> SourceSpec {
    SourceSpec::from_settings(
        "erp",
        &json!({"type": "relational_db", "driver": driver, "location": uri}),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relational_source_requires_a_driver() {
    let e = SourceSpec::from_settings(
        "erp",
        &json!({"type": "relational_db", "location": "file.db"}),
    )
    .unwrap_err();
    assert!(e.to_string().contains("driver"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relational_recipe_is_one_query_and_never_a_write() {
    // The fence fires before any driver loads — a bogus driver name
    // proves the refusal is ours, not the manager's.
    let s = relational_spec("no_such_driver", ":memory:");
    for sql in [
        "DELETE FROM orders",
        "DROP TABLE orders",
        "SELECT 1; SELECT 2",
        "UPDATE t SET a = 1",
    ] {
        let e = run_recipe(&s, sql).await.unwrap_err();
        assert!(e.to_string().contains("one SELECT"), "`{sql}`: {e}");
    }
    // A plain query passes the fence and fails only at driver load —
    // where the error teaches the installable slugs (the 2026-08-07 run
    // guessed `adbc_driver_sqlite` and got a bare NotFound).
    let e = run_recipe(&s, "SELECT 1").await.unwrap_err();
    assert!(!e.to_string().contains("one SELECT"), "{e}");
    assert!(
        e.to_string().contains("index slug") && e.to_string().contains("postgresql"),
        "{e}"
    );
}

/// The ADBC sqlite driver, if one is installed: the env var wins, then
/// the well-known pip wheel location. Absent → the live test skips.
fn sqlite_driver() -> Option<String> {
    if let Ok(path) = std::env::var("GLOSSQL_ADBC_SQLITE_DRIVER") {
        return Some(path);
    }
    let out = std::process::Command::new("python3")
        .args([
            "-c",
            "import adbc_driver_sqlite; print(adbc_driver_sqlite._driver_path())",
        ])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relational_recipe_lands_from_sqlite() {
    let Some(driver) = sqlite_driver() else {
        eprintln!("skipped: no ADBC sqlite driver (set GLOSSQL_ADBC_SQLITE_DRIVER)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("erp.db");
    // Seed through the driver itself — the test needs no sqlite CLI.
    {
        use adbc_core::options::{AdbcVersion, OptionDatabase, OptionStatement};
        use adbc_core::{
            Connection as _, Database as _, Driver as _, Optionable as _, Statement as _,
        };
        let mut d = adbc_driver_manager::ManagedDriver::load_from_name(
            &driver,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap();
        let database = d
            .new_database_with_opts([(OptionDatabase::Uri, db.display().to_string().into())])
            .unwrap();
        let mut conn = database.new_connection().unwrap();
        let mut st = conn.new_statement().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, true),
            Field::new("amount", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["12.50", "8.00", "1.25"])),
            ],
        )
        .unwrap();
        st.set_option(OptionStatement::TargetTable, "orders".into())
            .unwrap();
        st.bind(batch).unwrap();
        st.execute_update().unwrap();
    }

    let s = relational_spec(&driver, &db.display().to_string());
    let landed = run_recipe(&s, "SELECT order_id, amount FROM orders WHERE order_id > 1")
        .await
        .unwrap();
    assert_eq!(
        landed.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        2
    );
    assert!(
        landed.source_scans.is_empty(),
        "the source computed the SQL — this side scanned nothing"
    );
    assert_eq!(
        landed.dropped_rows(2),
        Some(0),
        "what came back is both what was read and what landed"
    );
    assert_eq!(landed.schema.field(0).name(), "order_id");

    // A probe stops at the cap: one row past it marks truncation.
    let probed = glossql_import::run_probe(&s, "SELECT * FROM orders", 1)
        .await
        .unwrap();
    let probed_rows: usize = probed.iter().map(|b| b.num_rows()).sum();
    assert!(
        (2..=3).contains(&probed_rows),
        "capped at one past 1, got {probed_rows}"
    );

    // The source's own catalog answers a key-harvest probe — the skill
    // teaches this spelling; declared keys are judge evidence, never
    // declared relationships (recipes reshape what lands).
    let keys = glossql_import::run_probe(&s, "SELECT * FROM pragma_table_info('orders')", 200)
        .await
        .unwrap();
    assert!(
        keys.iter().map(|b| b.num_rows()).sum::<usize>() >= 2,
        "pragma rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recipe_body_cannot_write_outside_its_read() {
    // Recipe and probe SQL runs in a scratch context that the statement
    // allowlist never sees, and DataFusion's default options permit DDL,
    // DML and COPY — a probe could write a parquet file anywhere the
    // process can (found 2026-08-06).
    let dir = tempfile::tempdir().unwrap();
    write_parquet_fixture(dir.path()).await;
    let spec = spec("parquet", dir.path());
    let escape = dir.path().join("escaped.parquet");

    let e = run_recipe(
        &spec,
        &format!(
            "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
            escape.display()
        ),
    )
    .await
    .unwrap_err();
    assert!(e.to_string().to_lowercase().contains("copy"), "{e}");
    assert!(!escape.exists(), "nothing was written");

    let e = glossql_import::run_probe(
        &spec,
        &format!(
            "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
            escape.display()
        ),
        200,
    )
    .await
    .unwrap_err();
    assert!(e.to_string().to_lowercase().contains("copy"), "{e}");
    assert!(!escape.exists(), "nothing was written");

    // Reading is untouched.
    let landed = run_recipe(&spec, "SELECT * FROM read_parquet('orders/*.parquet')")
        .await
        .unwrap();
    assert_eq!(
        landed.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        2
    );
}
