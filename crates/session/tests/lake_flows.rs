//! The fixture-11 flow against a real warehouse (corpus/11-flow-add-source),
//! under the authored-typing ruling (2026-08-04): the agent probes the
//! source through the statement door, the recipe carries the casts, the
//! landed table IS the typed table. Glosses carry snapshot ids; recipe
//! identity is content; `DROP TABLE` and the substrate allowlist hold the
//! lifecycle rules.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{Outcome, Session, SessionError};

async fn parquet_fixture(root: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["12.50", "8.00", "n/a"])),
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

async fn workspace(dir: &std::path::Path) -> Session {
    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open_memory().await.unwrap();
    Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .unwrap()
    .with_lake(lake)
}

fn done(outcome: &Outcome) -> &str {
    match outcome {
        Outcome::Done(s) => s,
        other => panic!("expected Done, got {other:?}"),
    }
}

fn single_value(outcomes: &[Outcome]) -> String {
    match outcomes.last().unwrap() {
        Outcome::Rows(batches) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1, "expected one row");
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap()
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fixture_11_add_source_flow() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;

    let session = workspace(dir.path()).await;
    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'working-capital analysis');\n\
             USE fin;\n\
             DECLARE SOURCE erp_export SET (type: parquet, location: '{}');",
            erp_root.display()
        ))
        .await
        .unwrap();

    // The probe: a recipe rehearsal — same SQL surface, same path
    // resolution, landing nothing.
    let filled = session
        .execute(
            "PROBE erp_export AS $$SELECT count(\"amount\") FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&filled), "3");
    let parsed = session
        .execute(
            "PROBE erp_export AS $$SELECT count(try_cast(\"amount\" AS DOUBLE)) \
             FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(single_value(&parsed), "2", "the probe finds one bad value");

    // Typing is authored: the recipe carries the casts, and this author
    // drops the unparseable row deliberately.
    let outcomes = session
        .execute(
            "DECLARE RECIPE orders ON fin FROM erp_export AS $$\
               SELECT order_id, try_cast(amount AS DOUBLE) AS amount \
               FROM read_parquet('orders/*.parquet') \
               WHERE try_cast(amount AS DOUBLE) IS NOT NULL$$;",
        )
        .await
        .unwrap();
    assert_eq!(
        done(&outcomes[0]),
        "DECLARE RECIPE orders ON fin (2 rows landed, 1 dropped; casts clean)",
        "the counts arrive at the decision moment — the WHERE dropped the \
         bad row, so the cells that landed are clean"
    );

    // The landed table is the typed table — no view, no raw twin.
    let total = session
        .execute("SELECT sum(amount) FROM orders;")
        .await
        .unwrap();
    assert_eq!(single_value(&total), "20.5");
    let qualified = session
        .execute("SELECT count(*) FROM fin.orders;")
        .await
        .unwrap();
    assert_eq!(single_value(&qualified), "2");

    // The engine keeps one number about what the recipe filtered away.
    let dropped = session
        .execute("SELECT dropped_rows_count FROM imports WHERE table_name = 'orders';")
        .await
        .unwrap();
    assert_eq!(
        single_value(&dropped),
        "1",
        "which row is the author's question"
    );

    // A gloss on a column subject carries the table's snapshot id.
    session
        .execute(
            r#"DECLARE ASPECT unit WITH $${
                 "type": "object", "required": ["value"],
                 "properties": {"value": {"type": "string"}}
               }$$ AS FACT;
               GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;"#,
        )
        .await
        .unwrap();
    let stamped = session
        .execute("SELECT snapshot_id FROM glossary WHERE aspect = 'unit';")
        .await
        .unwrap();
    assert_ne!(
        single_value(&stamped),
        "",
        "column gloss carries the snapshot id"
    );

    // A dataset-level gloss has no table to pin — snapshot id stays NULL.
    session
        .execute(r#"GLOSS unit ON fin AS $${"value": "EUR"}$$;"#)
        .await
        .unwrap();
    let unstamped = session
        .execute("SELECT count(*) FROM glossary WHERE subject = 'fin' AND snapshot_id IS NULL;")
        .await
        .unwrap();
    assert_eq!(single_value(&unstamped), "1");

    // §3: unchanged recipe is a no-op; a changed one supersedes and
    // re-lands (ruled 2026-08-06 — runs 5 and 6 both dead-ended on the
    // old refusal). Glosses stay; the cached evidence is swept.
    let redeclare = "DECLARE RECIPE orders ON fin FROM erp_export AS $$\
               SELECT order_id, try_cast(amount AS DOUBLE) AS amount \
               FROM read_parquet('orders/*.parquet') \
               WHERE try_cast(amount AS DOUBLE) IS NOT NULL$$;";
    let outcomes = session.execute(redeclare).await.unwrap();
    assert_eq!(
        done(&outcomes[0]),
        "DECLARE RECIPE orders ON fin (unchanged)"
    );
    let changed = "DECLARE RECIPE orders ON fin FROM erp_export AS $$SELECT order_id FROM read_parquet('orders/*.parquet')$$;";
    let outcomes = session.execute(changed).await.unwrap();
    assert!(
        done(&outcomes[0]).contains("superseded and re-landed"),
        "{}",
        done(&outcomes[0])
    );
    // The fresh landing is the new shape: amount is gone from the table…
    let err = session
        .execute("SELECT amount FROM orders;")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("amount"), "{err}");
    // …while the glosses survive as knowledge, and the import history
    // keeps both landings.
    let kept = session
        .execute("SELECT count(*) FROM glossary WHERE subject = 'fin';")
        .await
        .unwrap();
    assert_eq!(single_value(&kept), "1");
    let landings = session
        .execute("SELECT count(*) FROM imports WHERE table_name = 'orders';")
        .await
        .unwrap();
    assert_eq!(single_value(&landings), "2");

    // A re-land that cannot run leaves the landing it was replacing
    // (found 2026-08-06: the old table was dropped before the new recipe
    // had produced a single batch, so a typo destroyed it with no rollback).
    let broken = "DECLARE RECIPE orders ON fin FROM erp_export AS $$SELECT ordr_id FROM read_parquet('orders/*.parquet')$$;";
    let err = session.execute(broken).await.unwrap_err();
    assert!(err.to_string().contains("ordr_id"), "{err}");
    let survivors = session
        .execute("SELECT count(*) FROM orders;")
        .await
        .unwrap();
    assert_eq!(
        single_value(&survivors),
        "3",
        "the live landing is untouched by a recipe that never ran"
    );
    // And the recipe row still describes what actually landed, so the
    // retry does not answer `unchanged` over a table that was never made.
    let recipes = session
        .execute("SELECT count(*) FROM imports WHERE table_name = 'orders';")
        .await
        .unwrap();
    assert_eq!(single_value(&recipes), "2");

    // The substrate allowlist: schema-altering SQL is refused at the door.
    let err = session
        .execute("CREATE VIEW shadow AS SELECT * FROM orders;")
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::SubstrateClosed(_)), "{err}");
    let err = session
        .execute("INSERT INTO orders VALUES (9, 1.0);")
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::SubstrateClosed(_)), "{err}");

    // DROP TABLE refuses while the table holds data.
    let err = session.execute("DROP TABLE orders;").await.unwrap_err();
    assert!(
        matches!(&err, SessionError::DropRefused { reason, .. } if reason.contains("data")),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_removes_an_empty_misdeclaration_whole() {
    let dir = tempfile::tempdir().unwrap();
    let erp_root = dir.path().join("lake/erp");
    std::fs::create_dir_all(&erp_root).unwrap();
    parquet_fixture(&erp_root).await;

    let session = workspace(dir.path()).await;
    session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'test');\n\
             USE fin;\n\
             DECLARE SOURCE erp SET (type: parquet, location: '{}');\n\
             DECLARE RECIPE mistake ON fin FROM erp AS $$\
               SELECT order_id FROM read_parquet('orders/*.parquet') WHERE false$$;",
            erp_root.display()
        ))
        .await
        .unwrap();

    let outcomes = session.execute("DROP TABLE mistake;").await.unwrap();
    assert_eq!(done(&outcomes[0]), "DROP TABLE mistake");

    // Gone whole: the table, the recipe row, the import record — so the
    // name is free for a different SQL.
    let gone = session
        .execute("SELECT count(*) FROM imports WHERE table_name = 'mistake';")
        .await
        .unwrap();
    assert_eq!(single_value(&gone), "0");
    let outcomes = session
        .execute(
            "DECLARE RECIPE mistake ON fin FROM erp AS $$SELECT order_id FROM read_parquet('orders/*.parquet')$$;",
        )
        .await
        .unwrap();
    assert_eq!(
        done(&outcomes[0]),
        "DECLARE RECIPE mistake ON fin (3 rows landed, 0 dropped)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_discloses_its_cast_nulled_cells() {
    // The author keeps every row; two amount cells were `\N` and the cast
    // nulled them. The row counts say nothing about that — the account
    // does, at the decision moment and in the imports relation. The token
    // came from the data; nothing anywhere lists `\N` as a sentinel.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("balances.csv"),
        "account,balance\na,120.50\nb,\\N\nc,80.00\nd,\\N\n",
    )
    .unwrap();
    let session = workspace(dir.path()).await;
    let outcomes = session
        .execute(&format!(
            "DECLARE DATASET fin SET (purpose: 'test');\n\
             USE fin;\n\
             DECLARE SOURCE gl SET (type: csv, location: '{}');\n\
             DECLARE RECIPE balances ON fin FROM gl AS $$\
               SELECT account, try_cast(balance AS DOUBLE) AS balance \
               FROM read_csv('balances.csv')$$;",
            dir.path().display()
        ))
        .await
        .unwrap();
    assert_eq!(
        done(outcomes.last().unwrap()),
        "DECLARE RECIPE balances ON fin (4 rows landed, 0 dropped; \
         cast-nulled cells — balance: 2 ['\\N' ×2])"
    );

    // The full account persists where any read can find it.
    let stored = session
        .execute("SELECT cast_failures FROM imports WHERE table_name = 'balances';")
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&single_value(&stored)).unwrap();
    assert_eq!(json["checked"][0]["column"], "balance");
    assert_eq!(json["checked"][0]["failed"], 2);
    assert_eq!(json["checked"][0]["tokens"][0][0], "\\N");
}
