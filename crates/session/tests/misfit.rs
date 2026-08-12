//! The `misfit.` door end to end (ruled 2026-08-11, fixture 20): a
//! declared sample frame served back with a per-row misfit score. The
//! kernel here is a fake — deviation from each column's mean — so these
//! tests pin the door's mechanics (dispatch, the numeric surface and
//! its named exclusions, the stated caps, no cache), while the real
//! chain-rule density is graded in the sibling's suite and wired in
//! `glossql-scripts`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, FunctionRow, Store};
use glossql_session::{FunctionRuntime, Outcome, Session, SqlDoor};
use serde_json::Value;

/// A kernel that scores each row by its summed deviation from the
/// column means — the mechanics stand-in for the density: a planted
/// outlier must surface at the top, so surface-assembly mistakes show
/// as a wrong ranking.
#[derive(Debug, Default)]
struct MeanKernel {
    fits: AtomicUsize,
}

impl FunctionRuntime for MeanKernel {
    fn invoke(
        &self,
        function: &FunctionRow,
        _: &str,
        _: &Value,
        _: Arc<dyn SqlDoor>,
    ) -> Result<Value, String> {
        Err(format!("no scripts in this test (`{}`)", function.name))
    }

    fn misfit_scores(&self, x: &[f64], rows: usize, cols: usize) -> Result<Vec<f64>, String> {
        self.fits.fetch_add(1, Ordering::SeqCst);
        let mut means = vec![0f64; cols];
        for (c, mean) in means.iter_mut().enumerate() {
            let vals: Vec<f64> = (0..rows)
                .map(|r| x[r * cols + c])
                .filter(|v| !v.is_nan())
                .collect();
            *mean = vals.iter().sum::<f64>() / vals.len().max(1) as f64;
        }
        Ok((0..rows)
            .map(|r| {
                -(0..cols)
                    .map(|c| {
                        let v = x[r * cols + c];
                        if v.is_nan() {
                            0.0
                        } else {
                            (v - means[c]).abs()
                        }
                    })
                    .sum::<f64>()
            })
            .collect())
    }
}

/// A kernel that hands back a non-finite score — the door must refuse,
/// never serve NaN rows (finding 11, 2026-08-12).
#[derive(Debug, Default)]
struct NanKernel;

impl FunctionRuntime for NanKernel {
    fn invoke(
        &self,
        function: &FunctionRow,
        _: &str,
        _: &Value,
        _: Arc<dyn SqlDoor>,
    ) -> Result<Value, String> {
        Err(format!("no scripts in this test (`{}`)", function.name))
    }

    fn misfit_scores(&self, _: &[f64], rows: usize, _: usize) -> Result<Vec<f64>, String> {
        Ok(vec![f64::NAN; rows])
    }
}

async fn session_with_kernel() -> (Session, Arc<MeanKernel>) {
    let store = Store::open_memory().await.unwrap();
    let kernel = Arc::new(MeanKernel::default());
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .expect("session builds")
    .with_runtime(kernel.clone());
    (session, kernel)
}

async fn run(session: &Session, sql: &str) -> Vec<Outcome> {
    session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

async fn table(session: &Session, sql: &str) -> String {
    let outcomes = run(session, sql).await;
    let Some(Outcome::Rows(batches)) = outcomes.into_iter().next_back() else {
        panic!("`{sql}` produced no rows");
    };
    pretty_format_batches(&batches).unwrap().to_string()
}

/// 24 order rows over 2026, two per month, amounts near 100.0 — and one
/// planted outlier at 10000.0. An id column (excluded by name), a text
/// column, and a constant ride along to exercise the named exclusions.
fn orders_table() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("order_date", DataType::Date32, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("flag", DataType::Float64, false),
    ]));
    let offsets = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut dates = Vec::new();
    for o in offsets {
        dates.push(20454 + o + 5);
        dates.push(20454 + o + 20);
    }
    let n = dates.len();
    let ids: Vec<i64> = (1..=n as i64).collect();
    let amounts: Vec<f64> = (0..n)
        .map(|i| if i == 7 { 10000.0 } else { 100.0 + i as f64 })
        .collect();
    let regions: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "east" } else { "west" })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(vec![1.0; n])),
        ],
    )
    .unwrap();
    (schema, batch)
}

const SETUP: &str = r##"
DECLARE DATASET fin SET (purpose: 'misfit frames');
USE fin;
DECLARE ASPECT suspects WITH $${"title": "The March suspects", "x-kind": "sample"}$$ AS QUERY ON DATASET;
DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT ON DATASET;
DECLARE ASPECT unglossed WITH $${"title": "Declared, never glossed"}$$ AS QUERY ON DATASET;
GLOSS suspects ON fin AS $${"sql": "SELECT * FROM orders"}$$;
"##;

async fn frame_session() -> (Session, Arc<MeanKernel>) {
    let (session, kernel) = session_with_kernel().await;
    let (schema, batch) = orders_table();
    session
        .register_table(
            "orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    run(&session, SETUP).await;
    (session, kernel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_misfit_door_ranks_and_composes() {
    let (session, _) = frame_session().await;

    // The planted outlier tops the ranking; the basis names the ranked
    // columns and every exclusion with its reason.
    let top = table(
        &session,
        "SELECT amount, basis FROM misfit.suspects() ORDER BY misfit DESC LIMIT 1;",
    )
    .await;
    assert!(top.contains("10000.0"), "{top}");
    assert!(top.contains("order_date"), "{top}");
    assert!(top.contains("amount"), "{top}");
    assert!(top.contains("order_id (id)"), "{top}");
    assert!(top.contains("region (Utf8)"), "{top}");
    assert!(top.contains("flag (constant)"), "{top}");

    // Every frame row is served, and WHERE composes around the door.
    let n = table(
        &session,
        "SELECT count(*) AS n FROM misfit.suspects() WHERE misfit IS NOT NULL;",
    )
    .await;
    assert!(n.contains("24"), "{n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_door_refuses_what_is_not_a_frame() {
    let (session, _) = frame_session().await;

    let e = session
        .execute("SELECT * FROM misfit.nothing();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no aspect"), "{e}");

    let e = session
        .execute("SELECT * FROM misfit.note();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("is a fact aspect"), "{e}");

    let e = session
        .execute("SELECT * FROM misfit.unglossed();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no frame is glossed"), "{e}");

    let e = session
        .execute("SELECT * FROM misfit.suspects('x');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("takes no arguments"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stated_caps_and_the_surface_abstention_refuse_by_name() {
    let (session, _) = frame_session().await;

    // A frame past the row cap: refused with the cap, never cut. The
    // cap bounds the kernel's context (2000, measured 2026-08-12) —
    // the refusal teaches sampling in the frame SQL.
    let n = 2100;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from_iter_values((0..n).map(|i| i as f64))),
            Arc::new(Float64Array::from_iter_values(
                (0..n).map(|i| (i * 2) as f64),
            )),
        ],
    )
    .unwrap();
    session
        .register_table(
            "big",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    run(
        &session,
        r##"DECLARE ASPECT wide_net WITH $${"x-kind": "sample"}$$ AS QUERY ON DATASET;
            GLOSS wide_net ON fin AS $${"sql": "SELECT * FROM big"}$$;"##,
    )
    .await;
    let e = session
        .execute("SELECT * FROM misfit.wide_net();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("past the stated cap"), "{e}");
    assert!(e.to_string().contains("sample in the frame SQL"), "{e}");

    // Text plus a constant is no surface: the read abstains with the
    // reason instead of serving noise.
    run(
        &session,
        r##"DECLARE ASPECT thin WITH $${"x-kind": "sample"}$$ AS QUERY ON DATASET;
            GLOSS thin ON fin AS $${"sql": "SELECT region, flag FROM orders"}$$;"##,
    )
    .await;
    let e = session
        .execute("SELECT * FROM misfit.thin();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("too little numeric surface"), "{e}");
    assert!(e.to_string().contains("flag (constant)"), "{e}");

    // The read's own columns are reserved: a frame aliasing `misfit`
    // is told to rename, not silently shadowed.
    run(
        &session,
        r##"DECLARE ASPECT clash WITH $${"x-kind": "sample"}$$ AS QUERY ON DATASET;
            GLOSS clash ON fin AS $${"sql": "SELECT amount AS misfit, order_date FROM orders"}$$;"##,
    )
    .await;
    let e = session
        .execute("SELECT * FROM misfit.clash();")
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("already carries a `misfit` column"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_finite_score_refuses_the_read() {
    let store = Store::open_memory().await.unwrap();
    let session = Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "agent-1".into(),
        },
    )
    .expect("session builds")
    .with_runtime(Arc::new(NanKernel));
    let (schema, batch) = orders_table();
    session
        .register_table(
            "orders",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    run(&session, SETUP).await;

    let e = session
        .execute("SELECT * FROM misfit.suspects();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("non-finite"), "{e}");
    assert!(e.to_string().contains("null patterns"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_cache_and_supersession_narrows_immediately() {
    let (session, kernel) = frame_session().await;

    // Two reads, two fits: the ranking is ephemeral by design.
    run(&session, "SELECT * FROM misfit.suspects();").await;
    run(&session, "SELECT * FROM misfit.suspects();").await;
    assert_eq!(kernel.fits.load(Ordering::SeqCst), 2, "no cache");

    // Re-glossing narrows the frame; the next read serves the new one.
    run(
        &session,
        r##"GLOSS suspects ON fin AS $${"sql": "SELECT * FROM orders WHERE amount < 5000"}$$;"##,
    )
    .await;
    let n = table(&session, "SELECT count(*) AS n FROM misfit.suspects();").await;
    assert!(n.contains("23"), "{n}");
    let top = table(
        &session,
        "SELECT amount FROM misfit.suspects() ORDER BY misfit DESC LIMIT 1;",
    )
    .await;
    assert!(!top.contains("10000.0"), "{top}");
}
