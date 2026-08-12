//! The `whatif.` door end to end (ruled 2026-08-11, fixture 19): a
//! declared scenario replayed through the groundings at a bracketing
//! grid, banded through the runtime's kernel seam. The kernel here is a
//! fake — deterministic linear interpolation over the factor axis — so
//! these tests pin the door's mechanics (dispatch, plan rewrite, the
//! unmoved refusal, cache semantics), while the real ensemble kernel is
//! graded in the sibling's fidelity suite and wired in `glossql-scripts`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind, FunctionRow, Store};
use glossql_session::{FunctionRuntime, Outcome, Session, SqlDoor};
use serde_json::Value;

/// A kernel that interpolates linearly along the factor axis within each
/// month — the mechanics stand-in for the model: exact on the linear
/// replay surface, so frame-assembly mistakes show as wrong numbers.
#[derive(Debug, Default)]
struct LinearKernel {
    fits: AtomicUsize,
}

impl FunctionRuntime for LinearKernel {
    fn invoke(
        &self,
        function: &FunctionRow,
        _: &str,
        _: &Value,
        _: Arc<dyn SqlDoor>,
    ) -> Result<Value, String> {
        Err(format!("no scripts in this test (`{}`)", function.name))
    }

    fn band_grid(
        &self,
        train_x: &[f64],
        rows: usize,
        cols: usize,
        train_y: &[f64],
        test_x: &[f64],
        test_rows: usize,
        alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        self.fits.fetch_add(1, Ordering::SeqCst);
        let mut out = Vec::with_capacity(test_rows * alphas.len());
        for t in 0..test_rows {
            let f = test_x[t * cols];
            let month = test_x[t * cols + cols - 1];
            let mut pts: Vec<(f64, f64)> = (0..rows)
                .filter(|r| (train_x[r * cols + cols - 1] - month).abs() < 1e-9)
                .map(|r| (train_x[r * cols], train_y[r]))
                .collect();
            pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let y = interp(&pts, f);
            out.extend(alphas.iter().map(|_| y));
        }
        Ok(out)
    }
}

fn interp(pts: &[(f64, f64)], x: f64) -> f64 {
    match pts.iter().position(|(f, _)| *f >= x) {
        Some(0) => pts[0].1,
        None => pts.last().map(|(_, y)| *y).unwrap_or(0.0),
        Some(i) => {
            let ((x0, y0), (x1, y1)) = (pts[i - 1], pts[i]);
            y0 + (y1 - y0) * (x - x0) / (x1 - x0)
        }
    }
}

async fn session_with_kernel() -> (Session, Arc<LinearKernel>) {
    let store = Store::open_memory().await.unwrap();
    let kernel = Arc::new(LinearKernel::default());
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

/// One row per month of 2026: 10 units at price 100 — monthly revenue
/// 1000, exactly linear in a price factor. `line_amount` is the stored
/// total: a grounding reading it never sees a `unit_price` override.
fn sales_table() -> (Arc<Schema>, RecordBatch) {
    sales_batch(&(0..12).map(|_| (100.0, 2026)).collect::<Vec<_>>())
}

/// The sales shape over arbitrary months: one row per `(unit_price,
/// year)`, months cycling Jan..Dec in order, 10 units each,
/// `line_amount` the stored total.
fn sales_batch(rows: &[(f64, i32)]) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_date", DataType::Date32, false),
        Field::new("units", DataType::Float64, false),
        Field::new("unit_price", DataType::Float64, false),
        Field::new("line_amount", DataType::Float64, false),
    ]));
    let dates: Vec<i32> = rows
        .iter()
        .enumerate()
        .map(|(i, (_, year))| mid_month(*year, (i % 12) as i32 + 1))
        .collect();
    let prices: Vec<f64> = rows.iter().map(|(p, _)| *p).collect();
    let n = rows.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(dates)),
            Arc::new(Float64Array::from(vec![10.0; n])),
            Arc::new(Float64Array::from(prices.clone())),
            Arc::new(Float64Array::from(
                prices.iter().map(|p| 10.0 * p).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    (schema, batch)
}

/// Days since epoch for the 15th of (year, month) — the civil-calendar
/// arithmetic, so tables can span more than one year.
fn mid_month(year: i32, month: i32) -> i32 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + 14;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i32
}

const SETUP: &str = r##"
DECLARE DATASET fin SET (purpose: 'scenario flows');
USE fin;
DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;
DECLARE ASPECT booked WITH $${"title": "Booked totals"}$$ AS QUERY ON DATASET;
DECLARE ASPECT price_hike WITH $${"type": "object", "required": ["overrides"]}$$ AS FACT ON DATASET;
GLOSS revenue ON fin AS $${"sql": "SELECT order_date, units * unit_price AS value FROM sales"}$$;
GLOSS booked ON fin AS $${"sql": "SELECT order_date, line_amount AS value FROM sales"}$$;
"##;

const SCENARIO: &str = r##"GLOSS price_hike ON fin AS $${"overrides": [
  {"column": "sales.unit_price", "factor": 1.15, "from": "2026-07",
   "basis": "the declared lever"}]}$$;"##;

async fn scenario_session() -> (Session, Arc<LinearKernel>) {
    let (session, kernel) = session_with_kernel().await;
    let (schema, batch) = sales_table();
    session
        .register_table(
            "sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    run(&session, SETUP).await;
    run(&session, SCENARIO).await;
    (session, kernel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_whatif_door_replays_and_bands() {
    let (session, _) = scenario_session().await;

    // The covered concept: six post months, the mechanical replay at
    // exactly x1.15, the kernel's read sitting on the linear surface.
    let revenue = table(
        &session,
        "SELECT month, replay, p50, basis FROM whatif.price_hike() \
         WHERE concept = 'revenue' ORDER BY month;",
    )
    .await;
    for month in [
        "2026-07", "2026-08", "2026-09", "2026-10", "2026-11", "2026-12",
    ] {
        assert!(revenue.contains(month), "{revenue}");
    }
    assert!(!revenue.contains("2026-06"), "{revenue}");
    assert!(revenue.contains("1150.0"), "{revenue}");
    assert!(revenue.contains("in support"), "{revenue}");

    // The stored total never moves under the override: refused with the
    // reason, never served as a silently unchanged number.
    let booked = table(
        &session,
        "SELECT basis FROM whatif.price_hike() WHERE concept = 'booked';",
    )
    .await;
    assert!(booked.contains("unmoved by the overrides"), "{booked}");
    assert!(booked.contains("detect_derivations"), "{booked}");

    // WHERE composes around the door like any relation.
    let narrowed = table(
        &session,
        "SELECT count(*) AS n FROM whatif.price_hike() WHERE p50 IS NOT NULL;",
    )
    .await;
    assert!(narrowed.contains("6"), "{narrowed}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_scenario_read_caches_and_supersession_recomputes() {
    let (session, kernel) = scenario_session().await;

    run(&session, "SELECT * FROM whatif.price_hike();").await;
    let fits = kernel.fits.load(Ordering::SeqCst);
    assert!(fits > 0, "the first read fits the kernel");

    // The second read serves the cache — extract semantics.
    run(&session, "SELECT * FROM whatif.price_hike();").await;
    assert_eq!(kernel.fits.load(Ordering::SeqCst), fits, "cache served");

    // Superseding the scenario stales the cache; the next read replays.
    run(
        &session,
        r##"GLOSS price_hike ON fin AS $${"overrides": [
          {"column": "sales.unit_price", "factor": 1.10, "from": "2026-07",
           "basis": "revised down"}]}$$;"##,
    )
    .await;
    let revenue = table(
        &session,
        "SELECT replay FROM whatif.price_hike() WHERE concept = 'revenue' \
         AND month = '2026-08';",
    )
    .await;
    assert!(
        kernel.fits.load(Ordering::SeqCst) > fits,
        "supersession recomputes"
    );
    assert!(revenue.contains("1100.0"), "{revenue}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_door_refuses_what_is_not_a_scenario() {
    let (session, _) = scenario_session().await;

    let e = session
        .execute("SELECT * FROM whatif.nothing();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no aspect"), "{e}");

    let e = session
        .execute("SELECT * FROM whatif.revenue();")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("query aspect"), "{e}");

    let e = session
        .execute("SELECT * FROM whatif.price_hike('x');")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("takes no arguments"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scenario_beyond_the_recorded_history_is_refused_with_the_reason() {
    let (session, _) = session_with_kernel().await;
    let (schema, batch) = sales_table();
    session
        .register_table(
            "sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    run(&session, SETUP).await;
    run(
        &session,
        r##"GLOSS price_hike ON fin AS $${"overrides": [
          {"column": "sales.unit_price", "factor": 1.15, "from": "2027-06",
           "basis": "next year"}]}$$;"##,
    )
    .await;
    let rows = table(&session, "SELECT concept, basis FROM whatif.price_hike();").await;
    assert!(
        rows.contains("starts after the recorded history ends"),
        "{rows}"
    );
}

async fn session_with_sales(rows: &[(f64, i32)]) -> (Session, Arc<LinearKernel>) {
    let (session, kernel) = session_with_kernel().await;
    let (schema, batch) = sales_batch(rows);
    session
        .register_table(
            "sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    (session, kernel)
}

/// One QUERY concept whose grounding the tests gloss per case, plus the
/// scenario aspect.
const GATED_SETUP: &str = r##"
DECLARE DATASET fin SET (purpose: 'scenario flows');
USE fin;
DECLARE ASPECT gated WITH $${"title": "Gated"}$$ AS QUERY ON DATASET;
DECLARE ASPECT price_hike WITH $${"type": "object", "required": ["overrides"]}$$ AS FACT ON DATASET;
"##;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseding_a_concept_grounding_recomputes_the_cached_read() {
    let (session, kernel) = scenario_session().await;

    run(&session, "SELECT * FROM whatif.price_hike();").await;
    let fits = kernel.fits.load(Ordering::SeqCst);
    run(&session, "SELECT * FROM whatif.price_hike();").await;
    assert_eq!(kernel.fits.load(Ordering::SeqCst), fits, "cache served");

    // Superseding a concept's QUERY grounding — not the scenario —
    // stales the cache: the computation replayed that grounding
    // (finding 4, 2026-08-12).
    run(
        &session,
        r##"GLOSS revenue ON fin AS $${"sql": "SELECT order_date, units * unit_price + 1000 AS value FROM sales"}$$;"##,
    )
    .await;
    let revenue = table(
        &session,
        "SELECT replay FROM whatif.price_hike() WHERE concept = 'revenue' \
         AND month = '2026-08';",
    )
    .await;
    assert!(
        kernel.fits.load(Ordering::SeqCst) > fits,
        "a superseded grounding recomputes"
    );
    assert!(revenue.contains("2150.0"), "{revenue}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scenario_from_the_last_recorded_month_serves_the_one_month() {
    // One post month (finding 1's trigger): the month-index feature is
    // constant over the frame — the kernel seam drops it, the door
    // serves the month, never a dead read.
    let (session, _) =
        session_with_sales(&(0..12).map(|_| (100.0, 2026)).collect::<Vec<_>>()).await;
    run(&session, SETUP).await;
    run(
        &session,
        r##"GLOSS price_hike ON fin AS $${"overrides": [
          {"column": "sales.unit_price", "factor": 1.15, "from": "2026-12",
           "basis": "the last recorded month"}]}$$;"##,
    )
    .await;
    let revenue = table(
        &session,
        "SELECT month, replay, p50, basis FROM whatif.price_hike() WHERE concept = 'revenue';",
    )
    .await;
    assert!(revenue.contains("2026-12"), "{revenue}");
    assert!(revenue.contains("1150.0"), "{revenue}");
    assert!(revenue.contains("worlds x 1 months"), "{revenue}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_past_the_kernel_cap_is_refused_with_the_number() {
    // 6 worlds x 350 post months = 2100 training rows — past the bound
    // the misfit door measured; refused with the number and the fix
    // before any world replays (finding 8, 2026-08-12).
    let rows: Vec<(f64, i32)> = (0..350).map(|i| (100.0, 2000 + i / 12)).collect();
    let (session, kernel) = session_with_sales(&rows).await;
    run(&session, SETUP).await;
    run(
        &session,
        r##"GLOSS price_hike ON fin AS $${"overrides": [
          {"column": "sales.unit_price", "factor": 1.15, "from": "2000-01",
           "basis": "the whole history"}]}$$;"##,
    )
    .await;
    let served = table(
        &session,
        "SELECT basis FROM whatif.price_hike() WHERE concept = 'revenue';",
    )
    .await;
    assert!(served.contains("2100 rows"), "{served}");
    assert!(served.contains("past the kernel cap (2000)"), "{served}");
    assert!(
        served.contains("narrow the scenario's date range"),
        "{served}"
    );
    assert_eq!(
        kernel.fits.load(Ordering::SeqCst),
        0,
        "the kernel never ran"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_basis_counts_the_worlds_that_actually_contributed() {
    // A grounding gated to unit_price in [99, 121]: the 0.90 and 1.30
    // worlds push every post month out of the window, their rosters
    // change, and they are skipped — the served basis says 4 of 6 and
    // names them, never the full grid (finding 8, 2026-08-12).
    let (session, _) =
        session_with_sales(&(0..12).map(|_| (100.0, 2026)).collect::<Vec<_>>()).await;
    run(&session, GATED_SETUP).await;
    run(
        &session,
        r##"GLOSS gated ON fin AS $${"sql": "SELECT order_date, CASE WHEN unit_price BETWEEN 99 AND 121 THEN units * unit_price END AS value FROM sales"}$$;"##,
    )
    .await;
    run(&session, SCENARIO).await;
    let served = table(
        &session,
        "SELECT month, p50, basis FROM whatif.price_hike() \
         WHERE concept = 'gated' AND month = '2026-08';",
    )
    .await;
    assert!(served.contains("4 of 6 worlds"), "{served}");
    assert!(
        served.contains("skipped x[0.90, 1.30]: roster changed"),
        "{served}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_only_support_is_refused() {
    // The window admits only the baseline price and the declared 1.15:
    // every bracketed world's roster changes, so bands would rest on
    // baseline alone — refused, never served with a false support
    // claim (finding 8, 2026-08-12).
    let (session, kernel) =
        session_with_sales(&(0..12).map(|_| (100.0, 2026)).collect::<Vec<_>>()).await;
    run(&session, GATED_SETUP).await;
    run(
        &session,
        r##"GLOSS gated ON fin AS $${"sql": "SELECT order_date, CASE WHEN unit_price BETWEEN 99 AND 101 OR unit_price BETWEEN 114 AND 116 THEN units * unit_price END AS value FROM sales"}$$;"##,
    )
    .await;
    run(&session, SCENARIO).await;
    let served = table(
        &session,
        "SELECT basis FROM whatif.price_hike() WHERE concept = 'gated';",
    )
    .await;
    assert!(
        served.contains("every bracketed world changed the month roster"),
        "{served}"
    );
    assert!(served.contains("5 of 6 worlds skipped"), "{served}");
    assert_eq!(
        kernel.fits.load(Ordering::SeqCst),
        0,
        "the kernel never ran"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_roster_swap_of_equal_length_is_refused() {
    // September priced out of the window at baseline, October priced
    // out under the declared factor: the joint roster keeps its length
    // but swaps a month — caught by value, not count (finding 8,
    // 2026-08-12).
    let mut rows: Vec<(f64, i32)> = (0..12).map(|_| (100.0, 2026)).collect();
    rows[8].0 = 90.0; // Sep: out at baseline (< 95), in at x1.15 (103.5)
    rows[9].0 = 105.0; // Oct: in at baseline, out at x1.15 (120.75)
    let (session, _) = session_with_sales(&rows).await;
    run(&session, GATED_SETUP).await;
    run(
        &session,
        r##"GLOSS gated ON fin AS $${"sql": "SELECT order_date, CASE WHEN unit_price BETWEEN 95 AND 116 THEN units * unit_price END AS value FROM sales"}$$;"##,
    )
    .await;
    run(&session, SCENARIO).await;
    let served = table(
        &session,
        "SELECT basis FROM whatif.price_hike() WHERE concept = 'gated';",
    )
    .await;
    assert!(served.contains("changed the month roster"), "{served}");
}
