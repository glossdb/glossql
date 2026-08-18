//! The band plane end to end: the shipped metric_bands body walked
//! over synthetic monthly series through a real session — since
//! stage 5 the walk is the metric_band_walk door and the body is SQL —
//! and the band_breach detector over fabricated slots. The kernel's model loads from a weights directory
//! symlinked from the sibling port checkout — tests that need it skip
//! with a message when the sibling has no converted weights, and cost
//! ~3s each on Metal (measured 2026-08-13). The numeric fidelity of
//! the forward itself is the sibling repo's suite; here the contract
//! is shape, ordering, and policy.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use glossql_glossary::FunctionRow;
use glossql_scripts::RhaiRuntime;
use glossql_session::{FunctionRuntime, SqlDoor};
use serde_json::{Value, json};

fn sibling() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../tabicl-candle"
    ))
}

/// A workspace with a weights directory symlinked from the sibling
/// checkout (the flat deployment layout). Scripts arrive with their
/// declarations (fixture 24), so nothing is copied here.
fn workspace(dir: &Path) {
    let weights = dir.join("weights");
    std::fs::create_dir_all(&weights).unwrap();
    for (from, to) in [
        (
            "weights/tabicl-regressor.safetensors",
            "tabicl-regressor.safetensors",
        ),
        (
            "weights/tabicl-regressor.config.json",
            "tabicl-regressor.config.json",
        ),
        ("fixtures/DIGESTS", "DIGESTS"),
    ] {
        let _ = std::os::unix::fs::symlink(sibling().join(from), weights.join(to));
    }
}

fn have_weights() -> bool {
    sibling()
        .join("weights/tabicl-regressor.safetensors")
        .exists()
}

/// A refusing door for detector invocations — a detector sees slots
/// and threshold, never table data.
struct NoDoor;

impl SqlDoor for NoDoor {
    fn sql(&self, _query: &str) -> Result<Vec<RecordBatch>, String> {
        Err("no door in this test".into())
    }
}

fn invoke(dir: &Path, script: &str, subject: &str, context: Value) -> Value {
    let rt = RhaiRuntime::new(dir);
    rt.invoke(
        &FunctionRow {
            name: script.into(),
            scope_dataset: None,
            script: glossql_scripts::library::script(&format!("{script}.rhai"))
                .expect("shipped")
                .into(),
            accepts: vec![],
            returns: None,
        },
        subject,
        &context,
        Arc::new(NoDoor),
    )
    .unwrap()
}

/// Date32 day offsets for each month's first day, 2024-01 through
/// 2025-06 (2024 is a leap year); 19723 = 2024-01-01.
const FIRSTS: [i32; 18] = [
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397, 425, 456, 486, 517,
];

/// A session over a lake in `dir`, the walk declared from the shipped
/// body, `lines` and `levels` landed from the given rows.
async fn walk_session(
    dir: &Path,
    tables: Vec<(&str, Vec<i32>, Vec<f64>)>,
    glosses: &[&str],
) -> glossql_session::Session {
    use datafusion::datasource::MemTable;
    use glossql_catalog::Lake;
    use glossql_glossary::{Actor, ActorKind, Store};

    let lake = Lake::open(&dir.join("catalog.db"), &dir.join("warehouse"))
        .await
        .unwrap();
    let store = Store::open_scratch(lake).await.unwrap();
    let session = glossql_session::Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "t".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(RhaiRuntime::new(dir)));
    session
        .execute("DECLARE DATASET fin SET (purpose: 'band walks'); USE fin;")
        .await
        .unwrap();
    for (name, dates, values) in tables {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(datafusion::arrow::array::Date32Array::from(dates)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();
        session
            .register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .await
            .unwrap();
    }
    let declarations = glossql_scripts::library::splice(
        r#"DECLARE ASPECT metric_bands WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"},
                            "metrics": {"type": "array"}}}$$ AS MEASUREMENT ON DATASET;
           DECLARE FUNCTION metric_bands FOR GLOBAL AS $$metric_bands.sql$$
             RETURNS metric_bands;"#,
    )
    .expect("shipped body splices");
    session.execute(&declarations).await.unwrap();
    for g in glosses {
        session.execute(g).await.unwrap();
    }
    session
        .execute("SELECT metric_bands() FROM fin;")
        .await
        .unwrap();
    session
}

async fn walked(session: &glossql_session::Session) -> Value {
    let outcomes = session
        .execute("SELECT value FROM GLOSSARY(fin::metric_bands) WHERE state = 'current';")
        .await
        .unwrap();
    let Some(glossql_session::Outcome::Rows(batches)) = outcomes.last() else {
        panic!("a value row")
    };
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let text =
        datafusion::arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap();
    serde_json::from_str(&text).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn metric_bands_walks_and_reads_the_breach() {
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());

    // A year and a half of a rising monthly flow with seasonality; the
    // last month is an obvious breach. The stock series is a rising
    // level whose last month collapses — a lower breach.
    let months = 18usize;
    let (mut flow, mut stock) = (Vec::new(), Vec::new());
    for i in 0..months {
        let seasonal = if i % 12 == 11 { 20.0 } else { 0.0 };
        flow.push(100.0 + 3.0 * i as f64 + seasonal);
        stock.push(1000.0 + 5.0 * i as f64);
    }
    *flow.last_mut().unwrap() = 500.0;
    *stock.last_mut().unwrap() = 200.0;
    let dates: Vec<i32> = FIRSTS.iter().map(|f| 19723 + f).collect();

    let session = walk_session(
        dir.path(),
        vec![
            ("lines", dates.clone(), flow),
            ("levels", dates, stock),
        ],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
        ],
    )
    .await;
    let out = walked(&session).await;

    assert_eq!(out["applicable"], json!(true));
    let by_name = |name: &str| -> Value {
        out["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["metric"] == json!(name))
            .unwrap_or_else(|| panic!("metric {name} missing"))
            .clone()
    };

    for (name, aggregation) in [("revenue", "sum"), ("inventory", "latest-sum")] {
        let metric = by_name(name);
        assert_eq!(metric["grain"], json!("month"));
        assert_eq!(metric["aggregation"], json!(aggregation), "{name}");
        let points = metric["points"].as_array().unwrap();
        assert_eq!(points.len(), 6, "the walk covers the last six months");
        for p in points {
            let (p05, p10, p50, p90, p95) = (
                p["p05"].as_f64().unwrap(),
                p["p10"].as_f64().unwrap(),
                p["p50"].as_f64().unwrap(),
                p["p90"].as_f64().unwrap(),
                p["p95"].as_f64().unwrap(),
            );
            assert!(p05 <= p10 && p10 <= p50 && p50 <= p90 && p90 <= p95);
            let pit = p["pit"].as_f64().unwrap();
            assert!((0.0..=1.0).contains(&pit));
        }
        let inside = &points[points.len() - 3];
        let pit = inside["pit"].as_f64().unwrap();
        assert!(
            (0.02..0.98).contains(&pit),
            "{name}: an in-pattern month, pit {pit}"
        );
    }

    // The manufactured breaches: the flow's 500 is an extreme upper
    // breach; the stock's collapse to 200 an extreme lower one.
    let flow_last = by_name("revenue")["points"][5]["pit"].as_f64().unwrap();
    assert!(flow_last > 0.95, "flow breach month, pit {flow_last}");
    let stock_last = by_name("inventory")["points"][5]["pit"].as_f64().unwrap();
    assert!(stock_last < 0.05, "stock breach month, pit {stock_last}");
}

/// The 2026-08-14 regression: a multi-row stock's walk actuals must be
/// the month's latest-date SUM, not one arbitrary row. Three product
/// rows stand at each month's day 25 (sum 1750 + 30·m); a day-5 decoy
/// snapshot per month must be excluded by the latest-date rank.
#[tokio::test(flavor = "multi_thread")]
async fn the_stock_walk_sums_the_months_latest_snapshot() {
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());

    let months = 18usize;
    let (mut dates, mut values) = (Vec::new(), Vec::new());
    for m in 0..months {
        let month_start = 19723 + FIRSTS[m];
        for v in [1000.0, 500.0, 250.0] {
            dates.push(month_start + 24);
            values.push(v + 10.0 * m as f64);
        }
        dates.push(month_start + 4);
        values.push(9_000_000.0);
    }

    let session = walk_session(
        dir.path(),
        vec![("levels", dates, values)],
        &[
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
        ],
    )
    .await;
    let out = walked(&session).await;

    let metric = out["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["metric"] == json!("inventory"))
        .expect("inventory walked")
        .clone();
    assert_eq!(metric["aggregation"], json!("latest-sum"));
    let points = metric["points"].as_array().unwrap();
    assert_eq!(points.len(), 6, "the walk covers the last six months");
    for (i, p) in points.iter().enumerate() {
        let m = (months - 6 + i) as f64;
        let expected = 1750.0 + 30.0 * m;
        let actual = p["actual"].as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-6,
            "month index {m}: actual {actual}, expected the day-25 sum {expected}"
        );
    }
}

#[test]
fn band_breach_adjudicates_the_worst_pit() {
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());
    let slots = |pits: Vec<f64>| {
        json!({
            "subject": "fin", "aspect": "metric_bands", "witness": "w",
            "threshold": null,
            "slots": [{"body": {"applicable": true, "metrics": pits
                .iter()
                .map(|p| json!({"metric": "m", "points": [{"pit": p}]}))
                .collect::<Vec<_>>()}}],
        })
    };
    let read = |pits: Vec<f64>| invoke(dir.path(), "band_breach", "fin", slots(pits));

    let calm = read(vec![0.5, 0.62]);
    assert_eq!(calm["band"], json!("green"));
    assert!(calm["score"].as_f64().unwrap() < 0.3);

    let edge = read(vec![0.5, 0.93]);
    assert_eq!(edge["band"], json!("yellow"));

    let breach = read(vec![0.996, 0.5]);
    assert_eq!(breach["band"], json!("red"));
    assert!(breach["score"].as_f64().unwrap() > 0.98);
}

#[test]
fn band_grid_reads_the_replay_frame_with_the_real_ensemble() {
    // The whatif door's seam (ruled 2026-08-11) against the real model:
    // the eval's frame shape — (factor, month_index) over bracketing
    // support worlds, y exactly linear in the factor — read at the
    // held-out declared point. The numeric fidelity of the ensemble is
    // the sibling suite's business; here the contract is shape,
    // monotone quantiles, and a read that lands near the surface.
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());
    let rt = RhaiRuntime::new(dir.path());

    let factors = [1.0, 0.90, 1.05, 1.10, 1.20, 1.30];
    let months = 6..12;
    let mut train_x = Vec::new();
    let mut train_y = Vec::new();
    for f in factors {
        for m in months.clone() {
            train_x.extend([f, m as f64]);
            train_y.push(1000.0 * f);
        }
    }
    let mut test_x = Vec::new();
    for m in months.clone() {
        test_x.extend([1.15, m as f64]);
    }
    let alphas = [0.05, 0.10, 0.50, 0.90, 0.95];
    let q = rt
        .band_grid(&train_x, train_y.len(), 2, &train_y, &test_x, 6, &alphas)
        .unwrap();

    assert_eq!(q.len(), 6 * alphas.len());
    for row in q.chunks(alphas.len()) {
        for pair in row.windows(2) {
            assert!(pair[0] <= pair[1] + 1e-6, "monotone quantiles: {row:?}");
        }
        let p50 = row[2];
        assert!(
            (p50 - 1150.0).abs() / 1150.0 < 0.15,
            "the declared point reads near the surface: p50 = {p50}"
        );
    }
}

#[test]
fn misfit_scores_rank_the_planted_violator_with_the_real_density() {
    // The misfit door's seam (ruled 2026-08-11, fixture 20) against the
    // real chain-rule density: a frame whose columns cohere (y ≈ 2x,
    // z = x + y) with one planted row that betrays the relation while
    // every marginal value stays in range — the eval's shuffled-pairing
    // shape. The score's fidelity is the sibling suite's business; here
    // the contract is shape and that the violator ranks worst.
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());
    let rt = RhaiRuntime::new(dir.path());

    let n = 40;
    let mut x = Vec::with_capacity(n * 3);
    for i in 0..n {
        let a = 10.0 + (i as f64) * 37.0 % 90.0;
        let (a, b) = if i == 17 {
            (15.0, 170.0) // in-range marginals, impossible pairing
        } else {
            (a, 2.0 * a + (i as f64 % 5.0))
        };
        x.extend([a, b, a + b]);
    }
    let scores = rt.misfit_scores(&x, n, 3).unwrap();

    assert_eq!(scores.len(), n);
    let worst = scores
        .iter()
        .enumerate()
        .min_by(|a, b| a.1.total_cmp(b.1))
        .map(|(i, _)| i)
        .unwrap();
    assert_eq!(worst, 17, "the violator carries the lowest log density");
}

/// Not a gate — the measurement behind the row cap. Run explicitly:
/// `cargo test -p glossql-scripts --test bands -- --ignored --nocapture`
/// (GLOSSQL_DEVICE=cpu for the CPU numbers).
#[test]
#[ignore]
fn misfit_timing_by_frame_size() {
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());
    let rt = RhaiRuntime::new(dir.path());

    for rows in [128usize, 256, 512, 1024, 2048, 4096, 8192, 16384] {
        let cols = 4usize;
        let mut x = Vec::with_capacity(rows * cols);
        for i in 0..rows {
            let a = 10.0 + (i as f64) * 37.0 % 90.0;
            let b = 2.0 * a + (i as f64 % 5.0);
            x.extend([a, b, a + b, (i % 30) as f64]);
        }
        let t = std::time::Instant::now();
        let scores = rt.misfit_scores(&x, rows, cols).unwrap();
        assert_eq!(scores.len(), rows);
        eprintln!("misfit {rows} rows x {cols} cols: {:.2?}", t.elapsed());
    }
}
