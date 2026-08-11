//! The band plane end to end at the runtime seam: the shipped
//! metric_bands.rhai walked over a synthetic monthly series through a
//! query-routing fake door, and the band_breach detector over
//! fabricated slots. The kernel's model loads from a weights directory
//! symlinked from the sibling port checkout — tests that need it skip
//! with a message when the sibling has no converted weights. The
//! numeric fidelity of the forward itself is the sibling repo's suite;
//! here the contract is shape, ordering, and policy.

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

/// A workspace with the real shipped scripts and a weights directory
/// symlinked from the sibling checkout (the flat deployment layout).
fn workspace(dir: &Path) {
    let functions = dir.join("functions");
    std::fs::create_dir_all(&functions).unwrap();
    let shipped = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/functions"));
    for name in ["metric_bands.rhai", "band_breach.rhai"] {
        std::fs::copy(shipped.join(name), functions.join(name)).unwrap();
    }
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

/// Routes the three queries metric_bands.rhai sends: the glossary
/// collapse, the extract probe, and the monthly series.
struct MetricDoor;

impl SqlDoor for MetricDoor {
    fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, String> {
        if query.contains("FROM glossary") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("subject", DataType::Utf8, false),
                Field::new("aspect", DataType::Utf8, false),
                Field::new("actor_kind", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
            ]));
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["fin", "fin"])),
                        Arc::new(StringArray::from(vec!["revenue", "inventory"])),
                        Arc::new(StringArray::from(vec!["agent", "agent"])),
                        Arc::new(StringArray::from(vec![
                            r#"{"sql": "SELECT date, value FROM lines"}"#,
                            r#"{"sql": "SELECT date, value FROM levels", "behavior": "stock"}"#,
                        ])),
                    ],
                )
                .unwrap(),
            ]);
        }
        // A year and a half of a rising monthly flow with seasonality;
        // the last month is an obvious breach. The stock series is a
        // rising level whose last month collapses — a lower breach.
        let months = 18usize;
        let (mut periods, mut values) = (Vec::new(), Vec::new());
        let stock = query.contains("row_number");
        for i in 0..months {
            periods.push(format!(
                "20{:02}-{:02}-01T00:00:00",
                24 + i / 12,
                1 + i % 12
            ));
            if stock {
                values.push(1000.0 + 5.0 * i as f64);
            } else {
                let seasonal = if i % 12 == 11 { 20.0 } else { 0.0 };
                values.push(100.0 + 3.0 * i as f64 + seasonal);
            }
        }
        *values.last_mut().unwrap() = if stock { 200.0 } else { 500.0 }; // the breach
        if query.contains("date_trunc") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("period", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]));
            return Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(periods)),
                        Arc::new(Float64Array::from(values)),
                    ],
                )
                .unwrap(),
            ]);
        }
        // the probe: one row of the raw extract, date-typed time axis
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        Ok(vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(datafusion::arrow::array::Date32Array::from(vec![19000])),
                    Arc::new(Float64Array::from(vec![1.0])),
                ],
            )
            .unwrap(),
        ])
    }
}

fn invoke(dir: &Path, script: &str, subject: &str, context: Value) -> Value {
    let rt = RhaiRuntime::new(dir);
    rt.invoke(
        &FunctionRow {
            name: script.into(),
            scope_dataset: None,
            script: format!("functions/{script}.rhai"),
            accepts: vec![],
            returns: None,
        },
        subject,
        &context,
        Arc::new(MetricDoor),
    )
    .unwrap()
}

#[test]
fn metric_bands_walks_and_reads_the_breach() {
    if !have_weights() {
        eprintln!("skipping: no converted weights in the sibling checkout");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    workspace(dir.path());
    let out = invoke(dir.path(), "metric_bands", "fin", json!({}));

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

    for (name, aggregation) in [("revenue", "sum"), ("inventory", "last")] {
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
