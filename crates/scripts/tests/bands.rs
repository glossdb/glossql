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
        "/../../../dataraum-tabicl"
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
                        Arc::new(StringArray::from(vec!["fin"])),
                        Arc::new(StringArray::from(vec!["revenue"])),
                        Arc::new(StringArray::from(vec!["agent"])),
                        Arc::new(StringArray::from(vec![
                            r#"{"sql": "SELECT date, value FROM lines"}"#,
                        ])),
                    ],
                )
                .unwrap(),
            ]);
        }
        // A year and a half of a rising monthly flow with seasonality;
        // the last month is an obvious breach.
        let months = 18usize;
        let (mut periods, mut values) = (Vec::new(), Vec::new());
        for i in 0..months {
            periods.push(format!(
                "20{:02}-{:02}-01T00:00:00",
                24 + i / 12,
                1 + i % 12
            ));
            let seasonal = if i % 12 == 11 { 20.0 } else { 0.0 };
            values.push(100.0 + 3.0 * i as f64 + seasonal);
        }
        *values.last_mut().unwrap() = 500.0; // the breach
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
    let metric = &out["metrics"][0];
    assert_eq!(metric["metric"], json!("revenue"));
    assert_eq!(metric["grain"], json!("month"));
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
    // The in-pattern months read inside; the manufactured 500 reads as
    // an extreme upper breach.
    let last = points.last().unwrap();
    assert!(last["pit"].as_f64().unwrap() > 0.95, "the breach month");
    let inside = &points[points.len() - 3];
    let pit = inside["pit"].as_f64().unwrap();
    assert!(
        (0.02..0.98).contains(&pit),
        "an in-pattern month, pit {pit}"
    );
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
