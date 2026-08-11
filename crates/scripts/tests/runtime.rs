//! The runtime alone: invocation contract, path fence, kernels on the
//! zero-copy handles — through a fake door, no lake.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use glossql_glossary::FunctionRow;
use glossql_scripts::RhaiRuntime;
use glossql_session::{FunctionRuntime, SqlDoor};
use serde_json::{Value, json};

struct FakeDoor;

impl SqlDoor for FakeDoor {
    fn sql(&self, _query: &str) -> Result<Vec<RecordBatch>, String> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("12.50"),
                Some("8.00"),
                Some("n/a"),
                None,
            ]))],
        )
        .unwrap();
        Ok(vec![batch])
    }
}

fn function(script: &str) -> FunctionRow {
    FunctionRow {
        name: "t".into(),
        scope_dataset: None,
        script: script.into(),
        accepts: vec![],
        returns: Some("t_out".into()),
    }
}

fn runtime_with(dir: &std::path::Path, name: &str, body: &str) -> RhaiRuntime {
    std::fs::write(dir.join(name), body).unwrap();
    RhaiRuntime::new(dir)
}

#[test]
fn scope_carries_subject_context_and_the_door() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        dir.path(),
        "t.rhai",
        r#"
        let t = db.query("ignored");
        let c = t.col("v");
        #{
            subject: subject,
            hint: context.hint,
            rows: c.count(),
            nulls: c.null_count(),
            distinct: c.distinct(),
            min: c.min(),
            parse: c.parse_rate("DOUBLE"),
            matched: c.match_rate("^\\d+\\.\\d+$"),
        }
        "#,
    );
    let out = rt
        .invoke(
            &function("t.rhai"),
            "orders.amount",
            &json!({"hint": 7}),
            Arc::new(FakeDoor),
        )
        .unwrap();
    assert_eq!(out["subject"], json!("orders.amount"));
    assert_eq!(out["hint"], json!(7));
    assert_eq!(out["rows"], json!(4));
    assert_eq!(out["nulls"], json!(1));
    assert_eq!(out["distinct"], json!(3));
    assert_eq!(out["min"], json!("12.50"));
    // 2 of 3 non-null values parse as DOUBLE; the same 2 match the pattern.
    assert!((out["parse"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-9);
    assert!((out["matched"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-9);
}

#[test]
fn distribution_kernels_compute_textbook_values() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        dir.path(),
        "t.rhai",
        r#"
        let c = db.query("ignored").col("v");
        #{
            mean: c.mean(),
            stddev: c.stddev(),
            p50: c.percentile(0.5),
            mad: c.mad(),
            top: c.top_k(1),
            lengths: c.len_stats(),
        }
        "#,
    );
    let out = rt
        .invoke(&function("t.rhai"), "s", &Value::Null, Arc::new(FakeDoor))
        .unwrap();
    // Parsed values are [12.5, 8.0]: mean 10.25, sample stddev 3.1820…,
    // interpolated median 10.25, MAD 2.25.
    assert!((out["mean"].as_f64().unwrap() - 10.25).abs() < 1e-9);
    assert!((out["stddev"].as_f64().unwrap() - 3.181980515).abs() < 1e-6);
    assert!((out["p50"].as_f64().unwrap() - 10.25).abs() < 1e-9);
    assert!((out["mad"].as_f64().unwrap() - 2.25).abs() < 1e-9);
    // Ties break alphabetically; every non-null value appears once.
    assert_eq!(out["top"], json!([{"value": "12.50", "count": 1}]));
    assert_eq!(out["lengths"], json!({"min": 3, "max": 5, "avg": 4.0}));
}

#[test]
fn trial_casts_speak_the_substrates_type_spellings() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        dir.path(),
        "t.rhai",
        r#"
        let c = db.query("ignored").col("v");
        #{
            decimal: c.parse_rate("DECIMAL(12,2)"),
            double: c.parse_rate("DOUBLE PRECISION"),
            unsigned: c.parse_rate("INTEGER UNSIGNED"),
            micros: c.parse_rate("TIMESTAMP(6)"),
        }
        "#,
    );
    let out = rt
        .invoke(&function("t.rhai"), "s", &Value::Null, Arc::new(FakeDoor))
        .unwrap();
    // "12.50" and "8.00" read as decimals and doubles; neither is an
    // unsigned integer or a timestamp.
    assert!((out["decimal"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-9);
    assert!((out["double"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-9);
    assert_eq!(out["unsigned"], json!(0.0));
    assert_eq!(out["micros"], json!(0.0));

    // A spelling DataFusion rejects is refused, not silently trialed —
    // the defect class v0.3's duckdb_types module exists to close.
    let rt = runtime_with(
        dir.path(),
        "bad.rhai",
        r#"db.query("ignored").col("v").parse_rate("TIMESTAMP_NS")"#,
    );
    let err = rt
        .invoke(&function("bad.rhai"), "s", &Value::Null, Arc::new(FakeDoor))
        .unwrap_err();
    assert!(err.contains("not a cast target"), "{err}");
}

#[test]
fn script_paths_stay_under_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let rt = RhaiRuntime::new(dir.path());
    let err = rt
        .invoke(
            &function("../outside.rhai"),
            "s",
            &Value::Null,
            Arc::new(FakeDoor),
        )
        .unwrap_err();
    assert!(err.contains("must stay under the functions root"), "{err}");
}

#[test]
fn a_script_error_names_the_function() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(dir.path(), "t.rhai", r#"throw "no data";"#);
    let err = rt
        .invoke(&function("t.rhai"), "s", &Value::Null, Arc::new(FakeDoor))
        .unwrap_err();
    assert!(err.contains("`t`") && err.contains("no data"), "{err}");
}
