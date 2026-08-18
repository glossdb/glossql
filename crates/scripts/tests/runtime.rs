//! The runtime alone: the invocation contract for judges — no door, no
//! lake; a script computes from its scope constants alone.

use glossql_glossary::FunctionRow;
use glossql_scripts::RhaiRuntime;
use glossql_session::FunctionRuntime;
use serde_json::{Value, json};

fn function(script: &str) -> FunctionRow {
    FunctionRow {
        name: "t".into(),
        scope_dataset: None,
        script: script.into(),
        accepts: vec![],
        returns: Some("t_out".into()),
    }
}

/// The runtime needs a workspace only for the band model's weights;
/// scripts arrive with their declarations (fixture 24).
fn runtime(dir: &std::path::Path) -> RhaiRuntime {
    RhaiRuntime::new(dir)
}

#[test]
fn scope_carries_subject_and_context() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    let body = r#"
        #{
            subject: subject,
            hint: context.hint,
        }
        "#;
    let out = rt
        .invoke(&function(body), "orders.amount", &json!({"hint": 7}))
        .unwrap();
    assert_eq!(out["subject"], json!("orders.amount"));
    assert_eq!(out["hint"], json!(7));
}


#[test]
fn a_body_that_will_not_compile_names_the_function() {
    // What the path fence used to guard is gone with paths (fixture 24):
    // a declaration carries its body, so nothing resolves under a root.
    // The failure that replaced it is a body that will not compile, and
    // it must name the function rather than a line number alone.
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    let err = rt
        .invoke(&function("#{ unclosed:"), "s", &Value::Null)
        .unwrap_err();
    assert!(err.starts_with("`t`"), "{err}");
}

#[test]
fn a_script_error_names_the_function() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    let body = r#"throw "no data";"#;
    let err = rt
        .invoke(&function(body), "s", &Value::Null)
        .unwrap_err();
    assert!(err.contains("`t`") && err.contains("no data"), "{err}");
}
