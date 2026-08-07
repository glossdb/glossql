//! Fixture 12's judge pattern through the doors: an agent's gloss and a
//! human's disagreement meet on one slot, `slot_entropy` bands the
//! dispute, the collapsed read withholds — and each closure route ends
//! it: the voices converging by supersession, or the human striking the
//! disputed slot. Real runtime, real bootstrap, both doors.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_scripts::RhaiRuntime;
use glossql_serverd::{DoorConfig, Plane, bootstrap, router};
use serde_json::{Value, json};
use tower::ServiceExt;

/// A bootstrapped workspace behind both doors: the reference scripts on
/// disk, the measurement library declared, `slot_entropy` ready to band.
async fn app() -> (Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_memory().await.unwrap();
    let runtime = Arc::new(RhaiRuntime::new(dir.path().to_path_buf()));
    let plane = Arc::new(Plane::new(store.clone(), None, runtime));
    bootstrap(&store, &plane, dir.path(), Actor {
        kind: ActorKind::Human,
        id: "cockpit".into(),
    })
    .await
    .unwrap();
    let workspace = dir.path().to_path_buf();
    (router(plane, DoorConfig::default(), workspace), dir)
}

/// The agent's door: one `glossql` tool call, outcomes parsed from the
/// tool result; a refused call panics with what the door said.
async fn agent(app: &Router, id: u64, statements: &str) -> Value {
    let payload = json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {"name": "judge-agent", "version": "0"}
            },
            "name": "glossql",
            "arguments": {"statements": statements}
        }
    });
    let request = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "glossql")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
}

/// The human's door: statements through `/query`; sequences answer in
/// the wire JSON shape (a lone read would stream Arrow instead).
async fn human(app: &Router, statements: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::post("/query")
                .body(Body::from(statements.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// The shared opening: dataset, the `behavior` vocabulary with the
/// bootstrap's detector on its witness, and the agent's gloss.
async fn agent_glosses(app: &Router, subject: &str) {
    agent(
        app,
        1,
        &format!(
            r#"
            DECLARE DATASET fin SET (purpose: 'judge pattern');
            USE fin;
            DECLARE ASPECT behavior WITH $${{
              "type": "object", "required": ["value"],
              "properties": {{"value": {{"enum": ["stock", "flow"]}}}}
            }}$$ AS FACT;
            DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN)
              DETECTOR slot_entropy THRESHOLD 0.7;
            GLOSS behavior ON {subject} AS $${{"value": "flow"}}$$;
            "#
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_judge_pattern_contests_then_converges_through_the_doors() {
    let (app, _dir) = app().await;
    agent_glosses(&app, "trial_balance.debit_balance").await;

    // One voice: the detector has nothing to arbitrate.
    let attest = agent(
        &app,
        2,
        "SELECT band, score FROM ATTEST(trial_balance.debit_balance::behavior);",
    )
    .await;
    assert_eq!(attest[0]["rows"][0]["band"], json!("green"), "{attest}");

    // The human disagrees on the same slot, through their own door.
    human(
        &app,
        r#"USE fin; GLOSS behavior ON trial_balance.debit_balance AS $${"value": "stock"}$$;"#,
    )
    .await;

    // The dispute crosses the wire: red band, and the collapsed read
    // withholds the value rather than picking a winner.
    let attest = agent(
        &app,
        3,
        "SELECT band, score FROM ATTEST(trial_balance.debit_balance::behavior);",
    )
    .await;
    assert_eq!(attest[0]["rows"][0]["band"], json!("red"), "{attest}");
    let glossary = agent(
        &app,
        4,
        "SELECT value, state FROM GLOSSARY(trial_balance.debit_balance::behavior);",
    )
    .await;
    assert_eq!(glossary[0]["rows"][0]["state"], json!("contested"), "{glossary}");
    assert_eq!(glossary[0]["rows"][0]["value"], Value::Null, "{glossary}");

    // Closure by convergence: the human re-grounds, accepts the agent's
    // reading, and supersedes their own slot. The verdict is stale the
    // moment the newer slot lands, so the next read recomputes it.
    human(
        &app,
        r#"USE fin; GLOSS behavior ON trial_balance.debit_balance AS $${"value": "flow"}$$;"#,
    )
    .await;
    let attest = agent(
        &app,
        5,
        "SELECT band, score FROM ATTEST(trial_balance.debit_balance::behavior);",
    )
    .await;
    assert_eq!(attest[0]["rows"][0]["band"], json!("green"), "{attest}");
    let glossary = agent(
        &app,
        6,
        "SELECT value, state FROM GLOSSARY(trial_balance.debit_balance::behavior);",
    )
    .await;
    assert_eq!(glossary[0]["rows"][0]["state"], json!("current"), "{glossary}");
    assert!(
        glossary[0]["rows"][0]["value"]
            .as_str()
            .unwrap()
            .contains("flow"),
        "{glossary}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn closure_by_striking_the_disputed_slot_recomputes_the_verdict() {
    let (app, _dir) = app().await;
    agent_glosses(&app, "trial_balance.credit_balance").await;

    // The disagreement, and a read so the red verdict is actually cached
    // — the trap this test guards is that verdict outliving its slots.
    human(
        &app,
        r#"USE fin; GLOSS behavior ON trial_balance.credit_balance AS $${"value": "stock"}$$;"#,
    )
    .await;
    let glossary = agent(
        &app,
        2,
        "SELECT value, state FROM GLOSSARY(trial_balance.credit_balance::behavior);",
    )
    .await;
    assert_eq!(glossary[0]["rows"][0]["state"], json!("contested"), "{glossary}");

    // Closure by authority: the human strikes the agent's slot. Deletion
    // makes the slot set smaller, never newer — the strike itself must
    // invalidate the verdict, or the read would keep withholding a value
    // nobody disputes anymore.
    human(
        &app,
        "USE fin; DELETE FROM glossary \
         WHERE subject = 'trial_balance.credit_balance' \
         AND aspect = 'behavior' AND actor_kind = 'agent';",
    )
    .await;

    let attest = agent(
        &app,
        3,
        "SELECT band, score FROM ATTEST(trial_balance.credit_balance::behavior);",
    )
    .await;
    assert_eq!(attest[0]["rows"][0]["band"], json!("green"), "{attest}");
    let glossary = agent(
        &app,
        4,
        "SELECT value, state FROM GLOSSARY(trial_balance.credit_balance::behavior);",
    )
    .await;
    assert_eq!(glossary[0]["rows"][0]["state"], json!("current"), "{glossary}");
    assert!(
        glossary[0]["rows"][0]["value"]
            .as_str()
            .unwrap()
            .contains("stock"),
        "the human's voice stands alone: {glossary}"
    );
}
