//! The doors against a real plane, driven in-process through the router
//! (tower's oneshot — no sockets): `/query` streams Arrow IPC a reader
//! round-trips; the MCP door initializes, lists the one tool, executes
//! statements, and answers a failed statement as a tool error, not a
//! protocol error.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode, header};
use datafusion::arrow::array::Int64Array;
use glossql_glossary::Store;
use glossql_serverd::{ARROW_STREAM, DoorConfig, Plane, router};
use glossql_session::NoRuntime;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn app_with(doors: DoorConfig) -> Router {
    let store = Store::open_memory().await.unwrap();
    let plane = Arc::new(Plane::new(store, None, Arc::new(NoRuntime)));
    // No apps live here — the app door serves an empty home.
    router(plane, doors, std::env::temp_dir())
}

async fn app() -> Router {
    app_with(DoorConfig::default()).await
}

async fn body_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_query_door_streams_arrow_ipc() {
    let app = app().await;
    let response = app
        .oneshot(
            Request::post("/query")
                .body(Body::from("SELECT 1 + 1 AS two"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], ARROW_STREAM);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
            .unwrap();
    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "two");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(column.value(0), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_query_door_answers_a_statement_sequence_in_json() {
    let app = app().await;
    let response = app
        .oneshot(
            Request::post("/query")
                .body(Body::from("SELECT 1 AS a; SELECT 2 AS b"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let outcomes = body.as_array().unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0]["rows"][0]["a"], json!(1));
    assert_eq!(outcomes[1]["rows"][0]["b"], json!(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_query_door_answers_a_refusal_in_the_body() {
    let app = app().await;
    let response = app
        .oneshot(
            Request::post("/query")
                .body(Body::from("USE nothing"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(response).await;
    assert!(body["error"].as_str().unwrap().contains("nothing"), "{body}");
}

/// One stateless JSON-RPC POST to /mcp (the 2026-07-28 revision needs no
/// transport session; json_response mode answers in plain JSON).
async fn mcp(app: Router, payload: Value) -> Response<Body> {
    let method = payload["method"].as_str().unwrap().to_string();
    let mut request = Request::post("/mcp")
        // oneshot skips what every real client sends; the transport's
        // rebinding guard (allowed_hosts) rightly insists on it.
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    if let Some(name) = payload["params"]["name"].as_str() {
        request = request.header("mcp-name", name.to_string());
    }
    app.oneshot(request.body(Body::from(payload.to_string())).unwrap())
        .await
        .unwrap()
}

fn initialize() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {
            "protocolVersion": "2026-07-28",
            "capabilities": {},
            "clientInfo": {"name": "doors-test", "version": "0"}
        }
    })
}

/// The 2026-07-28 revision moved the handshake into every request: this
/// `_meta` is what a sessionless client stamps — and its `clientInfo` is
/// where the agent actor rides.
fn meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "doors-test", "version": "0"}
    })
}

/// The body, after insisting on 200 — a refused request panics with what
/// the transport said, not a bare status.
async fn expect_ok(response: Response<Body>) -> Value {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_initializes_and_lists_the_one_tool() {
    let body = expect_ok(mcp(app().await, initialize()).await).await;
    assert_eq!(body["result"]["serverInfo"]["name"], "glossql-serverd");

    let body = expect_ok(
        mcp(
            app().await,
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"_meta": meta()}}),
        )
        .await,
    )
    .await;
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "glossql");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_executes_and_reports_refusals_as_tool_errors() {
    let app = app().await;
    let call = |statements: &str| {
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "_meta": meta(),
                "name": "glossql",
                "arguments": {"statements": statements}
            }
        })
    };

    let body = expect_ok(mcp(app.clone(), call("SELECT 41 + 1 AS answer")).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["rows"][0]["answer"], json!(42));
    assert_eq!(outcomes[0]["row_count"], json!(1));
    assert_eq!(outcomes[0]["truncated"], json!(false));

    // A failed statement comes back as a tool error the agent can read,
    // never a protocol error.
    let body = expect_ok(mcp(app, call("USE nothing")).await).await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("nothing"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_reads_pass_the_cap_uncapped() {
    let app = app_with(DoorConfig {
        row_cap: 3,
        ..Default::default()
    })
    .await;
    let call = |id: u64, statements: &str| {
        json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {
                "_meta": meta(),
                "name": "glossql",
                "arguments": {"statements": statements}
            }
        })
    };

    // Five glosses through one batch call — the plane keeps the session,
    // so USE survives into the read.
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'cap test');
        USE fin;
        DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;
        GLOSS unit ON t.a AS $${"value": "EUR"}$$;
        GLOSS unit ON t.b AS $${"value": "EUR"}$$;
        GLOSS unit ON t.c AS $${"value": "EUR"}$$;
        GLOSS unit ON t.d AS $${"value": "EUR"}$$;
        GLOSS unit ON t.e AS $${"value": "EUR"}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call(7, setup)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // A metadata sweep wider than the cap arrives whole.
    let body = expect_ok(
        mcp(app.clone(), call(8, "SELECT subject FROM glossary;")).await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["row_count"], json!(5), "{outcomes}");
    assert_eq!(outcomes[0]["truncated"], json!(false));

    let body = expect_ok(
        mcp(app, call(9, "SELECT subject FROM GLOSSARY(fin, all => true);")).await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["truncated"], json!(false), "{outcomes}");
    assert!(outcomes[0]["row_count"].as_u64().unwrap() >= 5, "{outcomes}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_caps_rows_and_declares_it() {
    let app = app_with(DoorConfig {
        row_cap: 3,
        ..Default::default()
    })
    .await;
    let body = expect_ok(
        mcp(
            app,
            json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {
                    "_meta": meta(),
                    "name": "glossql",
                    "arguments": {"statements":
                        "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(v)"}
                }
            }),
        )
        .await,
    )
    .await;

    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["rows"].as_array().unwrap().len(), 3);
    assert_eq!(outcomes[0]["row_count"], json!(3));
    assert_eq!(outcomes[0]["truncated"], json!(true));
}
