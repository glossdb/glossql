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
async fn the_model_app_ships_in_the_binary() {
    // The workspace carries no apps — the built-in answers for the name.
    let app = app().await;
    let response = app
        .oneshot(Request::get("/app/model").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("World model"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_builtin_frame_names_the_missing_dataset() {
    // No dataset in the workspace: the frame states the condition
    // instead of failing opaquely — the tile renders the message.
    let app = app().await;
    let response = app
        .oneshot(
            Request::get("/app/model/frames/census")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(response).await;
    assert!(body["error"].as_str().unwrap().contains("no dataset"));
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
    assert!(
        body["error"].as_str().unwrap().contains("nothing"),
        "{body}"
    );
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
    // The revision's full tools/list contract, validated by shipping
    // clients (Claude Code, observed 2026-08-12): the SEP-2322
    // discriminator plus the list-caching fields the door injects
    // until rmcp models them.
    assert_eq!(body["result"]["resultType"], "complete", "{body}");
    assert!(body["result"]["ttlMs"].is_number(), "{body}");
    assert_eq!(body["result"]["cacheScope"], "private", "{body}");
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
    // Call results keep the discriminator; the caching fields belong to
    // tools/list alone — the middleware must not touch calls.
    assert_eq!(body["result"]["resultType"], "complete", "{body}");
    assert!(body["result"].get("ttlMs").is_none(), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["rows"][0]["answer"], json!(42));
    assert_eq!(outcomes[0]["row_count"], json!(1));
    assert_eq!(outcomes[0]["truncated"], json!(false));

    // A failed statement comes back as a tool error the agent can read,
    // never a protocol error.
    let body = expect_ok(mcp(app.clone(), call("USE nothing")).await).await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("nothing"), "{text}");

    // The connect-time brief (ruled 2026-08-12): every initialize after
    // a call serves live counts in its instructions — an agent
    // connecting now hears what stands before it acts.
    let body = expect_ok(mcp(app, initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("Live now:"), "{instructions}");
}

/// The elicitation round-trip needs the session-carrying lifecycle:
/// 2025-11-25 is the newest version that has one — SEP-2567 serves
/// 2026-07-28+ statelessly, and there a client's posted answer has no
/// route back to the waiting handler (rmcp 3.1.2).
#[tokio::test(flavor = "multi_thread")]
async fn an_elicited_answer_lands_with_human_standing() {
    use futures::StreamExt;

    let app = app_with(DoorConfig {
        elicit_probe: true,
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

    // Seed through the sessionless path — `meta()` advertises no
    // elicitation capability, so the probe skips instead of asking.
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'elicit test');
        USE fin;
        DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;
        GLOSS unit ON t.a AS $${"value": "EUR"}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call(30, setup)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let session_request = |session: Option<&str>, payload: String| {
        let mut request = Request::post("/mcp")
            .header(header::HOST, "127.0.0.1")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream");
        if let Some(id) = session {
            request = request
                .header("mcp-session-id", id)
                .header("mcp-protocol-version", "2025-11-25");
        }
        request.body(Body::from(payload)).unwrap()
    };

    // A session-carrying initialize, elicitation advertised.
    let init = json!({
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {"elicitation": {}},
            "clientInfo": {"name": "doors-test", "version": "0"}
        }
    });
    let response = app
        .clone()
        .oneshot(session_request(None, init.to_string()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let sid = response
        .headers()
        .get("mcp-session-id")
        .expect("a session id")
        .to_str()
        .unwrap()
        .to_string();
    let posted = app
        .clone()
        .oneshot(session_request(
            Some(&sid),
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string(),
        ))
        .await
        .unwrap();
    assert!(posted.status().is_success(), "{}", posted.status());

    // The tool call: the probe's question must ride this POST's own
    // SSE stream.
    let asked = json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "glossql", "arguments": {"statements": "SELECT 1 AS ok"}}
    });
    let response = app
        .clone()
        .oneshot(session_request(Some(&sid), asked.to_string()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();

    // Read SSE data lines until the elicitation request appears.
    let elicit_id = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let chunk = stream.next().await.expect("stream open").expect("chunk");
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = buffer.find('\n') {
                let line = buffer.drain(..=end).collect::<String>();
                if let Some(data) = line.trim().strip_prefix("data: ") {
                    let event: Value = serde_json::from_str(data).expect(data);
                    if event["method"] == json!("elicitation/create") {
                        return event["id"].clone();
                    }
                }
            }
        }
    })
    .await
    .expect("the elicitation request must reach this stream");

    // The human answers: a dictated gloss, posted as the JSON-RPC
    // response through the session.
    let answer = json!({
        "jsonrpc": "2.0", "id": elicit_id,
        "result": {
            "action": "accept",
            "content": {
                "subject": "t.b",
                "aspect": "unit",
                "body": "{\"value\": \"CHF\"}",
                "stance": "land it"
            }
        }
    });
    let posted = app
        .clone()
        .oneshot(session_request(Some(&sid), answer.to_string()))
        .await
        .unwrap();
    assert!(posted.status().is_success(), "{}", posted.status());

    // The handler unblocks and the tool result closes the stream.
    let done = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            while let Some(end) = buffer.find('\n') {
                let line = buffer.drain(..=end).collect::<String>();
                if let Some(data) = line.trim().strip_prefix("data: ") {
                    let event: Value = serde_json::from_str(data).expect(data);
                    if event["id"] == json!(2) {
                        return event;
                    }
                }
            }
            let chunk = stream.next().await.expect("stream open").expect("chunk");
            buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("the tool result must arrive after the answer");
    assert_ne!(done["result"]["isError"], json!(true), "{done}");
    let notes: Vec<&str> = done["result"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect();
    assert!(
        notes.iter().any(|text| text.contains("landed `unit` on `t.b`")),
        "{notes:?}"
    );

    // The landed slot carries human standing — the workspace's human,
    // never the calling agent.
    let body = expect_ok(
        mcp(
            app,
            call(31, "SELECT actor_kind, actor_id, subject FROM glossary;"),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    let rows = outcomes[0]["rows"].as_array().unwrap();
    assert!(
        rows.iter().any(|row| row["actor_kind"] == json!("human")
            && row["actor_id"] == json!("human")
            && row["subject"] == json!("t.b")),
        "{rows:?}"
    );
}

/// The sessionless mechanism (SEP-2322, MRTR): on 2026-07-28 the ask
/// is an `input_required` result and the answer arrives on the
/// client's retry of the same call — no transport session needed.
/// This is the lifecycle Claude Code speaks (measured 2026-08-13).
#[tokio::test(flavor = "multi_thread")]
async fn an_mrtr_retry_lands_with_human_standing() {
    let app = app_with(DoorConfig {
        elicit_probe: true,
        ..Default::default()
    })
    .await;

    // The sessionless stamp, elicitation advertised.
    let meta_elicit = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {"elicitation": {}},
        "io.modelcontextprotocol/clientInfo": {"name": "doors-test", "version": "0"}
    });
    let call = |id: u64, statements: &str, retry: Option<Value>| {
        let mut params = json!({
            "_meta": meta_elicit.clone(),
            "name": "glossql",
            "arguments": {"statements": statements}
        });
        if let Some(responses) = retry {
            params["inputResponses"] = responses;
            params["requestState"] = json!("elicit-probe:v1");
        }
        json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params})
    };

    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'mrtr test');
        USE fin;
        DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;
    "#;

    // Round 1: the ask arrives instead of execution.
    let body = expect_ok(mcp(app.clone(), call(40, setup, None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );
    let ask = &body["result"]["inputRequests"]["elicit-probe"];
    assert_eq!(ask["method"], json!("elicitation/create"), "{body}");
    assert_eq!(ask["params"]["mode"], json!("form"), "{body}");
    assert_eq!(body["result"]["requestState"], json!("elicit-probe:v1"));

    // Round 2: the same call retried with the answer riding along — a
    // skip, so the statements just run.
    let skip = json!({"elicit-probe": {"action": "accept", "content": {
        "subject": "x", "aspect": "x", "body": "{}", "stance": "skip it"}}});
    let body = expect_ok(mcp(app.clone(), call(41, setup, Some(skip))).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // A plain read asks again...
    let body = expect_ok(mcp(app.clone(), call(42, "SELECT 1 AS ok", None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );

    // ...and the dictation on its retry lands with human standing
    // before the read runs.
    let land = json!({"elicit-probe": {"action": "accept", "content": {
        "subject": "t.b", "aspect": "unit", "body": "{\"value\": \"CHF\"}", "stance": "land it"}}});
    let body = expect_ok(mcp(app.clone(), call(43, "SELECT 1 AS ok", Some(land))).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let notes: Vec<&str> = body["result"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect();
    assert!(
        notes.iter().any(|text| text.contains("landed `unit` on `t.b`")),
        "{notes:?}"
    );

    // The landed slot: the workspace's human, not the calling agent.
    let verify = json!({
        "jsonrpc": "2.0", "id": 44, "method": "tools/call",
        "params": {
            "_meta": meta(),
            "name": "glossql",
            "arguments": {"statements": "SELECT actor_kind, actor_id, subject FROM glossary;"}
        }
    });
    let body = expect_ok(mcp(app, verify).await).await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    let rows = outcomes[0]["rows"].as_array().unwrap();
    assert!(
        rows.iter().any(|row| row["actor_kind"] == json!("human")
            && row["actor_id"] == json!("human")
            && row["subject"] == json!("t.b")),
        "{rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_actor_id_is_the_clients_name_not_the_transports() {
    let app = app().await;
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

    // The sessionless lifecycle synthesizes a transport-level peer
    // identity ("rmcp"); the actor must come from the request's own
    // `_meta` clientInfo stamp — the plane keys channels by it.
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'actor test');
        USE fin;
        DECLARE ASPECT unit WITH $${"type": "object"}$$ AS FACT;
        GLOSS unit ON t.a AS $${"value": "EUR"}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call(20, setup)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let body =
        expect_ok(mcp(app, call(21, "SELECT actor_kind, actor_id FROM glossary;")).await).await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(
        outcomes[0]["rows"][0]["actor_kind"],
        json!("agent"),
        "{outcomes}"
    );
    assert_eq!(
        outcomes[0]["rows"][0]["actor_id"],
        json!("doors-test"),
        "{outcomes}"
    );
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
    let body = expect_ok(mcp(app.clone(), call(8, "SELECT subject FROM glossary;")).await).await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["row_count"], json!(5), "{outcomes}");
    assert_eq!(outcomes[0]["truncated"], json!(false));

    let body = expect_ok(
        mcp(
            app,
            call(9, "SELECT subject FROM GLOSSARY(fin, all => true);"),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["truncated"], json!(false), "{outcomes}");
    assert!(
        outcomes[0]["row_count"].as_u64().unwrap() >= 5,
        "{outcomes}"
    );
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
