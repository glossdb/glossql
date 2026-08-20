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
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_serverd::{ARROW_STREAM, DoorConfig, HUMAN, Plane, bootstrap, router};
use glossql_session::NoRuntime;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn app_with(doors: DoorConfig) -> (Router, tempfile::TempDir) {
    let (dir, store) = scratch_store().await;
    let plane = Arc::new(Plane::new(store, Arc::new(NoRuntime)));
    // No apps live here — the app door serves an empty home.
    (router(plane, doors, std::env::temp_dir()), dir)
}

async fn app() -> (Router, tempfile::TempDir) {
    app_with(DoorConfig::default()).await
}

async fn body_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_docket_app_ships_in_the_binary() {
    // The workspace carries no apps — the built-in answers for the name.
    let (app, _dir) = app().await;
    let response = app
        .oneshot(Request::get("/app/docket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("Docket"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_builtin_frame_names_the_missing_dataset() {
    // No dataset in the workspace: the frame states the condition
    // instead of failing opaquely — the tile renders the message.
    let (app, _dir) = app().await;
    let response = app
        .oneshot(
            Request::get("/app/docket/frames/census")
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
    let (app, _dir) = app().await;
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
    let (app, _dir) = app().await;
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
    let (app, _dir) = app().await;
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
    let (app, _dir) = app().await;
    let body = expect_ok(mcp(app.clone(), initialize()).await).await;
    assert_eq!(body["result"]["serverInfo"]["name"], "glossql-serverd");

    let body = expect_ok(
        mcp(
            app,
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"_meta": meta()}}),
        )
        .await,
    )
    .await;
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "glossql");
    // The revision's full tools/list contract, validated by shipping
    // clients (Claude Code): the SEP-2322
    // discriminator plus the list-caching fields the door injects
    // until rmcp models them.
    assert_eq!(body["result"]["resultType"], "complete", "{body}");
    assert!(body["result"]["ttlMs"].is_number(), "{body}");
    assert_eq!(body["result"]["cacheScope"], "private", "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_executes_and_reports_refusals_as_tool_errors() {
    let (app, _dir) = app().await;
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
    // Every read carries its shape as (name, type) — and the shape
    // survives an empty result: the LIMIT 0 rehearsal's whole point
    // (the workaround otherwise is landing a rehearsal
    // recipe just to DESCRIBE it).
    assert_eq!(
        outcomes[0]["columns"],
        json!([{"name": "answer", "type": "Int64"}]),
        "{outcomes}"
    );
    let body = expect_ok(mcp(app.clone(), call("SELECT 41 + 1 AS answer LIMIT 0")).await).await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["row_count"], json!(0), "{outcomes}");
    assert_eq!(
        outcomes[0]["columns"],
        json!([{"name": "answer", "type": "Int64"}]),
        "{outcomes}"
    );

    // A failed statement comes back as a tool error the agent can read,
    // never a protocol error.
    let body = expect_ok(mcp(app.clone(), call("USE nothing")).await).await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("nothing"), "{text}");

    // A refusal mid-sequence names its place, what landed, and what
    // never ran — after a silent abort the only recourse is reading
    // `imports` to learn what stood.
    let body = expect_ok(
        mcp(
            app.clone(),
            call("SELECT 1 AS a; USE nothing; SELECT 2 AS b"),
        )
        .await,
    )
    .await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("statement 2 of 3 refused"), "{text}");
    assert!(text.contains("statement 1 landed"), "{text}");
    assert!(text.contains("statement 3 not run"), "{text}");

    // The connect-time brief: every initialize after
    // a call serves live counts in its instructions — an agent
    // connecting now hears what stands before it acts.
    let body = expect_ok(mcp(app, initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("Live now:"), "{instructions}");
}

fn call_with(meta_value: Value, id: u64, statements: &str, retry: Option<(&str, Value)>) -> Value {
    let mut params = json!({
        "_meta": meta_value,
        "name": "glossql",
        "arguments": {"statements": statements}
    });
    if let Some((key, answer)) = retry {
        params["inputResponses"] = json!({ key: answer });
        params["requestState"] = json!("question-round:v1");
    }
    json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params})
}

/// The sessionless stamp with the elicitation capability advertised.
fn meta_elicit() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {"elicitation": {}},
        "io.modelcontextprotocol/clientInfo": {"name": "doors-test", "version": "0"}
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn the_round_never_asks_the_human_for_statistics() {
    // An unassessed
    // witnessed claim a measurement can settle (behavior, unit) is the
    // AGENT's backlog — behavior_evidence computes it — and the door
    // must not ask the human for it. The kit ships the vocabulary, a
    // table lands, a role gloss marks the measure, the behavior claim
    // stands owed — and the round stays silent; the brief counts no
    // question. (Judgment questions — loose assumptions — still ask:
    // the sibling test below.)
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("ledger.csv"),
        "month,value\n2026-01-01,10.5\n2026-02-01,4.0\n",
    )
    .unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    // The kit's witnesses carry detectors (slot_entropy), and reads
    // adjudicate — this test needs the real script runtime.
    let runtime = Arc::new(glossql_scripts::KernelRuntime::new(dir.path().to_path_buf()));
    let plane = Arc::new(Plane::new(store.clone(), runtime));
    let human = Actor {
        kind: ActorKind::Human,
        id: HUMAN.into(),
    };
    bootstrap(&plane, human).await.unwrap();
    let app = router(plane, DoorConfig::default(), dir.path().to_path_buf());

    let setup = format!(
        r#"DECLARE DATASET perf SET (purpose: 'kit test');
           USE perf;
           DECLARE SOURCE erp SET (type: csv, location: '{}');
           DECLARE RECIPE ledger ON perf FROM erp AS $$SELECT CAST(month AS DATE) AS month, CAST(value AS DOUBLE) AS value FROM read_csv('ledger.csv')$$;
           GLOSS role ON ledger.value AS $${{"value": "measure"}}$$;"#,
        dir.path().display()
    );
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 70, &setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // The owed behavior claim derives in the store (unassessed row) —
    // but the round asks nothing, even on a review-shaped call: no
    // input_required, no form.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 71, "SELECT subject, aspect FROM glossary LIMIT 5", None),
        )
        .await,
    )
    .await;
    assert_ne!(
        body["result"]["resultType"],
        json!("input_required"),
        "the door asked the human for a statistic: {body}"
    );

    // And the brief counts no open question — the claim is agent work.
    let body = expect_ok(mcp(app, initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(
        !instructions.contains("question"),
        "{instructions}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_key_is_answered_by_repeating_the_ruling() {
    // Run 4 spelled one decision with one key across three metrics, as
    // the practice skill says to, and was then asked about
    // `days-in-period` three times. Fanning one answer across every
    // aspect is the obvious cure and the wrong one — run 2's human
    // ruled `goods-only` two ways on purpose, and that must stay
    // possible. So the second ask offers the first ruling back as an
    // answer: agreeing costs a click, differing is still there.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'one key, one decision');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        DECLARE ASPECT dio WITH $${"title": "DIO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "convention", "key": "days-in-period",
            "assumption": "days are the month's own calendar days, 28 to 31",
            "basis": "judgment", "confidence": 0.8}]}$$;
        GLOSS dio ON fin AS $${"sql": "SELECT 2 AS v",
          "assumptions": [{"dimension": "convention", "key": "days-in-period",
            "assumption": "days are the month's own calendar days",
            "basis": "judgment", "confidence": 0.8}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 180, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let open = |id: u64| {
        let app = app.clone();
        async move {
            let body = expect_ok(
                mcp(
                    app,
                    call_with(meta(), id, "SELECT aspect, key FROM open_questions;", None),
                )
                .await,
            )
            .await;
            let text = body["result"]["content"][0]["text"].as_str().unwrap().to_string();
            let outcomes: Value = serde_json::from_str(&text).unwrap();
            outcomes[0]["rows"].as_array().cloned().unwrap_or_default()
        }
    };

    // One decision, disclosed twice — two rows stand open.
    assert_eq!(open(181).await.len(), 2);

    let review = "SELECT subject, aspect FROM glossary LIMIT 5";

    // dio asks first (aspect order) and the human corrects it. With
    // nothing yet ruled on this key, the form offers two stances.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 182, review, None)).await).await;
    let first = &body["result"]["inputRequests"]["loose:fin:dio:days-in-period"];
    assert!(first.is_object(), "{body}");
    let stances = first["params"]["requestedSchema"]["properties"]["stance"]["enum"].to_string();
    assert!(!stances.contains("same as before"), "nothing to repeat yet: {stances}");

    let corrected = json!({"action": "accept",
        "content": {"stance": "wrong", "correction": "use a fixed 30-day month"}});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                183,
                review,
                Some(("loose:fin:dio:days-in-period", corrected)),
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // dso asks next. The same key is already ruled next door, so the
    // form both names it and offers it back.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 184, review, None)).await).await;
    let second = &body["result"]["inputRequests"]["loose:fin:dso:days-in-period"];
    assert!(second.is_object(), "{body}");
    let message = second["params"]["message"].as_str().unwrap();
    assert!(message.contains("corrected on dio"), "{message}");
    let stances = second["params"]["requestedSchema"]["properties"]["stance"]["enum"].to_string();
    assert!(
        stances.contains("same as before (corrected on dio)"),
        "the repeat must be one click: {stances}"
    );

    // Taking it replays the stance AND the human's own words.
    let same = json!({"action": "accept",
        "content": {"stance": "same as before (corrected on dio)"}});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                185,
                review,
                Some(("loose:fin:dso:days-in-period", same)),
            ),
        )
        .await,
    )
    .await;
    assert!(
        body["result"]["content"].to_string().contains("ruled (corrected)"),
        "{body}"
    );

    // Nothing stands open now, and each aspect carries its own entry.
    assert!(open(186).await.is_empty());
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                187,
                "SELECT aspect, stance, note, assumption FROM ruling_entries ORDER BY aspect;",
                None,
            ),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    let rows = outcomes[0]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0]["aspect"], json!("dio"), "{rows:?}");
    assert_eq!(rows[1]["aspect"], json!("dso"), "{rows:?}");
    assert_eq!(rows[1]["stance"], json!("corrected"), "{rows:?}");
    assert!(
        rows[1]["note"].as_str().unwrap().contains("fixed 30-day"),
        "the repeat carries the human's own words: {rows:?}"
    );
    assert!(
        rows[1]["assumption"].as_str().unwrap().contains("28 to 31"),
        "each aspect keeps its own wording: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_round_rules_a_loose_assumption_on_retry() {
    // A judged assumption below full confidence becomes a
    // confirm/correct form; "stands as stated" lands a RULING entry —
    // the judgment alone, never a copy of the agent's body (a
    // frozen copy would outrank every later correction).
    // The ruling holds the question closed, the brief counts the
    // fold-in debt, and the agent's re-record clears it.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'loose test');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [
            {"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7},
            {"dimension": "grain", "key": "grain-preserving", "assumption": "grain-preserving", "basis": "measured", "confidence": 1.0}
          ]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 60, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 61, "SELECT subject, aspect FROM glossary LIMIT 5", None),
        )
        .await,
    )
    .await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );
    let ask = &body["result"]["inputRequests"]["loose:fin:dso:per-line"];
    assert_eq!(ask["method"], json!("elicitation/create"), "{body}");
    assert!(ask["params"]["message"].to_string().contains("per line"), "{body}");

    let answer = json!({"action": "accept", "content": {"stance": "stands as stated"}});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                62,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                Some(("loose:fin:dso:per-line", answer)),
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    assert!(
        body["result"]["content"].to_string().contains("ruled (confirmed)"),
        "{body}"
    );

    // The human slot is the ruling record alone — the judgment, never
    // the agent's grounding; the dso aspect stays agent-authored.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                63,
                "SELECT aspect, body FROM glossary WHERE actor_kind = 'human';",
                None,
            ),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    assert_eq!(outcomes[0]["row_count"], json!(1), "{outcomes}");
    assert_eq!(outcomes[0]["rows"][0]["aspect"], json!("ruling"), "{outcomes}");
    let human_body = outcomes[0]["rows"][0]["body"].as_str().unwrap();
    assert!(human_body.contains("per line"), "{human_body}");
    assert!(human_body.contains("confirmed"), "{human_body}");
    assert!(
        !human_body.contains("sql"),
        "the ruling froze a body copy: {human_body}"
    );

    // The ruling holds the question closed on the next review…
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 64, "SELECT subject, aspect FROM glossary LIMIT 5", None),
        )
        .await,
    )
    .await;
    assert_eq!(body["result"]["resultType"], json!("complete"), "{body}");

    // …and the brief counts the fold-in debt until the agent
    // re-records the grounding at full confidence, citing the ruling.
    let body = expect_ok(mcp(app.clone(), initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("1 ruling awaits the fold-in"),
        "{instructions}"
    );
    let fold_in = r#"GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
        "assumptions": [
          {"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "human-ruled", "confidence": 1.0},
          {"dimension": "grain", "key": "grain-preserving", "assumption": "grain-preserving", "basis": "measured", "confidence": 1.0}
        ]}$$;"#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 65, fold_in, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let body = expect_ok(mcp(app.clone(), initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(
        !instructions.contains("fold-in"),
        "the debt must clear on the re-record: {instructions}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_declined_question_rests_until_the_workspace_moves() {
    // Decline is defer: transport state, never the store — the app
    // still shows the open row. It rests only while the workspace
    // holds still; a writing call clears the deferral, so the next
    // review asks again — "not now"
    // never hardens into "never".
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'defer test');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 70, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let review = "SELECT subject, aspect FROM glossary LIMIT 5";
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 71, review, None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );

    let declined = json!({"action": "decline"});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 72, review, Some(("loose:fin:dso:per-line", declined))),
        )
        .await,
    )
    .await;
    assert!(body["result"]["content"].to_string().contains("deferred"), "{body}");
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 73, review, None)).await).await;
    assert_eq!(body["result"]["resultType"], json!("complete"), "{body}");

    // The workspace moves — an unrelated declaration — and the same
    // question stands again at the next review.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                74,
                r#"DECLARE ASPECT note WITH $${"title": "note"}$$ AS FACT;"#,
                None,
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let body =
        expect_ok(mcp(app, call_with(meta_elicit(), 75, review, None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "declined question must re-derive after a write: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_round_never_interrupts_a_working_call() {
    // Cadence: forms ride
    // only calls that read the record. A landing call and a plain data
    // read run uninterrupted even while a question stands open; the
    // review-shaped call carries the form.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'cadence test');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 90, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // A writing call: the question stands, the landing runs through.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                91,
                r#"DECLARE ASPECT note WITH $${"title": "note"}$$ AS FACT;"#,
                None,
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["resultType"], json!("input_required"), "{body}");
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // A plain data read: judging work, not a review — no form either.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 92, "SELECT 1 AS ok", None)).await)
            .await;
    assert_ne!(body["result"]["resultType"], json!("input_required"), "{body}");

    // The review-shaped call carries the form.
    let body = expect_ok(
        mcp(
            app,
            call_with(meta_elicit(), 93, "SELECT subject, aspect FROM glossary LIMIT 5", None),
        )
        .await,
    )
    .await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );
}

/// The session-carrying lifecycle (≤ 2025-11-25) gets the same round
/// as a server→client request on the call's own stream.
#[tokio::test(flavor = "multi_thread")]
async fn the_round_rides_a_transport_session_too() {
    use futures::StreamExt;

    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'session round');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 80, setup, None)).await).await;
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

    let asked = json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "glossql",
            "arguments": {"statements": "SELECT subject, aspect FROM glossary LIMIT 5"}}
    });
    let response = app
        .clone()
        .oneshot(session_request(Some(&sid), asked.to_string()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();

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
    .expect("the round must reach this stream");

    let answer = json!({
        "jsonrpc": "2.0", "id": elicit_id,
        "result": {"action": "accept", "content": {"stance": "stands as stated"}}
    });
    let posted = app
        .clone()
        .oneshot(session_request(Some(&sid), answer.to_string()))
        .await
        .unwrap();
    assert!(posted.status().is_success(), "{}", posted.status());

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
    assert!(
        done["result"]["content"].to_string().contains("ruled (confirmed)"),
        "{done}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_actor_id_is_the_clients_name_not_the_transports() {
    let (app, _dir) = app().await;
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
    let (app, _dir) = app_with(DoorConfig {
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
    let (app, _dir) = app_with(DoorConfig {
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

#[tokio::test(flavor = "multi_thread")]
async fn sequential_rulings_compose_instead_of_reverting() {
    // Ruling a second assumption must not revert the
    // first. Rulings accumulate as entries in the human's
    // one ruling slot — each append carries the earlier entries along,
    // and no ruling ever touches the agent's body.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'sequential rulings');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [
            {"dimension": "convention", "key": "flat-30-day-month", "assumption": "a flat 30-day month", "basis": "judgment", "confidence": 0.6},
            {"dimension": "definition", "key": "total-revenue-denominator", "assumption": "total revenue in the denominator", "basis": "judgment", "confidence": 0.7}
          ]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 80, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // Rule the first (lowest confidence asks first), then the second.
    for key in [
        "loose:fin:dso:flat-30-day-month",
        "loose:fin:dso:total-revenue-denominator",
    ] {
        let answer = json!({"action": "accept", "content": {"stance": "stands as stated"}});
        let body = expect_ok(
            mcp(
                app.clone(),
                call_with(meta_elicit(), 81, "SELECT 1 AS ok", Some((key, answer))),
            )
            .await,
        )
        .await;
        assert!(
            body["result"]["content"].to_string().contains("ruled (confirmed)"),
            "{key}: {body}"
        );
    }

    // Both rulings stand as entries in the one ruling slot — the first
    // did not revert — and the agent's dso body is untouched.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                82,
                "SELECT body FROM glossary WHERE actor_kind = 'human' ORDER BY written_at DESC LIMIT 1;
                 SELECT body FROM glossary WHERE actor_kind = 'agent' AND aspect = 'dso';",
                None,
            ),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    let human_body = outcomes[0]["rows"][0]["body"].as_str().unwrap();
    assert!(human_body.contains("a flat 30-day month"), "{human_body}");
    assert!(
        human_body.contains("total revenue in the denominator"),
        "{human_body}"
    );
    assert_eq!(human_body.matches("confirmed").count(), 2, "{human_body}");
    let agent_body = outcomes[1]["rows"][0]["body"].as_str().unwrap();
    assert!(agent_body.contains("0.6"), "the agent body moved: {agent_body}");
    assert!(agent_body.contains("0.7"), "the agent body moved: {agent_body}");

    // And the round is quiet — nothing re-derives.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 83, "SELECT subject, aspect FROM glossary LIMIT 5", None),
        )
        .await,
    )
    .await;
    assert_eq!(body["result"]["resultType"], json!("complete"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_round_names_the_sibling_ruling_on_the_same_key() {
    // Run 2 produced this: `purchases`'s goods-only assumption was
    // confirmed in the same session where `dpo`'s goods-only
    // assumption was corrected. The two groundings word the claim
    // differently on purpose — pairing rests on the declared key,
    // never on the prose (STRING EQUALITY ON NON-KEYS IS FORBIDDEN).
    //
    // The form is the whole mechanism: when the round asks about a key
    // the human already ruled under another aspect, the message names
    // that sibling ruling, and the human decides with both in view.
    // Nothing pairs the rulings afterwards — two aspects may genuinely
    // differ, and an agent that needs it settled again asks again.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'one claim, two rulings');
        USE fin;
        DECLARE ASPECT ruling WITH $${"type": "object", "required": ["rulings"],
          "properties": {"rulings": {"type": "array"}}}$$ AS FACT;
        DECLARE ASPECT purchases WITH $${"title": "P", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        DECLARE ASPECT dpo WITH $${"title": "D", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS purchases ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "scope", "key": "goods-only", "assumption": "goods suppliers only", "basis": "judgment", "confidence": 0.7}]}$$;
        GLOSS dpo ON fin AS $${"sql": "SELECT 2 AS v",
          "assumptions": [{"dimension": "scope", "key": "goods-only", "assumption": "restricted to invoices for goods, excluding services", "basis": "judgment", "confidence": 0.7}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 100, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let review = "SELECT subject, aspect FROM glossary LIMIT 5";

    // dpo asks first (aspect order); the human corrects it.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 101, review, None)).await).await;
    assert!(
        body["result"]["inputRequests"]["loose:fin:dpo:goods-only"].is_object(),
        "{body}"
    );
    let corrected = json!({"action": "accept",
        "content": {"stance": "wrong", "correction": "all suppliers, not goods only"}});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                102,
                review,
                Some(("loose:fin:dpo:goods-only", corrected)),
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // purchases asks next, and the form names what was already ruled
    // on that same key — the `sibling` column, carried by the read.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 103, review, None)).await).await;
    let form = &body["result"]["inputRequests"]["loose:fin:purchases:goods-only"];
    assert!(form.is_object(), "{body}");
    assert!(
        form["params"]["message"]
            .as_str()
            .unwrap()
            .contains("corrected on dpo"),
        "the form names the sibling ruling: {body}"
    );

    // The human confirms it anyway — legitimate, different aspect.
    let confirmed = json!({"action": "accept", "content": {"stance": "stands as stated"}});
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                104,
                review,
                Some(("loose:fin:purchases:goods-only", confirmed)),
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // Both rulings stand, each on its own aspect, and the round is
    // quiet — the record needs no third act.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 105, review, None)).await).await;
    assert_eq!(body["result"]["resultType"], json!("complete"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_brief_rides_the_call_that_moved_it() {
    // Run 2's friction 11: initialize instructions are fetched once
    // per connection, so a long-lived session never saw the counts
    // move. The brief now also rides any tool result whose call
    // changed it — and stays off the quiet ones.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'brief delivery');
        USE fin;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 120, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let blocks = body["result"]["content"].as_array().unwrap();
    let brief = blocks
        .iter()
        .find(|b| b["text"].as_str().is_some_and(|t| t.starts_with("brief: ")));
    let brief = brief.unwrap_or_else(|| panic!("the landing moved the brief: {body}"));
    assert!(
        brief["text"].as_str().unwrap().contains("judgment question"),
        "{body}"
    );

    // A quiet read moves nothing and carries nothing.
    let body =
        expect_ok(mcp(app.clone(), call_with(meta(), 121, "SELECT 1 AS ok", None)).await).await;
    let blocks = body["result"]["content"].as_array().unwrap();
    assert!(
        !blocks
            .iter()
            .any(|b| b["text"].as_str().is_some_and(|t| t.starts_with("brief: "))),
        "{body}"
    );

    // A KNOWN GAP, kept open: a second agent watching the same
    // workspace hears nothing until its own call moves something. One
    // shared baseline, so the mover is told. Per-actor delivery was
    // built and removed — no run has produced two agents on one
    // workspace, and the state it needed outweighed the case.
    let other = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "second-agent", "version": "0"}
    });
    let body =
        expect_ok(mcp(app.clone(), call_with(other, 122, "SELECT 1 AS ok", None)).await).await;
    let blocks = body["result"]["content"].as_array().unwrap();
    assert!(
        !blocks
            .iter()
            .any(|b| b["text"].as_str().is_some_and(|t| t.starts_with("brief: "))),
        "the shared baseline says nothing moved: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unkeyed_assumption_is_never_asked() {
    // A KNOWN, ACCEPTED GAP. Identity is the
    // declared `key`; an assumption disclosed without one cannot be
    // held closed by a ruling, so the round would re-ask it forever.
    // It is therefore never asked at all — the record still shows it,
    // and the skills make the key part of the disclosure shape. The
    // alternative — pairing on the prose — is forbidden.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'unkeyed assumptions');
        USE fin;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "assumption": "per line", "basis": "judgment", "confidence": 0.5}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 130, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let review = "SELECT subject, aspect FROM glossary LIMIT 5";
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 131, review, None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("complete"),
        "an unkeyed assumption is not askable: {body}"
    );

    // The same claim, keyed, does ask.
    let keyed = r#"GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
        "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.5}]}$$;"#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 132, keyed, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let body =
        expect_ok(mcp(app.clone(), call_with(meta_elicit(), 133, review, None)).await).await;
    assert!(
        body["result"]["inputRequests"]["loose:fin:dso:per-line"].is_object(),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_open_questions_read_composes_like_a_table() {
    // The point of the read library: the round's derivation is a
    // relation anyone can select from, so the door, the app's queue and
    // any ad-hoc query share one file instead of three copies. Filters
    // ride WHERE — the same posture `read.<aspect>()` and
    // `metric_series()` take.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'the read library');
        USE fin;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        DECLARE ASPECT dpo WITH $${"title": "DPO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.5}]}$$;
        GLOSS dpo ON fin AS $${"sql": "SELECT 2 AS v",
          "assumptions": [{"dimension": "definition", "key": "goods-only", "assumption": "goods suppliers only", "basis": "judgment", "confidence": 0.4},
                          {"dimension": "grain", "key": "monthly", "assumption": "month grain", "basis": "judgment", "confidence": 0.3}]}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 140, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let rows = |body: &Value| -> Value {
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        let outcomes: Value = serde_json::from_str(text).unwrap();
        outcomes[0].clone()
    };

    // The whole read: two askable rows. `grain` is the function map's
    // dimension, so the read drops it — the gate lives in the file now,
    // not in the door.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta(), 141, "SELECT * FROM open_questions;", None),
        )
        .await,
    )
    .await;
    let out = rows(&body);
    assert_eq!(out["row_count"], json!(2), "{out}");

    // A filter narrows it, a projection reshapes it, an aggregate closes
    // over it: it is a relation, not a door-shaped special case.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                142,
                "SELECT aspect, key FROM open_questions WHERE aspect = 'dpo';",
                None,
            ),
        )
        .await,
    )
    .await;
    let out = rows(&body);
    assert_eq!(out["row_count"], json!(1), "{out}");
    assert_eq!(out["rows"][0]["key"], json!("goods-only"), "{out}");

    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                143,
                "SELECT count(*) AS owed FROM open_questions;",
                None,
            ),
        )
        .await,
    )
    .await;
    let out = rows(&body);
    assert_eq!(out["rows"][0]["owed"], json!(2), "{out}");

    // And the round serves exactly what the read holds.
    let body = expect_ok(
        mcp(
            app,
            call_with(
                meta_elicit(),
                144,
                "SELECT subject, aspect FROM glossary LIMIT 5;",
                None,
            ),
        )
        .await,
    )
    .await;
    let asked = body["result"]["inputRequests"].as_object().unwrap();
    assert_eq!(asked.len(), 1, "one question at a time: {body}");
    assert!(
        asked.contains_key("loose:fin:dpo:goods-only"),
        "the least confident row is asked first: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_authors_an_app_over_the_tool() {
    // An app is glosses, one per part. As a directory
    // read from disk only, the one thing an agent
    // connected over MCP could not build was the surface a human looks
    // at — it has statements, not a filesystem. Nothing new carries it:
    // the parts travel as glosses, supersession versions each one, and
    // actor kind records whose hand shaped it.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'an agent authors an app');
        USE fin;
        DECLARE ASPECT app WITH $${"type": "object", "required": ["title"],
          "properties": {"title": {"type": "string"}, "dataset": {"type": "string"}}}$$ AS FACT;
        DECLARE ASPECT app_page WITH $${"type": "object", "required": ["html"],
          "properties": {"html": {"type": "string"}}}$$ AS FACT;
        DECLARE ASPECT app_frame WITH $${"type": "object", "required": ["sql"],
          "properties": {"sql": {"type": "string"}}}$$ AS FACT;
        GLOSS app ON cash AS $${"title": "Monday cash", "dataset": "fin"}$$;
        GLOSS app_page ON cash.index AS $${"html": "{% extends \"shell.html\" %}{% block main %}<h1>What stands open</h1>{% endblock %}"}$$;
        GLOSS app_frame ON cash.open AS $${"sql": "SELECT count(*) AS owed FROM open_questions"}$$;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 150, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // The page the agent wrote is served by the app door.
    let response = app
        .clone()
        .oneshot(Request::get("/app/cash").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("What stands open"), "{html}");
    assert!(html.contains("Monday cash"), "the manifest names it: {html}");

    // And its frame runs, over a shipped read, as Arrow IPC.
    let response = app
        .clone()
        .oneshot(
            Request::get("/app/cash/frames/open")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], ARROW_STREAM);

    // A re-gloss supersedes that one part; the rest of the app stands.
    let edit = r#"USE fin;
        GLOSS app_page ON cash.index AS $${"html": "{% extends \"shell.html\" %}{% block main %}<h1>Open work</h1>{% endblock %}"}$$;"#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 151, edit, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let response = app
        .oneshot(Request::get("/app/cash").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("Open work"), "{html}");
    assert!(!html.contains("What stands open"), "superseded: {html}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_workspace_says_what_it_affords() {
    // The map that replaces the staged manuals: what this workspace can
    // be extended through, how much of each stands, and what is open on
    // it. Not an order to follow — the agent judges what to do next;
    // this only says what the system affords.
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'the affordance map');
        USE fin;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
        GLOSS dso ON fin AS $${"sql": "SELECT 1 AS v",
          "assumptions": [{"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.5}]}$$;
        DECLARE ASPECT price_hike WITH $${"title": "Price +15%", "x-kind": "scenario"}$$ AS FACT ON DATASET;
        DECLARE ASPECT late_pairs WITH $${"title": "Late pairs", "x-kind": "sample"}$$ AS QUERY ON DATASET;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 160, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let body = expect_ok(
        mcp(
            app,
            call_with(
                meta(),
                161,
                "SELECT surface, stands, open FROM workspace_next ORDER BY surface;",
                None,
            ),
        )
        .await,
    )
    .await;
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).unwrap();
    let rows = outcomes[0]["rows"].as_array().unwrap();
    let surfaces: Vec<&str> = rows.iter().filter_map(|r| r["surface"].as_str()).collect();
    for expected in [
        "apps",
        "aspects",
        "claims",
        "functions",
        "metrics",
        "relationships",
        "rulings",
        "samples",
        "scenarios",
        "sources",
        "tables",
    ] {
        assert!(surfaces.contains(&expected), "{expected} missing: {rows:?}");
    }
    let row = |name: &str| {
        rows.iter()
            .find(|r| r["surface"] == json!(name))
            .unwrap_or_else(|| panic!("no {name} row"))
            .clone()
    };
    // One metric declared, one grounded, one assumption open on it. The
    // sample frame is a QUERY aspect too and must not inflate this.
    assert_eq!(row("metrics")["stands"], json!(1), "{rows:?}");
    assert_eq!(row("metrics")["open"], json!(1), "{rows:?}");
    assert_eq!(row("claims")["open"], json!(1), "{rows:?}");
    // The two model doors. Both are planner doors, not rows
    // in `functions`, so this map is the only place an agent meets them
    // at all. Each is declared here and never glossed — vocabulary
    // standing with no body, which is exactly what `open` means.
    assert_eq!(row("scenarios")["stands"], json!(1), "{rows:?}");
    assert_eq!(row("scenarios")["open"], json!(1), "{rows:?}");
    assert_eq!(row("samples")["stands"], json!(1), "{rows:?}");
    assert_eq!(row("samples")["open"], json!(1), "{rows:?}");
    // Nothing ruled yet, so nothing owes a fold-in.
    assert_eq!(row("rulings")["stands"], json!(0), "{rows:?}");
    assert_eq!(row("rulings")["open"], json!(0), "{rows:?}");
}

/// Run 4 read `workspace_next` twice in one session and got two
/// different answers: filtered by `surface`, it came back with every row
/// and reported `aspects` and `functions` as 0 standing, while the same
/// read unfiltered — and the relations read directly — reported the
/// real counts. A filter that is silently dropped is bad; one that also
/// corrupts the counts it returns is worse, so both halves are asserted
/// here against the same session.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_on_the_affordance_map_neither_widens_nor_zeroes_it() {
    let (app, _dir) = app().await;
    let setup = r#"
        DECLARE DATASET fin SET (purpose: 'the affordance map, filtered');
        USE fin;
        DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
    "#;
    let body = expect_ok(mcp(app.clone(), call_with(meta(), 170, setup, None)).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    let read = |sql: &'static str, id: u64| {
        let app = app.clone();
        async move {
            let body =
                expect_ok(mcp(app, call_with(meta(), id, sql, None)).await).await;
            let text = body["result"]["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            serde_json::from_str::<Value>(&text).unwrap_or(json!([]))
        }
    };

    let whole = read("SELECT surface, stands FROM workspace_next ORDER BY surface;", 171).await;
    let whole_rows = whole[0]["rows"].as_array().unwrap().clone();
    let standing = |rows: &[Value], name: &str| -> i64 {
        rows.iter()
            .find(|r| r["surface"] == json!(name))
            .and_then(|r| r["stands"].as_i64())
            .unwrap_or(-1)
    };
    let aspects_whole = standing(&whole_rows, "aspects");
    assert!(aspects_whole > 0, "aspects should stand: {whole_rows:?}");

    // The same question, filtered.
    let one = read(
        "SELECT surface, stands FROM workspace_next WHERE surface = 'aspects';",
        172,
    )
    .await;
    let one_rows = one[0]["rows"].as_array().unwrap().clone();
    assert_eq!(
        one_rows.len(),
        1,
        "the filter must bind — got every surface back: {one_rows:?}"
    );
    assert_eq!(
        standing(&one_rows, "aspects"),
        aspects_whole,
        "filtered and unfiltered disagree on the same count"
    );

    // And a filter that keeps several rows keeps exactly those.
    let some = read(
        "SELECT surface, stands FROM workspace_next \
         WHERE surface IN ('tables', 'sources') ORDER BY surface;",
        173,
    )
    .await;
    let some_rows = some[0]["rows"].as_array().unwrap().clone();
    let surfaces: Vec<&str> = some_rows
        .iter()
        .filter_map(|r| r["surface"].as_str())
        .collect();
    assert_eq!(surfaces, vec!["sources", "tables"], "{some_rows:?}");
}

/// A store over its own throwaway lake; hold the dir for the test's life.
async fn scratch_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    (dir, store)
}
