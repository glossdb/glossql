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
use std::collections::HashMap;

use glossql_serverd::{
    ARROW_STREAM, Access, BOOTSTRAP, DoorConfig, INSECURE_DEV_MODE, Login, Plane, bootstrap, router,
};

use crate::common;
use glossql_session::NoRuntime;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn app_with(doors: DoorConfig, login: Arc<Login>) -> (Router, tempfile::TempDir) {
    let (dir, store) = scratch_store().await;
    let plane = Arc::new(Plane::new(store, Arc::new(NoRuntime)));
    // No apps live here — the app door serves an empty home.
    let workspace = dir.path().to_path_buf();
    (router(plane, doors, workspace, Access::Gated(login)), dir)
}

async fn app() -> (Router, tempfile::TempDir) {
    app_with(DoorConfig::default(), common::login()).await
}

/// The same doors over a workspace that already holds one dataset.
/// `/query` and `/app` are dataset-scoped and 404 on a name the
/// workspace does not hold — only `/mcp` may bring one into being.
async fn app_on_fin() -> (Router, tempfile::TempDir) {
    let (app, dir) = app().await;
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                1,
                "DECLARE DATASET fin SET (purpose: 'door test');",
                None,
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    (app, dir)
}

async fn body_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_docket_app_ships_in_the_binary() {
    // The workspace carries no apps — the built-in answers for the name,
    // on whichever dataset the URL names.
    let (app, _dir) = app_on_fin().await;
    let response = app
        .oneshot(
            Request::get("/fin/app/docket")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("Docket"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dataset_the_workspace_does_not_hold_is_a_404() {
    // The URL is the binding, so a name nobody declared is a missing
    // resource, not a query that fails oddly three layers down. The
    // answer names what the workspace does hold — a mistyped dataset
    // should not require a second request to recover from.
    let (app, _dir) = app_on_fin().await;
    let response = app
        .clone()
        .oneshot(
            Request::get("/nope/app/docket/frames/census")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .oneshot(
            Request::post("/nope/query")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
                .body(Body::from("SELECT 1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_json(response).await;
    assert!(body["error"].as_str().unwrap().contains("fin"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_query_door_streams_arrow_ipc() {
    let (app, _dir) = app_on_fin().await;
    let response = app
        .oneshot(
            Request::post("/fin/query")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
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
    let (app, _dir) = app_on_fin().await;
    let response = app
        .oneshot(
            Request::post("/fin/query")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
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
    let (app, _dir) = app_on_fin().await;
    let response = app
        .oneshot(
            Request::post("/fin/query")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
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

/// The gate. A bearer token from the issuer names who is speaking; the
/// door names with which standing. Nothing in the tree signs a token
/// except the test issuer in `common`.
#[tokio::test(flavor = "multi_thread")]
async fn the_token_names_the_subject_and_the_door_names_the_standing() {
    let (app, _dir) = app().await;

    // Over the agent door: agent standing, the token's subject as the
    // id — never the handshake's `clientInfo` name, which a client
    // picks for itself on every request.
    let request = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, common::bearer("ada"))
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "glossql")
        .body(Body::from(
            call_with(
                meta(),
                1,
                "DECLARE DATASET fin SET (purpose: 'token test');\n\
                 USE fin;\n\
                 DECLARE ASPECT note WITH $${\"type\": \"object\"}$$ AS FACT ON DATASET;\n\
                 GLOSS note ON fin AS $${\"value\": \"through the agent door\"}$$;\n\
                 SELECT actor_id, actor_kind FROM glossary WHERE aspect = 'note';",
                None,
            )
            .to_string(),
        ))
        .unwrap();
    let body = expect_ok(app.clone().oneshot(request).await.unwrap()).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).expect(text);
    let last = outcomes.as_array().unwrap().last().unwrap();
    assert_eq!(last["rows"][0]["actor_id"], json!("ada"), "{outcomes}");
    assert_eq!(last["rows"][0]["actor_kind"], json!("agent"), "{outcomes}");

    // The same token over a human door: the same subject, human
    // standing. Both rows stand — the supersession key's third leg is
    // the kind, and the door set it.
    let response = app
        .oneshot(
            Request::post("/fin/query")
                .header(header::AUTHORIZATION, common::bearer("ada"))
                .body(Body::from(
                    "GLOSS note ON fin AS $${\"value\": \"through the human door\"}$$;\n\
                     SELECT actor_id, actor_kind FROM glossary WHERE aspect = 'note' \
                     ORDER BY actor_kind;",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let rows = body[1]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{body}"));
    assert_eq!(rows.len(), 2, "{body}");
    assert_eq!(rows[0]["actor_kind"], json!("agent"), "{body}");
    assert_eq!(rows[1]["actor_kind"], json!("human"), "{body}");
    assert!(rows.iter().all(|r| r["actor_id"] == json!("ada")), "{body}");
}

/// The explicit open arrangement (`Access::Open`, the
/// GLOSSQL_INSECURE_OPEN switch): no door asks for a token, and the
/// record still names who spoke — the well-known dev actor, with the
/// door's standing. No login and no discovery document are served;
/// with no 401 to answer, a client is never sent to authenticate.
#[tokio::test(flavor = "multi_thread")]
async fn open_doors_ask_no_token_and_stamp_the_dev_actor() {
    let (dir, store) = scratch_store().await;
    let plane = Arc::new(Plane::new(store, Arc::new(NoRuntime)));
    let app = router(
        plane,
        DoorConfig::default(),
        dir.path().to_path_buf(),
        Access::Open,
    );

    // The agent door, bare: agent standing, the dev actor as the id.
    let request = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "glossql")
        .body(Body::from(
            call_with(
                meta(),
                1,
                "DECLARE DATASET fin SET (purpose: 'open door test');\n\
                 USE fin;\n\
                 DECLARE ASPECT note WITH $${\"type\": \"object\"}$$ AS FACT ON DATASET;\n\
                 GLOSS note ON fin AS $${\"value\": \"through the open agent door\"}$$;\n\
                 SELECT actor_id, actor_kind FROM glossary WHERE aspect = 'note';",
                None,
            )
            .to_string(),
        ))
        .unwrap();
    let body = expect_ok(app.clone().oneshot(request).await.unwrap()).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    let outcomes: Value = serde_json::from_str(text).expect(text);
    let last = outcomes.as_array().unwrap().last().unwrap();
    assert_eq!(last["rows"][0]["actor_id"], json!(INSECURE_DEV_MODE));
    assert_eq!(last["rows"][0]["actor_kind"], json!("agent"));

    // A human door, bare: the same id, human standing — the
    // supersession key's third leg still works, the door still sets it.
    let response = app
        .clone()
        .oneshot(
            Request::post("/fin/query")
                .body(Body::from(
                    "GLOSS note ON fin AS $${\"value\": \"through the open human door\"}$$;\n\
                     SELECT actor_id, actor_kind FROM glossary WHERE aspect = 'note' \
                     ORDER BY actor_kind;",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let rows = body[1]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{body}"));
    assert_eq!(rows.len(), 2, "{body}");
    assert_eq!(rows[0]["actor_kind"], json!("agent"), "{body}");
    assert_eq!(rows[1]["actor_kind"], json!("human"), "{body}");
    assert!(
        rows.iter()
            .all(|r| r["actor_id"] == json!(INSECURE_DEV_MODE)),
        "{body}"
    );

    // Nothing to authenticate with is served: no login, no discovery.
    for path in ["/auth/login", "/.well-known/oauth-protected-resource"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

/// A request that brings no token, or one this issuer did not mint for
/// this server, is refused with the discovery pointer an OAuth-capable
/// client follows. Each refusal here is one check the gate makes:
/// signature, key, issuer, audience, expiry.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_or_foreign_token_is_refused_with_the_discovery_pointer() {
    let (app, _dir) = app().await;

    let none = app
        .clone()
        .oneshot(
            Request::post("/fin/query")
                .body(Body::from("SELECT 1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(none.status(), StatusCode::UNAUTHORIZED);
    let challenge = none.headers()[header::WWW_AUTHENTICATE]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        challenge.contains("oauth-protected-resource"),
        "{challenge}"
    );

    // The agent door refuses before the protocol ever sees the body.
    let agent_door = app
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header(header::HOST, "127.0.0.1")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(agent_door.status(), StatusCode::UNAUTHORIZED);

    let foreign = [
        ("not a token", "not.a.token".to_string()),
        (
            "another audience",
            common::token_for(
                "ada",
                common::ISSUER,
                "https://elsewhere.example",
                common::far_future(),
            ),
        ),
        (
            "another issuer",
            common::token_for(
                "ada",
                "https://other.issuer.test",
                common::RESOURCE,
                common::far_future(),
            ),
        ),
        (
            "expired",
            common::token_for("ada", common::ISSUER, common::RESOURCE, 1),
        ),
        (
            "a key the issuer does not publish",
            common::token_under_kid("ada", "rotated-away"),
        ),
        (
            "a rewritten subject",
            common::forged(&common::token("ada"), "mallory"),
        ),
    ];
    for (why, token) in foreign {
        let response = app
            .clone()
            .oneshot(
                Request::post("/fin/query")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from("SELECT 1"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{why}");
    }
}

/// The audience is the whole binding. RFC 8707, which the MCP
/// authorization spec makes a MUST, has the client ask for this
/// server's canonical URI with `resource=` and the issuer stamp it as
/// `aud`. A token that names some other resource is refused, and so is
/// one that names none — whatever application it was minted for, under
/// either name that claim goes by.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_token_that_names_this_server_opens_it() {
    let (app, _dir) = app_on_fin().await;
    let query = |token: String| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::post("/fin/query")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from("SELECT 1 AS one; SELECT 2 AS two"))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    let ours = query(common::token("ada")).await;
    assert_eq!(ours.status(), StatusCode::OK);

    let no_audience = query(common::token_from_app("ada", common::CLIENT_ID)).await;
    assert_eq!(no_audience.status(), StatusCode::UNAUTHORIZED);

    // RFC 9068 spells that same application claim `client_id`; neither
    // spelling stands in for an audience.
    let rfc_9068 = query(common::token_with(json!({
        "iss": common::ISSUER, "aud": [], "client_id": common::CLIENT_ID,
        "sub": "ada", "exp": common::far_future(), "iat": 1
    })))
    .await;
    assert_eq!(rfc_9068.status(), StatusCode::UNAUTHORIZED);

    let elsewhere = query(common::token_with(json!({
        "iss": common::ISSUER, "aud": "https://elsewhere.example", "azp": common::CLIENT_ID,
        "sub": "ada", "exp": common::far_future(), "iat": 1
    })))
    .await;
    assert_eq!(elsewhere.status(), StatusCode::UNAUTHORIZED);
}

/// A person in a browser holds no token, so a navigation to a door is
/// sent to sign in and brought back afterwards; a machine's call gets
/// the 401 it can act on.
#[tokio::test(flavor = "multi_thread")]
async fn a_browser_without_a_token_is_sent_to_sign_in() {
    let (app, _dir) = app().await;
    let response = app
        .oneshot(
            Request::get("/fin/app/docket?page=2")
                .header(header::ACCEPT, "text/html,application/xhtml+xml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        response.headers()[header::LOCATION],
        "/auth/login?next=%2Ffin%2Fapp%2Fdocket%3Fpage%3D2"
    );
}

/// The whole browser flow against a stand-in issuer: off to sign in
/// with PKCE and the resource named, back with a code, the code
/// exchanged at the token endpoint with the verifier and the
/// application's secret in the body, the token verified by the gate and
/// handed to the browser as the cookie — which then opens a human door.
#[tokio::test(flavor = "multi_thread")]
async fn a_browser_signs_in_at_the_issuer_and_comes_back_with_a_cookie() {
    // The issuer's token endpoint, stood in for: it records what it was
    // asked and answers with a token the test issuer signed.
    let asked: Arc<std::sync::Mutex<Option<HashMap<String, String>>>> = Arc::default();
    let recorder = Arc::clone(&asked);
    let issuer = Router::new().route(
        "/token",
        axum::routing::post(
            move |axum::Form(form): axum::Form<HashMap<String, String>>| {
                let recorder = Arc::clone(&recorder);
                async move {
                    *recorder.lock().unwrap() = Some(form);
                    axum::Json(json!({
                        "access_token": common::token("ada"),
                        "token_type": "Bearer",
                        "expires_in": 3600
                    }))
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_url = format!("http://{}/token", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, issuer).await.unwrap() });

    let (app, _dir) = app_with(DoorConfig::default(), common::login_with(&token_url)).await;
    expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                1,
                "DECLARE DATASET fin SET (purpose: 'login test');",
                None,
            ),
        )
        .await,
    )
    .await;

    // Off to the issuer, with everything the code flow needs.
    let going = app
        .clone()
        .oneshot(
            Request::get("/auth/login?next=/fin/app/docket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(going.status(), StatusCode::SEE_OTHER);
    let location = going.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_string();
    assert!(location.starts_with(common::AUTHORIZE), "{location}");
    let params: HashMap<String, String> = oauth2::url::Url::parse(&location)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect();
    assert_eq!(params["response_type"], "code");
    assert_eq!(params["client_id"], common::CLIENT_ID);
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(params["resource"], common::RESOURCE);
    assert_eq!(
        params["redirect_uri"],
        format!("{}/auth/callback", common::RESOURCE)
    );
    let state = params["state"].clone();
    let pending = going.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        pending.starts_with("glossql_login=")
            && pending.contains("HttpOnly")
            && pending.contains("Path=/auth"),
        "{pending}"
    );
    let pending_cookie = pending.split(';').next().unwrap().to_string();

    // Back with the wrong state: refused.
    let wrong = app
        .clone()
        .oneshot(
            Request::get("/auth/callback?code=abc&state=not-that-one")
                .header(header::COOKIE, &pending_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::BAD_REQUEST);

    // Back with the right one: exchanged, verified, handed over.
    let back = app
        .clone()
        .oneshot(
            Request::get(format!("/auth/callback?code=abc&state={state}"))
                .header(header::COOKIE, &pending_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = back.status();
    let headers = back.headers().clone();
    let said =
        String::from_utf8_lossy(&to_bytes(back.into_body(), usize::MAX).await.unwrap()).to_string();
    assert_eq!(status, StatusCode::SEE_OTHER, "{said}");
    assert_eq!(headers[header::LOCATION], "/fin/app/docket");
    let cookies: Vec<String> = headers
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    let token_cookie = cookies
        .iter()
        .find(|c| c.starts_with("glossql_token="))
        .unwrap_or_else(|| panic!("no token cookie in {cookies:?}"));
    assert!(
        token_cookie.contains("HttpOnly")
            && token_cookie.contains("SameSite=Lax")
            && token_cookie.contains("Path=/")
            && token_cookie.contains("Max-Age=3600")
            && !token_cookie.contains("Secure"),
        "{token_cookie}"
    );
    assert!(
        cookies
            .iter()
            .any(|c| c.starts_with("glossql_login=;") && c.contains("Max-Age=0")),
        "the login in progress is cleared: {cookies:?}"
    );
    let exchange = asked
        .lock()
        .unwrap()
        .clone()
        .expect("the token endpoint was asked");
    assert_eq!(exchange["grant_type"], "authorization_code");
    assert_eq!(exchange["code"], "abc");
    assert_eq!(exchange["client_id"], common::CLIENT_ID);
    assert_eq!(exchange["client_secret"], common::CLIENT_SECRET);
    assert_eq!(
        exchange["redirect_uri"],
        format!("{}/auth/callback", common::RESOURCE)
    );
    assert_eq!(exchange["resource"], common::RESOURCE);
    assert!(exchange.contains_key("code_verifier"), "{exchange:?}");

    // The cookie opens a human door.
    let cookie = token_cookie.split(';').next().unwrap().to_string();
    let docket = app
        .clone()
        .oneshot(
            Request::get("/fin/app/docket")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(docket.status(), StatusCode::OK);

    // And logout takes it away.
    let out = app
        .oneshot(Request::get("/auth/logout").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(out.status(), StatusCode::SEE_OTHER);
    assert!(
        out.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .starts_with("glossql_token=;")
    );
}

/// The discovery document sits outside the gate — it is where a client
/// learns how to authenticate, and a document that answered 401 would
/// point the client at itself. So do the app's own assets, which hold
/// no data.
#[tokio::test(flavor = "multi_thread")]
async fn the_discovery_document_and_the_assets_answer_without_a_token() {
    let (app, _dir) = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::get("/.well-known/oauth-protected-resource")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    assert_eq!(body["resource"], json!(common::RESOURCE), "{body}");
    assert_eq!(
        body["authorization_servers"],
        json!([common::ISSUER]),
        "{body}"
    );
    // RFC 9728 §3.1: a client given `…/mcp` asks under that path first.
    let under_path = app
        .clone()
        .oneshot(
            Request::get("/.well-known/oauth-protected-resource/mcp")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(under_path.status(), StatusCode::OK);
    assert_eq!(body_json(under_path).await, body);

    let asset = app
        .oneshot(
            Request::get("/assets/gl-rows.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(asset.status(), StatusCode::OK);
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
        .header(header::AUTHORIZATION, common::bearer("dev-agent"))
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    // SEP-2243: a call names its subject in the header too — from
    // `name` for tools and prompts, from `uri` for resources.
    if let Some(name) = payload["params"]["name"]
        .as_str()
        .or_else(|| payload["params"]["uri"].as_str())
    {
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

/// The tolerant floor, proven the way real clients arrive (the
/// 2026-08-27 ChatGPT run): an initialize declaring an older revision
/// is negotiated down to the library's floor rather than refused, and
/// a request that stamps no version marker at all — no header, no
/// `_meta` — is served at the server's own revision, which is what
/// the spec has a server assume for an absent header. Claude Code
/// stamps everything and rides the strict path above.
#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_serves_older_and_unstamped_clients() {
    let (app, _dir) = app().await;

    // An older client's initialize, stamped with its own revision the
    // way ChatGPT stamps it: answered, at the library's floor. The
    // exact floor moves with rmcp — when this assert breaks on an
    // upgrade, the new value is the new floor, and that is the point
    // of pinning it.
    let older = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, common::bearer("dev-agent"))
        .header("mcp-protocol-version", "2025-06-18")
        .body(Body::from(
            json!({
                "jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "older-client", "version": "0"}
                }
            })
            .to_string(),
        ))
        .unwrap();
    let body = expect_ok(app.clone().oneshot(older).await.unwrap()).await;
    assert_eq!(body["result"]["protocolVersion"], "2025-11-25", "{body}");

    // A request with no version marker anywhere: served, not refused.
    let bare = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, common::bearer("dev-agent"))
        .body(Body::from(
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}).to_string(),
        ))
        .unwrap();
    let body = expect_ok(app.oneshot(bare).await.unwrap()).await;
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools[0]["name"], "glossql", "{body}");
}

/// The teaching surface: every skill is a resource and a prompt, the
/// normative artifacts are resources, and what is read back is what
/// the build embedded.
#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_door_serves_the_skills_as_resources_and_prompts() {
    let (app, _dir) = app().await;
    let body = expect_ok(mcp(app.clone(), initialize()).await).await;
    assert!(
        body["result"]["capabilities"]["resources"].is_object(),
        "{body}"
    );
    assert!(
        body["result"]["capabilities"]["prompts"].is_object(),
        "{body}"
    );

    let body = expect_ok(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 3, "method": "resources/list", "params": {"_meta": meta()}}),
        )
        .await,
    )
    .await;
    let uris: Vec<&str> = body["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    for uri in [
        "skill://glossql/SKILL.md",
        "skill://glossql-metrics/SKILL.md",
        "skill://glossql-functions/SKILL.md",
        "skill://glossql-apps/SKILL.md",
        "skill://glossql-metrics/references/ground.md",
        "doc://SPEC.md",
        "doc://grammar.ebnf",
        "doc://docs/reference/reads.md",
    ] {
        assert!(uris.contains(&uri), "{uri} missing from {uris:?}");
    }
    // A tree page reads back as the file, under the skill's own root.
    let body = expect_ok(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 8, "method": "resources/read",
                   "params": {"_meta": meta(), "uri": "doc://docs/reference/reads.md"}}),
        )
        .await,
    )
    .await;
    assert!(
        body["result"]["contents"][0]["text"]
            .as_str()
            .unwrap()
            .contains("metric_axes()"),
        "{body}"
    );

    let body = expect_ok(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 4, "method": "resources/read",
                   "params": {"_meta": meta(), "uri": "skill://glossql/SKILL.md"}}),
        )
        .await,
    )
    .await;
    let text = body["result"]["contents"][0]["text"].as_str().unwrap();
    assert_eq!(text, glossql_serverd::skills::SKILLS[0].body, "{body}");
    assert_eq!(
        body["result"]["contents"][0]["mimeType"], "text/markdown",
        "{body}"
    );

    let body = expect_ok(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 5, "method": "prompts/list", "params": {"_meta": meta()}}),
        )
        .await,
    )
    .await;
    let prompts = body["result"]["prompts"].as_array().unwrap();
    assert_eq!(prompts.len(), 4, "{body}");
    assert!(
        prompts
            .iter()
            .all(|p| !p["description"].as_str().unwrap_or_default().is_empty()),
        "{body}"
    );

    let body = expect_ok(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 6, "method": "prompts/get",
                   "params": {"_meta": meta(), "name": "glossql"}}),
        )
        .await,
    )
    .await;
    assert_eq!(
        body["result"]["messages"][0]["content"]["text"],
        glossql_serverd::skills::SKILLS[0].body,
        "{body}"
    );

    // A URI this door does not hold is refused, never served as an
    // empty page. The 2026-07-28 revision spells resource-not-found
    // as invalid params (rmcp rewrites the older -32002), which the
    // transport maps to HTTP 400.
    let response = mcp(
        app,
        json!({"jsonrpc": "2.0", "id": 7, "method": "resources/read",
               "params": {"_meta": meta(), "uri": "doc://README.md"}}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], -32602, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("doc://README.md"),
        "{body}"
    );
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
    // What landed rides beside the refusal, in the usual shape — the
    // first statement's rows are not discarded with the second's error.
    let landed = body["result"]["content"][1]["text"].as_str().unwrap();
    assert!(
        landed.contains("\"landed\"") && landed.contains("\"a\""),
        "{landed}"
    );

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
    let runtime = Arc::new(glossql_scripts::KernelRuntime::new(
        dir.path().to_path_buf(),
    ));
    let plane = Arc::new(Plane::new(store.clone(), runtime));
    let human = Actor {
        kind: ActorKind::Human,
        id: BOOTSTRAP.into(),
    };
    bootstrap(&plane, human).await.unwrap();
    let app = router(
        plane,
        DoorConfig::default(),
        dir.path().to_path_buf(),
        Access::Gated(common::login()),
    );

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
            call_with(
                meta_elicit(),
                71,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                None,
            ),
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
    assert!(!instructions.contains("question"), "{instructions}");
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
            let text = body["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string();
            let outcomes: Value = serde_json::from_str(&text).unwrap();
            outcomes[0]["rows"].as_array().cloned().unwrap_or_default()
        }
    };

    // One decision, disclosed twice — two rows stand open.
    assert_eq!(open(181).await.len(), 2);

    let review = "SELECT subject, aspect FROM glossary LIMIT 5";

    // dio asks first (aspect order) and the human corrects it. With
    // nothing yet ruled on this key, there is no box to repeat one.
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 182, review, None)).await).await;
    let first = &body["result"]["inputRequests"]["loose:fin:dio:days-in-period"];
    assert!(first.is_object(), "{body}");
    let boxes = &first["params"]["requestedSchema"]["properties"];
    assert!(
        boxes.get("3_same_as").is_none(),
        "nothing to repeat yet: {boxes}"
    );

    let corrected = json!({"action": "accept",
        "content": {"5_correction": "use a fixed 30-day month"}});
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
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 184, review, None)).await).await;
    let second = &body["result"]["inputRequests"]["loose:fin:dso:days-in-period"];
    assert!(second.is_object(), "{body}");
    // The prior ruling is its own box, and its title says which one.
    // The message carries identity only — it is the one surface the
    // client clips.
    let repeat = &second["params"]["requestedSchema"]["properties"]["3_same_as"];
    assert_eq!(repeat["type"], json!("boolean"), "{second}");
    assert!(
        repeat["description"]
            .as_str()
            .is_some_and(|d| d.contains("corrected on dio")),
        "the repeat must be one click: {second}"
    );

    // Ticking it replays the stance AND the human's own words.
    let same = json!({"action": "accept",
        "content": {"3_same_as": true}});
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
        body["result"]["content"]
            .to_string()
            .contains("ruled (corrected)"),
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
            call_with(
                meta_elicit(),
                61,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                None,
            ),
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
    // The claim rides the description of the box that confirms it: a
    // message is clipped at three lines and a title is truncated to
    // share its line with the value, while a description renders whole.
    assert!(
        ask["params"]["requestedSchema"]["properties"]["1_stands"]["description"]
            .as_str()
            .is_some_and(|d| d.contains("per line")),
        "{body}"
    );

    let answer = json!({"action": "accept", "content": {"1_stands": true}});
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
        body["result"]["content"]
            .to_string()
            .contains("ruled (confirmed)"),
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
    assert_eq!(
        outcomes[0]["rows"][0]["aspect"],
        json!("ruling"),
        "{outcomes}"
    );
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
            call_with(
                meta_elicit(),
                64,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                None,
            ),
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
async fn every_way_out_that_is_not_an_answer_is_a_defer() {
    // The dialog has two outcomes. Accepting with something said is
    // the save; Decline, Esc, and a form confirmed empty are all the
    // same defer — nothing recorded, the claim still derives, and the
    // next review asks it again. Nobody may opt out of being asked:
    // the question is what the numbers rest on, and the way to make it
    // stop is to answer it, `unclear` included.
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
    let mut id = 71;
    for way_out in [
        json!({"action": "decline"}),
        json!({"action": "cancel"}),
        json!({"action": "accept", "content": {}}),
    ] {
        let body =
            expect_ok(mcp(app.clone(), call_with(meta_elicit(), id, review, None)).await).await;
        assert_eq!(
            body["result"]["resultType"],
            json!("input_required"),
            "still open before {way_out}: {body}"
        );
        let body = expect_ok(
            mcp(
                app.clone(),
                call_with(
                    meta_elicit(),
                    id + 1,
                    review,
                    Some(("loose:fin:dso:per-line", way_out.clone())),
                ),
            )
            .await,
        )
        .await;
        assert!(
            body["result"]["content"].to_string().contains("deferred"),
            "{way_out} defers: {body}"
        );
        id += 2;
    }

    // No write in between, and it is asked again every time — a defer
    // buys no quiet, because the claim is still what the numbers rest
    // on.
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), id, review, None)).await).await;
    assert_eq!(
        body["result"]["resultType"],
        json!("input_required"),
        "a deferred question stands open: {body}"
    );

    // And nothing was recorded by any of the three.
    let body = expect_ok(
        mcp(
            app,
            call_with(
                meta(),
                id + 1,
                "SELECT count(*) AS n FROM ruling_entries;",
                None,
            ),
        )
        .await,
    )
    .await;
    assert!(
        body["result"]["content"][0]["text"]
            .to_string()
            .contains("\\\"n\\\":0"),
        "a defer records nothing: {body}"
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
    assert_ne!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // A plain data read: judging work, not a review — no form either.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(meta_elicit(), 92, "SELECT 1 AS ok", None),
        )
        .await,
    )
    .await;
    assert_ne!(
        body["result"]["resultType"],
        json!("input_required"),
        "{body}"
    );

    // The review-shaped call carries the form.
    let body = expect_ok(
        mcp(
            app,
            call_with(
                meta_elicit(),
                93,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                None,
            ),
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

/// The handshake's own name never reaches the record. `clientInfo`
/// carries one here and the token carries another; the record takes
/// the token's, because a caller names itself on every request and the
/// string proves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_clients_own_name_never_reaches_the_record() {
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
    let claimed = meta()["io.modelcontextprotocol/clientInfo"]["name"].clone();
    assert!(
        claimed.is_string(),
        "the handshake names a client: {claimed}"
    );

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
    // `mcp()` speaks with the test issuer's token for `dev-agent`.
    assert_eq!(
        outcomes[0]["rows"][0]["actor_id"],
        json!("dev-agent"),
        "{outcomes}"
    );
    assert_ne!(outcomes[0]["rows"][0]["actor_id"], claimed, "{outcomes}");
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_reads_pass_the_cap_uncapped() {
    let (app, _dir) = app_with(DoorConfig { row_cap: 3 }, common::login()).await;
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

    // Five glosses through one batch call. `USE` binds only the
    // statements after it *within this call*; the reads below stand on
    // their own, one naming its dataset and one reading a relation that
    // has none.
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
    let (app, _dir) = app_with(DoorConfig { row_cap: 3 }, common::login()).await;
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
        let answer = json!({"action": "accept", "content": {"1_stands": true}});
        let body = expect_ok(
            mcp(
                app.clone(),
                call_with(meta_elicit(), 81, "SELECT 1 AS ok", Some((key, answer))),
            )
            .await,
        )
        .await;
        assert!(
            body["result"]["content"]
                .to_string()
                .contains("ruled (confirmed)"),
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
    assert!(
        agent_body.contains("0.6"),
        "the agent body moved: {agent_body}"
    );
    assert!(
        agent_body.contains("0.7"),
        "the agent body moved: {agent_body}"
    );

    // And the round is quiet — nothing re-derives.
    let body = expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta_elicit(),
                83,
                "SELECT subject, aspect FROM glossary LIMIT 5",
                None,
            ),
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
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 101, review, None)).await).await;
    assert!(
        body["result"]["inputRequests"]["loose:fin:dpo:goods-only"].is_object(),
        "{body}"
    );
    let corrected = json!({"action": "accept",
        "content": {"5_correction": "all suppliers, not goods only"}});
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

    // purchases asks next, and the form offers what was already ruled
    // on that same key as an answer — the `sibling` column, carried by
    // the read, standing as its own box with the prior words beneath.
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 103, review, None)).await).await;
    let form = &body["result"]["inputRequests"]["loose:fin:purchases:goods-only"];
    assert!(form.is_object(), "{body}");
    let repeat = &form["params"]["requestedSchema"]["properties"]["3_same_as"];
    let said = repeat["description"].as_str().expect("a description");
    assert!(
        said.contains("corrected on dpo"),
        "the form offers the sibling ruling: {body}"
    );
    assert!(
        said.contains("all suppliers, not goods only"),
        "the prior words ride with it: {body}"
    );

    // The human confirms it anyway — legitimate, different aspect.
    let confirmed = json!({"action": "accept", "content": {"1_stands": true}});
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
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 105, review, None)).await).await;
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
    // The grounding's write answers with its fact row through the
    // door — the `metric_axes()` shape, abstaining here with its
    // reason (this app declares no `cube` aspect) and landed all the
    // same.
    let outcomes = blocks[0]["text"].as_str().unwrap();
    assert!(
        outcomes.contains("\"unadmitted\"") && outcomes.contains("\"applicable\":false"),
        "{outcomes}"
    );
    let brief = blocks
        .iter()
        .find(|b| b["text"].as_str().is_some_and(|t| t.starts_with("brief: ")));
    let brief = brief.unwrap_or_else(|| panic!("the landing moved the brief: {body}"));
    let brief = brief["text"].as_str().unwrap();
    assert!(brief.contains("judgment question"), "{body}");
    // Debt before presence: what owes an act leads, the record's size
    // closes — a count of human writings is never read as work.
    assert!(
        brief.find("judgment question") < brief.find("Record: 0 human writings"),
        "{brief}"
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
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 131, review, None)).await).await;
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
    let body = expect_ok(mcp(app.clone(), call_with(meta_elicit(), 133, review, None)).await).await;
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
    // A round is one grounding: the least confident open claim picks
    // the aspect, and every open claim on THAT aspect travels together.
    // dpo and dso are two groundings, so only dpo's is asked — and the
    // message says how many stand open in the whole, so the end of a
    // round never reads as the end of the work.
    let asked = body["result"]["inputRequests"].as_object().unwrap();
    assert_eq!(asked.len(), 1, "one grounding to a round: {body}");
    let ask = &asked["loose:fin:dpo:goods-only"];
    assert!(
        ask["params"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("1 of 2 open")),
        "the round names the whole it is part of: {body}"
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
        .oneshot(
            Request::get("/fin/app/cash")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("What stands open"), "{html}");
    assert!(
        html.contains("Monday cash"),
        "the manifest names it: {html}"
    );

    // And its frame runs, over a shipped read, as Arrow IPC.
    let response = app
        .clone()
        .oneshot(
            Request::get("/fin/app/cash/frames/open")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
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
        .oneshot(
            Request::get("/fin/app/cash")
                .header(header::AUTHORIZATION, common::bearer("dev-human"))
                .body(Body::empty())
                .unwrap(),
        )
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
            let body = expect_ok(mcp(app, call_with(meta(), id, sql, None)).await).await;
            let text = body["result"]["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            serde_json::from_str::<Value>(&text).unwrap_or(json!([]))
        }
    };

    let whole = read(
        "SELECT surface, stands FROM workspace_next ORDER BY surface;",
        171,
    )
    .await;
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

/// The opening names where to begin, from what stands: a workspace
/// before its first dataset has no brief to sweep — the brief's
/// reads all need one — and the door says so instead of sending the
/// agent to them. Once a dataset stands, the brief is the opening,
/// and it is a read, never a gate.
#[tokio::test(flavor = "multi_thread")]
async fn the_opening_names_where_to_begin() {
    let (app, _dir) = app().await;
    // A call refreshes the brief; the opening rides initialize.
    expect_ok(
        mcp(
            app.clone(),
            call_with(meta(), 1, "SELECT * FROM datasets;", None),
        )
        .await,
    )
    .await;
    let body = expect_ok(mcp(app.clone(), initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("No dataset stands yet"),
        "{instructions}"
    );
    assert!(instructions.contains("workspace_next"), "{instructions}");

    expect_ok(
        mcp(
            app.clone(),
            call_with(
                meta(),
                2,
                "DECLARE DATASET fin SET (purpose: 'door test');",
                None,
            ),
        )
        .await,
    )
    .await;
    let body = expect_ok(mcp(app, initialize()).await).await;
    let instructions = body["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions.contains("Open with the brief"),
        "{instructions}"
    );
    assert!(instructions.contains("not a gate"), "{instructions}");
    assert!(
        !instructions.contains("No dataset stands yet"),
        "{instructions}"
    );
}
