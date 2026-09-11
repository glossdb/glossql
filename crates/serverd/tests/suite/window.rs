//! The window and the function listings. The graph's vocabulary is the
//! server's own; every key names a read and a column it serves; every
//! node reaches `End`; the listings are the registries; and the window
//! rides the tool result when a keyed condition holds on the record and
//! stays off an edge whose condition does not.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode, header};
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_serverd::{Access, BOOTSTRAP, DoorConfig, Plane, bootstrap, functions, router, window};
use glossql_session::{DOORS, NoRuntime, Session};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::common;

/// A fresh workspace with the shipped system landed — the vocabulary
/// the graph's `GLOSS <aspect>` and function nodes name.
async fn scratch_plane() -> (tempfile::TempDir, Arc<Plane>) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let plane =
        Plane::new(store, Arc::new(NoRuntime)).with_pages(glossql_serverd::skills::door_pages());
    bootstrap(
        &plane,
        Actor {
            kind: ActorKind::Human,
            id: BOOTSTRAP.into(),
        },
    )
    .await
    .unwrap();
    (dir, Arc::new(plane))
}

fn human() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: "window-test".into(),
    }
}

/// One column of a read, as a set of strings.
async fn names(session: &Session, sql: &str, column: &str) -> BTreeSet<String> {
    let outcomes = session.execute(sql).await.unwrap();
    let mut out = BTreeSet::new();
    for outcome in outcomes {
        if let glossql_session::Outcome::Rows(batches) = outcome {
            for batch in batches {
                let idx = batch.schema().index_of(column).unwrap();
                let col = batch.column(idx);
                let strings = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("{column} is not a string column"));
                out.extend(strings.iter().flatten().map(str::to_string));
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn every_window_node_names_a_real_thing() {
    let (_dir, plane) = scratch_plane().await;
    let session = plane.channel(human(), None).await.unwrap();
    let aspects = names(&session, "SELECT name FROM aspects", "name").await;
    let declared = names(&session, "SELECT name FROM functions", "name").await;
    let registered: BTreeSet<String> = session
        .registered_functions()
        .into_iter()
        .filter(|f| f.kind == "table")
        .map(|f| f.name)
        .collect();
    let reads: BTreeSet<&str> = glossql_session::library_reads().into_iter().collect();
    let kinds = [
        "Start",
        "End",
        "USE",
        "PROBE",
        "EXTRACT",
        "SQL",
        "GLOSS",
        "DECLARE ASPECT",
        "DECLARE SOURCE",
        "DECLARE RECIPE",
        "DECLARE DATASET",
        "DECLARE RELATIONSHIP",
        "DECLARE FUNCTION",
        "DECLARE WITNESS",
    ];
    let families = ["read.<name>", "misfit.<name>", "whatif.<name>"];
    let graph = window::graph();
    assert!(!graph.nodes.is_empty());
    for node in &graph.nodes {
        let n = node.as_str();
        let ok = kinds.contains(&n)
            || ["query", "fact", "measurement"]
                .iter()
                .any(|k| n == format!("DECLARE ASPECT {k}") || n == format!("GLOSS {k}"))
            || n.strip_prefix("GLOSS ")
                .is_some_and(|a| aspects.contains(a))
            || declared.contains(n)
            || registered.contains(n)
            || DOORS.iter().any(|(d, _)| d.eq_ignore_ascii_case(n))
            || reads.contains(n)
            || glossql_glossary::relation_columns(n).is_some()
            || families.contains(&n);
        assert!(ok, "window.json names nothing the server has: `{node}`");
    }
    for e in &graph.edges {
        for end in [&e.from, &e.to] {
            assert!(
                graph.nodes.contains(end),
                "edge endpoint is not a node: `{end}` ({} → {})",
                e.from,
                e.to
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_key_names_a_read_and_a_column_it_serves() {
    let (_dir, plane) = scratch_plane().await;
    plane
        .execute(
            human(),
            None,
            "DECLARE DATASET fin SET (purpose: 'window test')",
        )
        .await
        .unwrap();
    let session = plane.channel(human(), Some("fin")).await.unwrap();
    let graph = window::graph();
    let mut checked = BTreeSet::new();
    for e in &graph.edges {
        let Some(keys) = &e.when.key else { continue };
        for key in keys.each() {
            let sql = format!("{} LIMIT 0", window::read_sql(&key.read, "fin"));
            let query = session
                .query_stream(&sql)
                .await
                .unwrap_or_else(|err| panic!("the key read `{}` does not plan: {err}", key.read));
            let columns: BTreeSet<String> = query
                .stream
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            for field in key.conditions.keys() {
                assert!(
                    columns.contains(field),
                    "the key on `{}` ({} → {}) names no column `{field}`; it serves {columns:?}",
                    key.read,
                    e.from,
                    e.to
                );
            }
            checked.insert(key.read.clone());
        }
    }
    assert!(checked.len() >= 5, "keyed reads: {checked:?}");
}

#[test]
fn every_node_reaches_end() {
    let graph = window::graph();
    let mut reaches: BTreeSet<&str> = BTreeSet::from(["End"]);
    let mut queue: VecDeque<&str> = VecDeque::from(["End"]);
    while let Some(to) = queue.pop_front() {
        for e in graph.edges.iter().filter(|e| e.to == to) {
            if reaches.insert(e.from.as_str()) {
                queue.push_back(e.from.as_str());
            }
        }
    }
    for e in &graph.edges {
        assert!(
            reaches.contains(e.from.as_str()),
            "no path to End from `{}`",
            e.from
        );
    }
}

#[test]
fn the_localizer_names_the_last_act() {
    let graph = window::graph();
    let node = |statements: &str, ran: Option<usize>, refused: bool| match window::locate(
        graph, statements, ran, refused,
    )
    .act
    {
        window::Act::Node(n) => n,
        window::Act::Gloss(a) => format!("gloss:{a}"),
    };
    assert_eq!(
        node(
            "USE fin; SELECT * FROM metric_axes() WHERE applicable",
            None,
            false
        ),
        "metric_axes"
    );
    assert_eq!(
        node("USE fin; SELECT temporal() FROM fin.orders", None, false),
        "temporal"
    );
    assert_eq!(node("USE fin; SELECT * FROM owed", None, false), "owed");
    assert_eq!(node("SELECT 1 AS one", None, false), "SQL");
    assert_eq!(node("USE fin", None, false), "USE");
    // the refused statement's place, not the sequence's end
    assert_eq!(
        node(
            "SELECT * FROM owed; SELECT * FROM workspace_next",
            Some(1),
            false
        ),
        "owed"
    );
    // a gloss localizes by its aspect, resolved against the record later
    assert_eq!(
        node(
            "USE fin; GLOSS formulas ON fin AS $${\"formulas\": {}}$$",
            None,
            false
        ),
        "GLOSS formulas"
    );
    assert_eq!(
        node(
            "USE fin; GLOSS churn ON fin AS $${\"sql\": \"SELECT 1\"}$$",
            None,
            false
        ),
        "gloss:churn"
    );
    // the parser refused the call: the text still localizes
    assert_eq!(node("SELEC * FROM owed", None, false), "owed");
    let located = window::locate(graph, "USE fin; SELEC * FROM owed", None, false);
    assert_eq!(located.dataset.as_deref(), Some("fin"));
    // a refused USE bound nothing: the act is the last that landed, the
    // dataset the last USE that did
    let located = window::locate(
        graph,
        "USE fin; SELECT * FROM owed; USE nothing",
        Some(3),
        true,
    );
    assert!(matches!(&located.act, window::Act::Node(n) if n == "owed"));
    assert_eq!(located.dataset.as_deref(), Some("fin"));
}

/// The rows of a page's table, as (function, kind).
fn table_rows(body: &str) -> BTreeSet<(String, String)> {
    body.lines()
        .filter(|l| l.starts_with("| `"))
        .map(|l| {
            let cells: Vec<&str> = l.split('|').map(str::trim).collect();
            (cells[1].trim_matches('`').to_string(), cells[2].to_string())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_function_listings_are_the_registries() {
    let (_dir, plane) = scratch_plane().await;
    let pages = functions::pages(&plane).await.unwrap();
    let page = |uri: &str| {
        pages
            .iter()
            .find(|p| p.uri == uri)
            .unwrap_or_else(|| panic!("no page {uri}"))
    };
    let session = plane.channel(human(), None).await.unwrap();
    let mut door: BTreeSet<(String, String)> = session
        .registered_functions()
        .into_iter()
        .map(|f| (f.name, f.kind.to_string()))
        .collect();
    door.extend(
        DOORS
            .iter()
            .map(|(n, _)| ((*n).to_string(), "door".to_string())),
    );
    door.extend(
        names(&session, "SELECT name FROM functions", "name")
            .await
            .into_iter()
            .map(|n| (n, "extract".to_string())),
    );
    let served = table_rows(&page("doc://functions/door.md").body);
    assert_eq!(served, door);
    // the three contexts differ where the registrations differ
    assert!(
        served.contains(&("json_get_str".into(), "scalar".into())),
        "{served:?}"
    );
    assert!(served.contains(&("temporal".into(), "extract".into())));
    assert!(served.contains(&("metric_axes".into(), "door".into())));
    let recipe = table_rows(&page("doc://functions/recipe.md").body);
    assert_eq!(
        recipe,
        glossql_session::reader_functions()
            .into_iter()
            .map(|f| (f.name, f.kind.to_string()))
            .collect()
    );
    assert!(recipe.contains(&("read_parquet".into(), "table".into())));
    assert!(!recipe.contains(&("json_get_str".into(), "scalar".into())));
    let detector = table_rows(&page("doc://functions/detector.md").body);
    assert_eq!(
        detector,
        glossql_session::detector_functions()
            .into_iter()
            .map(|f| (f.name, f.kind.to_string()))
            .collect()
    );
    assert!(!detector.contains(&("read_parquet".into(), "table".into())));
}

// ---- the door -----------------------------------------------------------

fn meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "window-test", "version": "0"}
    })
}

fn call(id: u64, statements: &str) -> Value {
    json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {"_meta": meta(), "name": "glossql", "arguments": {"statements": statements}}
    })
}

async fn mcp(app: Router, payload: Value) -> Response<Body> {
    let request = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, common::bearer("dev-agent"))
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "glossql");
    app.oneshot(request.body(Body::from(payload.to_string())).unwrap())
        .await
        .unwrap()
}

async fn body_of(response: Response<Body>) -> Value {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// The window block of a tool result, if one rides it.
fn window_of(body: &Value) -> Option<String> {
    body["result"]["content"]
        .as_array()?
        .iter()
        .filter_map(|b| b["text"].as_str())
        .find(|t| t.starts_with("[procedural graph]"))
        .map(str::to_string)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_window_rides_the_result_when_its_condition_holds() {
    let (_dir, plane) = scratch_plane().await;
    let app = router(
        Arc::clone(&plane),
        DoorConfig::default(),
        Access::Gated(common::login()),
    );
    let body = body_of(
        mcp(
            app.clone(),
            call(1, "DECLARE DATASET fin SET (purpose: 'window test');"),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // At `owed` on a fresh dataset: nothing is owed, so the keyed edge
    // to End holds and the keyed edge to the ruling entries (a fold-in
    // owed) does not; the keyless edges out of `owed` stay.
    let body = body_of(mcp(app.clone(), call(2, "USE fin; SELECT * FROM owed")).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let window = window_of(&body).expect("the window rides the result");
    assert!(
        window.starts_with("[procedural graph] you are at: owed\n"),
        "{window}"
    );
    assert!(window.contains("- End — when nothing owed"), "{window}");
    assert!(!window.contains("ruling_entries"), "{window}");
    assert!(window.lines().any(|l| l.starts_with("then: ")), "{window}");

    // A refused call localizes to the refused statement.
    let body = body_of(
        mcp(
            app.clone(),
            call(3, "USE fin; SELECT * FROM owed; USE nothing"),
        )
        .await,
    )
    .await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let window = window_of(&body).unwrap_or_else(|| panic!("no window on the refusal: {body}"));
    assert!(
        window.starts_with("[procedural graph] you are at: owed\n"),
        "{window}"
    );

    // Off is off: the control arm of the run that measures the window.
    let off = router(
        plane,
        DoorConfig {
            window: false,
            ..DoorConfig::default()
        },
        Access::Gated(common::login()),
    );
    let body = body_of(mcp(off, call(4, "USE fin; SELECT * FROM owed")).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    assert!(window_of(&body).is_none(), "{body}");
}
