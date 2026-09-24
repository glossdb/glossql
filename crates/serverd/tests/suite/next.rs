//! `next` and the two lines on every result: the function listings are
//! the registries; the routes answer from the record, filtered by
//! surface or whole; and the door carries the `situation:` and `next:`
//! lines and serves `next://` as a resource template.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode, header};
use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use glossql_glossary::{Actor, ActorKind};
use glossql_serverd::{Access, BOOTSTRAP, DoorConfig, Plane, bootstrap, functions, router, window};
use glossql_session::{DOORS, NoRuntime, Outcome, Session};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::common;

/// A fresh workspace with the shipped system landed.
async fn scratch_plane() -> (tempfile::TempDir, Arc<Plane>) {
    let (dir, store) = common::scratch_store().await;
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
        id: "next-test".into(),
    }
}

/// The rows of a read, as JSON objects.
async fn rows(session: &Session, sql: &str) -> Vec<Value> {
    let outcomes = session
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    let mut out = Vec::new();
    for outcome in outcomes {
        if let Outcome::Rows { batches, .. } = outcome
            && !batches.is_empty()
        {
            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            let refs: Vec<&RecordBatch> = batches.iter().collect();
            writer.write_batches(&refs).unwrap();
            writer.finish().unwrap();
            let value: Value = serde_json::from_slice(&writer.into_inner()).unwrap();
            out.extend(value.as_array().cloned().unwrap_or_default());
        }
    }
    out
}

/// One column of a read, as a set of strings.
async fn names(session: &Session, sql: &str, column: &str) -> BTreeSet<String> {
    rows(session, sql)
        .await
        .iter()
        .filter_map(|r| r.get(column)?.as_str().map(str::to_string))
        .collect()
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
    assert!(!detector.contains(&("read_parquet".into(), "table".into())));
}

// ---- the routes ----------------------------------------------------------

fn races() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("race_date", DataType::Date32, false),
        Field::new("track", DataType::Utf8, false),
        Field::new("takings", DataType::Float64, false),
    ]));
    // days since the epoch for the 15th of each month from 2024-01
    let days_from_civil = |y: i64, m: i64, d: i64| -> i32 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let mp = (m + 9) % 12;
        let doy = (153 * mp + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        (era * 146097 + doe - 719468) as i32
    };
    let days: Vec<i32> = (0..18)
        .map(|i| days_from_civil(2024 + i / 12, i % 12 + 1, 15))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(StringArray::from(
                (0..18)
                    .map(|i| if i % 2 == 0 { "north" } else { "south" })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..18).map(|i| 100.0 + 3.7 * i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    (schema, batch)
}

fn by_surface(answers: &[Value]) -> std::collections::BTreeMap<String, Value> {
    answers
        .iter()
        .map(|r| (r["surface"].as_str().unwrap().to_string(), r.clone()))
        .collect()
}

/// One goal's row, by name.
async fn goal(session: &Session, surface: &str) -> Value {
    let sql = format!("SELECT * FROM next WHERE surface = '{surface}'");
    let mut answers = rows(session, &sql).await;
    assert_eq!(answers.len(), 1, "`{sql}`: {answers:?}");
    answers.pop().unwrap()
}

const SURFACES: [&str; 7] = [
    "structure",
    "metrics",
    "slices",
    "bands",
    "checks",
    "app",
    "rulings",
];

#[tokio::test(flavor = "multi_thread")]
async fn the_routes_answer_from_the_record() {
    let (_dir, plane) = scratch_plane().await;
    plane
        .execute(
            human(),
            None,
            "DECLARE DATASET fin SET (purpose: 'next test')",
        )
        .await
        .unwrap();
    let session = plane.channel(human(), Some("fin")).await.unwrap();

    // A fresh dataset: nothing landed, nothing grounded. One row per
    // goal, in the goals' order.
    let all = rows(&session, "SELECT * FROM next ORDER BY goal").await;
    let order: Vec<&str> = all.iter().map(|r| r["surface"].as_str().unwrap()).collect();
    assert_eq!(order, SURFACES, "{all:?}");
    let fresh = by_surface(&all);
    assert_eq!(fresh["structure"]["state"], "next", "{fresh:?}");
    assert_eq!(fresh["structure"]["act"], "DECLARE SOURCE");
    assert_eq!(fresh["structure"]["step"], 1);
    assert_eq!(fresh["slices"]["state"], "blocked");
    assert_eq!(fresh["bands"]["state"], "blocked");
    assert_eq!(fresh["app"]["state"], "blocked");
    assert_eq!(fresh["metrics"]["state"], "next", "{fresh:?}");
    assert_eq!(fresh["metrics"]["act"], "DECLARE ASPECT query");
    assert_eq!(fresh["metrics"]["say"], "declare the first concept");
    // the names are the human's: no statement to hand
    assert_eq!(fresh["metrics"]["statement"], "", "{fresh:?}");
    assert_eq!(fresh["rulings"]["state"], "done");
    assert!(
        fresh["checks"]["statement"]
            .as_str()
            .unwrap()
            .contains("DECLARE WITNESS"),
        "{fresh:?}"
    );
    // A goal named is that goal's row, the same row; a name nobody
    // routes is no row.
    for surface in SURFACES {
        assert_eq!(goal(&session, surface).await, fresh[surface], "{surface}");
    }
    assert!(
        rows(&session, "SELECT * FROM next WHERE surface = 'nothing'")
            .await
            .is_empty()
    );

    // A declared metric the dataset claims (its definitions entry
    // stands) and nobody grounded: its grounding is the next act.
    let (schema, batch) = races();
    session
        .register_table(
            "races",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();
    // Declared and claimed by nobody: the claim is the act, with the
    // name filled — the declare counts on the declare.
    session
        .execute("DECLARE ASPECT takings WITH $${\"title\": \"Takings\"}$$ AS QUERY ON DATASET")
        .await
        .unwrap();
    let unclaimed = goal(&session, "metrics").await;
    assert_eq!(unclaimed["act"], "GLOSS definitions", "{unclaimed:?}");
    assert_eq!(unclaimed["say"], "claim takings for fin");
    assert!(
        unclaimed["statement"]
            .as_str()
            .unwrap()
            .starts_with("GLOSS definitions ON fin AS $${\"definitions\": {\"takings\":"),
        "{unclaimed:?}"
    );
    session
        .execute("GLOSS definitions ON fin AS $${\"definitions\": {\"takings\": {\"unit\": \"GBP\", \"meaning\": \"gate takings per race\"}}}$$")
        .await
        .unwrap();
    let declared = goal(&session, "metrics").await;
    assert_eq!(declared["act"], "GLOSS query");
    assert_eq!(declared["say"], "ground takings, or stop it");
    let statement = declared["statement"].as_str().unwrap();
    assert!(
        statement.starts_with("GLOSS takings ON fin AS $${\n  \"sql\":"),
        "{statement}"
    );
    assert!(
        statement.contains(
            "-- or, where no number should be served: GLOSS takings ON fin AS $${\"stopped\":"
        ),
        "{statement}"
    );

    // Grounded: the narrow frame asks for a dimension column, the
    // walk is owed, the app is admissible — once the date is judged.
    session
        .execute(
            "GLOSS takings ON fin AS $${\"sql\": \"SELECT race_date, takings AS value FROM races\"}$$",
        )
        .await
        .unwrap();
    let judged = session
        .execute("SELECT temporal() FROM fin.races.race_date")
        .await;
    let answers = by_surface(&rows(&session, "SELECT * FROM next ORDER BY goal").await);
    match judged {
        Ok(_) => {
            // Nobody judged a column of races: the detector is the act,
            // one statement per unserved column, filled.
            let slices = &answers["slices"];
            assert_eq!(slices["state"], "next", "{answers:?}");
            assert_eq!(slices["act"], "dimension_relevance", "{answers:?}");
            assert_eq!(
                slices["say"], "judge the unserved columns of races, none is judged",
                "{answers:?}"
            );
            assert_eq!(
                slices["statement"], "SELECT dimension_relevance() FROM fin.races.track",
                "{answers:?}"
            );
            // A role names track a dimension and no verdict stands on
            // it: the detector's form is filled with the column.
            session
                .execute("GLOSS role ON fin.races.track AS $${\"value\": \"dimension\"}$$")
                .await
                .unwrap();
            let roled = goal(&session, "slices").await;
            assert_eq!(roled["say"], "judge track of races", "{roled:?}");
            assert_eq!(
                roled["statement"], "SELECT dimension_relevance() FROM fin.races.track",
                "{roled:?}"
            );
            // A gloss admits track: the wider frame names it, and
            // nothing a verdict or a gloss did not admit.
            session
                .execute("GLOSS dimension ON fin.races.track AS $${\"value\": \"supporting\"}$$")
                .await
                .unwrap();
            let slices = goal(&session, "slices").await;
            assert_eq!(
                slices["say"], "re-record takings serving one of races.track",
                "{slices:?}"
            );
            let statement = slices["statement"].as_str().unwrap();
            assert!(
                statement.contains("GLOSS takings ON fin AS $$"),
                "{statement}"
            );
            assert!(
                statement.contains("SELECT race_date, takings AS value FROM races"),
                "{statement}"
            );
            // The empty list on a flow does not hold — takings add up
            // to their total by any column — so the goal still asks
            // for the axis; on a distinct count it does, and the
            // author's word closes the goal for that metric.
            session
                .execute(
                    "GLOSS takings ON fin AS $${\"sql\": \"SELECT race_date, takings AS value FROM races\", \"axes\": []}$$",
                )
                .await
                .unwrap();
            let held = goal(&session, "slices").await;
            assert_eq!(held["state"], "next", "{held:?}");
            assert_eq!(
                held["say"], "re-record takings serving one of races.track",
                "{held:?}"
            );
            session
                .execute(
                    "GLOSS takings ON fin AS $${\"sql\": \"SELECT race_date, CAST(count(DISTINCT track) AS DOUBLE) AS value FROM races GROUP BY race_date\", \"axes\": []}$$",
                )
                .await
                .unwrap();
            let closed = goal(&session, "slices").await;
            assert_eq!(closed["state"], "done", "{closed:?}");
            assert_eq!(answers["bands"]["act"], "metric_bands", "{answers:?}");
            assert_eq!(answers["bands"]["say"], "run the walk", "{answers:?}");
            assert_eq!(answers["app"]["state"], "next", "{answers:?}");
            assert_eq!(
                answers["app"]["say"], "write the first page over 1 metric",
                "{answers:?}"
            );
            let app = answers["app"]["statement"].as_str().unwrap();
            assert!(
                app.contains("GLOSS app ON review AS $${\"title\": \"fin review\"}$$;"),
                "{app}"
            );
            assert!(app.contains("metric IN ('takings')"), "{app}");
            assert!(app.contains("\\\"$schema\\\":"), "{app}");
            assert!(app.contains("GLOSS app_page ON review.index AS"), "{app}");
            assert_eq!(answers["app"]["then"], "-- serves at /fin/app/review");
            // The manifest alone is not an app: the door serves
            // index.html, so the page is the act until it stands.
            session
                .execute("GLOSS app ON review AS $${\"title\": \"fin review\"}$$")
                .await
                .unwrap();
            let paged = goal(&session, "app").await;
            assert_eq!(paged["state"], "next", "{paged:?}");
            assert_eq!(paged["act"], "GLOSS app_page", "{paged:?}");
            assert_eq!(paged["say"], "write the page of review", "{paged:?}");
            assert!(
                paged["statement"]
                    .as_str()
                    .unwrap()
                    .starts_with("GLOSS app_page ON review.index AS $${\"html\":"),
                "{paged:?}"
            );
            session
                .execute(
                    "GLOSS app_page ON review.index AS $${\"html\": \"{% extends \\\"shell.html\\\" %}{% block main %}<p>fin</p>{% endblock %}\"}$$",
                )
                .await
                .unwrap();
            let stands = goal(&session, "app").await;
            assert_eq!(stands["state"], "done", "{stands:?}");
            assert_eq!(
                stands["why"], "an app stands: /fin/app/review",
                "{stands:?}"
            );
            // An app named like its dataset: the part's subject is the
            // app's, not a dataset path, so the page lands on the app
            // and the goal closes on it.
            session
                .execute("GLOSS app ON fin AS $${\"title\": \"fin\"}$$")
                .await
                .unwrap();
            session
                .execute("GLOSS app_page ON fin.index AS $${\"html\": \"<p>fin</p>\"}$$")
                .await
                .unwrap();
            let parts = rows(
                &session,
                "SELECT path FROM app_parts WHERE dataset = 'fin' AND app = 'fin' ORDER BY path",
            )
            .await;
            let paths: Vec<&str> = parts.iter().map(|r| r["path"].as_str().unwrap()).collect();
            assert_eq!(paths, ["app", "index.html"], "{parts:?}");
            let stands = goal(&session, "app").await;
            assert_eq!(stands["state"], "done", "{stands:?}");
            assert_eq!(stands["why"], "an app stands: /fin/app/fin", "{stands:?}");
        }
        Err(e) => {
            eprintln!(
                "temporal() did not run under NoRuntime ({e}); the judged path is not covered here"
            );
            assert_eq!(answers["slices"]["state"], "blocked", "{answers:?}");
        }
    }
    // A measure the cube refuses holds the metrics goal with the
    // cube's reason and the standing body to re-record; a relation
    // serves no number by design and a stop is the author's word, so
    // neither does.
    session
        .execute("DECLARE ASPECT gate WITH $${\"title\": \"Gate\"}$$ AS QUERY ON DATASET")
        .await
        .unwrap();
    session
        .execute("GLOSS gate ON fin AS $${\"sql\": \"SELECT race_date, takings FROM races\"}$$")
        .await
        .unwrap();
    session
        .execute("DECLARE ASPECT entries WITH $${\"title\": \"Entries\", \"x-kind\": \"relation\"}$$ AS QUERY ON DATASET")
        .await
        .unwrap();
    session
        .execute("GLOSS entries ON fin AS $${\"sql\": \"SELECT race_date, track FROM races\"}$$")
        .await
        .unwrap();
    let refused = goal(&session, "metrics").await;
    assert_eq!(refused["state"], "next", "{refused:?}");
    assert_eq!(refused["act"], "GLOSS query", "{refused:?}");
    // The say carries the refusal's head — the line is what the agent
    // reads; the why on the page carries the whole reason.
    assert_eq!(
        refused["say"], "re-record gate — no value column",
        "{refused:?}"
    );
    assert!(
        refused["why"].as_str().unwrap().contains("no value column"),
        "{refused:?}"
    );
    assert!(
        refused["statement"]
            .as_str()
            .unwrap()
            .contains("SELECT race_date, takings FROM races"),
        "{refused:?}"
    );
    session
        .execute("GLOSS gate ON fin AS $${\"stopped\": \"races carry no gate count\"}$$")
        .await
        .unwrap();
    let stopped = goal(&session, "metrics").await;
    assert_eq!(stopped["state"], "done", "{stopped:?}");
    // The done row names what the record left unused, as facts: a
    // table no grounding reads, a judged column none serves.
    let (schema, batch) = races();
    session
        .register_table(
            "venues",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .await
        .unwrap();
    session
        .execute("GLOSS dimension ON fin.venues.track AS $${\"value\": \"supporting\"}$$")
        .await
        .unwrap();
    let done = goal(&session, "metrics").await;
    let why = done["why"].as_str().unwrap();
    assert!(
        why.starts_with("every metric the dataset claims is served or stopped")
            && why.ends_with(" — unread: venues; judged and unserved: venues.track"),
        "{done:?}"
    );
}

// ---- the door -------------------------------------------------------------

fn meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "next-test", "version": "0"}
    })
}

fn call(id: u64, statements: &str) -> Value {
    json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {"_meta": meta(), "name": "glossql", "arguments": {"statements": statements}}
    })
}

async fn mcp(app: Router, payload: Value) -> Response<Body> {
    let method = payload["method"].as_str().unwrap().to_string();
    let mut request = Request::post("/mcp")
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, common::bearer("dev-agent"))
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
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

async fn body_of(response: Response<Body>) -> Value {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(status, StatusCode::OK, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// The situation block of a tool result, if one rides it.
fn situation_of(body: &Value) -> Option<String> {
    body["result"]["content"]
        .as_array()?
        .iter()
        .filter_map(|b| b["text"].as_str())
        .find(|t| t.starts_with("situation:"))
        .map(str::to_string)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_door_says_where_the_call_left_the_agent_and_what_is_next() {
    let (_dir, plane) = scratch_plane().await;
    let app = router(
        Arc::clone(&plane),
        DoorConfig::default(),
        Access::Gated(common::login()),
    );
    // An unbound call: the situation and no next, nothing is bound.
    let body = body_of(
        mcp(
            app.clone(),
            call(1, "DECLARE DATASET fin SET (purpose: 'next test');"),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let block = situation_of(&body).expect("the situation rides every result");
    assert_eq!(block, "situation: landed", "{block}");

    let body = body_of(mcp(app.clone(), call(2, "USE fin; SELECT * FROM owed")).await).await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let block = situation_of(&body).expect("the situation rides every result");
    let mut lines = block.lines();
    assert_eq!(lines.next(), Some("situation: landed"), "{block}");
    let next = lines.next().expect("the next line rides a bound call");
    assert!(next.starts_with("next: structure → "), "{block}");
    assert!(
        next.contains("metrics → declare the first concept (next://fin/metrics)"),
        "{block}"
    );
    assert!(
        next.contains("slices → blocked: no applicable metric stands"),
        "{block}"
    );
    assert!(next.ends_with("rulings: done"), "{block}");

    // A refused call: the refusal's first line, and the next line on
    // the dataset the call bound.
    let body = body_of(
        mcp(
            app.clone(),
            call(3, "USE fin; SELECT * FROM owed; SELECT * FROM nothing"),
        )
        .await,
    )
    .await;
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    let block =
        situation_of(&body).unwrap_or_else(|| panic!("no situation on the refusal: {body}"));
    assert!(
        block.starts_with("situation: refused — statement 3 of 3 refused"),
        "{block}"
    );
    assert!(block.contains("next://fin/checks"), "{block}");

    // The resource template, and one page through it.
    let body = body_of(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 4, "method": "resources/templates/list",
                   "params": {"_meta": meta()}}),
        )
        .await,
    )
    .await;
    let templates = body["result"]["resourceTemplates"]
        .as_array()
        .unwrap_or_else(|| panic!("{body}"));
    assert!(
        templates
            .iter()
            .any(|t| t["uriTemplate"] == "next://{dataset}/{surface}"),
        "{body}"
    );
    let body = body_of(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 5, "method": "resources/read",
                   "params": {"_meta": meta(), "uri": "next://fin/metrics"}}),
        )
        .await,
    )
    .await;
    let text = body["result"]["contents"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{body}"));
    assert!(text.starts_with("# next on fin"), "{text}");
    assert!(text.contains("## metrics: next"), "{text}");
    assert!(text.contains("declare the first concept"), "{text}");
    assert!(!text.contains("```glossql\n\n```"), "no empty form: {text}");
    let body = body_of(
        mcp(
            app.clone(),
            json!({"jsonrpc": "2.0", "id": 6, "method": "resources/read",
                   "params": {"_meta": meta(), "uri": "next://fin"}}),
        )
        .await,
    )
    .await;
    let text = body["result"]["contents"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{body}"));
    assert!(text.contains("## app: blocked"), "{text}");

    // A grounding's write carries its fact row on the line — the last
    // statement's, behind the `USE` — even where the row abstains.
    let body = body_of(
        mcp(
            app.clone(),
            call(
                20,
                "DECLARE ASPECT takings WITH $${\"title\": \"Takings\"}$$ AS QUERY ON DATASET; \
                 USE fin; GLOSS takings ON fin AS $${\"sql\": \"SELECT CAST('2024-01-15' AS DATE) AS race_date, 1.0 AS value\"}$$",
            ),
        )
        .await,
    )
    .await;
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    let block = situation_of(&body).expect("the situation rides every result");
    assert!(
        block.starts_with("situation: landed — takings: not applicable — no judged time column"),
        "{block}"
    );
    // The line of a re-record's row: what the author's word closed and
    // over what, and the drift against the other writing.
    let line = window::situation(
        None,
        Some(&json!({
            "metric": "takings", "applicable": true, "behavior": "flow",
            "dims": [], "axes_basis": "authored",
            "unadmitted": ["track", "venue"],
            "unadmitted_act": ["closed over verdict", "closed"],
            "wanted": [],
            "superseded_divergence": "no gap against the writing it supersedes over 12 shared periods"
        })),
    );
    // The drift comes first: a moved total unsays the axes after it.
    assert_eq!(
        line,
        "situation: landed — takings: no gap against the writing it supersedes over 12 shared \
         periods; applicable; axes [] (the grounding's word — closes track, venue; a verdict \
         admits track); unadmitted [track, venue]; wanted []"
    );
    let line = window::situation(
        None,
        Some(&json!({
            "metric": "takings", "applicable": true, "behavior": "flow",
            "dims": ["track"], "axes_basis": "measured over authored",
            "unadmitted": [], "unadmitted_act": [], "wanted": []
        })),
    );
    assert_eq!(
        line,
        "situation: landed — takings: applicable; axes [track] (measured over the authored \
         empty list — a flow keeps its verdicts; the empty list closes a distinct count or a \
         ratio); unadmitted []; wanted []"
    );
    // A call the parser refuses ran nothing: the parser's word is the
    // result, and the line says refused with no next.
    let body = body_of(mcp(app.clone(), call(7, "USE fin; SELEC * FROM owed")).await).await;
    let block = situation_of(&body).unwrap_or_else(|| panic!("{body}"));
    assert!(block.starts_with("situation: refused — "), "{block}");
    assert!(!block.contains("next:"), "{block}");
}
