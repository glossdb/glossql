//! The app door against a real plane and a landed table, driven
//! in-process through the router (tower oneshot, mounted at `/app`
//! exactly as serverd mounts it): pages render from the app's own
//! directory, frames stream Arrow IPC with URL params bound as plan
//! placeholders, and everything that should refuse refuses.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, Response, StatusCode, header};
use datafusion::arrow::array::Float64Array;
use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{NoRuntime, Plane};
use tower::ServiceExt;

async fn workspace() -> (Router, Arc<Plane>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("ledger.csv"),
        "month,cohort,value\n2026-01-01,a,10.5\n2026-01-01,b,2.0\n2026-02-01,a,4.0\n",
    )
    .unwrap();

    let store = Store::open_memory().await.unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let plane = Arc::new(Plane::new(store, Some(lake), Arc::new(NoRuntime)));
    let session = plane
        .session(Actor {
            kind: ActorKind::Agent,
            id: "builder".into(),
        })
        .await
        .unwrap();
    session
        .execute(&format!(
            "DECLARE DATASET perf SET (purpose: 'app door test');\n\
             USE perf;\n\
             DECLARE SOURCE erp SET (type: csv, location: '{}');\n\
             DECLARE RECIPE ledger ON perf FROM erp AS $$\
               SELECT CAST(month AS DATE) AS month, cohort, \
                      CAST(value AS DOUBLE) AS value \
               FROM read_csv('ledger.csv')$$;",
            dir.path().display()
        ))
        .await
        .unwrap();

    let apps = dir.path().join("apps/perf");
    std::fs::create_dir_all(apps.join("frames")).unwrap();
    std::fs::create_dir_all(apps.join("specs")).unwrap();
    std::fs::write(
        apps.join("app.toml"),
        "title = \"Perf\"\ndataset = \"perf\"\n",
    )
    .unwrap();
    std::fs::write(
        apps.join("index.html"),
        "{% extends \"shell.html\" %}\n\
         {% import \"modules/tiles.html\" as tiles %}\n\
         {% block main %}\n\
         <div class=\"tiles\">\n\
         {{ tiles::chart(frame=\"frames/monthly\", spec=\"specs/monthly.vl.json\", title=\"Monthly\") }}\n\
         {{ tiles::table(frame=\"frames/monthly\") }}\n\
         </div>\n\
         {% endblock %}\n",
    )
    .unwrap();
    std::fs::write(
        apps.join("frames/monthly.sql"),
        "SELECT month, sum(value) AS value FROM ledger GROUP BY month ORDER BY month",
    )
    .unwrap();
    std::fs::write(
        apps.join("frames/by_cohort.sql"),
        "SELECT month, sum(value) AS value FROM ledger \
         WHERE cohort = $cohort GROUP BY month ORDER BY month",
    )
    .unwrap();
    std::fs::write(apps.join("frames/evil.sql"), "DROP TABLE ledger").unwrap();
    std::fs::write(
        apps.join("specs/monthly.vl.json"),
        "{\"mark\": \"bar\", \"encoding\": {}}",
    )
    .unwrap();

    let workspace = dir.path().to_path_buf();
    let router = Router::new().nest("/app", glossql_apps::router(Arc::clone(&plane), workspace));
    (router, plane, dir)
}

async fn get(app: &Router, uri: &str) -> Response<Body> {
    app.clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn text(response: Response<Body>) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn values(response: Response<Body>) -> Vec<f64> {
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/vnd.apache.arrow.stream"
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
            .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let index = batch.schema().index_of("value").unwrap();
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        out.extend(column.iter().map(|v| v.unwrap()));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn pages_render_and_frames_stream() {
    let (app, _plane, _dir) = workspace().await;

    // Home lists the app.
    let home = get(&app, "/app").await;
    assert_eq!(home.status(), StatusCode::OK);
    let home = text(home).await;
    assert!(home.contains("Perf"), "home should list the app:\n{home}");

    // The page renders through shell + macros, carrying the app root.
    let page = get(&app, "/app/perf").await;
    assert_eq!(page.status(), StatusCode::OK);
    let page = text(page).await;
    assert!(page.contains("data-approot=\"/app/perf/\""), "{page}");
    assert!(
        page.contains("<gl-chart frame=\"frames/monthly\""),
        "{page}"
    );
    assert!(
        page.contains("<gl-table frame=\"frames/monthly\""),
        "{page}"
    );

    // A frame streams IPC: two months, summed.
    let frame = get(&app, "/app/perf/frames/monthly").await;
    assert_eq!(frame.status(), StatusCode::OK);
    assert_eq!(values(frame).await, vec![12.5, 4.0]);

    // URL params bind as plan placeholders — cohort a only.
    let filtered = get(&app, "/app/perf/frames/by_cohort?cohort=a").await;
    assert_eq!(filtered.status(), StatusCode::OK);
    assert_eq!(values(filtered).await, vec![10.5, 4.0]);

    // An unbound placeholder is the read telling the author what the
    // URL owed it.
    let unbound = get(&app, "/app/perf/frames/by_cohort").await;
    assert_eq!(unbound.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let unbound = text(unbound).await;
    assert!(unbound.contains("cohort"), "{unbound}");

    // The spec serves as authored.
    let spec = get(&app, "/app/perf/specs/monthly.vl.json").await;
    assert_eq!(spec.status(), StatusCode::OK);
    assert!(text(spec).await.contains("\"mark\""));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_door_refuses_what_it_should() {
    let (app, _plane, _dir) = workspace().await;

    // Frames read; a write in a frame file is not one query.
    let evil = get(&app, "/app/perf/frames/evil").await;
    assert_eq!(evil.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // Unknown app, unknown frame, unknown page.
    assert_eq!(get(&app, "/app/nope").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get(&app, "/app/perf/frames/nope").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, "/app/perf/p/nope").await.status(),
        StatusCode::NOT_FOUND
    );

    // A frame name cannot walk out of the app directory.
    let escape = get(&app, "/app/perf/frames/..%2Fapp.toml").await;
    assert_eq!(escape.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_directory_without_a_manifest_refuses_loudly() {
    let (app, _plane, dir) = workspace().await;

    // `model` ships in the binary; a workspace directory of the same
    // name holding pages but no app.toml must not silently lose them
    // to the built-in (found 2026-08-12).
    let shadow = dir.path().join("apps/model");
    std::fs::create_dir_all(&shadow).unwrap();
    std::fs::write(shadow.join("index.html"), "the author's page").unwrap();
    let page = get(&app, "/app/model").await;
    assert_eq!(page.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let page = text(page).await;
    assert!(page.contains("app.toml"), "{page}");

    // A directory naming no built-in refuses the same way — the app
    // exists in the workspace, it just cannot serve.
    std::fs::create_dir_all(dir.path().join("apps/draft")).unwrap();
    let draft = get(&app, "/app/draft").await;
    assert_eq!(draft.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(text(draft).await.contains("app.toml"));
}

fn enc(s: &str) -> String {
    // Form-urlencode a value: everything outside the unreserved set as
    // %XX — enough for the JSON bodies the pin tests carry.
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

async fn post_form(app: &Router, uri: &str, pairs: &[(&str, &str)]) -> Response<Body> {
    let body: String = pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", enc(v)))
        .collect::<Vec<_>>()
        .join("&");
    app.clone()
        .oneshot(
            Request::post(uri)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pin_door_writes_the_human_slot() {
    // The pin loop (ruled 2026-08-12): approving a proposed
    // (subject, aspect, body) writes it as a HUMAN gloss with the
    // pinner's name as the actor — the answer outranks the agent slot
    // and needs no relay.
    let (app, plane, _dir) = workspace().await;
    let agent = plane
        .session(Actor {
            kind: ActorKind::Agent,
            id: "builder".into(),
        })
        .await
        .unwrap();
    agent
        .execute(
            r#"USE perf;
               DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;
               GLOSS note ON ledger AS $${"value": "the agent's guess"}$$;"#,
        )
        .await
        .unwrap();

    // An undeclared aspect refuses readable — the store speaking.
    let refused = post_form(
        &app,
        "/app/perf/pin",
        &[("subject", "ledger"), ("aspect", "nope"), ("body", "{}")],
    )
    .await;
    assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // The pin: a human approves a body, signed with their name.
    let pinned = post_form(
        &app,
        "/app/perf/pin",
        &[
            ("subject", "ledger"),
            ("aspect", "note"),
            ("body", "{\"value\": \"the engineer's answer\"}"),
            ("pinned_by", "philipp"),
        ],
    )
    .await;
    assert_eq!(pinned.status(), StatusCode::SEE_OTHER);
    assert_eq!(pinned.headers()[header::LOCATION], "/app/perf");

    // The HUMAN slot exists under the pinner's name and outranks the
    // agent slot in the collapsed read.
    let raw = agent
        .execute("SELECT actor, body FROM GLOSSARY(ledger, all => true) WHERE aspect = 'note';")
        .await
        .unwrap();
    let raw = format!("{raw:?}");
    assert!(raw.contains("philipp"), "{raw}");
    let collapsed = agent
        .execute("SELECT value FROM GLOSSARY(ledger) WHERE aspect = 'note';")
        .await
        .unwrap();
    let collapsed = format!("{collapsed:?}");
    assert!(collapsed.contains("the engineer's answer"), "{collapsed}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pin_door_refuses_what_could_smuggle() {
    let (app, _plane, _dir) = workspace().await;

    // A subject that is not a path of identifiers.
    let bad_subject = post_form(
        &app,
        "/app/perf/pin",
        &[("subject", "ledger; DROP TABLE ledger"), ("aspect", "note"), ("body", "{}")],
    )
    .await;
    assert_eq!(bad_subject.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // A body that is not JSON.
    let bad_body = post_form(
        &app,
        "/app/perf/pin",
        &[("subject", "ledger"), ("aspect", "note"), ("body", "not json")],
    )
    .await;
    assert_eq!(bad_body.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // A JSON body carrying the dollar-quote terminator: after it, text
    // would parse as further statements.
    let smuggle = post_form(
        &app,
        "/app/perf/pin",
        &[
            ("subject", "ledger"),
            ("aspect", "note"),
            ("body", "{\"value\": \"$$; DELETE FROM glossary; --\"}"),
        ],
    )
    .await;
    assert_eq!(smuggle.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let said = text(smuggle).await;
    assert!(said.contains("$$"), "{said}");
}

async fn row_count(response: Response<Body>) -> usize {
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/vnd.apache.arrow.stream"
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
            .unwrap();
    reader.map(|b| b.unwrap().num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pins_frame_serves_the_agenda_and_empties_on_answer() {
    // The whole loop: the agent glosses its agenda (pin_questions, one
    // entry per option), the built-in model app serves it as a queue,
    // a pin writes the HUMAN slot, and the question leaves the queue
    // by derivation — the answer exists, nothing was dismissed.
    let (app, plane, _dir) = workspace().await;
    let agent = plane
        .session(Actor {
            kind: ActorKind::Agent,
            id: "builder".into(),
        })
        .await
        .unwrap();
    agent
        .execute(
            r#"USE perf;
               DECLARE ASPECT definitions WITH $${"type": "object"}$$ AS FACT;
               DECLARE ASPECT pin_questions WITH $${"type": "object"}$$ AS FACT ON DATASET;
               GLOSS pin_questions ON perf AS $${"questions": [
                 {"subject": "perf", "aspect": "definitions",
                  "question": "value: which grain?", "option": "per line",
                  "body": {"definitions": {"value": {"meaning": "per line"}}},
                  "chosen": true, "grounds": "row counts", "confidence": 0.7},
                 {"subject": "perf", "aspect": "definitions",
                  "question": "value: which grain?", "option": "per document",
                  "body": {"definitions": {"value": {"meaning": "per document"}}},
                  "chosen": false, "confidence": 0.7}
               ]}$$;"#,
        )
        .await
        .unwrap();

    // The built-in model app binds the sole dataset; its pins frame
    // serves one row per open option.
    let frame = get(&app, "/app/model/frames/pins").await;
    assert_eq!(frame.status(), StatusCode::OK);
    assert_eq!(row_count(frame).await, 2);

    // Approve the chosen option.
    let pinned = post_form(
        &app,
        "/app/model/pin",
        &[
            ("subject", "perf"),
            ("aspect", "definitions"),
            ("body", "{\"definitions\": {\"value\": {\"meaning\": \"per line\"}}}"),
            ("pinned_by", "philipp"),
        ],
    )
    .await;
    assert_eq!(pinned.status(), StatusCode::SEE_OTHER);

    // Both options of the answered question are gone; the HUMAN slot
    // is the durable record.
    let after = get(&app, "/app/model/frames/pins").await;
    assert_eq!(after.status(), StatusCode::OK);
    assert_eq!(row_count(after).await, 0);
}
