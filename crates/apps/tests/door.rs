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
    post_form_with(app, uri, pairs, None).await
}

async fn post_form_with(
    app: &Router,
    uri: &str,
    pairs: &[(&str, &str)],
    cookie: Option<&str>,
) -> Response<Body> {
    let body: String = pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", enc(v)))
        .collect::<Vec<_>>()
        .join("&");
    let mut request = Request::post(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    app.clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sign_in_simulation_signs_pins() {
    // The sign-in simulation (ruled 2026-08-12): POST /app/session sets
    // a JWT cookie; the pin door prefers its verified subject over any
    // form-carried name; a forged token verifies to nothing and the
    // fallback applies.
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
               DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT;"#,
        )
        .await
        .unwrap();

    // Sign in; the cookie comes back Set-Cookie, HttpOnly, /app-scoped.
    let signed = post_form(&app, "/app/session", &[("name", "Philipp Suter")]).await;
    assert_eq!(signed.status(), StatusCode::SEE_OTHER);
    let cookie = signed.headers()[header::SET_COOKIE].to_str().unwrap().to_string();
    assert!(cookie.starts_with("gl_actor="), "{cookie}");
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    let pair = cookie.split(';').next().unwrap().to_string();

    // A bad name refuses readable.
    let bad = post_form(&app, "/app/session", &[("name", "x; DROP TABLE")]).await;
    assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // The cookie's subject outranks the form's pinned_by.
    let pinned = post_form_with(
        &app,
        "/app/perf/pin",
        &[
            ("subject", "ledger"),
            ("aspect", "note"),
            ("body", "{\"value\": \"signed\"}"),
            ("pinned_by", "impostor"),
        ],
        Some(&pair),
    )
    .await;
    assert_eq!(pinned.status(), StatusCode::SEE_OTHER);
    let raw = agent
        .execute("SELECT actor FROM GLOSSARY(ledger, all => true) WHERE aspect = 'note';")
        .await
        .unwrap();
    let raw = format!("{raw:?}");
    assert!(raw.contains("Philipp Suter"), "{raw}");
    assert!(!raw.contains("impostor"), "{raw}");

    // A forged token is no token: the form fallback names the actor.
    let forged = post_form_with(
        &app,
        "/app/perf/pin",
        &[
            ("subject", "ledger"),
            ("aspect", "note"),
            ("body", "{\"value\": \"forged\"}"),
            ("pinned_by", "fallback name"),
        ],
        Some("gl_actor=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJyb290In0.Zm9yZ2Vk"),
    )
    .await;
    assert_eq!(forged.status(), StatusCode::SEE_OTHER);
    let raw = agent
        .execute("SELECT actor FROM GLOSSARY(ledger, all => true) WHERE aspect = 'note';")
        .await
        .unwrap();
    let raw = format!("{raw:?}");
    assert!(raw.contains("fallback name"), "{raw}");
    assert!(!raw.contains("root"), "{raw}");

    // Sign-out clears the cookie.
    let out = post_form(&app, "/app/session/out", &[]).await;
    let cleared = out.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
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

    // The built-in model app binds the first dataset by name; its pins frame
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

    // The rounds (found on the first real run, 2026-08-12): several
    // questions on one aspect retire together on the first pin, so the
    // agent's next agenda — glossed after the pin, re-composed on top
    // of the human's map — must serve again. The frame's derivation is
    // timestamp-bounded, not existence-bounded.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    agent
        .execute(
            r#"GLOSS pin_questions ON perf AS $${"questions": [
                 {"subject": "perf", "aspect": "definitions",
                  "question": "value: net or gross?", "option": "net of credits",
                  "body": {"definitions": {"value": {"meaning": "per line, net of credits"}}},
                  "chosen": true, "grounds": "credit rows exist", "confidence": 0.7},
                 {"subject": "perf", "aspect": "definitions",
                  "question": "value: net or gross?", "option": "gross",
                  "body": {"definitions": {"value": {"meaning": "per line, gross"}}},
                  "chosen": false, "confidence": 0.7}
               ]}$$;"#,
        )
        .await
        .unwrap();
    let round_two = get(&app, "/app/model/frames/pins").await;
    assert_eq!(round_two.status(), StatusCode::OK);
    assert_eq!(row_count(round_two).await, 2);
}

/// Seed the shapes the model app's frames read: a grounded metric with
/// assumptions, a formulas map, definitions, and a pin agenda.
async fn seed_model_shapes(plane: &Arc<Plane>) {
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
               DECLARE ASPECT dso WITH $${"title": "DSO", "x-kind": "metric"}$$ AS QUERY ON DATASET;
               DECLARE ASPECT definitions WITH $${"type": "object"}$$ AS FACT ON DATASET;
               DECLARE ASPECT formulas WITH $${"type": "object"}$$ AS FACT ON DATASET;
               DECLARE ASPECT pin_questions WITH $${"type": "object"}$$ AS FACT ON DATASET;
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [
                   {"dimension": "definition", "assumption": "per line", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "grain", "assumption": "grain-preserving", "basis": "measured", "confidence": 1.0}
                 ]}$$;
               GLOSS formulas ON perf AS $${"formulas": {"dso": "ar / revenue * days"}}$$;
               GLOSS definitions ON perf AS $${"definitions": {"dso": {"meaning": "days outstanding"}}}$$;
               GLOSS pin_questions ON perf AS $${"questions": [
                 {"subject": "perf", "aspect": "definitions",
                  "question": "q?", "option": "a",
                  "body": {"definitions": {"dso": {"meaning": "days outstanding"}}},
                  "chosen": true, "confidence": 0.7}
               ]}$$;"#,
        )
        .await
        .unwrap();
}

fn assert_classic(dt: &datafusion::arrow::datatypes::DataType, frame: &str, field: &str) {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        // The browser's arrow reader speaks only classic types — a view
        // type in a frame schema renders as `Unrecognized type` in every
        // tile bound to it (glossql-apps: cast view types back).
        DataType::Utf8View | DataType::BinaryView => {
            panic!("frame {frame} field {field} carries view type {dt}")
        }
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            assert_classic(f.data_type(), frame, field)
        }
        DataType::Struct(fields) => {
            for f in fields {
                assert_classic(f.data_type(), frame, field)
            }
        }
        _ => {}
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_builtin_frame_executes_and_serves_classic_types() {
    // Parse-only coverage let two defects ship (found live 2026-08-12):
    // an optimizer error that only fires at plan time, and view-typed
    // columns the browser reader cannot decode. Every built-in frame
    // must execute end to end and stream classic types — against a
    // workspace carrying the shapes the frames read.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let model = glossql_apps::BUILTINS
        .iter()
        .find(|b| b.name == "model")
        .unwrap();
    for (path, _) in model.files {
        let Some(stem) = path.strip_prefix("frames/").and_then(|p| p.strip_suffix(".sql"))
        else {
            continue;
        };
        // Extra params are ignored by frames that bind neither.
        let uri = format!("/app/model/frames/{stem}?metric=dso&subject=ledger");
        let response = get(&app, &uri).await;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "frame {stem} refused: {}",
            String::from_utf8_lossy(&bytes)
        );
        let reader =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
                .unwrap();
        for field in reader.schema().fields() {
            assert_classic(field.data_type(), stem, field.name());
        }
        for batch in reader {
            batch.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_queue_hides_assumptions_an_open_question_covers() {
    // The one queue (ruled 2026-08-12): a loose assumption renders as
    // an investigate row only while no open agenda question targets
    // its (subject, aspect) — once the agent composes the answer, the
    // pinnable card is the row and the investigate twin disappears.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    // The seed's dso gloss carries one loose assumption (0.7) and its
    // agenda question targets definitions, not dso — the queue shows it.
    let before = get(&app, "/app/model/frames/queue").await;
    assert_eq!(before.status(), StatusCode::OK);
    assert_eq!(row_count(before).await, 1);

    // The agent composes the answer: an agenda question on (perf, dso).
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
               GLOSS pin_questions ON perf AS $${"questions": [
                 {"subject": "perf", "aspect": "dso",
                  "question": "grain: per line?", "option": "per line at 1.0",
                  "body": {"sql": "SELECT month, value FROM ledger",
                           "assumptions": [{"dimension": "definition", "assumption": "per line",
                                            "basis": "engineer", "confidence": 1.0}]},
                  "chosen": true, "confidence": 0.7}
               ]}$$;"#,
        )
        .await
        .unwrap();
    let after = get(&app, "/app/model/frames/queue").await;
    assert_eq!(after.status(), StatusCode::OK);
    assert_eq!(row_count(after).await, 0, "the covered assumption must leave");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metric_faces_serve_the_winning_slot_once() {
    // Found live 2026-08-12: after a pin, `formulas` holds two slots
    // (human and agent) and the dossier rendered the formula and the
    // materialization twice. The faces read the winning slot only —
    // human outranking agent, exactly like the collapsed read.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let before = get(&app, "/app/model/frames/metric?metric=dso").await;
    assert_eq!(before.status(), StatusCode::OK);
    assert_eq!(row_count(before).await, 1);

    // A human pins the formula, then supersedes the metric gloss itself.
    for (aspect, body) in [
        ("formulas", "{\"formulas\": {\"dso\": \"ar / revenue * 360\"}}"),
        (
            "dso",
            "{\"sql\": \"SELECT month, value FROM ledger\", \"assumptions\": [{\"dimension\": \"definition\", \"assumption\": \"pinned\", \"basis\": \"engineer\", \"confidence\": 1.0}]}",
        ),
    ] {
        let pinned = post_form(
            &app,
            "/app/model/pin",
            &[
                ("subject", "perf"),
                ("aspect", aspect),
                ("body", body),
                ("pinned_by", "philipp"),
            ],
        )
        .await;
        assert_eq!(pinned.status(), StatusCode::SEE_OTHER, "pin on {aspect}");
    }

    // Still one row per face, and it is the human's.
    let metric = get(&app, "/app/model/frames/metric?metric=dso").await;
    assert_eq!(row_count(metric).await, 1);
    let assumptions = get(&app, "/app/model/frames/assumptions?metric=dso").await;
    assert_eq!(
        row_count(assumptions).await,
        1,
        "the human body carries one assumption; the agent's two must not show"
    );
}
