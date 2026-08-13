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

/// Seed the shapes the model app's frames read: a grounded metric with
/// assumptions, a formulas map, and definitions.
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
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [
                   {"dimension": "definition", "assumption": "per line", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "grain", "assumption": "grain-preserving", "basis": "measured", "confidence": 1.0}
                 ]}$$;
               GLOSS formulas ON perf AS $${"formulas": {"dso": "ar / revenue * days"}}$$;
               GLOSS definitions ON perf AS $${"definitions": {"dso": {"meaning": "days outstanding"}}}$$;
               DECLARE DATASET second SET (purpose: 'multi-dataset guard');"#,
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

    for builtin in glossql_apps::BUILTINS {
        for (path, _) in builtin.files {
            let Some(stem) = path.strip_prefix("frames/").and_then(|p| p.strip_suffix(".sql"))
            else {
                continue;
            };
            // Extra params are ignored by frames that bind none of them.
            let uri = format!(
                "/app/{}/frames/{stem}?metric=dso&subject=ledger&dim=region",
                builtin.name
            );
            let response = get(&app, &uri).await;
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                status,
                StatusCode::OK,
                "frame {}/{stem} refused: {}",
                builtin.name,
                String::from_utf8_lossy(&bytes)
            );
            let reader = arrow_ipc::reader::StreamReader::try_new(
                std::io::Cursor::new(bytes.to_vec()),
                None,
            )
            .unwrap();
            for field in reader.schema().fields() {
                assert_classic(field.data_type(), stem, field.name());
            }
            for batch in reader {
                batch.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_dossier_faces_survive_a_second_dataset() {
    // Found live by the lead (2026-08-12): with two datasets in the
    // workspace, frames that scanned the `datasets` relation fanned
    // every joined row out — two formulas, two materializations. The
    // bound dataset rides in as $dataset instead; one row per face.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await; // seeds a second dataset

    let metric = get(&app, "/app/metrics/frames/metric?metric=dso").await;
    assert_eq!(metric.status(), StatusCode::OK);
    assert_eq!(row_count(metric).await, 1);
    let assumptions = get(&app, "/app/metrics/frames/assumptions?metric=dso").await;
    assert_eq!(assumptions.status(), StatusCode::OK);
    assert_eq!(row_count(assumptions).await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_brief_counts_what_waits_on_the_agent() {
    // The waiting derivation (ruled 2026-08-12): a human formula
    // answer newer than the metric's recorded materialization owes the
    // agent a re-alignment — the two forms of one definition. The
    // brief shows it until the agent re-records; nothing is a
    // maintained flag. The answer arrives through a session (the app
    // carries no write since 2026-08-13).
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    // Seeded state: agent formulas + agent dso gloss — nothing waits.
    let before = get(&app, "/app/model/frames/brief").await;
    assert_eq!(before.status(), StatusCode::OK);
    assert_eq!(row_count(before).await, 0);

    // The human's answer lands on their channel: newer than the
    // recorded dso gloss.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let human = plane
        .channel(
            Actor {
                kind: ActorKind::Human,
                id: "human".into(),
            },
            Some("perf"),
        )
        .await
        .unwrap();
    human
        .execute(r#"GLOSS formulas ON perf AS $${"formulas": {"dso": "ar / revenue * 360"}}$$;"#)
        .await
        .unwrap();
    let after = get(&app, "/app/model/frames/brief").await;
    assert_eq!(
        row_count(after).await,
        1,
        "the formula answer waits on the agent"
    );

    // The agent re-records the materialization — the wait clears.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [{"dimension": "definition", "assumption": "360-day year",
                                  "basis": "engineer-ruled formula", "confidence": 1.0}]}$$;"#,
        )
        .await
        .unwrap();
    let cleared = get(&app, "/app/model/frames/brief").await;
    assert_eq!(row_count(cleared).await, 0, "re-recording clears the wait");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metric_faces_serve_the_winning_slot_once() {
    // Found live 2026-08-12: with two slots on `formulas` (human and
    // agent) the dossier rendered the formula and the materialization
    // twice. The faces read the winning slot only — human outranking
    // agent, exactly like the collapsed read.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let before = get(&app, "/app/metrics/frames/metric?metric=dso").await;
    assert_eq!(before.status(), StatusCode::OK);
    assert_eq!(row_count(before).await, 1);

    // The human answers the formula, then supersedes the metric gloss
    // itself — both through their session channel.
    let human = plane
        .channel(
            Actor {
                kind: ActorKind::Human,
                id: "human".into(),
            },
            Some("perf"),
        )
        .await
        .unwrap();
    human
        .execute(
            r#"GLOSS formulas ON perf AS $${"formulas": {"dso": "ar / revenue * 360"}}$$;
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [{"dimension": "definition", "assumption": "ruled",
                                  "basis": "engineer", "confidence": 1.0}]}$$;"#,
        )
        .await
        .unwrap();

    // Still one row per face, and it is the human's.
    let metric = get(&app, "/app/metrics/frames/metric?metric=dso").await;
    assert_eq!(row_count(metric).await, 1);
    let assumptions = get(&app, "/app/metrics/frames/assumptions?metric=dso").await;
    assert_eq!(
        row_count(assumptions).await,
        1,
        "the human body carries one assumption; the agent's two must not show"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_faces_serve_the_cached_cube() {
    // The business surface end to end from the cube cache: the pulse
    // carries the latest month and the admitted axes, the picker lists
    // them, a picked slice serves its members, and the trend carries
    // the disclosed rival beside the chosen reading. The script that
    // builds this body is the scripts suite's business — here it is
    // planted as the measurement would cache it.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let cube = serde_json::json!({
        "applicable": true,
        "caps": {"dims": 2, "members": 24, "months": 24},
        "metrics": [{
            "metric": "dso", "applicable": true, "behavior": "flow",
            "dims": ["cohort"], "alternative": "days on billings",
            "rows": [
                ["", "", "2026-01", 12.5], ["", "", "2026-02", 4.0],
                ["cohort", "a", "2026-01", 10.5], ["cohort", "a", "2026-02", 4.0],
                ["cohort", "b", "2026-01", 2.0],
                ["alternative", "days on billings", "2026-01", 11.0],
                ["alternative", "days on billings", "2026-02", 5.0]
            ]
        }]
    });
    plane
        .store()
        .cache_put("perf", "perf", "metric_cube", None, &cube.to_string(), None)
        .await
        .unwrap();

    let pulse = get(&app, "/app/metrics/frames/pulse").await;
    assert_eq!(pulse.status(), StatusCode::OK);
    assert_eq!(row_count(pulse).await, 1, "one declared surface, one row");

    let dims = get(&app, "/app/metrics/frames/dims?metric=dso").await;
    assert_eq!(row_count(dims).await, 1, "cohort is the one admitted axis");

    let slices = get(&app, "/app/metrics/frames/slices?metric=dso&dim=cohort").await;
    assert_eq!(row_count(slices).await, 3, "two members over two months, one sparse");

    let trend = get(&app, "/app/metrics/frames/trend?metric=dso").await;
    assert_eq!(
        row_count(trend).await,
        4,
        "the chosen reading and the rival, two months each"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_pages_render_both_states() {
    // The pulse and the dossier, as pages: the front renders the list
    // and the front counts; a metric URL renders the dossier with the
    // slice picker, and a picked dim adds the slice chart tile.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let front = get(&app, "/app/metrics").await;
    assert_eq!(front.status(), StatusCode::OK);
    let front = text(front).await;
    assert!(front.contains("frames/pulse"), "{front}");
    assert!(front.contains("frames/front"), "{front}");

    let dossier = text(get(&app, "/app/metrics?metric=dso").await).await;
    assert!(dossier.contains("frames/trend"), "{dossier}");
    assert!(dossier.contains("frames/dims"), "{dossier}");
    assert!(
        !dossier.contains("frames/slices"),
        "no dim picked — the slice tile must not render"
    );

    let sliced = text(get(&app, "/app/metrics?metric=dso&dim=cohort").await).await;
    assert!(sliced.contains("frames/slices"), "{sliced}");
}
