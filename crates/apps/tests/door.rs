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
use glossql_glossary::{Actor, ActorKind, FunctionRow, Store};
use glossql_session::{FunctionRuntime, Plane};
use tower::ServiceExt;

/// Verdicts compute at read, so a frame that reads ATTEST invokes the
/// witness's detector live — this stub is that detector.
#[derive(Debug)]
struct StubDetector;

impl FunctionRuntime for StubDetector {
    fn invoke(
        &self,
        _: &FunctionRow,
        subject: &str,
        context: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "subject": subject,
            "aspect": context["aspect"],
            "witness": context["witness"],
            "band": "orange",
            "score": 0.42,
            "computed_at": "2026-08-17T00:00:00.000Z",
        }))
    }
}

async fn current_pin(store: &Store, dataset: &str) -> glossql_glossary::Pin {
    let lake = store.lake();
    let mut snaps = std::collections::HashMap::new();
    for t in lake.table_names(dataset).await.unwrap() {
        if let Some(s) = lake.snapshot_id(dataset, &t).await.unwrap() {
            snaps.insert(t, s);
        }
    }
    store.pin(dataset, &snaps).await.unwrap()
}

async fn workspace() -> (Router, Arc<Plane>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("ledger.csv"),
        "month,cohort,value\n2026-01-01,a,10.5\n2026-01-01,b,2.0\n2026-02-01,a,4.0\n",
    )
    .unwrap();

    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open_scratch(lake).await.unwrap();
    let plane = Arc::new(Plane::new(store, Arc::new(StubDetector)));
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
    let router = Router::new().nest("/app", glossql_apps::router(Arc::clone(&plane), workspace, "human".into()));
    (router, plane, dir)
}

/// The SHIPPED ruling aspect, cut from the KPI kit the binary
/// bootstraps — so the ruling tests exercise the schema a real
/// workspace enforces. The hand-rolled permissive copy this replaces
/// hid a real refusal: the shipped enum lacked the `unclear` stance
/// and the docket's button answered 422 (found live, 2026-08-18).
fn shipped_ruling_declaration() -> &'static str {
    let kit = glossql_scripts::library::KIT;
    let start = kit
        .find("DECLARE ASPECT ruling")
        .expect("the kit ships the ruling aspect");
    let len = kit[start..]
        .find("AS FACT;")
        .expect("the declaration closes")
        + "AS FACT;".len();
    &kit[start..start + len]
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

    // `docket` ships in the binary; a workspace directory of the same
    // name holding pages but no app.toml must not silently lose them
    // to the built-in (found 2026-08-12).
    let shadow = dir.path().join("apps/docket");
    std::fs::create_dir_all(&shadow).unwrap();
    std::fs::write(shadow.join("index.html"), "the author's page").unwrap();
    let page = get(&app, "/app/docket").await;
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

#[tokio::test(flavor = "multi_thread")]
async fn a_glossed_part_may_not_take_a_builtin_name() {
    let (app, plane, _dir) = workspace().await;

    // Add an app, don't fork the built-in (ruled 2026-08-15). The
    // directory branch has refused a half-shadow since 2026-08-12, but
    // glossed parts reach the same hazard by the route an MCP-only
    // agent actually takes — and they carry no manifest requirement, so
    // one frame under the built-in's name would resolve the whole app
    // to that single file and 404 every page the docket ships.
    let session = plane
        .session(Actor {
            kind: ActorKind::Agent,
            id: "author".into(),
        })
        .await
        .unwrap();
    session
        .execute(
            r#"USE perf;
               DECLARE ASPECT app_frame WITH $${"type": "object", "required": ["sql"],
                 "properties": {"sql": {"type": "string"}}}$$ AS FACT;
               GLOSS app_frame ON docket.mine AS $${"sql": "SELECT 1 AS v"}$$;"#,
        )
        .await
        .unwrap();

    let page = get(&app, "/app/docket").await;
    assert_eq!(page.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let page = text(page).await;
    assert!(page.contains("ships in the binary"), "{page}");

    // The same part under its own name serves — including without a
    // manifest, which binds to the workspace's sole dataset.
    session
        .execute(r#"GLOSS app_frame ON cash.mine AS $${"sql": "SELECT 1 AS v"}$$;"#)
        .await
        .unwrap();
    assert_eq!(
        get(&app, "/app/cash/frames/mine").await.status(),
        StatusCode::OK
    );
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

/// Every cell of the frame, flattened to one string — for asserting a
/// rendered value is present without caring which column carries it.
async fn body_text(response: Response<Body>) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
            .unwrap();
    let mut out = String::new();
    for batch in reader {
        let batch = batch.unwrap();
        for column in batch.columns() {
            for i in 0..batch.num_rows() {
                if let Ok(v) =
                    datafusion::arrow::util::display::array_value_to_string(column, i)
                {
                    out.push_str(&v);
                    out.push('\n');
                }
            }
        }
    }
    out
}

/// Seed the shapes the docket's frames read: a grounded metric with
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
                   {"dimension": "definition", "key": "per-line", "assumption": "per line", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "grain", "key": "grain-preserving", "assumption": "grain-preserving", "basis": "measured", "confidence": 1.0}
                 ]}$$;
               GLOSS formulas ON perf AS $${"formulas": {"dso": "ar[end of w] / revenue[w] * days[w]"}}$$;
               GLOSS definitions ON perf AS $${"definitions": {"dso": {
                 "meaning": "receivables outstanding expressed in days of revenue",
                 "unit": "days", "owner": "Finance", "source": "KPI handbook v3"}}}$$;
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
async fn frames_declare_their_class_and_data_frames_never_read_the_glossary() {
    // Metadata and data are not one pile (ruled 2026-08-18): every
    // frame response carries `glossql-frame-class`, derived by the
    // session's pre-pass from what the frame's expansion actually
    // resolves — never a curated list. `record` frames read the
    // glossary somewhere and can change under a ruling; `data` frames
    // provably cannot, so the browser's store keeps them across
    // rulings. The stale banner is record on purpose: staleness is a
    // fact about the record, read through workspace_next.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    for (frame, expected) in [
        ("open", "record"),
        ("settled", "record"),
        ("owed", "record"),
        ("assumptions", "record"),
        ("stale", "record"),
        ("metric", "record"),
        ("trend", "data"),
        ("slices", "data"),
        ("dims", "data"),
    ] {
        let response = get(
            &app,
            &format!("/app/docket/frames/{frame}?metric=dso&subject=ledger&dim=region"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "{frame}");
        assert_eq!(
            response
                .headers()
                .get("glossql-frame-class")
                .and_then(|v| v.to_str().ok()),
            Some(expected),
            "frame `{frame}`"
        );
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

    let metric = get(&app, "/app/docket/frames/metric?metric=dso").await;
    assert_eq!(metric.status(), StatusCode::OK);
    let served = body_text(metric).await;
    assert_eq!(served.lines().filter(|l| l.starts_with("dso")).count(), 1);
    // The formula the seed wrote, not the empty-state prose. Counting
    // rows alone is how the face came to render nothing in every real
    // workspace: the fixture wrote a formulas map no skill ever taught
    // an agent to write, so the join returned a row and the value was
    // null everywhere but here (found 2026-08-15).
    assert!(
        served.contains("ar[end of w] / revenue[w] * days[w]"),
        "the formula face served no formula:\n{served}"
    );
    // Unit and meaning come from the `definitions` registry, never the
    // aspect blob — a declaration cannot be superseded, and the ruling
    // (2026-08-12) was forced by exactly this field going stale there.
    // The seed's blob carries no unit, so a face reading `x-unit` shows
    // nothing and this fails.
    assert!(
        served.contains("receivables outstanding expressed in days of revenue"),
        "the metric face served no meaning:\n{served}"
    );
    assert!(
        served.contains("DSO · metric · days"),
        "the meta line took its unit from the registry:\n{served}"
    );
    let assumptions = get(&app, "/app/docket/frames/assumptions?metric=dso").await;
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
    let before = get(&app, "/app/docket/frames/owed").await;
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
        .execute(r#"GLOSS formulas ON perf AS $${"formulas": {"dso": "ar[end of w] / revenue[w] * 360"}}$$;"#)
        .await
        .unwrap();
    let after = get(&app, "/app/docket/frames/owed").await;
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
    let cleared = get(&app, "/app/docket/frames/owed").await;
    assert_eq!(row_count(cleared).await, 0, "re-recording clears the wait");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metric_faces_serve_the_winning_slot_once() {
    // Found live 2026-08-12: with two slots on `formulas` (human and
    // agent) the dossier rendered the formula and the materialization
    // twice. The metric face reads the winning slot only — human
    // outranking agent, exactly like the collapsed read. The
    // assumptions ledger is different since the 2026-08-14 ruling: it
    // is the AGENT's working record (rulings annotate it, and a human
    // supersede of the metric governs reads without replacing it).
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    let before = get(&app, "/app/docket/frames/metric?metric=dso").await;
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
            r#"GLOSS formulas ON perf AS $${"formulas": {"dso": "ar[end of w] / revenue[w] * 360"}}$$;
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [{"dimension": "definition", "key": "per-line",
                                  "assumption": "ruled", "basis": "engineer",
                                  "confidence": 1.0}]}$$;"#,
        )
        .await
        .unwrap();

    // Still one row on the metric face, and it is the human's; the
    // assumptions ledger keeps serving the agent's working record —
    // its two disclosed assumptions — never a blend of the two bodies.
    let metric = get(&app, "/app/docket/frames/metric?metric=dso").await;
    let served = body_text(metric).await;
    assert_eq!(served.lines().filter(|l| l.starts_with("dso")).count(), 1);
    assert!(
        served.contains("ar[end of w] / revenue[w] * 360"),
        "the human's formula must outrank the agent's:\n{served}"
    );
    let assumptions = get(&app, "/app/docket/frames/assumptions?metric=dso").await;
    assert_eq!(
        row_count(assumptions).await,
        2,
        "the ledger is the agent's record — the human supersede must not replace it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ruling_closes_its_question_and_annotates_the_ledger() {
    // Ruled 2026-08-14: a human ruling is its own record — the queue
    // holds the ruled question closed by its declared key, and the
    // assumptions ledger shows the judgment beside the assumption it
    // rules ("the rulings that lead to the agreed facts").
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;

    // The seed's loose `per line` assumption queues.
    let queue = get(&app, "/app/docket/frames/open").await;
    assert_eq!(row_count(queue).await, 1, "the loose assumption queues");

    // The human's ruling lands (the door writes it in production; a
    // session write is the same statement).
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
    human.execute(shipped_ruling_declaration()).await.unwrap();
    human
        .execute(
            r#"GLOSS ruling ON perf AS $${"rulings": [
                 {"aspect": "dso", "dimension": "definition", "key": "per-line",
                  "assumption": "per line", "stance": "confirmed"}]}$$;"#,
        )
        .await
        .unwrap();

    // The queue holds the question closed; the ledger shows the ruling
    // beside its assumption, still awaiting the fold-in.
    let queue = get(&app, "/app/docket/frames/open").await;
    assert_eq!(row_count(queue).await, 0, "the ruling closes the question");
    let assumptions = get(&app, "/app/docket/frames/assumptions?metric=dso").await;
    let text = body_text(assumptions).await;
    assert!(text.contains("ruled: confirmed"), "{text}");
    assert!(text.contains("awaiting the fold-in"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_docket_takes_a_ruling_and_refuses_a_stale_one() {
    // The door's one write (2026-08-15). Run 4 found that a human who
    // steps away has no way back into the record at all: the MCP round
    // can only ask while they are watching, and an agent may never
    // speak for them. The docket is already the page of open
    // questions, so answering there is the gesture it was drawn for.
    //
    // Only the claim's identity is posted. The prose the ruling records
    // is re-derived from `open_questions` — so a tab left open across a
    // fold-in is refused rather than believed, and a browser can never
    // put words in the workspace's mouth.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;
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
            shipped_ruling_declaration(),
        )
        .await
        .unwrap();

    let post = |form: &'static str| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::post("/app/docket/rule")
                    .header(
                        axum::http::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .body(Body::from(form))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    assert_eq!(row_count(get(&app, "/app/docket/frames/open").await).await, 1);

    // A correction with the human's own words. The answer is the write
    // event, not a navigation: 204 with the trigger header the store
    // and every component listen for.
    let response = post(
        "subject=perf&aspect=dso&key=per-line&stance=corrected&note=per+order,+not+per+line",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT, "{:?}", response);
    assert_eq!(
        response
            .headers()
            .get("HX-Trigger")
            .and_then(|v| v.to_str().ok()),
        Some("glossql:written")
    );

    // The question closes and the ruling stands in the human's words.
    assert_eq!(
        row_count(get(&app, "/app/docket/frames/open").await).await,
        0,
        "the ruling closes its question"
    );
    let settled = body_text(get(&app, "/app/docket/frames/settled").await).await;
    assert!(settled.contains("per order, not per line"), "{settled}");
    assert!(settled.contains("corrected"), "{settled}");

    // The same post again: the question no longer derives, so the door
    // refuses instead of writing a second ruling from a stale page —
    // and the refusal carries the trigger too, so the stale tab's
    // panels re-derive to the current state on their own.
    let response =
        post("subject=perf&aspect=dso&key=per-line&stance=confirmed").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response
            .headers()
            .get("HX-Trigger")
            .and_then(|v| v.to_str().ok()),
        Some("glossql:written")
    );

    // A correction has to say what is right — closing a question with
    // "wrong" and nothing else tells the agent nothing.
    let response = post("subject=perf&aspect=dso&key=per-line&stance=corrected").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unclear_ruling_closes_the_question_without_taking_a_side() {
    // The third stance (ruled 2026-08-18): the human refuses the
    // QUESTION, not the claim. A sloppily worded question could
    // previously only be deferred, which re-asks the same words
    // forever. `unclear` lands like any ruling — this key closes, what
    // confused the reader rides as the note — and what the agent owes
    // is a reformulation under a NEW key (whose clearer wording derives
    // its own question), never a fold-in.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;
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
            shipped_ruling_declaration(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::post("/app/docket/rule")
                .header(
                    axum::http::header::CONTENT_TYPE,
                    "application/x-www-form-urlencoded",
                )
                .body(Body::from(
                    "subject=perf&aspect=dso&key=per-line&stance=unclear&note=which+lines%3F",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT, "{response:?}");

    // The question closes — the refusal holds this key, and only a
    // re-record with a clearer assumption asks again.
    assert_eq!(row_count(get(&app, "/app/docket/frames/open").await).await, 0);
    let settled = body_text(get(&app, "/app/docket/frames/settled").await).await;
    assert!(settled.contains("unclear"), "{settled}");
    assert!(settled.contains("which lines?"), "{settled}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_checks_face_serves_verdicts_not_the_vocabulary() {
    // Rebuilt 2026-08-14: a standing check is an ATTEST row — a
    // detector's live verdict beside its witness's expectation. A
    // witness without a detector is a speaker gate; its spoken slots
    // never render as checks (the old frame showed them with the
    // band column reading `current`).
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;
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
               DECLARE ASPECT meaning WITH $${"type": "object"}$$ AS FACT ON COLUMN;
               DECLARE WITNESS meaning_w ON meaning BY (AGENT, HUMAN);
               GLOSS meaning ON ledger.value AS $${"value": "the money"}$$;
               DECLARE FUNCTION probe_check FOR GLOBAL AS $$#{}$$;
               DECLARE WITNESS dso_w ON dso DETECTOR probe_check THRESHOLD 0.9;"#,
        )
        .await
        .unwrap();
    // The verdict computes at read — the stub detector answers.

    let checks = get(&app, "/app/docket/frames/checks").await;
    let status = checks.status();
    if status != StatusCode::OK {
        let body = to_bytes(checks.into_body(), usize::MAX).await.unwrap();
        panic!("checks frame: {status} — {}", String::from_utf8_lossy(&body));
    }
    assert_eq!(
        row_count(checks).await,
        1,
        "one detector verdict, and the meaning voice is not a check"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_faces_serve_the_measured_cube() {
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
                {"dimension": "", "member": "", "period": "2026-01", "value": 12.5},
                {"dimension": "", "member": "", "period": "2026-02", "value": 4.0},
                {"dimension": "cohort", "member": "a", "period": "2026-01", "value": 10.5},
                {"dimension": "cohort", "member": "a", "period": "2026-02", "value": 4.0},
                {"dimension": "cohort", "member": "b", "period": "2026-01", "value": 2.0},
                {"dimension": "alternative", "member": "days on billings", "period": "2026-01", "value": 11.0},
                {"dimension": "alternative", "member": "days on billings", "period": "2026-02", "value": 5.0}
            ]
        }]
    });
    let pin = current_pin(plane.store(), "perf").await;
    plane
        .store()
        .measurement_put("perf", "metric_cube", "perf", "metric_cube", &pin, &cube.to_string())
        .await
        .unwrap();

    let pulse = get(&app, "/app/docket/frames/pulse").await;
    assert_eq!(pulse.status(), StatusCode::OK);
    assert_eq!(row_count(pulse).await, 1, "one declared surface, one row");

    let dims = get(&app, "/app/docket/frames/dims?metric=dso").await;
    assert_eq!(row_count(dims).await, 1, "cohort is the one admitted axis");

    let slices = get(&app, "/app/docket/frames/slices?metric=dso&dim=cohort").await;
    assert_eq!(row_count(slices).await, 3, "two members over two months, one sparse");

    let trend = get(&app, "/app/docket/frames/trend?metric=dso").await;
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

    let front = get(&app, "/app/docket/p/metrics").await;
    assert_eq!(front.status(), StatusCode::OK);
    let front = text(front).await;
    assert!(front.contains("frames/pulse"), "{front}");

    // the front counts live on the docket itself, not the metric list
    let open = text(get(&app, "/app/docket").await).await;
    assert!(open.contains("frames/front"), "{open}");
    assert!(open.contains("frames/open"), "{open}");
    assert!(open.contains("frames/settled"), "{open}");

    let dossier = text(get(&app, "/app/docket/p/metrics?metric=dso").await).await;
    assert!(dossier.contains("frames/trend"), "{dossier}");
    assert!(dossier.contains("frames/dims"), "{dossier}");
    assert!(
        !dossier.contains("frames/slices"),
        "no dim picked — the slice tile must not render"
    );

    let sliced = text(get(&app, "/app/docket/p/metrics?metric=dso&dim=cohort").await).await;
    assert!(sliced.contains("frames/slices"), "{sliced}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ruling_answers_with_the_write_event_never_a_navigation() {
    // Signed off 2026-08-18, retiring the 303-to-Referer machinery: a
    // 303's job is to send the reader somewhere else to see the result,
    // and the reader never leaves this page — the docket is
    // client-rendered, so the response is an event. Success is 204 with
    // `HX-Trigger: glossql:written`, never a Location; the Referer is
    // ignored entirely, so there is nothing a forged one can steer.
    let (app, plane, _dir) = workspace().await;
    seed_model_shapes(&plane).await;
    plane
        .channel(
            Actor {
                kind: ActorKind::Human,
                id: "human".into(),
            },
            Some("perf"),
        )
        .await
        .unwrap()
        .execute(
            shipped_ruling_declaration(),
        )
        .await
        .unwrap();
    // One open question per case below: a ruling closes the question it
    // answered, so re-posting the same key would be refused as stale
    // and never reach the redirect at all.
    plane
        .session(Actor {
            kind: ActorKind::Agent,
            id: "builder".into(),
        })
        .await
        .unwrap()
        .execute(
            r#"USE perf;
               GLOSS dso ON perf AS $${"sql": "SELECT month, value FROM ledger",
                 "assumptions": [
                   {"dimension": "definition", "key": "k0", "assumption": "a", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "definition", "key": "k1", "assumption": "b", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "definition", "key": "k2", "assumption": "c", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "definition", "key": "k3", "assumption": "d", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "definition", "key": "k4", "assumption": "e", "basis": "judgment", "confidence": 0.7},
                   {"dimension": "definition", "key": "k5", "assumption": "f", "basis": "judgment", "confidence": 0.7}
                 ]}$$;"#,
        )
        .await
        .unwrap();

    // Pages are never cached: they are live views of a mutable record.
    let page = get(&app, "/app/docket").await;
    assert_eq!(
        page.headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-store"),
        "a docket page must not be cacheable"
    );

    let post = |key: &'static str, referer: Option<&'static str>| {
        let app = app.clone();
        async move {
            let mut request = Request::post("/app/docket/rule").header(
                axum::http::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            );
            if let Some(referer) = referer {
                request = request.header(axum::http::header::REFERER, referer);
            }
            app.oneshot(
                request
                    .body(Body::from(format!(
                        "subject=perf&aspect=dso&key={key}&stance=confirmed"
                    )))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    // The same contract wherever the reader ruled from — a metric's
    // page, a forged Referer, no Referer at all: 204, the trigger, and
    // no Location for anything to follow.
    for (key, referer) in [
        ("k0", Some("http://127.0.0.1:8113/app/docket/p/metrics?metric=dso")),
        ("k1", Some("http://evil.example/steal")),
        ("k2", Some("//evil.example/steal")),
        ("k3", Some("/app/other/p/metrics")),
        ("k4", Some("not a url at all")),
        ("k5", None),
    ] {
        let response = post(key, referer).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "referer {referer:?}");
        assert_eq!(
            response
                .headers()
                .get("HX-Trigger")
                .and_then(|v| v.to_str().ok()),
            Some("glossql:written"),
            "referer {referer:?}"
        );
        assert!(
            response.headers().get(axum::http::header::LOCATION).is_none(),
            "referer {referer:?}"
        );
    }
}
