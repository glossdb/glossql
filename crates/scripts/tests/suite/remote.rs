//! The remote runtime against a stand-in kernel service: the wire —
//! nulls for NaN, the bearer on every call, a walk's points as one
//! `/bands` request and the replay grid as another, the answers'
//! shapes, a full queue waited out, a refusal reported by its text, an
//! absent service by its address — and, with no service named, the
//! three doors refusing at plan time by name, before any read.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use glossql_scripts::KernelRuntime;
use glossql_session::{BandRead, FunctionRuntime, Matrix};
use serde_json::{Value, json};

/// What the stand-in received, route by route.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<(String, Value)>>>);

fn bearer_ok(headers: &HeaderMap) -> bool {
    headers.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer k1")
}

/// `/bands` as the service answers it: per read, a row of quantiles per
/// test row — the read's index plus the alpha's — and, where an actual
/// came, a PIT per test row. The first call after `busy` is set is
/// refused as a full queue, with how long to wait.
async fn bands(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    if !bearer_ok(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            HeaderMap::new(),
            Json(json!({"error": "unauthorized: a bearer key this service issued"})),
        );
    }
    let alphas = body["alphas"].as_array().map_or(0, Vec::len);
    let reads: Vec<Value> = body["reads"]
        .as_array()
        .map(|reads| {
            reads
                .iter()
                .enumerate()
                .map(|(i, read)| {
                    let rows = read["test_x"].as_array().map_or(0, Vec::len);
                    let quantiles: Vec<Vec<f64>> = (0..rows)
                        .map(|_| (0..alphas).map(|a| i as f64 + 1.0 + a as f64).collect())
                        .collect();
                    match read.get("actual") {
                        Some(_) => json!({"quantiles": quantiles, "pit": vec![0.25; rows]}),
                        None => json!({"quantiles": quantiles}),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let mut calls = seen.0.lock().unwrap();
    if calls.iter().any(|(route, _)| route == "busy") {
        calls.retain(|(route, _)| route != "busy");
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "1".parse().unwrap());
        return (
            StatusCode::TOO_MANY_REQUESTS,
            headers,
            Json(json!({"error": "the kernel's queue is full — retry shortly"})),
        );
    }
    calls.push(("bands".into(), body));
    (
        StatusCode::OK,
        HeaderMap::new(),
        Json(json!({"reads": reads})),
    )
}

async fn misfit(State(seen): State<Seen>, Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    let rows = body["x"].as_array().map_or(0, Vec::len);
    seen.0.lock().unwrap().push(("misfit".into(), body));
    if rows < 3 {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "misfit: the stand-in wants three rows"})),
        );
    }
    (StatusCode::OK, Json(json!({"scores": [0.5, null, -1.0]})))
}

async fn stub() -> (String, Seen) {
    let seen = Seen::default();
    let app = Router::new()
        .route("/bands", post(bands))
        .route("/misfit", post(misfit))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serving") });
    (format!("http://{addr}"), seen)
}

const ALPHAS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];

#[tokio::test(flavor = "multi_thread")]
async fn nans_ride_as_null_and_the_bearer_rides_every_call() {
    let (url, seen) = stub().await;
    let rt = KernelRuntime::with_remote(&format!("{url}/"), Some("k1")).unwrap();
    assert!(rt.carries_model());
    assert_eq!(rt.kernel_url(), Some(url.as_str()));
    let read = BandRead {
        train_x: vec![1.0, 2.0, 3.0, f64::NAN, 5.0, 6.0],
        train_y: vec![1.0, 2.0, 3.0],
        test_x: vec![7.0, 8.0],
        actual: 2.5,
    };
    let history = vec![1.0; 100];
    let (q, pit) = rt
        .band_points(std::slice::from_ref(&read), &ALPHAS, Some(&history))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(q, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(pit, 0.25);
    let calls = seen.0.lock().unwrap();
    let (route, body) = &calls[0];
    assert_eq!(route, "bands");
    assert_eq!(body["members"], json!(1));
    assert_eq!(body["alphas"], json!(ALPHAS));
    assert_eq!(body["pit_history"], json!(history));
    let read = &body["reads"][0];
    assert_eq!(
        read["train_x"],
        json!([[1.0, 2.0], [3.0, null], [5.0, 6.0]])
    );
    assert_eq!(read["test_x"], json!([[7.0, 8.0]]));
    assert_eq!(read["actual"], json!([2.5]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_walks_points_ride_one_request_in_order_and_a_full_queue_is_waited_out() {
    let (url, seen) = stub().await;
    let rt = KernelRuntime::with_remote(&url, Some("k1")).unwrap();
    let reads: Vec<BandRead> = (0..3)
        .map(|i| BandRead {
            train_x: vec![1.0, 2.0, 3.0, 4.0],
            train_y: vec![1.0, 2.0],
            test_x: vec![i as f64, 0.0],
            actual: 1.0,
        })
        .collect();
    seen.0.lock().unwrap().push(("busy".into(), Value::Null));
    let answered = rt.band_points(&reads, &ALPHAS, None).await.unwrap();
    assert!(seen.0.lock().unwrap()[0].1.get("pit_history").is_none());
    assert_eq!(answered.len(), 3);
    for (i, (q, pit)) in answered.iter().enumerate() {
        // The stand-in's quantile is the read's index plus the alpha's.
        assert_eq!(q[0], i as f64 + 1.0, "read {i}");
        assert_eq!(q[4], i as f64 + 5.0, "read {i}");
        assert_eq!(*pit, 0.25);
    }
    let calls = seen.0.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "one request for the three points, after the wait"
    );
    assert_eq!(calls[0].1["reads"].as_array().unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_bearer_is_a_refusal_by_text() {
    let (url, _seen) = stub().await;
    let rt = KernelRuntime::with_remote(&url, Some("nope")).unwrap();
    let read = BandRead {
        train_x: vec![1.0, 2.0],
        train_y: vec![1.0, 2.0],
        test_x: vec![3.0],
        actual: 1.0,
    };
    let e = rt
        .band_points(std::slice::from_ref(&read), &ALPHAS, None)
        .await
        .unwrap_err();
    assert!(
        e.contains("refused (401") && e.contains("unauthorized"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn band_grid_comes_back_row_major_by_alphas() {
    let (url, seen) = stub().await;
    let rt = KernelRuntime::with_remote(&url, Some("k1")).unwrap();
    let train = [1.0, 2.0, 3.0, 4.0];
    let test = [5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let q = rt
        .band_grid(
            Matrix {
                data: &train,
                rows: 2,
                cols: 2,
            },
            &[1.0, 2.0],
            Matrix {
                data: &test,
                rows: 3,
                cols: 2,
            },
            &[0.1, 0.9],
        )
        .await
        .unwrap();
    // Row-major: three test rows of two alphas, the stand-in's 1 + alpha index.
    assert_eq!(q, vec![1.0, 2.0, 1.0, 2.0, 1.0, 2.0]);
    let calls = seen.0.lock().unwrap();
    assert_eq!(calls[0].1["members"], json!(8));
    assert!(calls[0].1["reads"][0].get("actual").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn misfit_nulls_come_back_nan_and_a_4xx_is_its_text() {
    let (url, _seen) = stub().await;
    let rt = KernelRuntime::with_remote(&url, Some("k1")).unwrap();
    let x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let scores = rt
        .misfit_scores(Matrix {
            data: &x,
            rows: 3,
            cols: 2,
        })
        .await
        .unwrap();
    assert_eq!(scores.len(), 3);
    assert_eq!(scores[0], 0.5);
    assert!(scores[1].is_nan());
    assert_eq!(scores[2], -1.0);
    let e = rt
        .misfit_scores(Matrix {
            data: &x[..4],
            rows: 2,
            cols: 2,
        })
        .await
        .unwrap_err();
    assert!(
        e.contains("refused (422") && e.contains("three rows"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_that_does_not_answer_is_reported_by_address() {
    let rt = KernelRuntime::with_remote("http://127.0.0.1:9", None).unwrap();
    let e = rt
        .misfit_scores(Matrix {
            data: &[1.0, 2.0, 3.0, 4.0],
            rows: 2,
            cols: 2,
        })
        .await
        .unwrap_err();
    assert!(
        e.contains("did not answer") && e.contains("127.0.0.1:9"),
        "{e}"
    );
    assert!(KernelRuntime::with_remote("kernel.local", None).is_err());
}

/// The walk reads its points through the record, point in time: the
/// months called go to the service one request each, and each carries
/// the PITs of every past walk's months before it — none on the first
/// walk, and on the second (after a re-record moved the pin) the first
/// walk's, growing along the months. The PITs the record keeps are the
/// raw ones the service returned.
#[tokio::test(flavor = "multi_thread")]
async fn the_walk_reads_through_the_records_earlier_months_only() {
    let (url, seen) = stub().await;
    let rt = Arc::new(KernelRuntime::with_remote(&url, Some("k1")).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let dates: Vec<i32> = super::bands::FIRSTS.iter().map(|f| 19723 + f).collect();
    let flow: Vec<f64> = (0..18).map(|i| 100.0 + 3.0 * i as f64).collect();
    let stock: Vec<f64> = (0..18).map(|i| 1000.0 + 5.0 * i as f64).collect();
    let session = super::bands::walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![("lines", dates.clone(), flow), ("levels", dates, stock)],
        &[
            r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#,
            r#"DECLARE ASPECT inventory WITH $${"title": "Inventory"}$$ AS QUERY ON DATASET;"#,
            r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#,
            r#"GLOSS inventory ON fin AS $${"sql": "SELECT date, value FROM levels", "behavior": "stock"}$$;"#,
        ],
    )
    .await;
    let histories = |calls: &[(String, Value)]| -> Vec<f64> {
        calls
            .iter()
            .filter(|(route, _)| route == "bands")
            .map(|(_, body)| {
                body["pit_history"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c.as_f64().unwrap())
                    .sum()
            })
            .collect()
    };
    // The first walk: six months, two metrics each, every request over an empty record.
    let first = histories(&seen.0.lock().unwrap());
    assert_eq!(first, vec![0.0; 6], "{first:?}");
    let walk = super::bands::walked(&session).await;
    for metric in walk["metrics"].as_array().unwrap() {
        for point in metric["points"].as_array().unwrap() {
            assert_eq!(
                point["pit"],
                json!(0.25),
                "the raw PIT the service answered"
            );
        }
    }

    // A re-record that changes a grounding owes the walk again, over the
    // same months; a re-record that keeps the series would not.
    seen.0.lock().unwrap().clear();
    session
        .execute(r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value * 1.0 AS value FROM lines"}$$;"#)
        .await
        .unwrap();
    session
        .execute("SELECT metric_bands() FROM fin;")
        .await
        .unwrap();
    let calls = seen.0.lock().unwrap();
    // Month k of six carries the PITs of the k-1 earlier months, two metrics each.
    assert_eq!(histories(&calls), vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);
    let last = &calls.iter().rev().find(|(r, _)| r == "bands").unwrap().1;
    assert_eq!(
        last["pit_history"][25],
        json!(10.0),
        "PITs of 0.25 count in the 25th hundredth"
    );
    assert_eq!(
        last["reads"].as_array().unwrap().len(),
        2,
        "both metrics' points for the month"
    );
}

/// No service named: every model door refuses at plan time, by its own
/// name, with the variable to set — before any aspect is looked up.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_service_the_doors_refuse_at_plan_time_by_name() {
    use glossql_catalog::Lake;
    use glossql_glossary::{Actor, ActorKind, Store};

    let rt = KernelRuntime::native();
    assert!(!rt.carries_model());
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    let session = glossql_session::Session::new(
        store,
        Actor {
            kind: ActorKind::Agent,
            id: "t".into(),
        },
    )
    .unwrap()
    .with_runtime(Arc::new(rt));
    session
        .execute("DECLARE DATASET fin SET (purpose: 'no model'); USE fin;")
        .await
        .unwrap();
    let declarations = glossql_scripts::library::splice(
        r#"DECLARE ASPECT metric_bands WITH $${
             "type": "object", "required": ["applicable"],
             "properties": {"applicable": {"type": "boolean"},
                            "metrics": {"type": "array"}}}$$ AS MEASUREMENT ON DATASET;
           DECLARE FUNCTION metric_bands FOR GLOBAL AS $$metric_bands.sql$$
             RETURNS metric_bands;"#,
    )
    .expect("shipped body splices");
    session.execute(&declarations).await.unwrap();

    for (statement, door) in [
        ("SELECT * FROM misfit.late_pairs();", "misfit.late_pairs()"),
        ("SELECT * FROM whatif.surge();", "whatif.surge()"),
        ("SELECT metric_bands() FROM fin;", "metric_bands()"),
    ] {
        let e = session.execute(statement).await.unwrap_err().to_string();
        assert!(
            e.contains(door) && e.contains("GLOSSQL_TABICL_URL") && e.contains("carries no model"),
            "{statement}: {e}"
        );
    }
}
