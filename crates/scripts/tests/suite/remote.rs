//! The remote runtime against a stand-in service: the wire — one Arrow
//! IPC stream each way, the specification and the answer in the
//! schema's metadata, nulls kept, the bearer on every call — the
//! answers' shapes, a full queue waited out, a refusal reported with
//! its text and the fields it names, an absent service by its address;
//! the walk sending every metric's series as one table and reading the
//! bands, the actual and the PIT back; and, with no service named, the
//! points refusing at plan time by name, before any read.

use std::sync::{Arc, Mutex};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use glossql_scripts::{Bearer, KernelRuntime};
use glossql_session::{FORECAST, FunctionRuntime, MISFIT, WHATIF};
use serde_json::{Value, json};

const ARROW: &str = "application/vnd.apache.arrow.stream";

/// What the stand-in received, route by route: the table as JSON rows
/// and the specification.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<(String, Value)>>>);

fn key(k: &str) -> Bearer {
    Bearer::Key(k.into())
}

/// The token the stand-in metadata server mints: a JWT in shape, its
/// payload saying who and until when (a fixed day in 2100, so the token
/// is the same string every time), unsigned — the service verifies; the
/// client only reads the expiry.
fn minted_token() -> String {
    use base64::Engine;
    let exp = 4_102_444_800u64;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        json!({"aud": "https://thesi.example", "email": "glossql@proj.iam.gserviceaccount.com", "exp": exp})
            .to_string(),
    );
    format!("eyJhbGciOiJSUzI1NiJ9.{payload}.sig")
}

fn bearer_ok(headers: &HeaderMap) -> bool {
    let bearer = headers.get("authorization").and_then(|v| v.to_str().ok());
    bearer == Some("Bearer k1") || bearer == Some(&format!("Bearer {}", minted_token()))
}

/// The metadata server's identity endpoint as GCP serves it: a token
/// for the audience asked, only to a caller saying `Metadata-Flavor: Google`.
async fn identity(
    State(seen): State<Seen>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, String) {
    if headers.get("metadata-flavor").and_then(|v| v.to_str().ok()) != Some("Google") {
        return (
            StatusCode::FORBIDDEN,
            "Missing Metadata-Flavor:Google header.".into(),
        );
    }
    seen.0
        .lock()
        .unwrap()
        .push(("identity".into(), json!(query.get("audience"))));
    (StatusCode::OK, minted_token())
}

/// The request as the service reads it: the table and the `spec` in
/// its schema's metadata.
fn parse(headers: &HeaderMap, body: &Bytes) -> Result<(RecordBatch, Value), String> {
    let media = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if media != ARROW {
        return Err(format!("the content type is {ARROW}, not {media:?}"));
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(body.as_ref()), None)
        .map_err(|e| format!("the body is not an Arrow IPC stream: {e}"))?;
    let schema = reader.schema();
    let spec: Value = schema
        .metadata()
        .get("spec")
        .ok_or("the schema's metadata holds no `spec`")
        .and_then(|s| serde_json::from_str(s).map_err(|_| "`spec` is not JSON"))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let bare = Arc::new(schema.as_ref().clone().with_metadata(Default::default()));
    let table =
        datafusion::arrow::compute::concat_batches(&bare, &batches).map_err(|e| e.to_string())?;
    Ok((table, spec))
}

/// A table as JSON rows, nulls as null — what the stand-in records.
fn rows_of(table: &RecordBatch) -> Value {
    let mut rows = Vec::new();
    for i in 0..table.num_rows() {
        let mut row = serde_json::Map::new();
        for (field, col) in table.schema().fields().iter().zip(table.columns()) {
            let v = if col.is_null(i) {
                Value::Null
            } else if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
                json!(f.value(i))
            } else {
                json!(array_value_to_string(col, i).unwrap())
            };
            row.insert(field.name().clone(), v);
        }
        rows.push(Value::Object(row));
    }
    Value::Array(rows)
}

/// The answer as the service writes it: the table, the answer object
/// in the schema's metadata.
fn answer(table: RecordBatch, answer: Value) -> Response {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("answer".to_string(), answer.to_string());
    let schema = table.schema().as_ref().clone().with_metadata(metadata);
    let table = table.with_schema(Arc::new(schema.clone())).unwrap();
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &schema).unwrap();
    writer.write(&table).unwrap();
    writer.finish().unwrap();
    ([(axum::http::header::CONTENT_TYPE, ARROW)], out).into_response()
}

fn refuse(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

/// The queue and stopping flags a test raises before a call: the first
/// call after is refused with how long to wait.
fn flagged(seen: &Seen) -> Option<Response> {
    let mut calls = seen.0.lock().unwrap();
    if calls.iter().any(|(route, _)| route == "draining") {
        calls.retain(|(route, _)| route != "draining");
        return Some(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("retry-after", "1")],
                Json(json!({"error": "this instance is stopping — retry"})),
            )
                .into_response(),
        );
    }
    if calls.iter().any(|(route, _)| route == "busy") {
        calls.retain(|(route, _)| route != "busy");
        return Some(
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "1")],
                Json(json!({"error": "the queue is full — retry shortly"})),
            )
                .into_response(),
        );
    }
    None
}

/// `/forecast` as the stand-in answers the walk: the last `origins`
/// periods of every member are forecast, one row per level — the value
/// the level's index plus one — with the member's own value as the
/// actual and a PIT of 0.25. Without `origins` it answers the periods
/// ahead — the rows whose `status` is not `actual` — carrying the
/// member's last actual forward: the median is that value, the bands
/// at ±8% and ±10% of it, a total the sum of its steps' medians. A
/// specification without `levels` is the refusal the service gives,
/// naming the field.
async fn forecast(State(seen): State<Seen>, headers: HeaderMap, body: Bytes) -> Response {
    if !bearer_ok(&headers) {
        return refuse(
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized: a bearer this service accepts"}),
        );
    }
    if let Some(response) = flagged(&seen) {
        return response;
    }
    let (table, spec) = match parse(&headers, &body) {
        Ok(parsed) => parsed,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, json!({"error": e})),
    };
    seen.0.lock().unwrap().push((
        "forecast".into(),
        json!({"rows": rows_of(&table), "spec": spec}),
    ));
    let Some(levels) = spec["levels"].as_array() else {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "the specification does not fit",
                   "errors": [{"field": "levels", "error": "a non-empty list of quantile levels"}]}),
        );
    };
    let origins = spec["origins"].as_u64().unwrap_or(0) as usize;
    let member = table.column_by_name("member").unwrap();
    let period = table.column_by_name("period").unwrap();
    let value = table
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Members' rows are contiguous and in period order.
    let mut members: Vec<(String, Vec<usize>)> = Vec::new();
    for i in 0..table.num_rows() {
        let m = array_value_to_string(member, i).unwrap();
        match members.last_mut() {
            Some((name, rows)) if *name == m => rows.push(i),
            _ => members.push((m, vec![i])),
        }
    }
    if origins == 0 {
        return ahead(&table, &spec, levels, &members);
    }
    let (mut m_col, mut p_col, mut l_col, mut v_col, mut a_col, mut pit_col) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for (name, rows) in &members {
        let n = rows.len();
        for j in (1..=origins).rev() {
            let Some(o) = (n - 1).checked_sub(j) else {
                continue;
            };
            let at = rows[o + 1];
            for (k, level) in levels.iter().enumerate() {
                m_col.push(name.clone());
                p_col.push(array_value_to_string(period, at).unwrap());
                l_col.push(level.as_f64().unwrap());
                v_col.push(k as f64 + 1.0);
                a_col.push((!value.is_null(at)).then(|| value.value(at)));
                pit_col.push(Some(0.25));
            }
        }
    }
    let n = m_col.len();
    let table = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("member", DataType::Utf8, false),
            Field::new("origin", DataType::Utf8, true),
            Field::new("step", DataType::Int64, false),
            Field::new("period", DataType::Utf8, true),
            Field::new("span", DataType::Int64, true),
            Field::new("column", DataType::Utf8, false),
            Field::new("level", DataType::Float64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("actual", DataType::Float64, true),
            Field::new("pit", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from(m_col)),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(StringArray::from(p_col)),
            Arc::new(Int64Array::from(vec![None::<i64>; n])),
            Arc::new(StringArray::from(vec!["value"; n])),
            Arc::new(Float64Array::from(l_col)),
            Arc::new(Float64Array::from(v_col)),
            Arc::new(Float64Array::from(a_col)),
            Arc::new(Float64Array::from(pit_col)),
        ],
    )
    .unwrap();
    answer(
        table,
        json!({"service": "stand-in", "version": "test", "errors": []}),
    )
}

/// The forecast without origins: per member, the actual rows are the
/// history and the rows of another status the periods ahead, one step
/// each; the answer carries the last actual forward, and says its 80%
/// band held 0.75 at every step and 0.72 for every total.
fn ahead(
    table: &RecordBatch,
    spec: &Value,
    levels: &[Value],
    members: &[(String, Vec<usize>)],
) -> Response {
    let period = table.column_by_name("period").unwrap();
    let value = table
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let status = table
        .column_by_name("status")
        .map(|c| c.as_any().downcast_ref::<StringArray>().unwrap().clone());
    let totals: Vec<i64> = spec["totals"]
        .as_array()
        .map(|t| t.iter().map(|s| s.as_i64().unwrap()).collect())
        .unwrap_or_default();
    let (mut m_col, mut o_col, mut s_col, mut p_col, mut span_col, mut l_col, mut v_col) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for (name, rows) in members {
        let is_actual = |i: usize| status.as_ref().is_none_or(|s| s.value(i) == "actual");
        let Some(&last) = rows.iter().rfind(|&&i| is_actual(i)) else {
            continue;
        };
        let steps: Vec<usize> = rows.iter().copied().filter(|&i| !is_actual(i)).collect();
        let origin = array_value_to_string(period, last).unwrap();
        let median = value.value(last);
        let mut push = |step: i64, span: Option<i64>, center: f64| {
            for level in levels {
                let l = level.as_f64().unwrap();
                m_col.push(name.clone());
                o_col.push(Some(origin.clone()));
                s_col.push(step);
                p_col.push(array_value_to_string(period, steps[(step - 1) as usize]).unwrap());
                span_col.push(span);
                l_col.push(l);
                v_col.push(center * (1.0 + (l - 0.5) * 0.2));
            }
        };
        for step in 1..=steps.len() as i64 {
            push(step, None, median);
        }
        for span in &totals {
            push(*span, Some(*span), median * *span as f64);
        }
    }
    let horizon = spec["horizon"].as_i64().unwrap_or(1);
    let mut coverage: Vec<Value> = (1..=horizon)
        .map(|step| json!({"step": step, "span": null, "low": 0.1, "high": 0.9, "share": 0.75, "points": 600}))
        .collect();
    coverage.extend(totals.iter().map(|span| {
        json!({"step": span, "span": span, "low": 0.1, "high": 0.9, "share": 0.72, "points": 470})
    }));
    let n = m_col.len();
    let table = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("member", DataType::Utf8, false),
            Field::new("origin", DataType::Utf8, true),
            Field::new("step", DataType::Int64, false),
            Field::new("period", DataType::Utf8, true),
            Field::new("span", DataType::Int64, true),
            Field::new("column", DataType::Utf8, false),
            Field::new("level", DataType::Float64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("actual", DataType::Float64, true),
            Field::new("pit", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from(m_col)),
            Arc::new(StringArray::from(o_col)),
            Arc::new(Int64Array::from(s_col)),
            Arc::new(StringArray::from(p_col)),
            Arc::new(Int64Array::from(span_col)),
            Arc::new(StringArray::from(vec!["value"; n])),
            Arc::new(Float64Array::from(l_col)),
            Arc::new(Float64Array::from(v_col)),
            Arc::new(Float64Array::from(vec![None::<f64>; n])),
            Arc::new(Float64Array::from(vec![None::<f64>; n])),
        ],
    )
    .unwrap();
    answer(
        table,
        json!({"service": "stand-in", "version": "test", "errors": [], "coverage": coverage}),
    )
}

/// `/misfit` as the stand-in answers it: a frame of three rows, scored
/// 0.5, null and -1.0; fewer rows are the service's refusal.
async fn misfit(State(seen): State<Seen>, headers: HeaderMap, body: Bytes) -> Response {
    let (table, spec) = match parse(&headers, &body) {
        Ok(parsed) => parsed,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, json!({"error": e})),
    };
    seen.0.lock().unwrap().push((
        "misfit".into(),
        json!({"rows": rows_of(&table), "spec": spec}),
    ));
    if table.num_rows() < 3 {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "misfit: the stand-in wants three rows"}),
        );
    }
    let table = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("row", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0i64, 1, 2])),
            Arc::new(Float64Array::from(vec![Some(0.5), None, Some(-1.0)])),
        ],
    )
    .unwrap();
    answer(
        table,
        json!({"service": "stand-in", "version": "test", "errors": [],
               "assumptions": [{"field": "score", "value": 1.0, "basis": "assumed: a floor of one nat"}]}),
    )
}

/// `/whatif` as the stand-in answers it: per member, the scenario rows
/// at the three levels asked, the median the last actual times the
/// scenario input over the planned one, the band at half and one and a
/// half of it; the sign `+` with its basis on every row.
async fn whatif(State(seen): State<Seen>, headers: HeaderMap, body: Bytes) -> Response {
    let (table, spec) = match parse(&headers, &body) {
        Ok(parsed) => parsed,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, json!({"error": e})),
    };
    seen.0.lock().unwrap().push((
        "whatif".into(),
        json!({"rows": rows_of(&table), "spec": spec}),
    ));
    let Some(levels) = spec["levels"].as_array() else {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "the specification does not fit",
                   "errors": [{"field": "levels", "error": "a non-empty list of quantile levels"}]}),
        );
    };
    let names: Vec<String> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let input = names
        .iter()
        .find(|n| !["member", "period", "status", "value"].contains(&n.as_str()))
        .expect("one input")
        .clone();
    let member = table.column_by_name("member").unwrap();
    let period = table.column_by_name("period").unwrap();
    let status = table
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value = table
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let moved = table
        .column_by_name(&input)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let (mut m_col, mut p_col, mut l_col, mut v_col) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut last: Option<(String, f64, Option<f64>)> = None;
    for i in 0..table.num_rows() {
        let m = array_value_to_string(member, i).unwrap();
        match status.value(i) {
            "actual" => last = Some((m, value.value(i), None)),
            "plan" => {
                if let Some(l) = last.as_mut() {
                    l.2 = (!moved.is_null(i)).then(|| moved.value(i));
                }
            }
            _ => {
                let (_, actual, planned) = last.clone().unwrap();
                let median = actual * moved.value(i) / planned.unwrap();
                for level in levels {
                    let l = level.as_f64().unwrap();
                    m_col.push(m.clone());
                    p_col.push(array_value_to_string(period, i).unwrap());
                    l_col.push(l);
                    v_col.push(median * (0.5 + l));
                }
            }
        }
    }
    let n = m_col.len();
    let table = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("member", DataType::Utf8, false),
            Field::new("period", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("level", DataType::Float64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("sign", DataType::Utf8, true),
            Field::new("basis", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(m_col)),
            Arc::new(StringArray::from(p_col)),
            Arc::new(StringArray::from(vec!["value"; n])),
            Arc::new(StringArray::from(vec!["scenario"; n])),
            Arc::new(Float64Array::from(l_col)),
            Arc::new(Float64Array::from(v_col)),
            Arc::new(StringArray::from(vec!["+"; n])),
            Arc::new(StringArray::from(vec![
                "the history shows the input moving the column";
                n
            ])),
        ],
    )
    .unwrap();
    answer(
        table,
        json!({"service": "stand-in", "version": "test", "errors": [], "changed": input}),
    )
}

async fn stub() -> (String, Seen) {
    let seen = Seen::default();
    let app = Router::new()
        .route("/forecast", post(forecast))
        .route("/misfit", post(misfit))
        .route("/whatif", post(whatif))
        .route(
            "/computeMetadata/v1/instance/service-accounts/default/identity",
            axum::routing::get(identity),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serving") });
    (format!("http://{addr}"), seen)
}

const LEVELS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];

/// A runtime with every point at the stand-in.
fn runtime(url: &str, bearer: Bearer) -> KernelRuntime {
    KernelRuntime::native()
        .with_point(FORECAST, url, bearer.clone())
        .unwrap()
        .with_point(MISFIT, url, bearer.clone())
        .unwrap()
        .with_point(WHATIF, url, bearer)
        .unwrap()
}

/// A member's monthly series as the walk sends it.
fn series(rows: &[(&str, &str, Option<f64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("member", DataType::Utf8, false),
            Field::new("period", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(m, ..)| *m).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, p, _)| *p).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|(.., v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_table_and_the_specification_ride_one_stream_and_the_bearer_rides_every_call() {
    let (url, seen) = stub().await;
    let rt = runtime(&format!("{url}/"), key("k1"));
    assert!(rt.serves(FORECAST) && rt.serves(MISFIT));
    assert_eq!(rt.url(FORECAST), Some(url.as_str()));
    let spec = json!({"targets": ["value"], "origins": 1, "horizon": 1, "levels": LEVELS});
    let table = series(&[
        ("m", "2026-01", Some(1.0)),
        ("m", "2026-02", None),
        ("m", "2026-03", Some(3.0)),
    ]);
    let answered = rt.table(FORECAST, table, spec.clone()).await.unwrap();
    // What arrived: the rows with their null, the specification whole.
    let calls = seen.0.lock().unwrap();
    let (route, body) = &calls[0];
    assert_eq!(route, "forecast");
    assert_eq!(body["spec"], spec);
    assert_eq!(
        body["rows"],
        json!([
            {"member": "m", "period": "2026-01", "value": 1.0},
            {"member": "m", "period": "2026-02", "value": null},
            {"member": "m", "period": "2026-03", "value": 3.0},
        ])
    );
    // What came back: one row per level for the one origin, the answer
    // object beside the table.
    assert_eq!(answered.answer["service"], json!("stand-in"));
    assert_eq!(answered.answer["version"], json!("test"));
    assert_eq!(answered.table.num_rows(), 5);
    let schema = answered.table.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "member", "origin", "step", "period", "span", "column", "level", "value", "actual",
            "pit"
        ]
    );
    let value = answered
        .table
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(value.values(), &[1.0, 2.0, 3.0, 4.0, 5.0]);
    assert!(
        answered.table.schema().metadata().is_empty(),
        "the answer leaves the metadata"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_queue_is_waited_out() {
    let (url, seen) = stub().await;
    let rt = runtime(&url, key("k1"));
    seen.0.lock().unwrap().push(("busy".into(), Value::Null));
    let answered = rt
        .table(
            FORECAST,
            series(&[("m", "2026-01", Some(1.0)), ("m", "2026-02", Some(2.0))]),
            json!({"targets": ["value"], "origins": 1, "horizon": 1, "levels": [0.5]}),
        )
        .await
        .unwrap();
    assert_eq!(answered.table.num_rows(), 1);
    let calls = seen.0.lock().unwrap();
    assert_eq!(calls.len(), 1, "one request answered, after the wait");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_identity_is_minted_once_and_rides_every_call_and_a_stopping_instance_is_waited_out() {
    let (url, seen) = stub().await;
    let rt = KernelRuntime::native()
        .with_point(
            FORECAST,
            &url,
            Bearer::Identity {
                audience: "https://thesi.example".into(),
                metadata: url.clone(),
            },
        )
        .unwrap();
    let spec = json!({"targets": ["value"], "origins": 1, "horizon": 1, "levels": [0.5]});
    let table = || series(&[("m", "2026-01", Some(1.0)), ("m", "2026-02", Some(2.0))]);
    seen.0
        .lock()
        .unwrap()
        .push(("draining".into(), Value::Null));
    let first = rt.table(FORECAST, table(), spec.clone()).await.unwrap();
    let second = rt.table(FORECAST, table(), spec).await.unwrap();
    assert_eq!(first.table.num_rows(), 1);
    assert_eq!(second.table.num_rows(), 1);
    let calls = seen.0.lock().unwrap();
    let routes: Vec<&str> = calls.iter().map(|(route, _)| route.as_str()).collect();
    // One token for the audience, before the first call; the 503 waited
    // out; the second call rides the kept token with no new mint.
    assert_eq!(routes, ["identity", "forecast", "forecast"]);
    assert_eq!(calls[0].1, json!("https://thesi.example"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_bearer_is_a_refusal_by_text() {
    let (url, _seen) = stub().await;
    let rt = runtime(&url, key("nope"));
    let e = rt
        .table(
            FORECAST,
            series(&[("m", "2026-01", Some(1.0))]),
            json!({"levels": [0.5]}),
        )
        .await
        .unwrap_err();
    assert!(
        e.contains("refused (401") && e.contains("unauthorized"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_names_the_fields_to_repair() {
    let (url, _seen) = stub().await;
    let rt = runtime(&url, key("k1"));
    let e = rt
        .table(
            FORECAST,
            series(&[("m", "2026-01", Some(1.0))]),
            json!({"targets": ["value"]}),
        )
        .await
        .unwrap_err();
    assert!(e.contains("refused (422"), "{e}");
    assert!(e.contains("the specification does not fit"), "{e}");
    assert!(
        e.contains("levels: a non-empty list of quantile levels"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn misfit_nulls_come_back_null_and_a_4xx_is_its_text() {
    let (url, seen) = stub().await;
    let rt = runtime(&url, key("k1"));
    let frame = |n: usize| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Float64, false),
                Field::new("kind", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Float64Array::from(
                    (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["x"; n])),
            ],
        )
        .unwrap()
    };
    let answered = rt
        .table(MISFIT, frame(3), json!({"cells": false}))
        .await
        .unwrap();
    let score = answered
        .table
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(score.len(), 3);
    assert_eq!(score.value(0), 0.5);
    assert!(score.is_null(1), "a null stays a null");
    assert_eq!(score.value(2), -1.0);
    assert_eq!(answered.answer["assumptions"][0]["field"], json!("score"));
    // The frame crossed with its text column as text.
    let second_row = seen.0.lock().unwrap()[0].1["rows"][1].clone();
    assert_eq!(second_row, json!({"a": 1.0, "kind": "x"}));
    let e = rt
        .table(MISFIT, frame(2), json!({"cells": false}))
        .await
        .unwrap_err();
    assert!(
        e.contains("refused (422") && e.contains("three rows"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_service_that_does_not_answer_is_reported_by_address() {
    let rt = KernelRuntime::native()
        .with_point(MISFIT, "http://127.0.0.1:9", Bearer::None)
        .unwrap();
    let e = rt
        .table(MISFIT, series(&[("m", "2026-01", Some(1.0))]), json!({}))
        .await
        .unwrap_err();
    assert!(
        e.contains("did not answer") && e.contains("127.0.0.1:9"),
        "{e}"
    );
    assert!(
        KernelRuntime::native()
            .with_point(MISFIT, "thesi.local", Bearer::None)
            .is_err()
    );
    let e = KernelRuntime::native()
        .with_point("bands", "http://127.0.0.1:9", Bearer::None)
        .unwrap_err();
    assert!(
        e.contains("no service point") && e.contains("forecast, misfit"),
        "{e}"
    );
    // A point not named refuses in the runtime too.
    let e = rt
        .table(FORECAST, series(&[("m", "2026-01", Some(1.0))]), json!({}))
        .await
        .unwrap_err();
    assert!(e.contains("fills no forecast point"), "{e}");
}

/// The walk sends every metric's monthly series as one table — member
/// the metric, period the month, one value column — with the
/// contract's specification, and reads per metric and walked month the
/// bands, the actual and the PIT; the measurement records the service
/// that answered. A re-record that changes a grounding owes the walk
/// again, and the walk is one call however many metrics stand.
#[tokio::test(flavor = "multi_thread")]
async fn the_walk_sends_the_series_as_one_table_and_reads_the_bands() {
    let (url, seen) = stub().await;
    let rt = Arc::new(runtime(&url, key("k1")));
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
    {
        let calls = seen.0.lock().unwrap();
        let sent: Vec<&(String, Value)> = calls.iter().filter(|(r, _)| r == "forecast").collect();
        assert_eq!(sent.len(), 1, "one table for the whole walk");
        let body = &sent[0].1;
        assert_eq!(
            body["spec"],
            json!({"targets": ["value"], "origins": 6, "horizon": 1, "levels": LEVELS})
        );
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 36, "two metrics of eighteen months");
        assert_eq!(
            rows[0],
            json!({"member": "inventory", "period": "2024-01", "value": 1000.0})
        );
        assert_eq!(
            rows[35],
            json!({"member": "revenue", "period": "2025-06", "value": 151.0})
        );
    }
    let walk = super::bands::walked(&session).await;
    assert_eq!(walk["applicable"], json!(true));
    for metric in walk["metrics"].as_array().unwrap() {
        assert_eq!(metric["served_by"], json!("stand-in test"));
        let points = metric["points"].as_array().unwrap();
        assert_eq!(points.len(), 6, "the last six months");
        assert_eq!(points[0]["period"], json!("2025-01"));
        assert_eq!(points[5]["period"], json!("2025-06"));
        for point in points {
            assert_eq!(point["pit"], json!(0.25), "the PIT the service answered");
            assert_eq!(point["p05"], json!(1.0));
            assert_eq!(point["p95"], json!(5.0));
            assert_eq!(point["partial"], json!(false));
        }
    }
    let revenue = walk["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["metric"] == json!("revenue"))
        .unwrap();
    assert_eq!(revenue["points"][5]["actual"], json!(151.0));

    // A re-record that changes a grounding owes the walk again.
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
    assert_eq!(
        calls.iter().filter(|(r, _)| r == "forecast").count(),
        1,
        "walked again, in one call"
    );
}

/// No service named: every point's door refuses at plan time, by its
/// own name, with the variable to set — before any aspect is looked up.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_service_the_doors_refuse_at_plan_time_by_name() {
    use glossql_catalog::Lake;
    use glossql_glossary::{Actor, ActorKind, Store};

    let rt = KernelRuntime::native();
    assert!(!rt.serves(FORECAST) && !rt.serves(MISFIT));
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

    for (statement, door, variable) in [
        (
            "SELECT * FROM misfit.late_pairs();",
            "misfit.late_pairs()",
            "GLOSSQL_MISFIT_URL",
        ),
        (
            "SELECT metric_bands() FROM fin;",
            "metric_bands()",
            "GLOSSQL_FORECAST_URL",
        ),
    ] {
        let e = session.execute(statement).await.unwrap_err().to_string();
        assert!(
            e.contains(door) && e.contains(variable) && e.contains("names none"),
            "{statement}: {e}"
        );
    }
}

/// The forecast lands as a measurement at the origin: the cube's cells
/// and the periods ahead go as one table with the contract's
/// specification, and the reads say what landed — `forecast_points()`
/// joins the actual at read, `forecast_coverage()` counts the landed
/// points over every forecast in the record once a landing has moved
/// the origin.
#[tokio::test(flavor = "multi_thread")]
async fn the_forecast_lands_at_the_origin_and_the_reads_follow_the_actuals() {
    use datafusion::arrow::util::pretty::pretty_format_batches;

    let (url, seen) = stub().await;
    let rt = Arc::new(runtime(&url, key("k1")));
    let dir = tempfile::tempdir().unwrap();
    // Eighteen months of a rising flow, landed from a file source so a
    // later export can append to the table.
    let root = dir.path().join("export");
    std::fs::create_dir_all(root.join("lines")).unwrap();
    let mut csv = String::from("date,value\n");
    for i in 0..18 {
        csv += &format!("{}-{:02}-01,{}\n", 2024 + i / 12, i % 12 + 1, 100 + 10 * i);
    }
    std::fs::write(root.join("lines/2024.csv"), csv).unwrap();
    let statements = [
        format!(
            "DECLARE SOURCE export SET (type: csv, location: '{}');",
            root.display()
        ),
        "DECLARE RECIPE lines ON fin FROM export AS $$SELECT CAST(date AS DATE) AS date, \
         CAST(value AS DOUBLE) AS value FROM read_csv('lines/*.csv')$$;"
            .to_string(),
        "IMPORT lines;".to_string(),
        r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#.to_string(),
        r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value FROM lines"}$$;"#.to_string(),
        // The cube admits a series on a judged time column.
        "SELECT judge_time() FROM lines.date;".to_string(),
    ];
    let session = super::bands::walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![],
        &statements.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    let table = |sql: &'static str| {
        let session = &session;
        async move {
            let outcomes = session.execute(sql).await.unwrap();
            let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
                panic!("rows")
            };
            pretty_format_batches(batches).unwrap().to_string()
        }
    };

    seen.0.lock().unwrap().clear();
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .unwrap();
    {
        let calls = seen.0.lock().unwrap();
        let sent: Vec<&(String, Value)> = calls.iter().filter(|(r, _)| r == "forecast").collect();
        assert_eq!(sent.len(), 1, "one table for the flow");
        let body = &sent[0].1;
        assert_eq!(
            body["spec"],
            json!({"targets": ["value"], "horizon": 12, "levels": LEVELS,
                   "grain": "month", "totals": [3, 6]})
        );
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            30,
            "eighteen months of history and twelve ahead"
        );
        assert_eq!(rows[17]["status"], json!("actual"));
        assert_eq!(rows[17]["value"], json!(270.0));
        assert_eq!(rows[18]["status"], json!("forecast"));
        assert!(rows[18]["value"].is_null(), "{}", rows[18]);
    }
    let points = table(
        "SELECT origin, step, span, to_go, period, p50, actual, landed, served_by, \
                held_80, held_from \
         FROM forecast_points() WHERE metric = 'revenue' ORDER BY point_seq;",
    )
    .await;
    let lines: Vec<&str> = points
        .lines()
        .filter(|l| l.contains("2025-06-01"))
        .collect();
    assert_eq!(
        lines.len(),
        14,
        "twelve steps, the quarter and the year to go: {points}"
    );
    assert!(
        lines[0].contains("2025-07-01")
            && lines[0].contains("270.0")
            && lines[0].contains("stand-in test"),
        "{}",
        lines[0]
    );
    // The service's share rides over the wire and lands on the point;
    // the record has nothing landed, so it is the share served.
    assert!(lines[0].contains("| 0.75    | service "), "{}", lines[0]);
    assert!(
        lines[12].contains("quarter") && lines[12].contains("810.0"),
        "{}",
        lines[12]
    );
    assert!(lines[12].contains("| 0.72    | service "), "{}", lines[12]);
    assert!(
        lines[13].contains("year") && lines[13].contains("1620.0"),
        "{}",
        lines[13]
    );
    // Nothing ahead has landed: no actual, nowhere landed.
    let empty =
        table("SELECT count(actual) AS n, count(landed) AS m FROM forecast_points();").await;
    assert!(empty.contains("| 0 | 0 |"), "{empty}");
    let none = table("SELECT count(*) AS n FROM forecast_coverage();").await;
    assert!(none.contains("| 0 |"), "{none}");

    // A landing moves the origin: two more months arrive at the source,
    // the first inside the recorded band (p10 248.4 to p90 291.6), the
    // second above p95 (297).
    std::fs::write(
        root.join("lines/2025-h2.csv"),
        "date,value\n2025-07-01,275\n2025-08-01,300\n",
    )
    .unwrap();
    session.execute("IMPORT lines;").await.unwrap();
    session
        .execute("SELECT metric_forecast() FROM fin;")
        .await
        .unwrap();
    let moved = table(
        "SELECT origin, step, span, to_go, period FROM forecast_points() \
         WHERE metric = 'revenue' ORDER BY point_seq;",
    )
    .await;
    let lines: Vec<&str> = moved.lines().filter(|l| l.contains("2025-08-01")).collect();
    assert_eq!(lines.len(), 14, "{moved}");
    assert!(lines[0].contains("2025-09-01"), "{}", lines[0]);
    assert!(
        lines[12].contains("quarter") && lines[12].contains("| 1 "),
        "{}",
        lines[12]
    );
    assert!(
        lines[13].contains("year") && lines[13].contains("| 4 "),
        "{}",
        lines[13]
    );

    // The first forecast's first two steps have their actuals now: one
    // inside both bands, one above both. The dataset row and the
    // metric's row agree, since one metric stands.
    let coverage = table(
        "SELECT metric, grain, step, span, points, inside_80, inside_90, forecasts \
         FROM forecast_coverage() ORDER BY metric, step;",
    )
    .await;
    let rows: Vec<&str> = coverage.lines().filter(|l| l.contains("month")).collect();
    assert_eq!(rows.len(), 4, "{coverage}");
    for (row, step, share) in [
        (0, "1", "1.0"),
        (1, "2", "0.0"),
        (2, "1", "1.0"),
        (3, "2", "0.0"),
    ] {
        let line = rows[row];
        assert!(line.contains(&format!("| {step} ")), "{line}");
        assert!(line.contains(&format!("| {share} ")), "{line}");
        assert!(line.contains("| 1 "), "one point from one forecast: {line}");
    }
    assert!(
        rows[2].contains("revenue") && !rows[0].contains("revenue"),
        "{coverage}"
    );
}

/// The scenario world end to end (fixture 19): a lever declared on a
/// landed column, a scenario of factors per period on it, the
/// arithmetic replayed through the grounding where the data holds the
/// periods, and the periods ahead estimated by the what-if point — the
/// metric's series with the lever's beside it, as planned and under the
/// scenario, over the wire as one table.
#[tokio::test(flavor = "multi_thread")]
async fn the_scenario_world_is_replayed_and_estimated_through_the_whatif_point() {
    use datafusion::arrow::util::pretty::pretty_format_batches;

    let (url, seen) = stub().await;
    let rt = Arc::new(runtime(&url, key("k1")));
    let dir = tempfile::tempdir().unwrap();
    // Eighteen months of a rising flow at a unit price of one.
    let root = dir.path().join("export");
    std::fs::create_dir_all(root.join("lines")).unwrap();
    let mut csv = String::from("date,value,price\n");
    for i in 0..18 {
        csv += &format!(
            "{}-{:02}-01,{},1\n",
            2024 + i / 12,
            i % 12 + 1,
            100 + 10 * i
        );
    }
    std::fs::write(root.join("lines/2024.csv"), csv).unwrap();
    let statements = [
        format!(
            "DECLARE SOURCE export SET (type: csv, location: '{}');",
            root.display()
        ),
        "DECLARE RECIPE lines ON fin FROM export AS $$SELECT CAST(date AS DATE) AS date, \
         CAST(value AS DOUBLE) AS value, CAST(price AS DOUBLE) AS price FROM read_csv('lines/*.csv')$$;"
            .to_string(),
        "IMPORT lines;".to_string(),
        r#"DECLARE ASPECT revenue WITH $${"title": "Revenue"}$$ AS QUERY ON DATASET;"#.to_string(),
        r#"GLOSS revenue ON fin AS $${"sql": "SELECT date, value * price AS value FROM lines"}$$;"#.to_string(),
        "SELECT judge_time() FROM lines.date;".to_string(),
        r#"DECLARE ASPECT lever WITH $${"type": "object", "required": ["value"], "properties": {"value": {"type": "boolean"}, "grounds": {"type": "string"}}}$$ AS FACT ON COLUMN;"#.to_string(),
        r#"GLOSS lever ON lines.price AS $${"value": true, "grounds": "the list price"}$$;"#.to_string(),
        r#"DECLARE ASPECT price_up WITH $${"title": "Price up", "x-kind": "scenario", "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}}$$ AS FACT ON DATASET;"#.to_string(),
        r#"GLOSS price_up ON fin AS $${"levers": [{"column": "lines.price", "factor": {"2025-05": 1.5, "2025-06": 1.5, "2025-07": 1.5}, "basis": "the price list"}]}$$;"#.to_string(),
    ];
    let session = super::bands::walk_session(
        dir.path(),
        Arc::clone(&rt),
        vec![],
        &statements.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    let table = |sql: &'static str| {
        let session = &session;
        async move {
            let outcomes = session.execute(sql).await.unwrap();
            let Some(glossql_session::Outcome::Rows { batches, .. }) = outcomes.last() else {
                panic!("rows")
            };
            pretty_format_batches(batches).unwrap().to_string()
        }
    };

    seen.0.lock().unwrap().clear();
    let points = table(
        "SELECT period, step, pass, value, low, high, sign, served_by \
         FROM scenario_points(scenario => 'price_up') ORDER BY period;",
    )
    .await;
    let lines: Vec<&str> = points.lines().filter(|l| l.contains("2025")).collect();
    assert_eq!(lines.len(), 3, "{points}");
    // May and June replayed at one and a half: 390, 405. July estimated
    // from the last actual, 270, at one and a half: 405, the band 243
    // to 567, served by the stand-in.
    assert!(
        lines[0].contains("| 2025-05-01 |      | replayed  | 390.0 |"),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].contains("| 2025-06-01 |      | replayed  | 405.0 |"),
        "{}",
        lines[1]
    );
    assert!(
        lines[2].contains(
            "| 2025-07-01 | 1    | estimated | 405.0 | 243.0 | 567.0 | +    | stand-in test"
        ),
        "{}",
        lines[2]
    );
    {
        let calls = seen.0.lock().unwrap();
        let sent: Vec<&(String, Value)> = calls.iter().filter(|(r, _)| r == "whatif").collect();
        assert_eq!(sent.len(), 1, "one table for the month grain");
        let body = &sent[0].1;
        assert_eq!(
            body["spec"],
            json!({"targets": ["value"], "levels": [0.1, 0.5, 0.9]})
        );
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            20,
            "eighteen months of history and one ahead, twice"
        );
        assert_eq!(rows[17]["status"], json!("actual"));
        assert_eq!(rows[17]["value"], json!(270.0));
        assert_eq!(rows[17]["lines.price"], json!(1.0));
        assert_eq!(rows[18]["status"], json!("plan"));
        assert!(rows[18]["value"].is_null(), "{}", rows[18]);
        assert_eq!(rows[18]["lines.price"], json!(1.0));
        assert_eq!(rows[19]["status"], json!("scenario"));
        assert_eq!(rows[19]["lines.price"], json!(1.5));
    }
    // The world beside the actual one: the three cells, the median for
    // the estimate.
    let cells = table(
        "SELECT world, period, value FROM metric_series(world => 'scenario', scenario => 'price_up') \
         ORDER BY period;",
    )
    .await;
    assert_eq!(
        cells.lines().filter(|l| l.contains("scenario")).count(),
        3,
        "{cells}"
    );
    assert!(cells.contains("| 2025-07-01T00:00:00 | 405.0"), "{cells}");
}
