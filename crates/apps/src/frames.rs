//! Frames: an app's declared queries, served as Arrow IPC. The frame
//! store in the browser fetches each frame once per state and shares
//! the table across every tile bound to it. URL params bind as plan
//! placeholders (`$from` in the SQL, `?from=…` on the URL) — typed
//! values through the plan, never text spliced into SQL. Everything
//! arrives as Utf8; the frame SQL casts explicitly, the same posture
//! recipes take. A placeholder nobody bound fails at execution: the
//! read tells the author what the URL owed it.

use std::collections::HashMap;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use arrow_ipc::writer::StreamWriter;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::execution::SendableRecordBatchStream;
use futures::{SinkExt, StreamExt, channel::mpsc};
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;
use crate::app::AppDef;

pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

pub async fn frame(
    State(door): State<AppDoor>,
    Path((app, frame)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let def = match AppDef::load(&door.workspace, &app) {
        Ok(Some(def)) => def,
        Ok(None) => return fail(StatusCode::NOT_FOUND, format!("no app `{app}`")),
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let Some(sql) = def.read("frames", &format!("{frame}.sql")) else {
        return fail(
            StatusCode::NOT_FOUND,
            format!("no frame `{frame}` in `{app}`"),
        );
    };
    // The app's channel on the plane — Human-kind, like `/query`, keyed
    // (actor, dataset). The binding is fixed at channel construction,
    // so concurrent frames never steer each other; an unknown dataset
    // fails the channel here, before any query runs.
    let dataset = match resolve_dataset(&door, &def).await {
        Ok(dataset) => dataset,
        Err(response) => return response,
    };
    let actor = Actor {
        kind: ActorKind::Human,
        id: format!("app:{}", def.name),
    };
    let session = match door.plane.channel(actor, Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    };
    let values: HashMap<String, ScalarValue> = params
        .into_iter()
        .map(|(k, v)| (k, ScalarValue::Utf8(Some(v))))
        .collect();
    match session
        .query_stream_with_params(&sql, Some(ParamValues::from(values)))
        .await
    {
        Ok(query) => stream(query.stream),
        Err(e) => fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}

/// The app's dataset: the manifest's pin, or — for an app that pins
/// none, like the built-in model app — the first workspace dataset by
/// name, resolved per request so the binding follows the workspace.
/// Multi-dataset workspaces stay first-class (the lead, 2026-08-12:
/// the one-container-one-dataset question is a deployment concern,
/// held open) — an unpinned app shows the first and a selector is a
/// later concern; pin `dataset` in app.toml to choose.
pub(crate) async fn resolve_dataset(
    door: &crate::AppDoor,
    def: &AppDef,
) -> Result<String, Response> {
    match &def.dataset {
        Some(dataset) => Ok(dataset.clone()),
        None => match door.plane.datasets().await {
            Ok(mut names) => {
                names.sort();
                match names.into_iter().next() {
                    Some(first) => Ok(first),
                    None => Err(fail(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "no dataset in the workspace yet — the app binds once a \
                         source lands"
                            .to_string(),
                    )),
                }
            }
            Err(e) => Err(fail(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        },
    }
}

/// Encode into a chunked body as batches arrive — the same shape as
/// serverd's `/query` door (crates/serverd/src/query.rs): the channel
/// capacity is the backpressure, an error after bytes flowed truncates
/// the stream, and a client hanging up cancels the engine's work.
fn stream(mut batches: SendableRecordBatchStream) -> Response {
    let schema = batches.schema();
    let (mut tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        let mut writer = match StreamWriter::try_new(Vec::new(), schema.as_ref()) {
            Ok(writer) => writer,
            Err(e) => {
                let _ = tx.send(Err(std::io::Error::other(e))).await;
                return;
            }
        };
        if ship(&mut writer, &mut tx).await.is_err() {
            return;
        }
        while let Some(batch) = batches.next().await {
            let written = batch
                .map_err(std::io::Error::other)
                .and_then(|b| writer.write(&b).map_err(std::io::Error::other));
            if let Err(e) = written {
                let _ = tx.send(Err(e)).await;
                return;
            }
            if ship(&mut writer, &mut tx).await.is_err() {
                return;
            }
        }
        if let Err(e) = writer.finish() {
            let _ = tx.send(Err(std::io::Error::other(e))).await;
            return;
        }
        let _ = ship(&mut writer, &mut tx).await;
    });
    (
        [(header::CONTENT_TYPE, ARROW_STREAM)],
        Body::from_stream(rx),
    )
        .into_response()
}

async fn ship(
    writer: &mut StreamWriter<Vec<u8>>,
    tx: &mut mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ()> {
    let chunk = std::mem::take(writer.get_mut());
    if chunk.is_empty() {
        return Ok(());
    }
    tx.send(Ok(Bytes::from(chunk))).await.map_err(|_| ())
}

/// Frame errors answer as JSON — the browser components render the
/// message inside the tile, which is the error-at-read posture on a
/// visible surface.
fn fail(status: StatusCode, error: String) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
