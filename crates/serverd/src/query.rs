//! The cockpit's query door: plain HTTP in, Arrow IPC out. A single
//! query streams straight from the engine — batches are encoded as they
//! arrive, memory rides one batch, no cap, and a client that hangs up
//! cancels the work upstream. Everything else answers in the wire JSON
//! shape.
//!
//! The dataset is the URL's first segment and one that does not exist
//! is a 404 — this door reads and writes, it does not bring datasets
//! into being. `USE` in the body still moves the statements after it,
//! for the length of this request.

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};

use arrow_ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use futures::{SinkExt, StreamExt, channel::mpsc};
use glossql_session::{Caller, SessionError};

use crate::AppState;
use crate::wire;

pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

pub async fn query(
    State(state): State<AppState>,
    Path(dataset): Path<String>,
    Extension(Caller(actor)): Extension<Caller>,
    body: String,
) -> Response {
    let known = state.plane.datasets().await.unwrap_or_default();
    if !known.contains(&dataset) {
        return fail(
            StatusCode::NOT_FOUND,
            format!(
                "no dataset `{dataset}` — this workspace holds {}",
                if known.is_empty() {
                    "none yet".to_string()
                } else {
                    known.join(", ")
                }
            ),
        );
    }
    let session = match state.plane.channel(actor.clone(), Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match session.query_stream(&body).await {
        // The Arrow door never caps — metadata or data, the client
        // drains a stream; `metadata_only` is the MCP door's concern.
        Ok(query) => stream(query.stream),
        // Not one query: statement sequences, declarations, and writes
        // run at the plane (`USE` selects the actor's channel there)
        // and answer in JSON.
        Err(SessionError::NotOneRead) => {
            match state.plane.execute(actor, Some(&dataset), &body).await {
                Ok(outcomes) => match wire::outcomes_json(&outcomes, state.row_cap) {
                    Ok(rendered) => Json(rendered).into_response(),
                    Err(e) => fail(StatusCode::INTERNAL_SERVER_ERROR, e),
                },
                // The statement was refused: the body says why — and what
                // a sequence had already landed rides beside the reason.
                Err(e) => {
                    let landed = match &e {
                        SessionError::Sequence { landed, .. } if !landed.is_empty() => {
                            wire::outcomes_json(landed, state.row_cap).ok()
                        }
                        _ => None,
                    };
                    let mut body = serde_json::json!({ "error": e.to_string() });
                    if let Some(landed) = landed {
                        body["landed"] = landed;
                    }
                    (StatusCode::UNPROCESSABLE_ENTITY, Json(body)).into_response()
                }
            }
        }
        Err(e) => fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}

/// Encode into a chunked body as batches arrive. The channel's capacity
/// is the backpressure: the encoder waits for the client to drain. An
/// error after bytes flowed can only break the stream — the IPC reader
/// on the other end sees a truncated stream, which is the truth.
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
        // The schema message is written at construction — ship it first.
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

/// Drain the writer's buffer into the channel; a gone receiver (client
/// hung up) errors, the task returns, and dropping the batch stream
/// cancels the engine's work.
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

fn fail(status: StatusCode, error: String) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
