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

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};

use std::sync::Arc;

use futures::StreamExt;
use glossql_session::{Caller, Plane, SessionError};

use crate::wire;

pub async fn query(
    State(plane): State<Arc<Plane>>,
    Path(dataset): Path<String>,
    Extension(Caller(actor)): Extension<Caller>,
    body: String,
) -> Response {
    if let Some(missing) = crate::missing_dataset(&plane, &dataset).await {
        return fail(StatusCode::NOT_FOUND, missing);
    }
    let session = match plane.channel(actor.clone(), Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return fail(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match session.query_stream(&body).await {
        // The Arrow door never caps — metadata or data, the client
        // drains a stream; paging is the MCP door's concern.
        // The first poll runs the plan: a read the engine cannot start
        // — a scan it refuses, an expression that fails on the first
        // row — is a refusal with its text, not a 200 and a truncated
        // stream. Nothing is on the wire yet, so the status is still
        // ours to set.
        Ok(mut query) => match query.stream.next().await {
            Some(Err(e)) => fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
            first => (
                [(header::CONTENT_TYPE, glossql_apps::ipc::ARROW_STREAM)],
                glossql_apps::ipc::body(query.stream, first),
            )
                .into_response(),
        },
        // Not one query: statement sequences, declarations, and writes
        // run at the plane (`USE` selects the actor's channel there)
        // and answer in JSON.
        Err(SessionError::NotOneRead) => {
            match plane.execute(actor, Some(&dataset), &body).await {
                Ok(outcomes) => match wire::outcomes_json(&outcomes) {
                    Ok(rendered) => Json(rendered).into_response(),
                    Err(e) => fail(StatusCode::INTERNAL_SERVER_ERROR, e),
                },
                // The statement was refused: the body says why — and what
                // a sequence had already landed rides beside the reason.
                Err(e) => {
                    let landed = match &e {
                        SessionError::Sequence { landed, .. } if !landed.is_empty() => {
                            wire::outcomes_json(landed).ok()
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

fn fail(status: StatusCode, error: String) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
