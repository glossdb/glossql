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
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use datafusion::common::{ParamValues, ScalarValue};
use futures::StreamExt;
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;
use crate::app::AppDef;

pub async fn frame(
    State(door): State<AppDoor>,
    Path((dataset, app, frame)): Path<(String, String, String)>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    if let Some(missing) = crate::missing(&door, &dataset).await {
        return fail(StatusCode::NOT_FOUND, missing);
    }
    // `draft` among the params asks for the newest parts, not the
    // release's; the page's params reach every frame it fetches.
    let draft = params.iter().any(|(k, _)| k == "draft");
    let glossed = crate::glossed::parts(&door, &dataset, draft).await.parts;
    let def = match AppDef::load(&app, &glossed) {
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
    // so concurrent frames never steer each other; a dataset that does
    // not exist fails the channel here, before any query runs.
    //
    // A frame reads; it does not write, and the reads collapse human
    // over agent anyway. So the app itself is the reader, whoever asked
    // — one channel per app rather than one per visitor.
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
    // No `$dataset` param. The channel above is bound to the URL's
    // dataset, so the session already knows which one it is on and the
    // `current_dataset` relation says so inside the SQL — a frame joins
    // it the way any read does. A reserved parameter would be a second
    // spelling of the same fact, and the guard it needed (a query
    // string must not override the path) exists only because there was
    // something to override. Frames must still never scan the
    // `datasets` relation for it: in a multi-dataset workspace that
    // fans every joined row out.
    let mut query = match session
        .query_stream_with_params(&sql, Some(ParamValues::from(values)))
        .await
    {
        Ok(query) => query,
        Err(e) => return fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    };
    // The first poll runs the plan. A frame the engine refuses answers
    // as the JSON its tile renders; nothing is on the wire yet, so the
    // status is still ours to set.
    match query.stream.next().await {
        Some(Err(e)) => fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
        first => (
            [
                (header::CONTENT_TYPE.as_str(), crate::ipc::ARROW_STREAM),
                (FRAME_CLASS, if query.record { "record" } else { "data" }),
            ],
            crate::ipc::body(query.stream, first),
        )
            .into_response(),
    }
}

/// One open question, as the workspace currently derives it.
///
/// The docket's ruling write is gated on this: a posted answer is
/// accepted only if `open_questions` still carries that exact
/// `(subject, aspect, key)`, and the prose it records is this read's,
/// never the browser's. A tab left open across a fold-in posts a
/// question that no longer stands, and gets told so.
pub(crate) struct OpenQuestion {
    pub dimension: String,
    pub assumption: String,
}

pub(crate) async fn one_open_question(
    session: &glossql_session::Session,
    subject: &str,
    aspect: &str,
    key: &str,
) -> Result<Option<OpenQuestion>, String> {
    let mut values: HashMap<String, ScalarValue> = HashMap::new();
    values.insert("subject".into(), ScalarValue::Utf8(Some(subject.into())));
    values.insert("aspect".into(), ScalarValue::Utf8(Some(aspect.into())));
    values.insert("key".into(), ScalarValue::Utf8(Some(key.into())));
    let query = session
        .query_stream_with_params(
            // `open_questions` answers for the whole workspace and the
            // gate is a write's, so the join is not tidiness: two
            // datasets may hold the same subject under the same key,
            // and without it another dataset's open question admits a
            // ruling into this one — carrying that dataset's prose,
            // since the words recorded are this read's.
            "SELECT coalesce(o.dimension, '-') AS dimension, o.assumption \
             FROM open_questions o JOIN current_dataset d ON d.dataset = o.dataset \
             WHERE o.subject = CAST($subject AS VARCHAR) \
               AND o.aspect = CAST($aspect AS VARCHAR) \
               AND o.key = CAST($key AS VARCHAR) LIMIT 1",
            Some(ParamValues::from(values)),
        )
        .await
        .map_err(|e| e.to_string())?;
    let batches: Vec<_> = query
        .stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let text = |name: &str| -> String {
            batch
                .column_by_name(name)
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<datafusion::arrow::array::StringArray>()
                        .map(|s| s.value(0).to_string())
                })
                .unwrap_or_default()
        };
        return Ok(Some(OpenQuestion {
            dimension: text("dimension"),
            assumption: text("assumption"),
        }));
    }
    Ok(None)
}

/// The frame-class header: `record` when the frame's expansion reads
/// the glossary anywhere (derived by the session's pre-pass, never
/// curated), `data` when it provably does not. The browser's frame
/// store evicts record entries on a ruling and keeps data entries —
/// the cube survives every glossary write.
pub const FRAME_CLASS: &str = "glossql-frame-class";

/// Frame errors answer as JSON — the browser components render the
/// message inside the tile, which is the error-at-read posture on a
/// visible surface.
fn fail(status: StatusCode, error: String) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
