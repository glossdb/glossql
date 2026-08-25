//! The docket's second write, a compute act: re-run every measurement
//! that stands from before the last change.
//!
//! Every write moves the pin, and a function voice landed at an earlier
//! pin is served and marked (SPEC.md §7): the numbers are current (the
//! cube rebuilds at every pin); the judged axes and the check verdicts
//! stand on those voices until they run again. The banner counts them
//! (the raw read's `current`), and this lands the next rows — the same
//! extractions an agent would run, nothing authored: the functions
//! speak.
//!
//! The response is the write event, as the ruling's is — and it names
//! itself: 204 with `HX-Trigger: glossql:remeasured, glossql:written`.
//! A ruling cannot change the cube, so the store keeps data frames
//! across one; a re-measure can (the axes), so on `glossql:remeasured`
//! the store drops them too, and every tile refetches in place. The
//! next cube read rebuilds at the new version.

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use glossql_session::Caller;

use crate::AppDoor;
use crate::app::AppDef;
use crate::rule::plain;

/// Both events, the cube's own first: the store evicts on each in
/// capture phase before any tile's own listener refetches.
const REMEASURED: [(&str, &str); 1] = [("HX-Trigger", "glossql:remeasured, glossql:written")];

pub async fn remeasure(
    State(door): State<AppDoor>,
    Path((dataset, app)): Path<(String, String)>,
    Extension(Caller(actor)): Extension<Caller>,
) -> Response {
    let known = crate::known(&door).await;
    if !known.contains(&dataset) {
        return plain(
            StatusCode::NOT_FOUND,
            crate::no_such_dataset(&dataset, &known),
        );
    }
    let glossed = crate::glossed::parts(&door, &dataset).await;
    match AppDef::load(&door.workspace, &app, &glossed) {
        Ok(Some(_)) => {}
        Ok(None) => return plain(StatusCode::NOT_FOUND, format!("no app `{app}`")),
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let human = match door.plane.channel(actor, Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match human.remeasure().await {
        Ok(_) => (StatusCode::NO_CONTENT, REMEASURED).into_response(),
        Err(e) => plain(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}
