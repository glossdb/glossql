//! The docket's second write, a compute act: re-run the profilers the
//! cube admits on — `temporal_profile` over the served date columns,
//! `dimension_relevance` over the served dimension columns, of every
//! current grounding.
//!
//! A measurement is reachable at its own pin and every write moves the
//! pin, so after a ruling or an import the cube's axes stand on
//! verdicts from an earlier moment. The numbers are current (the cube
//! rebuilds at every pin); the axes may not be. The banner says so
//! (`metric_axes().judged_current`), and this lands the next verdicts
//! — the same extractions an agent would run, nothing authored: the
//! functions speak.
//!
//! The response is the write event, as the ruling's is: 204 with
//! `HX-Trigger: glossql:written`. The record frames refetch, the banner
//! clears, and the next cube read rebuilds at the new version.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;
use crate::app::AppDef;
use crate::rule::{WRITTEN, plain};

pub async fn remeasure(State(door): State<AppDoor>, Path(app): Path<String>) -> Response {
    let glossed = crate::glossed::parts(&door).await;
    let def = match AppDef::load(&door.workspace, &app, &glossed) {
        Ok(Some(def)) => def,
        Ok(None) => return plain(StatusCode::NOT_FOUND, format!("no app `{app}`")),
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let Some(dataset) = crate::frames::bound_dataset(&door, &def).await else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "no dataset is bound".to_string(),
        );
    };
    let actor = Actor {
        kind: ActorKind::Human,
        id: door.human.clone(),
    };
    let human = match door.plane.channel(actor, Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    match human.remeasure_cube().await {
        Ok(_) => (StatusCode::NO_CONTENT, WRITTEN).into_response(),
        Err(e) => plain(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
    }
}
