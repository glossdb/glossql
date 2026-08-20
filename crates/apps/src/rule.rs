//! The docket's one write: a human ruling on a claim that stands open.
//!
//! The gate is the derivation itself. A posted ruling is accepted only
//! if `open_questions` still carries that exact `(subject, aspect,
//! key)` — so the page cannot rule something already ruled, already
//! folded in, or never disclosed, and a stale tab is refused rather
//! than believed. The prose the ruling records comes from that read,
//! not from the form: what the human agreed with is what the workspace
//! says, never what a browser posted.
//!
//! Nothing else about the page changes. It still holds no other write,
//! and the ruling lands exactly where the round's would — the human's
//! own slot, on the human's own channel, witnessed by this server.
//!
//! The response is an event, not a navigation (not a 303 back to
//! the Referer — PRG bent out of shape, since
//! the reader never leaves the page and the docket is client-rendered
//! anyway). Success is 204 with `HX-Trigger: glossql:written`; the
//! store hears the event, drops its frame caches, and every connected
//! component refetches in place. The stale-tab 409 carries the same
//! trigger, so a tab that ruled a dead question re-derives to the
//! current state instead of asking for a reload.

use axum::Form;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use glossql_glossary::{Actor, ActorKind};
use glossql_session::rulings::{self, Ruling};
use serde::Deserialize;

use crate::AppDoor;
use crate::app::AppDef;

#[derive(Deserialize)]
pub struct Answer {
    subject: String,
    aspect: String,
    key: String,
    /// `confirmed`, `corrected`, or `unclear` — the stances the round
    /// serves. `unclear` refuses the question rather than the claim:
    /// the agent owes a reformulation, not a fold-in. Admission is the
    /// one gate: the ruling aspect's schema holds the enum, so an
    /// unknown stance answers with the store's own refusal.
    stance: String,
    /// The human's own words, empty allowed — the MCP door writes the
    /// same act the same way.
    #[serde(default)]
    note: String,
}

/// The write announced: htmx dispatches this on the posting form and it
/// bubbles — the store clears on it, components refetch on it.
pub(crate) const WRITTEN: [(&str, &str); 1] = [("HX-Trigger", "glossql:written")];

pub async fn rule(
    State(door): State<AppDoor>,
    Path(app): Path<String>,
    Form(answer): Form<Answer>,
) -> Response {
    let stance = answer.stance.as_str();
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

    // The question must still stand, and its own words are what the
    // ruling records.
    let standing = match crate::frames::one_open_question(
        &human,
        &answer.subject,
        &answer.aspect,
        &answer.key,
    )
    .await
    {
        Ok(Some(standing)) => standing,
        Ok(None) => {
            // The trigger rides the refusal too: the stale tab's panels
            // re-derive to the current state on their own.
            return (
                StatusCode::CONFLICT,
                WRITTEN,
                "that question no longer stands — the page has re-derived".to_string(),
            )
                .into_response();
        }
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e),
    };

    let note = (!answer.note.trim().is_empty()).then(|| answer.note.trim().to_string());
    match rulings::land(
        &human,
        Ruling {
            subject: &answer.subject,
            aspect: &answer.aspect,
            dimension: &standing.dimension,
            key: &answer.key,
            assumption: &standing.assumption,
            stance,
            note,
        },
    )
    .await
    {
        Ok(_) => (StatusCode::NO_CONTENT, WRITTEN).into_response(),
        Err(e) => plain(StatusCode::UNPROCESSABLE_ENTITY, e),
    }
}

pub(crate) fn plain(status: StatusCode, message: String) -> Response {
    (status, message).into_response()
}
