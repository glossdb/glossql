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

use axum::Form;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
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
    /// `confirmed` or `corrected` — the two stances the round serves.
    stance: String,
    /// The human's own words. Required in practice for a correction:
    /// "wrong" without saying what is right closes a question and
    /// tells the agent nothing.
    #[serde(default)]
    note: String,
}

/// Where to send the browser back to: the page the form was posted
/// from, or the app's index when that cannot be established.
///
/// Only the path and query of the Referer are ever used, and only when
/// the path is under this app — so a forged header can redirect a user
/// to another page of the same docket at worst, never off the site.
/// Without this a ruling made on a metric's page bounced the reader to
/// the index, which reads as the page losing their place.
fn back_to(headers: &HeaderMap, app: &str) -> String {
    let home = format!("/app/{app}");
    headers
        .get(header::REFERER)
        .and_then(|r| r.to_str().ok())
        .map(|r| {
            // Drop scheme and authority if the Referer is absolute, and
            // any fragment. What is left is path and query.
            let rest = match r.split_once("://") {
                Some((_, after)) => match after.find('/') {
                    Some(slash) => &after[slash..],
                    None => "/",
                },
                None => r,
            };
            rest.split('#').next().unwrap_or("/")
        })
        // The whole guard: whatever the header said, the redirect is a
        // path under this app or it is the app's index. A protocol-
        // relative `//host/…` fails this too, since it does not begin
        // with `/app/<app>`.
        .filter(|p| *p == home || p.starts_with(&format!("{home}/")))
        .map(str::to_string)
        .unwrap_or(home)
}

pub async fn rule(
    State(door): State<AppDoor>,
    Path(app): Path<String>,
    headers: HeaderMap,
    Form(answer): Form<Answer>,
) -> Response {
    let stance = match answer.stance.as_str() {
        "confirmed" | "corrected" => answer.stance.as_str(),
        other => return plain(StatusCode::BAD_REQUEST, format!("unknown stance `{other}`")),
    };
    if stance == "corrected" && answer.note.trim().is_empty() {
        return plain(
            StatusCode::BAD_REQUEST,
            "a correction has to say what is right".to_string(),
        );
    }
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
            return plain(
                StatusCode::CONFLICT,
                "that question no longer stands — reload the docket".to_string(),
            );
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
        Ok(_) => Redirect::to(&back_to(&headers, &app)).into_response(),
        Err(e) => plain(StatusCode::UNPROCESSABLE_ENTITY, e),
    }
}

fn plain(status: StatusCode, message: String) -> Response {
    (status, message).into_response()
}
