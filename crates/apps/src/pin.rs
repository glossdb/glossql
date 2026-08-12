//! The pin door: the one write an app carries (ruled 2026-08-12). A
//! human approving a proposed (subject, aspect, body) lands it as a
//! HUMAN gloss — superseding, actor-stamped, outranking the agent slot
//! in every collapsed read. Everything else about the write is the
//! ordinary gloss path: aspect schema validation, grain gating, the
//! witness speaker gate, ACCEPTS invalidation. The door only composes
//! the statement and the actor; it decides nothing.

use axum::Form;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use glossql_glossary::{Actor, ActorKind};
use serde::Deserialize;

use crate::AppDoor;
use crate::app::AppDef;
use crate::frames::resolve_dataset;

#[derive(Deserialize)]
pub struct PinForm {
    pub subject: String,
    pub aspect: String,
    /// The gloss body, as JSON. The form carries the full body the pin
    /// writes — the frame proposed it, the human approved it.
    pub body: String,
    /// Who pins, when no sign-in cookie rides the request — a direct
    /// post's fallback. The cookie's verified subject wins where both
    /// exist; either way the name is provenance, not permission — it
    /// lands as the HUMAN actor id.
    #[serde(default)]
    pub pinned_by: String,
    /// Where the browser returns after a plain form post. Local paths
    /// only.
    #[serde(default)]
    pub back: String,
}

/// Path subjects only: 1–3 identifier segments (`fin`, `orders`,
/// `orders.amount`, or a declared source's name). Pair subjects pin
/// through a session, not through the door, until a surface needs
/// them.
fn ident_path(s: &str, max_segments: usize) -> bool {
    let segments: Vec<&str> = s.split('.').collect();
    !segments.is_empty()
        && segments.len() <= max_segments
        && segments.iter().all(|seg| {
            !seg.is_empty()
                && seg.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

pub async fn pin(
    State(door): State<AppDoor>,
    Path(app): Path<String>,
    headers: HeaderMap,
    Form(form): Form<PinForm>,
) -> Response {
    let def = match AppDef::load(&door.workspace, &app) {
        Ok(Some(def)) => def,
        Ok(None) => return refuse(format!("no app `{app}`")),
        Err(e) => return refuse(e),
    };
    if !ident_path(&form.subject, 3) {
        return refuse(format!(
            "`{}` is not a path subject (identifier segments, dots between)",
            form.subject
        ));
    }
    if !ident_path(&form.aspect, 1) {
        return refuse(format!("`{}` is not an aspect name", form.aspect));
    }
    // The body must be JSON, and the spliced text must not carry the
    // dollar-quote terminator — after it, further text would parse as
    // further statements, and this door writes exactly one gloss.
    let value: serde_json::Value = match serde_json::from_str(&form.body) {
        Ok(v) => v,
        Err(e) => return refuse(format!("the body is not JSON: {e}")),
    };
    let body = value.to_string();
    if body.contains("$$") {
        return refuse("a body carrying `$$` cannot ride the dollar-quoted statement".into());
    }
    let dataset = match resolve_dataset(&door, &def).await {
        Ok(dataset) => dataset,
        Err(response) => return response,
    };
    let who = crate::auth::subject(door.secret.as_ref(), &headers)
        .or_else(|| crate::auth::actor_name(&form.pinned_by).map(str::to_string));
    let actor = Actor {
        kind: ActorKind::Human,
        id: who.unwrap_or_else(|| format!("app:{}", def.name)),
    };
    let session = match door.plane.channel(actor, Some(&dataset)).await {
        Ok(session) => session,
        Err(e) => return refuse(e.to_string()),
    };
    let statement = format!("GLOSS {} ON {} AS $${body}$$;", form.aspect, form.subject);
    if let Err(e) = session.execute(&statement).await {
        // The refusal is the store speaking — unknown aspect, schema
        // mismatch, grain, speaker gate. Serve it readable.
        return refuse(e.to_string());
    }
    // An htmx post gets a bare 204: the question card retires in
    // place (its own rest state) instead of a full reload — the row
    // leaves the queue by derivation on the next natural navigation.
    // A plain form post gets a redirect to a local path.
    if headers.contains_key("hx-request") {
        return StatusCode::NO_CONTENT.into_response();
    }
    let back = if form.back.starts_with('/') && !form.back.starts_with("//") {
        form.back
    } else {
        format!("/app/{app}")
    };
    (StatusCode::SEE_OTHER, [(header::LOCATION, back)]).into_response()
}

fn refuse(error: String) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        axum::Json(serde_json::json!({ "error": error })),
    )
        .into_response()
}
