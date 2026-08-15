//! The app door (`/app`): server-rendered data apps over the session
//! plane. An app is a directory in the workspace — `apps/<name>/`
//! holding tera pages, frame queries (`frames/*.sql`), and vega-lite
//! specs (`specs/*.vl.json`) — rendered against module templates and
//! assets embedded in the binary. Pages are hypermedia (htmx); data
//! reaches the browser once per frame as Arrow IPC and lives in the
//! frame store; the URL is the only state. Everything an app author —
//! agent or human — writes is declarative: templates, SQL, specs,
//! prose. Never code.

mod app;
mod assets;
mod builtin;
mod frames;
pub mod glossed;
mod pages;
mod rule;

pub use app::AppDef;
pub use builtin::{BUILTINS, BuiltinApp};

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use glossql_session::Plane;

/// State behind the door: the shared plane, the workspace root the apps
/// live under, and the id a human writes under. Frames speak as a Human
/// actor per app (`app:<name>`) and can only read — they ride the
/// one-query streaming path. The door assumes its `/app` mount; asset
/// and page URLs in the templates are absolute against it.
#[derive(Clone)]
pub struct AppDoor {
    pub plane: Arc<Plane>,
    pub workspace: PathBuf,
    /// Who a ruling is written as. Anonymous by ruling (2026-08-13):
    /// standing comes from the server having witnessed the act, not
    /// from an identity it cannot check.
    pub human: String,
}

/// The door takes exactly ONE write, and it is a ruling.
///
/// Every other affordance retired with the pins, and the reason holds:
/// a page that can change the record invites a second way to say
/// everything the language already says. A ruling is the exception
/// because it is the one thing only a person can supply, its shape is
/// fixed (a stance on a claim the workspace already derived), and the
/// alternative is worse — run 4 found that a human who steps away has
/// no way back into the record at all, since the MCP round can only
/// ask while they are watching and an agent may never speak for them.
/// The docket is already the page of open questions; answering there
/// is the gesture the page was drawn for.
pub fn router(plane: Arc<Plane>, workspace: PathBuf, human: String) -> Router {
    Router::new()
        .route("/", get(pages::home))
        .route("/assets/{*file}", get(assets::asset))
        .route("/{app}", get(pages::index))
        .route("/{app}/p/{page}", get(pages::page))
        .route("/{app}/frames/{frame}", get(frames::frame))
        .route("/{app}/specs/{spec}", get(pages::spec))
        .route("/{app}/rule", axum::routing::post(rule::rule))
        .with_state(AppDoor {
            plane,
            workspace,
            human,
        })
}
