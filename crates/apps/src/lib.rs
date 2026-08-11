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
mod pages;

pub use app::AppDef;
pub use builtin::{BUILTINS, BuiltinApp};

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use glossql_session::Plane;

/// State behind the door: the shared plane and the workspace root the
/// apps live under. Frames speak as a Human actor per app
/// (`app:<name>`) and can only read — they ride the one-query
/// streaming path. The door assumes its `/app` mount; asset and page
/// URLs in the templates are absolute against it.
#[derive(Clone)]
pub struct AppDoor {
    pub plane: Arc<Plane>,
    pub workspace: PathBuf,
}

pub fn router(plane: Arc<Plane>, workspace: PathBuf) -> Router {
    Router::new()
        .route("/", get(pages::home))
        .route("/assets/{*file}", get(assets::asset))
        .route("/{app}", get(pages::index))
        .route("/{app}/p/{page}", get(pages::page))
        .route("/{app}/frames/{frame}", get(frames::frame))
        .route("/{app}/specs/{spec}", get(pages::spec))
        .with_state(AppDoor { plane, workspace })
}
