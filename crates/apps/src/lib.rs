//! The app door (`/<dataset>/app`): server-rendered data apps over the
//! session plane. An app is a directory in the workspace —
//! `apps/<name>/` holding tera pages, frame queries (`frames/*.sql`),
//! and vega-lite specs (`specs/*.vl.json`) — rendered against module
//! templates and assets embedded in the binary. Pages are hypermedia
//! (htmx); data reaches the browser once per frame as Arrow IPC and
//! lives in the frame store; the URL is the only state. Everything an
//! app author — agent or human — writes is declarative: templates, SQL,
//! specs, prose. Never code.
//!
//! The dataset is the URL's first segment, so an app serves every
//! dataset in the workspace and switching is a link. Assets are not
//! dataset-scoped and hang off the workspace root.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

mod app;
mod assets;
mod builtin;
mod frames;
pub mod glossed;
mod pages;
mod remeasure;
mod rule;

pub use app::AppDef;
pub use builtin::{BUILTINS, BuiltinApp};

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use glossql_session::Plane;

/// State behind the door: the shared plane and the workspace root the
/// apps live under. Frames speak as a Human actor per app
/// (`app:<name>`) and can only read — they ride the one-query streaming
/// path.
#[derive(Clone)]
pub struct AppDoor {
    pub plane: Arc<Plane>,
    pub workspace: PathBuf,
}

/// The workspace's datasets, for the doors that must refuse a name it
/// does not hold. The URL is the binding, so an unknown one is a
/// missing resource — and the answer names what there is, because a
/// mistyped dataset should not cost a second request to recover from.
pub(crate) async fn known(door: &AppDoor) -> Vec<String> {
    door.plane.datasets().await.unwrap_or_default()
}

pub(crate) fn no_such_dataset(dataset: &str, known: &[String]) -> String {
    if known.is_empty() {
        format!("no dataset `{dataset}` — this workspace holds none yet")
    } else {
        format!(
            "no dataset `{dataset}` — this workspace holds {}",
            known.join(", ")
        )
    }
}

/// The door takes exactly TWO writes, and both are human acts: this is
/// a human door, so the gate stamps human standing on every caller
/// that reaches it (`glossql_serverd::auth`).
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
///
/// Mounted under `/{dataset}/app`; the dataset arrives in every
/// handler's path tuple.
pub fn router(plane: Arc<Plane>, workspace: PathBuf) -> Router {
    Router::new()
        .route("/", get(pages::home))
        .route("/{app}", get(pages::index))
        .route("/{app}/p/{page}", get(pages::page))
        .route("/{app}/frames/{frame}", get(frames::frame))
        .route("/{app}/specs/{spec}", get(pages::spec))
        .route("/{app}/rule", axum::routing::post(rule::rule))
        .route(
            "/{app}/remeasure",
            axum::routing::post(remeasure::remeasure),
        )
        .with_state(AppDoor { plane, workspace })
}

/// The embedded static assets, mounted at the workspace root: one copy
/// for every dataset, and one browser cache entry.
pub fn assets_router() -> Router {
    Router::new().route("/{*file}", get(assets::asset))
}

/// The workspace root: which datasets there are, and the way into each.
/// It is the one page that is not about a dataset, so it is where a
/// visitor who has just swapped a startup token for a cookie lands.
pub fn root_router(plane: Arc<Plane>, workspace: PathBuf) -> Router {
    Router::new()
        .route("/", get(pages::datasets))
        .with_state(AppDoor { plane, workspace })
}
