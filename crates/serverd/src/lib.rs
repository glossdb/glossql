//! serverd — the workspace's doors (M5): one axum listener carrying the
//! MCP shim at `/mcp` (rmcp streamable HTTP; the 2026-07-28 protocol
//! revision serves it statelessly, so session continuity lives in the
//! [`Plane`]) and the cockpit's Arrow IPC query door at `/query`.
//! Flight SQL is a future door, cut from M5 (project lead, 2026-08-04):
//! pyarrow reads the same HTTP stream.

mod bootstrap;
mod mcp;
mod query;
mod wire;

pub use bootstrap::bootstrap;
pub use glossql_session::Plane;
pub use mcp::GlossqlMcp;
pub use query::ARROW_STREAM;
pub use wire::DEFAULT_ROW_CAP;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::post;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

/// Who the doors speak as, and how much an agent sees at once.
#[derive(Clone)]
pub struct DoorConfig {
    /// Fallback agent actor id for MCP calls no initialize named.
    pub agent: String,
    /// The human actor id behind `/query` — the UI door speaks as one
    /// person until governance opens (held open, SPEC.md §9).
    pub human: String,
    /// Rows an MCP tool result ships before declaring `truncated`.
    pub row_cap: usize,
}

impl Default for DoorConfig {
    fn default() -> Self {
        DoorConfig {
            agent: "agent".into(),
            human: "cockpit".into(),
            row_cap: DEFAULT_ROW_CAP,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub plane: Arc<Plane>,
    pub human: String,
    pub row_cap: usize,
}

/// The doors: `/mcp` (agent), `/query` (Arrow IPC), `/app` (the
/// server-rendered app door; `workspace` is where its apps live).
pub fn router(plane: Arc<Plane>, doors: DoorConfig, workspace: PathBuf) -> Router {
    let mcp_plane = Arc::clone(&plane);
    let app_plane = Arc::clone(&plane);
    let agent = doors.agent;
    let row_cap = doors.row_cap;
    // Plain JSON answers; nothing here streams partial results.
    let mut config = StreamableHttpServerConfig::default();
    config.json_response = true;
    let mcp = StreamableHttpService::new(
        move || Ok(GlossqlMcp::new(Arc::clone(&mcp_plane), agent.clone(), row_cap)),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    Router::new()
        .route("/query", post(query::query))
        .with_state(AppState {
            plane,
            human: doors.human,
            row_cap: doors.row_cap,
        })
        .nest("/app", glossql_apps::router(app_plane, workspace))
        .nest_service("/mcp", mcp)
}
