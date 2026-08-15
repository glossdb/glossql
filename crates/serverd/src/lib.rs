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

/// The one anonymous human actor every door speaks for (ruled
/// 2026-08-13): human standing is unsigned for now — human > agent >
/// function holds, and how to identify *which* human is found out
/// later, not faked by a flag.
pub const HUMAN: &str = "human";

/// Who the doors speak as, and how much an agent sees at once.
#[derive(Clone)]
pub struct DoorConfig {
    /// Fallback agent actor id for MCP calls no initialize named.
    pub agent: String,
    /// Rows an MCP tool result ships before declaring `truncated`.
    pub row_cap: usize,
}

impl Default for DoorConfig {
    fn default() -> Self {
        DoorConfig {
            agent: "agent".into(),
            row_cap: DEFAULT_ROW_CAP,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub plane: Arc<Plane>,
    pub row_cap: usize,
}

/// The doors: `/mcp` (agent), `/query` (Arrow IPC), `/app` (the
/// server-rendered app door; `workspace` is where its apps live).
pub fn router(plane: Arc<Plane>, doors: DoorConfig, workspace: PathBuf) -> Router {
    let mcp_plane = Arc::clone(&plane);
    let app_plane = Arc::clone(&plane);
    let mcp_doors = doors.clone();
    // Plain JSON answers; a mid-call elicitation still falls back to an
    // SSE answer on that POST.
    let mut config = StreamableHttpServerConfig::default();
    config.json_response = true;
    // The connect-time brief: shared across handler instances, boot-
    // filled, refreshed after every tool call (see mcp::refresh_brief).
    // It carries its own facts and per-actor delivery state.
    let brief = Arc::new(mcp::Brief::default());
    {
        let plane = Arc::clone(&plane);
        let brief = Arc::clone(&brief);
        tokio::spawn(async move { GlossqlMcp::refresh_brief(&plane, &brief).await });
    }
    // Declined questions defer for the server run — transport state,
    // shared across the per-request handler instances.
    let deferred = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    // One question in flight at a time, shared the same way: the ask no
    // longer blocks the call that carried it, so this is what keeps the
    // next read from asking again while a form is still on screen.
    let asking = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let mcp = StreamableHttpService::new(
        move || {
            Ok(GlossqlMcp::new(
                Arc::clone(&mcp_plane),
                mcp_doors.clone(),
                Arc::clone(&brief),
                Arc::clone(&deferred),
                Arc::clone(&asking),
            ))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );
    // The revision's list-caching fields ride in from the door until the
    // library models them (see mcp::amend_tools_list).
    let mcp = Router::new()
        .fallback_service(mcp)
        .layer(axum::middleware::from_fn(mcp::amend_tools_list));
    Router::new()
        .route("/query", post(query::query))
        .with_state(AppState {
            plane,
            row_cap: doors.row_cap,
        })
        .nest(
            "/app",
            glossql_apps::router(app_plane, workspace, HUMAN.into()),
        )
        .nest_service("/mcp", mcp)
}
