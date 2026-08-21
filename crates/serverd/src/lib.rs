//! serverd — the workspace's doors (M5): one axum listener carrying the
//! MCP shim at `/<dataset>/mcp` (rmcp streamable HTTP; the 2026-07-28
//! protocol revision serves it statelessly) and the cockpit's Arrow IPC
//! query door at `/<dataset>/query`. Flight SQL is a future door:
//! pyarrow reads the same HTTP stream.
//!
//! **The dataset is the resource; the doors are protocols over it.**
//! A call arrives already bound — no door keeps a cursor, so a restart
//! cannot lose one and two callers on two datasets cannot steer each
//! other. `USE` inside a call moves the statements after it and expires
//! with the call. A dataset that does not exist 404s on `/query` and
//! `/app`; over `/mcp` it is where an agent declares it, which is what
//! makes the URL an intent rather than a lookup.
//!
//! Every door is behind one gate ([`auth`]): a bearer token, verified
//! against a public key, whose claims say who is speaking and with
//! which standing.

mod auth;
mod bootstrap;
mod mcp;
mod query;
mod wire;

pub use auth::{Gate, Handout, hand_out};
pub use bootstrap::bootstrap;
pub use glossql_session::Plane;
pub use mcp::GlossqlMcp;
pub use query::ARROW_STREAM;
pub use wire::DEFAULT_ROW_CAP;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

/// Who a request with no token writes under, on the doors that write as
/// a human. Reachable only while the server runs without
/// `--require-token`; a verified caller is themselves.
pub const HUMAN: &str = glossql_apps::ANONYMOUS;

/// Who the doors speak as when nothing proves otherwise, and how much
/// an agent sees at once.
#[derive(Clone)]
pub struct DoorConfig {
    /// Fallback agent actor id for untokened MCP calls.
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

/// The doors. `/` is the workspace — which datasets there are;
/// everything else hangs off one of them.
pub fn router(plane: Arc<Plane>, doors: DoorConfig, workspace: PathBuf, gate: Arc<Gate>) -> Router {
    let mcp_plane = Arc::clone(&plane);
    let app_plane = Arc::clone(&plane);
    let root_plane = Arc::clone(&plane);
    let mcp_doors = doors.clone();
    // Plain JSON answers; a mid-call elicitation still falls back to an
    // SSE answer on that POST.
    let mut config = StreamableHttpServerConfig::default();
    config.json_response = true;
    // The connect-time brief: shared across handler instances, boot-
    // filled, refreshed after every writing call (see
    // mcp::refresh_brief). One shared baseline, no per-actor state.
    let brief = Arc::new(mcp::Brief::default());
    {
        let plane = Arc::clone(&plane);
        let brief = Arc::clone(&brief);
        tokio::spawn(async move { GlossqlMcp::refresh_brief(&plane, &brief).await });
    }
    let mcp = StreamableHttpService::new(
        move || {
            Ok(GlossqlMcp::new(
                Arc::clone(&mcp_plane),
                mcp_doors.clone(),
                Arc::clone(&brief),
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
    let metadata = {
        let gate = Arc::clone(&gate);
        move || {
            let gate = Arc::clone(&gate);
            async move { axum::Json(gate.metadata()) }
        }
    };
    Router::new()
        .merge(glossql_apps::root_router(root_plane, workspace.clone()))
        .route("/.well-known/oauth-protected-resource", get(metadata))
        .nest("/assets", glossql_apps::assets_router())
        .route(
            "/{dataset}/query",
            post(query::query).with_state(AppState {
                plane,
                row_cap: doors.row_cap,
            }),
        )
        .nest("/{dataset}/app", glossql_apps::router(app_plane, workspace))
        .nest_service("/{dataset}/mcp", mcp)
        // One gate above every door: it runs while the URI is still the
        // one the client sent, which is the only place `/mcp` can be
        // told which dataset it was called on.
        .layer(axum::middleware::from_fn_with_state(gate, auth::gate))
}
