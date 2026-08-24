//! serverd — the workspace's doors (M5): one axum listener carrying the
//! MCP shim at `/mcp` (rmcp streamable HTTP, 2026-07-28 and nothing
//! behind it) and the cockpit's Arrow IPC query door at
//! `/<dataset>/query`. Flight SQL is a future door: pyarrow reads the
//! same HTTP stream.
//!
//! **The two door kinds scope differently, because their callers do.**
//! A browser is pointed at a dataset and stays there, so `/query` and
//! `/app` carry it in the path — a link someone can share. An agent is
//! pointed at a workspace, so `/mcp` is one endpoint and the dataset
//! arrives in the statements, as `USE`. That is the shape every
//! database MCP server converges on, because the protocol has no
//! session to hold it: ClickHouse names the database in the SQL,
//! Snowflake locks it at configuration time, and the spec's own
//! example takes it as a tool argument.
//!
//! No door keeps a cursor, so a restart cannot lose one and two callers
//! on two datasets cannot steer each other. `USE` moves the statements
//! after it and expires with the call. A dataset that does not exist
//! 404s on `/query` and `/app`; over `/mcp` it is where an agent
//! declares it.
//!
//! Every door is behind one gate ([`auth`]): a bearer token, verified
//! against a public key, whose claims say who is speaking and with
//! which standing.

mod auth;
mod bootstrap;
mod mcp;
mod query;
mod wire;

pub use auth::Gate;
pub use bootstrap::bootstrap;
pub use glossql_session::Plane;
pub use mcp::GlossqlMcp;
pub use query::ARROW_STREAM;
pub use wire::DEFAULT_ROW_CAP;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

/// Who a request with no token writes under, on the doors that write as
/// a human. Reachable only while the server runs without
/// `--require-token`; a verified caller is themselves.
pub const HUMAN: &str = glossql_apps::ANONYMOUS;

/// The same for the doors that write as an agent. A name a caller picks
/// for itself is not a proof, so an untokened call writes under one
/// constant rather than under whatever it called itself: the record
/// then says plainly that nothing was verified, instead of carrying a
/// string that reads like an identity and is not one.
pub const AGENT: &str = "agent";

/// How much an agent sees at once.
#[derive(Clone)]
pub struct DoorConfig {
    /// Rows an MCP tool result ships before declaring `truncated`.
    pub row_cap: usize,
}

impl Default for DoorConfig {
    fn default() -> Self {
        DoorConfig {
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
pub fn router(
    plane: Arc<Plane>,
    doors: DoorConfig,
    workspace: PathBuf,
    gate: Option<Arc<Gate>>,
) -> Router {
    let mcp_plane = Arc::clone(&plane);
    let app_plane = Arc::clone(&plane);
    let root_plane = Arc::clone(&plane);
    let mcp_doors = doors.clone();
    // One revision, 2026-07-28, and nothing behind it. Sessions were
    // removed there (SEP-2567), so `legacy_session_mode: false` is the
    // whole of it: no `Mcp-Session-Id` minted or echoed, no GET stream,
    // no DELETE, no resumability. It is also what puts `json_response`
    // in play — the library honours it only off the session path.
    // `stateless_protocol_metadata_required` then holds a caller to the
    // per-request metadata this revision requires, so a request that
    // omits it is refused rather than read as an older one's.
    let mut config = StreamableHttpServerConfig::default();
    config.json_response = true;
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = true;
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
        Arc::new(NeverSessionManager::default()),
        config,
    );
    let doors_router = Router::new()
        .merge(glossql_apps::root_router(root_plane, workspace.clone()))
        .nest("/assets", glossql_apps::assets_router())
        .route(
            "/{dataset}/query",
            post(query::query).with_state(AppState {
                plane,
                row_cap: doors.row_cap,
            }),
        )
        .nest("/{dataset}/app", glossql_apps::router(app_plane, workspace))
        .nest_service("/mcp", mcp);
    // No public key, no gate: there is nothing to verify against, and a
    // resource-metadata document with no authorization server behind it
    // would point a client at nowhere. The doors then write as they did
    // before tokens existed.
    let Some(gate) = gate else {
        return doors_router;
    };
    let metadata = {
        let gate = Arc::clone(&gate);
        move || {
            let gate = Arc::clone(&gate);
            async move { axum::Json(gate.metadata()) }
        }
    };
    doors_router
        // One gate above every door, so identity is read the same way
        // for all of them.
        .layer(axum::middleware::from_fn_with_state(gate, auth::gate))
        // Added after the layer, so it stays outside it: a 401 names
        // this document as where to learn how to authenticate, and a
        // document that answers 401 points the client at itself. axum
        // applies a layer to the routes declared before the call.
        .route("/.well-known/oauth-protected-resource", get(metadata))
}
