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
//! against the issuer's published keys, says who is speaking; the door
//! it came through says with which standing.

mod auth;
mod bootstrap;
mod login;
mod mcp;
mod query;
mod wire;

pub use auth::{Endpoints, Gate};
pub use bootstrap::bootstrap;
pub use glossql_session::Plane;
pub use login::Login;
pub use mcp::GlossqlMcp;
pub use query::ARROW_STREAM;
pub use wire::DEFAULT_ROW_CAP;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use glossql_glossary::ActorKind;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

/// The server's own hand: the actor the shipped system is bootstrapped
/// under, and the one the door reads with when a read needs a channel
/// and no request is behind it. Human standing, so the shipped kit
/// outranks what an agent later glosses on the same key. Never a
/// request's actor — every request carries its own subject.
pub const BOOTSTRAP: &str = "human";

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
/// everything else hangs off one of them. The login carries the gate
/// every door verifies with.
pub fn router(
    plane: Arc<Plane>,
    doors: DoorConfig,
    workspace: PathBuf,
    login: Arc<Login>,
) -> Router {
    let gate = Arc::clone(login.gate());
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
    // One gate, instantiated per door with that door's standing: the
    // agent door stamps agent, the human doors stamp human. Identity is
    // read the same way at every door; only the kind differs, and it is
    // the door's to say (SPEC.md §1, the actor rides the transport).
    let human = Router::new()
        .merge(glossql_apps::root_router(root_plane, workspace.clone()))
        .route(
            "/{dataset}/query",
            post(query::query).with_state(AppState {
                plane,
                row_cap: doors.row_cap,
            }),
        )
        .nest("/{dataset}/app", glossql_apps::router(app_plane, workspace))
        .layer(axum::middleware::from_fn_with_state(
            (Arc::clone(&gate), ActorKind::Human),
            auth::gate,
        ));
    let agent =
        Router::new()
            .nest_service("/mcp", mcp)
            .layer(axum::middleware::from_fn_with_state(
                (Arc::clone(&gate), ActorKind::Agent),
                auth::gate,
            ));
    let metadata = move || {
        let gate = Arc::clone(&gate);
        async move { axum::Json(gate.metadata()) }
    };
    Router::new()
        .merge(human)
        .merge(agent)
        // Outside the gate, all three: the login is where a browser
        // goes to get a token; the assets are the app's own script and
        // styles and hold no data; and the discovery document is where
        // a client learns how to authenticate — a document that
        // answered 401 would point the client at itself. The document
        // answers at the root and under any path (RFC 9728 §3.1 forms
        // the well-known URI from the resource's path, and a client
        // given `…/mcp` asks for `…/oauth-protected-resource/mcp`
        // first); there is one resource here, so one document.
        .merge(login::router(login))
        .nest("/assets", glossql_apps::assets_router())
        .route(
            "/.well-known/oauth-protected-resource",
            get(metadata.clone()),
        )
        .route(
            "/.well-known/oauth-protected-resource/{*path}",
            get(metadata),
        )
}
