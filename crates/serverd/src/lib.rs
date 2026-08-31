// The MCP handler's futures reach through the session into the store
// and from there into iceberg's own internals; proving them `Send`
// descends further than the default limit allows.
#![recursion_limit = "256"]

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
//! it came through says with which standing. The one way around the
//! gate is explicit ([`Access::Open`], the `GLOSSQL_INSECURE_OPEN`
//! switch): every caller becomes [`INSECURE_DEV_MODE`], the door still
//! saying with which standing.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

mod auth;
mod bootstrap;
mod login;
mod mcp;
mod query;
pub mod skills;
pub mod telemetry;
pub mod tls;
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
use glossql_glossary::{Actor, ActorKind};
use glossql_session::Caller;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use tower_http::trace::TraceLayer;

/// The server's own hand: the actor the shipped system is bootstrapped
/// under, and the one the door reads with when a read needs a channel
/// and no request is behind it. Human standing, because a person
/// decided what ships, so the shipped kit outranks what an agent later
/// glosses on the same key; the id names the mechanism, so a
/// `GLOSSARY()` reader sees a gloss came with the system rather than
/// from anyone at this workspace. Never a request's actor — every
/// request carries its own subject, the token's.
pub const BOOTSTRAP: &str = "bootstrap";

/// The other well-known actor id, [`BOOTSTRAP`]'s counterpart: what
/// every caller becomes when the doors are served open
/// ([`Access::Open`]). The name is the warning — a gloss carrying it
/// says on the record that nobody was verified.
pub const INSECURE_DEV_MODE: &str = "insecure_dev_mode";

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

/// Who may speak at the doors: verified by the gate the login carries,
/// or — by the explicit `GLOSSQL_INSECURE_OPEN=true` switch — anyone,
/// every caller stamped [`INSECURE_DEV_MODE`] with the door's
/// standing.
pub enum Access {
    /// Every door verifies with the gate the login carries.
    Gated(Arc<Login>),
    /// No gate — the dev arrangement the switch's name warns about.
    Open,
}

/// The doors. `/` is the workspace — which datasets there are;
/// everything else hangs off one of them.
pub fn router(plane: Arc<Plane>, doors: DoorConfig, workspace: PathBuf, access: Access) -> Router {
    let mcp_plane = Arc::clone(&plane);
    let app_plane = Arc::clone(&plane);
    let root_plane = Arc::clone(&plane);
    let mcp_doors = doors.clone();
    // The door speaks 2026-07-28 first and serves every revision the
    // library carries beneath it (2025-11-25 today) by negotiation —
    // statelessly for all of them: `legacy_session_mode: false` means
    // no `Mcp-Session-Id` minted or echoed, no GET stream, no DELETE,
    // no resumability, whatever revision a caller negotiated. It is
    // also what puts `json_response` in play — the library honours it
    // only off the session path.
    //
    // A request without the per-request version marker is served at
    // the server's own revision rather than refused
    // (`stateless_protocol_metadata_required: false`): the spec has
    // the server assume a default when the header is absent, and the
    // clients that omit it are real — ChatGPT's stamps some startup
    // requests and not others, stdio bridges stamp none. A client
    // that stamps every request (Claude Code) is unaffected.
    let mut config = StreamableHttpServerConfig::default();
    config.json_response = true;
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = false;
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
    // The doors, standing by kind: the agent door stamps agent, the
    // human doors stamp human. Identity is read the same way at every
    // door; only the kind differs, and it is the door's to say
    // (SPEC.md §1, the actor rides the transport).
    let human = Router::new()
        .merge(glossql_apps::root_router(root_plane, workspace.clone()))
        .route(
            "/{dataset}/query",
            post(query::query).with_state(AppState {
                plane,
                row_cap: doors.row_cap,
            }),
        )
        .nest("/{dataset}/app", glossql_apps::router(app_plane, workspace));
    let agent = Router::new().nest_service("/mcp", mcp);
    let routes = match access {
        Access::Gated(login) => {
            // One gate, instantiated per door with that door's standing.
            let gate = Arc::clone(login.gate());
            let human = human.layer(axum::middleware::from_fn_with_state(
                (Arc::clone(&gate), ActorKind::Human),
                auth::gate,
            ));
            let agent = agent.layer(axum::middleware::from_fn_with_state(
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
                // Outside the gate, both: the login is where a browser
                // goes to get a token, and the discovery document is
                // where a client learns how to authenticate — a
                // document that answered 401 would point the client at
                // itself. The document answers at the root and under
                // any path (RFC 9728 §3.1 forms the well-known URI
                // from the resource's path, and a client given `…/mcp`
                // asks for `…/oauth-protected-resource/mcp` first);
                // there is one resource here, so one document.
                .merge(login::router(login))
                .route(
                    "/.well-known/oauth-protected-resource",
                    get(metadata.clone()),
                )
                .route(
                    "/.well-known/oauth-protected-resource/{*path}",
                    get(metadata),
                )
        }
        // Open: the same doors, a fixed stamp in place of the gate. No
        // login and no discovery document — with no 401 to answer, a
        // client is never sent to authenticate.
        Access::Open => {
            let stamp = |kind| {
                axum::Extension(Caller(Actor {
                    kind,
                    id: INSECURE_DEV_MODE.into(),
                }))
            };
            Router::new()
                .merge(human.layer(stamp(ActorKind::Human)))
                .merge(agent.layer(stamp(ActorKind::Agent)))
        }
    };
    routes
        // The assets are the app's own script and styles and hold no
        // data — outside the gate in either arrangement.
        .nest("/assets", glossql_apps::assets_router())
        // The request span, outermost: every door, the gate included,
        // works inside it. tower-http's own layer; what the span holds
        // is telemetry's to say.
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(telemetry::request_span)
                .on_request(())
                .on_response(telemetry::request_done),
        )
}
