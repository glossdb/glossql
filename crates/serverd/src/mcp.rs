//! The MCP shim: one door, one tool. `glossql` takes statements and
//! returns outcomes — the door tells, skills teach. Everything an agent
//! must *learn* (grammar, rhai authoring, flows) ships as skills;
//! everything live (declared functions, the glossary, the tables) is
//! read through the language itself, where it is always current.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};

use glossql_glossary::{Actor, ActorKind};
use glossql_session::SessionError;

use crate::wire;
use glossql_session::Plane;

const INSTRUCTIONS: &str = "glossql workspace server. One tool: `glossql` executes \
statements — declarations, USE, GLOSS, extraction, probes, and plain SQL. Live state \
(datasets, functions, aspects, witnesses, sources, glossary, cache, imports) reads as \
plain tables through the tool. The glossql skills teach the grammar and the flows; \
start with SELECT * FROM datasets, then USE <dataset>.";

/// Requests and tools/list responses on this door stay small; tool-call
/// responses can be large and are never buffered here.
const MCP_BODY_CAP: usize = 16 * 1024 * 1024;

/// The 2026-07-28 revision's tools/list result carries list-caching
/// metadata — `ttlMs` (number) and `cacheScope` (`"public" | "private"`)
/// — beside the SEP-2322 `resultType` discriminator. rmcp 3.1.2 models
/// the discriminator but not the caching fields, and a client on this
/// revision validates all three (Claude Code, observed 2026-08-12:
/// `ttlMs` "expected number", `cacheScope` "expected public|private",
/// and an omitted `resultType` refused with "the absent-means-complete
/// bridge applies only to earlier-revision servers"). Until the library
/// carries them, the door injects what is true of this server: the tool
/// list is static per process (an hour's TTL) and workspace-local
/// (private). Only tools/list responses are buffered — everything else
/// passes through untouched.
pub async fn amend_tools_list(request: Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MCP_BODY_CAP).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return Response::builder()
                .status(axum::http::StatusCode::PAYLOAD_TOO_LARGE)
                .body(Body::from(format!("request body: {e}")))
                .expect("static response");
        }
    };
    let is_list = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| {
            v.get("method")
                .and_then(|m| m.as_str())
                .map(|m| m == "tools/list")
        })
        .unwrap_or(false);
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    if !is_list {
        return response;
    }
    let json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
    if !json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, MCP_BODY_CAP).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("response body: {e}")))
                .expect("static response");
        }
    };
    let amended = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|mut message| {
            let result = message.get_mut("result")?.as_object_mut()?;
            if !result.contains_key("tools") {
                return None;
            }
            result
                .entry("ttlMs")
                .or_insert(serde_json::json!(3_600_000));
            result
                .entry("cacheScope")
                .or_insert(serde_json::json!("private"));
            serde_json::to_vec(&message).ok()
        });
    let body = match amended {
        Some(body) => body,
        None => bytes.to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(body))
}

#[derive(Clone)]
pub struct GlossqlMcp {
    plane: Arc<Plane>,
    /// Who speaks when no initialize identifies the client — the
    /// stateless transport serves tool calls without one.
    fallback: String,
    row_cap: usize,
    /// The connect-time brief (ruled 2026-08-12, delivery option B):
    /// one composed line over live counts, appended to the
    /// instructions every initialize/discover serves. Shared across
    /// the per-session handler instances; refreshed after every tool
    /// call, so a connect after activity reads current state.
    brief: Arc<std::sync::RwLock<String>>,
}

impl GlossqlMcp {
    pub fn new(
        plane: Arc<Plane>,
        fallback: String,
        row_cap: usize,
        brief: Arc<std::sync::RwLock<String>>,
    ) -> Self {
        GlossqlMcp {
            plane,
            fallback,
            row_cap,
            brief,
        }
    }

    /// Recompose the brief from the store. Cheap (two counts), awaited
    /// at the end of every tool call and once at boot.
    pub async fn refresh_brief(plane: &Plane, brief: &std::sync::RwLock<String>) {
        let line = match plane.store().brief_counts().await {
            Ok(counts) => {
                let mut line = format!(
                    "Live now: {} human writing{} stand{}",
                    counts.human_writings,
                    if counts.human_writings == 1 { "" } else { "s" },
                    match &counts.latest_human_at {
                        Some(at) => format!(" (latest {at})"),
                        None => String::new(),
                    },
                );
                if counts.approvals_pending > 0 {
                    line.push_str(&format!(
                        "; {} approved recipe change{} await the re-declare",
                        counts.approvals_pending,
                        if counts.approvals_pending == 1 { "" } else { "s" },
                    ));
                }
                line.push_str(
                    ". Start with the brief the glossql skill teaches — human slots, \
                     contested, red bands, the open agenda — before acting.",
                );
                line
            }
            Err(e) => format!("Live now: the brief could not be read ({e})."),
        };
        if let Ok(mut slot) = brief.write() {
            *slot = line;
        }
    }

    fn tool(&self) -> Tool {
        let serde_json::Value::Object(schema) = serde_json::json!({
            "type": "object",
            "properties": {
                "statements": {
                    "type": "string",
                    "description": "glossql statements, `;`-separated"
                }
            },
            "required": ["statements"]
        }) else {
            unreachable!("literal object")
        };
        Tool::new(
            "glossql",
            format!(
                "Execute glossql statements against the workspace and return their outcomes. \
                 Row output is capped at {} rows; `truncated` says when a result held more. \
                 Metadata reads — GLOSSARY(), ATTEST(), and the store relations — sent as \
                 their own single statement are uncapped.",
                self.row_cap
            ),
            schema,
        )
    }
}

impl ServerHandler for GlossqlMcp {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("glossql-serverd", env!("CARGO_PKG_VERSION"));
        let brief = self
            .brief
            .read()
            .map(|line| line.clone())
            .unwrap_or_default();
        info.instructions = Some(format!("{INSTRUCTIONS}\n\n{brief}"));
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult {
            tools: vec![self.tool()],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        if request.name != "glossql" {
            return Err(McpError::invalid_params(
                format!("unknown tool `{}`", request.name),
                None,
            ));
        }
        let arguments = request.arguments.unwrap_or_default();
        let statements = arguments
            .get("statements")
            .and_then(|v| v.as_str())
            .ok_or_else(|| McpError::invalid_params("`statements` (string) is required", None))?;

        // Actor rides the connection: the initialize handshake names the
        // client, and that name is the agent actor for the session.
        let id = context
            .peer
            .peer_info()
            .map(|info| info.client_info.name.clone())
            .unwrap_or_else(|| self.fallback.clone());
        // The monitor line: what the agent actually sends, as it sends it.
        println!("glossql <- {id}: {statements}");
        let actor = Actor {
            kind: ActorKind::Agent,
            id: id.clone(),
        };
        let session = self
            .plane
            .session(actor.clone())
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        // A single query streams from the engine and stops at the cap —
        // what the agent won't see is never computed. Metadata reads
        // (GLOSSARY(), ATTEST(), the store relations) are exempt from
        // the cap (project lead, 2026-08-04): the map must be whole,
        // and the store bounds it. Everything else runs through execute.
        let rendered = match session.query_stream(statements).await {
            Ok(query) => {
                let cap = if query.metadata_only {
                    usize::MAX
                } else {
                    self.row_cap
                };
                wire::stream_json(query.stream, cap)
                    .await
                    .map(|rows| serde_json::Value::Array(vec![rows]))
            }
            // Statement sequences run at the plane: `USE` selects the
            // actor's channel there, never rebinds a session.
            Err(SessionError::NotOneRead) => match self.plane.execute(actor, statements).await {
                Ok(outcomes) => wire::outcomes_json(&outcomes, self.row_cap),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        };
        // The next connect's brief sees this call's writes.
        Self::refresh_brief(&self.plane, &self.brief).await;
        Ok(match rendered {
            Ok(body) => CallToolResult::success(vec![ContentBlock::text(body.to_string())]),
            // A failed statement is the agent's business, not the
            // transport's: an error result, never a protocol error.
            Err(e) => {
                println!("glossql !! {id}: {e}");
                CallToolResult::error(vec![ContentBlock::text(e)])
            }
        }
        .into())
    }
}
