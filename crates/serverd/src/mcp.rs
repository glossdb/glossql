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
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ElicitRequest,
    ElicitRequestParams, ElicitResult, ElicitationAction, ElicitationSchema, EnumSchema,
    Implementation, InputRequest, InputRequests, InputRequiredResult, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
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
    let message = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
    let method = message
        .as_ref()
        .and_then(|v| v.get("method").and_then(|m| m.as_str()))
        .unwrap_or("-")
        .to_string();
    let is_list = method == "tools/list";
    // The wire monitor: which lifecycle each client actually speaks.
    // The negotiated version decides whether an elicitation answer has
    // a route back to the waiting handler — the session-carrying
    // lifecycle (≤ 2025-11-25) only, in rmcp 3.1.2.
    let version = message
        .as_ref()
        .and_then(|v| v.pointer("/params/protocolVersion"))
        .and_then(|p| p.as_str())
        .map(str::to_string)
        .or_else(|| {
            parts
                .headers
                .get("mcp-protocol-version")
                .and_then(|h| h.to_str().ok())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "-".into());
    let lifecycle = if parts.headers.contains_key("mcp-session-id") {
        "session"
    } else {
        "sessionless"
    };
    println!("mcp    <- {method} @{version} {lifecycle}");
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
    /// The door's knobs: the fallback agent id for calls no handshake
    /// named, the row cap, the probe flag. Human writes land as
    /// [`crate::HUMAN`] — anonymous by ruling (2026-08-13).
    doors: crate::DoorConfig,
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
        doors: crate::DoorConfig,
        brief: Arc<std::sync::RwLock<String>>,
    ) -> Self {
        GlossqlMcp { plane, doors, brief }
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
                     contested, red bands, the open queue — before acting.",
                );
                line
            }
            Err(e) => format!("Live now: the brief could not be read ({e})."),
        };
        if let Ok(mut slot) = brief.write() {
            *slot = line;
        }
    }

    /// The probe's one question, shared by both mechanisms: a dictated
    /// (subject, aspect, body) with a land-it/skip-it stance.
    fn probe_params() -> Result<ElicitRequestParams, String> {
        let schema = ElicitationSchema::builder()
            .required_string("subject")
            .required_string("aspect")
            .required_string("body")
            .required_enum_schema(
                "stance",
                EnumSchema::builder(vec!["land it".into(), "skip it".into()]).build(),
            )
            .build()
            .map_err(|e| e.to_string())?;
        Ok(ElicitRequestParams::FormElicitationParams {
            meta: None,
            message: "elicit-probe: dictate one gloss to land with human standing \
                      — subject, aspect, JSON body — or skip it."
                .into(),
            requested_schema: schema,
        })
    }

    /// What the human said, landed or noted — shared by both
    /// mechanisms. An accepting dictation lands; everything else is
    /// witnessed in the monitor note only.
    async fn digest_answer(&self, answer: ElicitResult, dataset: Option<String>) -> String {
        if answer.action != ElicitationAction::Accept {
            return format!("elicit-probe: the human chose {:?}", answer.action);
        }
        let Some(content) = answer.content else {
            return "elicit-probe: accepted without content".into();
        };
        if content.get("stance").and_then(|v| v.as_str()) != Some("land it") {
            return "elicit-probe: answered, nothing landed".into();
        }
        let fields = (
            content.get("subject").and_then(|v| v.as_str()),
            content.get("aspect").and_then(|v| v.as_str()),
            content.get("body").and_then(|v| v.as_str()),
        );
        let (Some(subject), Some(aspect), Some(body)) = fields else {
            return "elicit-probe: the answer misses subject/aspect/body".into();
        };
        match self.land_human_answer(dataset, subject, aspect, body).await {
            Ok(note) => note,
            Err(e) => format!("elicit-probe: refused: {e}"),
        }
    }

    /// The session-lifecycle mechanism (≤ 2025-11-25): a server→client
    /// `elicitation/create` on this call's own stream, the answer
    /// routed back through the transport session.
    async fn elicit_peer(
        &self,
        context: &RequestContext<RoleServer>,
        dataset: Option<String>,
    ) -> String {
        let params = match Self::probe_params() {
            Ok(params) => params,
            Err(e) => return format!("elicit-probe: schema refused: {e}"),
        };
        let asked = context
            .peer
            .create_elicitation_with_timeout(params, Some(std::time::Duration::from_secs(120)))
            .await;
        match asked {
            Ok(answer) => self.digest_answer(answer, dataset).await,
            Err(e) => format!("elicit-probe: no round-trip: {e}"),
        }
    }

    /// Land a server-witnessed answer as a HUMAN gloss on the human's
    /// own channel. The door composes the statement and the actor and
    /// decides nothing — aspect schema, grain, and the witness speaker
    /// gate all belong to the store.
    async fn land_human_answer(
        &self,
        dataset: Option<String>,
        subject: &str,
        aspect: &str,
        body: &str,
    ) -> Result<String, String> {
        if !ident_path(subject, 3) {
            return Err(format!(
                "`{subject}` is not a path subject (identifier segments, dots between)"
            ));
        }
        if !ident_path(aspect, 1) {
            return Err(format!("`{aspect}` is not an aspect name"));
        }
        // The body must be JSON, and the spliced text must not carry the
        // dollar-quote terminator — after it, further text would parse
        // as further statements, and this hook writes exactly one gloss.
        let value: serde_json::Value =
            serde_json::from_str(body).map_err(|e| format!("the body is not JSON: {e}"))?;
        let body = value.to_string();
        if body.contains("$$") {
            return Err("a body carrying `$$` cannot ride the dollar-quoted statement".into());
        }
        let Some(dataset) = dataset else {
            return Err("no dataset is bound — USE one first".into());
        };
        let actor = Actor {
            kind: ActorKind::Human,
            id: crate::HUMAN.into(),
        };
        let session = self
            .plane
            .channel(actor, Some(&dataset))
            .await
            .map_err(|e| e.to_string())?;
        session
            .execute(&format!("GLOSS {aspect} ON {subject} AS $${body}$$;"))
            .await
            .map_err(|e| e.to_string())?;
        Ok(format!(
            "elicit-probe: landed `{aspect}` on `{subject}` with human standing"
        ))
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
                self.doors.row_cap
            ),
            schema,
        )
    }
}

/// The probe's key in the MRTR `inputRequests` map, and the opaque
/// state the client must echo on its retry.
const PROBE_KEY: &str = "elicit-probe";
const PROBE_STATE: &str = "elicit-probe:v1";

/// Path subjects only: 1–3 identifier segments (`fin`, `orders`,
/// `orders.amount`). Mirrors the pin door's gate until that door
/// retires.
fn ident_path(s: &str, max_segments: usize) -> bool {
    let segments: Vec<&str> = s.split('.').collect();
    !segments.is_empty()
        && segments.len() <= max_segments
        && segments.iter().all(|seg| {
            !seg.is_empty()
                && seg
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
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

        // Actor rides the connection: the handshake names the client, and
        // that name is the agent actor for the session. `client_info()`
        // reads the per-request `_meta` stamp on the sessionless lifecycle
        // (the transport's own `peer_info()` is synthetic there) and the
        // initialize handshake on legacy sessions.
        let id = context
            .client_info()
            .map(|info| info.name)
            .unwrap_or_else(|| self.doors.agent.clone());
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

        // The elicitation spike rides ahead of execution, by the
        // mechanism the negotiated lifecycle carries. 2026-07-28+ is
        // sessionless: the ask is an MRTR `input_required` result
        // (SEP-2322) and the answer arrives on the client's retry of
        // this same call. Session lifecycles get the server→client
        // request on this call's own stream instead.
        let mut probed = None;
        if self.doors.elicit_probe {
            if let Some(responses) = &request.input_responses {
                // The retry round: `requestState` is untrusted — ours
                // is a version tag, checked, never parsed.
                let note = if request.request_state.as_deref() != Some(PROBE_STATE) {
                    "elicit-probe: a retry without the echoed state".to_string()
                } else {
                    match responses.get(PROBE_KEY).cloned() {
                        Some(raw) => match serde_json::from_value::<ElicitResult>(raw) {
                            Ok(answer) => self.digest_answer(answer, session.dataset()).await,
                            Err(e) => format!("elicit-probe: the answer does not parse: {e}"),
                        },
                        None => "elicit-probe: the retry carries no answer".into(),
                    }
                };
                println!("glossql ?? {id}: {note}");
                probed = Some(note);
            } else if context
                .client_capabilities()
                .and_then(|caps| caps.elicitation)
                .is_none()
            {
                // The capability must come from the request's own stamp
                // — the transport's peer_info is synthetic on the
                // sessionless path.
                let note = "elicit-probe: the client does not advertise elicitation";
                println!("glossql ?? {id}: {note}");
                probed = Some(note.into());
            } else if context
                .protocol_version()
                .is_some_and(|v| v >= ProtocolVersion::V_2026_07_28)
            {
                match Self::probe_params() {
                    Ok(params) => {
                        let mut asks = InputRequests::new();
                        asks.insert(
                            PROBE_KEY.into(),
                            InputRequest::Elicitation(ElicitRequest::new(params)),
                        );
                        println!("glossql ?? {id}: elicit-probe: asking (input_required round)");
                        return Ok(InputRequiredResult::new(
                            Some(asks),
                            Some(PROBE_STATE.into()),
                        )
                        .into());
                    }
                    Err(e) => {
                        let note = format!("elicit-probe: schema refused: {e}");
                        println!("glossql ?? {id}: {note}");
                        probed = Some(note);
                    }
                }
            } else {
                let note = self.elicit_peer(&context, session.dataset()).await;
                println!("glossql ?? {id}: {note}");
                probed = Some(note);
            }
        }

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
                    self.doors.row_cap
                };
                wire::stream_json(query.stream, cap)
                    .await
                    .map(|rows| serde_json::Value::Array(vec![rows]))
            }
            // Statement sequences run at the plane: `USE` selects the
            // actor's channel there, never rebinds a session.
            Err(SessionError::NotOneRead) => match self.plane.execute(actor, statements).await {
                Ok(outcomes) => wire::outcomes_json(&outcomes, self.doors.row_cap),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        };
        // The next connect's brief sees this call's writes.
        Self::refresh_brief(&self.plane, &self.brief).await;
        Ok(match rendered {
            Ok(body) => {
                let mut blocks = vec![ContentBlock::text(body.to_string())];
                if let Some(note) = probed {
                    blocks.push(ContentBlock::text(note));
                }
                CallToolResult::success(blocks)
            }
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
