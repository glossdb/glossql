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
use glossql_session::{Plane, Session, SessionError};

use crate::wire;

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
    /// named, the row cap. Human writes land as [`crate::HUMAN`] —
    /// anonymous by ruling (2026-08-13).
    doors: crate::DoorConfig,
    /// The connect-time brief (ruled 2026-08-12, delivery option B):
    /// one composed line over live counts, appended to the
    /// instructions every initialize/discover serves. Shared across
    /// the per-session handler instances; refreshed after every tool
    /// call, so a connect after activity reads current state.
    brief: Arc<std::sync::RwLock<String>>,
    /// Questions the human declined this server run — transport
    /// state, never the store (no ledger, ruled 2026-08-13). A
    /// landing clears nothing here: a landed slot stops deriving.
    deferred: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl GlossqlMcp {
    pub fn new(
        plane: Arc<Plane>,
        doors: crate::DoorConfig,
        brief: Arc<std::sync::RwLock<String>>,
        deferred: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ) -> Self {
        GlossqlMcp {
            plane,
            doors,
            brief,
            deferred,
        }
    }

    /// Recompose the brief from the store and the question derivation.
    /// Cheap (four bounded reads), awaited at the end of every tool
    /// call and once at boot.
    pub async fn refresh_brief(plane: &Plane, brief: &std::sync::RwLock<String>) {
        let questions = open_question_count(plane).await.unwrap_or(0);
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
                if questions > 0 {
                    line.push_str(&format!(
                        "; {} judgment question{} stand{} open for the human (assumptions \
                         below full confidence — conventions and definitions, never \
                         statistics) — sweep the round (call the tool until it stays \
                         quiet) or relay them in chat",
                        questions,
                        if questions == 1 { "" } else { "s" },
                        if questions == 1 { "s" } else { "" },
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

    /// The next open question the workspace derives — judged
    /// assumptions below full confidence on the winning slot, lowest
    /// confidence first. Judgment only, never statistics (ruled
    /// 2026-08-13): a claim a measurement can settle — behavior, unit,
    /// role — is the agent's work through the shipped functions, and
    /// the door never asks the human for it. A workspace with no
    /// dataset bound (or nothing open) derives nothing, and the round
    /// stays silent.
    async fn derive_question(&self, session: &Session, skip_deferred: bool) -> Option<Question> {
        let deferred = self.deferred.lock().expect("deferred lock").clone();
        let loose = read_rows(session, LOOSE_SQL).await;
        if let Err(e) = &loose {
            println!("glossql ?? question-round: the loose derivation failed: {e}");
        }
        if let Ok(rows) = loose {
            for row in rows {
                let fields = (
                    row["subject"].as_str(),
                    row["aspect"].as_str(),
                    row["idx"].as_u64(),
                    row["assumption"].as_str(),
                );
                let (Some(subject), Some(aspect), Some(idx), Some(assumption)) = fields else {
                    continue;
                };
                let key = format!("loose:{subject}:{aspect}:{idx}");
                if skip_deferred && deferred.contains(&key) {
                    continue;
                }
                return Some(Question::Loose {
                    subject: subject.into(),
                    aspect: aspect.into(),
                    idx,
                    dimension: row["dimension"].as_str().unwrap_or("-").into(),
                    assumption: assumption.into(),
                    confidence: row["conf"].as_f64().unwrap_or(0.0),
                });
            }
        }
        None
    }

    /// The retry is stateless, so re-derivation is the only trust: an
    /// answer lands only if its question still derives.
    async fn question_for_key(&self, session: &Session, key: &str) -> Option<Question> {
        // Walk the live derivation for the key rather than trusting
        // the echoed shape.
        let mut probe = self.derive_all(session).await;
        probe.retain(|q| q.key() == key);
        probe.pop()
    }

    async fn derive_all(&self, session: &Session) -> Vec<Question> {
        let mut all = Vec::new();
        if let Ok(rows) = read_rows(session, LOOSE_SQL).await {
            for row in rows {
                let fields = (
                    row["subject"].as_str(),
                    row["aspect"].as_str(),
                    row["idx"].as_u64(),
                    row["assumption"].as_str(),
                );
                if let (Some(subject), Some(aspect), Some(idx), Some(assumption)) = fields {
                    all.push(Question::Loose {
                        subject: subject.into(),
                        aspect: aspect.into(),
                        idx,
                        dimension: row["dimension"].as_str().unwrap_or("-").into(),
                        assumption: assumption.into(),
                        confidence: row["conf"].as_f64().unwrap_or(0.0),
                    });
                }
            }
        }
        all
    }

    /// Land what the human said — or defer, or hand a correction to
    /// the agent. The monitor note is the whole account.
    async fn digest_round(&self, key: &str, answer: ElicitResult, session: &Session) -> String {
        if answer.action != ElicitationAction::Accept {
            self.deferred
                .lock()
                .expect("deferred lock")
                .insert(key.to_string());
            return format!("question-round: deferred ({:?})", answer.action);
        }
        let Some(content) = answer.content else {
            return "question-round: accepted without content".into();
        };
        let Some(question) = self.question_for_key(session, key).await else {
            return "question-round: the question no longer stands — nothing landed".into();
        };
        match question {
            Question::Loose {
                subject,
                aspect,
                idx,
                ..
            } => match content.get("stance").and_then(|v| v.as_str()) {
                Some("stands as stated") => {
                    self.rule_assumption(session, &subject, &aspect, idx).await
                }
                Some("wrong") => {
                    // The door composes, never re-grounds: the
                    // correction is the agent's work, so it rides the
                    // tool result and the key defers while they act.
                    self.deferred
                        .lock()
                        .expect("deferred lock")
                        .insert(key.to_string());
                    let correction = content
                        .get("correction")
                        .and_then(|v| v.as_str())
                        .unwrap_or("(no correction text)");
                    format!(
                        "question-round: the human says the `{aspect}` assumption is wrong — \
                         re-ground it: {correction}"
                    )
                }
                _ => "question-round: the answer names no stance".into(),
            },
        }
    }

    /// The human confirms an assumption: the WINNING body — their own
    /// standing slot if one exists, else the agent's — lands as the
    /// human slot with that assumption at full confidence and the
    /// ruling as its basis. Composition only — nothing else in the
    /// body moves. Composing from the agent slot unconditionally was
    /// the first live run's loop (found 2026-08-13): a second ruling
    /// reverted the first, which then derived and asked again.
    async fn rule_assumption(
        &self,
        session: &Session,
        subject: &str,
        aspect: &str,
        idx: u64,
    ) -> String {
        if !ident_path(subject, 3) || !ident_path(aspect, 1) {
            return "question-round: refused: not an identifier path".into();
        }
        let sql = format!(
            "SELECT body FROM glossary \
             WHERE subject = '{subject}' AND aspect = '{aspect}' \
               AND actor_kind IN ('human', 'agent') \
             ORDER BY CASE actor_kind WHEN 'human' THEN 0 ELSE 1 END, \
                      written_at DESC LIMIT 1"
        );
        let rows = match read_rows(session, &sql).await {
            Ok(rows) => rows,
            Err(e) => return format!("question-round: the slot read failed: {e}"),
        };
        let Some(body_text) = rows.first().and_then(|r| r["body"].as_str()) else {
            return "question-round: the agent slot is gone — nothing landed".into();
        };
        let Ok(mut body) = serde_json::from_str::<serde_json::Value>(body_text) else {
            return "question-round: the slot body is not JSON".into();
        };
        let Some(entry) = body
            .get_mut("assumptions")
            .and_then(|a| a.get_mut(idx as usize))
            .and_then(|e| e.as_object_mut())
        else {
            return "question-round: the assumption is gone — nothing landed".into();
        };
        entry.insert("confidence".into(), serde_json::json!(1.0));
        entry.insert("basis".into(), serde_json::json!("human-ruled"));
        match self
            .land_human_answer(session.dataset(), subject, aspect, &body.to_string())
            .await
        {
            Ok(note) => note,
            Err(e) => format!("question-round: refused: {e}"),
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

/// One read, rows as JSON — the round's derivations run through
/// the session exactly as frames do.
async fn read_rows(session: &Session, sql: &str) -> Result<Vec<serde_json::Value>, String> {
    let query = session.query_stream(sql).await.map_err(|e| e.to_string())?;
    let out = wire::stream_json(query.stream, usize::MAX).await?;
    Ok(out["rows"].as_array().cloned().unwrap_or_default())
}

/// How many questions the round would serve right now — the same two
/// derivations, counted on the human's channel of the first dataset
/// (the binding an unpinned app uses). No dataset, no count: a
/// workspace before its first landing has nothing to ask.
async fn open_question_count(plane: &Plane) -> Option<usize> {
    let mut names = plane.datasets().await.ok()?;
    names.sort();
    let dataset = names.into_iter().next()?;
    let actor = Actor {
        kind: ActorKind::Human,
        id: crate::HUMAN.into(),
    };
    let session = plane.channel(actor, Some(&dataset)).await.ok()?;
    Some(read_rows(&session, LOOSE_SQL).await.ok()?.len())
}

/// The round's opaque state tag, echoed by MRTR retries. Untrusted —
/// landing rests on re-derivation, never on the echo.
const ROUND_STATE: &str = "question-round:v1";

/// Judged assumptions below full confidence, winning slot only (the
/// same guard as the app's queue frame). This is the round's ONLY
/// derivation: unassessed witnessed claims (behavior, unit, role) are
/// the agent's measurement backlog, never human questions — the
/// shipped functions settle them (ruled 2026-08-13).
const LOOSE_SQL: &str = "SELECT g.subject, g.aspect, i.i AS idx, \
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dimension, \
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS assumption, \
       json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf \
    FROM GLOSSARY(all => true) g \
    CROSS JOIN generate_series(0, 19) AS i(i) \
    WHERE g.kind = 'query' \
      AND i.i < json_length(g.body, 'assumptions') \
      AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0 \
      AND (EXISTS (SELECT 1 FROM glossary me \
                   WHERE me.subject = g.subject AND me.aspect = g.aspect \
                     AND me.actor_id = g.actor AND me.actor_kind = 'human') \
           OR NOT EXISTS (SELECT 1 FROM glossary h \
                          WHERE h.subject = g.subject AND h.aspect = g.aspect \
                            AND h.actor_kind = 'human')) \
    ORDER BY conf ASC, g.subject, g.aspect, i.i";

/// One open question, derived — never stored. The key names it in
/// the MRTR map; the form is composed from it.
enum Question {
    /// A judged assumption below full confidence.
    Loose {
        subject: String,
        aspect: String,
        idx: u64,
        dimension: String,
        assumption: String,
        confidence: f64,
    },
}

impl Question {
    fn key(&self) -> String {
        match self {
            Question::Loose {
                subject,
                aspect,
                idx,
                ..
            } => format!("loose:{subject}:{aspect}:{idx}"),
        }
    }

    fn params(&self) -> Result<ElicitRequestParams, String> {
        match self {
            Question::Loose {
                subject,
                aspect,
                dimension,
                assumption,
                confidence,
                ..
            } => {
                let schema = ElicitationSchema::builder()
                    .required_enum_schema(
                        "stance",
                        EnumSchema::builder(vec![
                            "stands as stated".into(),
                            "wrong".into(),
                        ])
                        .build(),
                    )
                    .optional_string("correction")
                    .build()
                    .map_err(|e| e.to_string())?;
                Ok(ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: format!(
                        "{subject} · {aspect} — {dimension}: \"{assumption}\" \
                         (confidence {confidence}). Does this stand? If wrong, say \
                         what is right. Decline to defer."
                    ),
                    requested_schema: schema,
                })
            }
        }
    }
}

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

        // The question round rides ahead of execution, by the
        // mechanism the negotiated lifecycle carries. 2026-07-28+ is
        // sessionless: the ask is an MRTR `input_required` result
        // (SEP-2322) and the answer arrives on the client's retry of
        // this same call. Session lifecycles get the server→client
        // request on this call's own stream instead. One question per
        // call, only while the workspace derives open items; the
        // capability must come from the request's own stamp — the
        // transport's peer_info is synthetic on the sessionless path.
        let mut probed = None;
        if let Some(responses) = &request.input_responses {
            let note = if request.request_state.as_deref() != Some(ROUND_STATE) {
                "question-round: a retry without the echoed state".to_string()
            } else if let Some((key, raw)) = responses.iter().next() {
                match serde_json::from_value::<ElicitResult>(raw.clone()) {
                    Ok(answer) => self.digest_round(key, answer, &session).await,
                    Err(e) => format!("question-round: the answer does not parse: {e}"),
                }
            } else {
                "question-round: the retry carries no answer".into()
            };
            println!("glossql ?? {id}: {note}");
            probed = Some(note);
        } else if context
            .client_capabilities()
            .and_then(|caps| caps.elicitation)
            .is_some()
        {
            if let Some(question) = self.derive_question(&session, true).await {
                match question.params() {
                    Ok(params) => {
                        if context
                            .protocol_version()
                            .is_some_and(|v| v >= ProtocolVersion::V_2026_07_28)
                        {
                            let mut asks = InputRequests::new();
                            asks.insert(
                                question.key(),
                                InputRequest::Elicitation(ElicitRequest::new(params)),
                            );
                            println!("glossql ?? {id}: question-round: asking {}", question.key());
                            return Ok(InputRequiredResult::new(
                                Some(asks),
                                Some(ROUND_STATE.into()),
                            )
                            .into());
                        }
                        // Session lifecycle: the ask rides this call's
                        // own stream, the answer routes back through
                        // the transport session.
                        let asked = context
                            .peer
                            .create_elicitation_with_timeout(
                                params,
                                Some(std::time::Duration::from_secs(120)),
                            )
                            .await;
                        let note = match asked {
                            Ok(answer) => {
                                self.digest_round(&question.key(), answer, &session).await
                            }
                            Err(e) => format!("question-round: no round-trip: {e}"),
                        };
                        println!("glossql ?? {id}: {note}");
                        probed = Some(note);
                    }
                    Err(e) => {
                        let note = format!("question-round: form refused: {e}");
                        println!("glossql ?? {id}: {note}");
                        probed = Some(note);
                    }
                }
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
