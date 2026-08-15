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

/// What the brief is decided on: the store's counts plus the open
/// question count. Movement is a comparison of these FACTS — never of
/// rendered lines (string equality on non-keys is forbidden, ruled
/// 2026-08-14; a rendered string is display, not identity).
#[derive(Clone, PartialEq, Eq)]
pub struct BriefFacts {
    counts: glossql_glossary::BriefCounts,
    questions: usize,
    conflicts: usize,
}

/// The brief's shared state, one per door process: the composed line
/// (served in the initialize instructions) and the facts it renders.
/// One shared baseline — the call that moved the facts carries the new
/// line back. A second agent on the same workspace hears nothing until
/// its own call moves something; that is a known gap, kept open rather
/// than paid for with per-actor state no run has needed.
#[derive(Default)]
pub struct Brief {
    line: std::sync::RwLock<String>,
    facts: std::sync::RwLock<Option<BriefFacts>>,
}

impl Brief {
    pub fn line(&self) -> String {
        self.line.read().map(|l| l.clone()).unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct GlossqlMcp {
    plane: Arc<Plane>,
    /// The door's knobs: the fallback agent id for calls no handshake
    /// named, the row cap. Human writes land as [`crate::HUMAN`] —
    /// anonymous by ruling (2026-08-13).
    doors: crate::DoorConfig,
    /// The brief (ruled 2026-08-12, delivery option B; extended
    /// 2026-08-14): one composed line over live counts, appended to
    /// the instructions every initialize/discover serves — and, since
    /// a client fetches those once per connection, ALSO appended as a
    /// content block to any tool result whose call moved the facts.
    /// Shared across the per-session handler instances; refreshed after
    /// every tool call.
    brief: Arc<Brief>,
    /// Questions the human declined — transport state, never the
    /// store (no ledger, ruled 2026-08-13). A decline rests only
    /// until the workspace moves: any writing call clears the set,
    /// so "not now" never hardens into "never" (cadence ruling,
    /// 2026-08-14). A landed slot stops deriving on its own.
    deferred: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Questions asked on the session lifecycle and not yet answered.
    /// The ask no longer blocks the agent's call (run 4, 2026-08-15),
    /// so nothing else would stop the next read from asking the same
    /// thing again — and a person should be looking at one form, not a
    /// stack of them. Non-empty means an ask is in flight and the
    /// round stays quiet until it resolves.
    asking: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

/// Take a set without letting a poisoned lock end the door. A panic
/// inside the round is a bug to fix, never a reason for every later
/// call to fail on the mutex it left behind.
fn set_of(
    lock: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> std::sync::MutexGuard<'_, std::collections::HashSet<String>> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl GlossqlMcp {
    pub fn new(
        plane: Arc<Plane>,
        doors: crate::DoorConfig,
        brief: Arc<Brief>,
        deferred: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
        asking: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ) -> Self {
        GlossqlMcp {
            plane,
            doors,
            brief,
            deferred,
            asking,
        }
    }

    /// Recompose the brief from the store and the question derivation.
    /// Cheap (four bounded reads), awaited at the end of every tool
    /// call and once at boot.
    pub async fn refresh_brief(plane: &Plane, brief: &Brief) {
        let (facts, line) = Self::compose_brief(plane).await;
        if let Ok(mut slot) = brief.facts.write() {
            *slot = facts;
        }
        if let Ok(mut slot) = brief.line.write() {
            *slot = line;
        }
    }

    /// The facts and their rendering, in one read pass.
    async fn compose_brief(plane: &Plane) -> (Option<BriefFacts>, String) {
        let questions = open_question_count(plane).await.unwrap_or(0);
        let conflicts = conflict_count(plane).await.unwrap_or(0);
        match plane.store().brief_counts().await {
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
                if counts.rulings_owed > 0 {
                    line.push_str(&format!(
                        "; {} ruling{} await{} the fold-in — re-record each ruled \
                         grounding citing its ruling",
                        counts.rulings_owed,
                        if counts.rulings_owed == 1 { "" } else { "s" },
                        if counts.rulings_owed == 1 { "s" } else { "" },
                    ));
                }
                if questions > 0 {
                    line.push_str(&format!(
                        "; {} judgment question{} stand{} open for the human (assumptions \
                         below full confidence — conventions and definitions, never \
                         statistics) — sweep the round (forms ride record reads: call \
                         with a glossary/GLOSSARY()/ATTEST() read until it stays \
                         quiet) or relay them in chat",
                        questions,
                        if questions == 1 { "" } else { "s" },
                        if questions == 1 { "s" } else { "" },
                    ));
                }
                if conflicts > 0 {
                    line.push_str(&format!(
                        "; {} claim{} ruled two ways — read `ruling_conflicts` and \
                         reconcile in your own groundings",
                        conflicts,
                        if conflicts == 1 { " is" } else { "s are" },
                    ));
                }
                line.push_str(
                    ". Start with the brief the glossql skill teaches — human slots, \
                     contested, red bands, the open queue — before acting.",
                );
                (
                    Some(BriefFacts {
                        counts,
                        questions,
                        conflicts,
                    }),
                    line,
                )
            }
            // No facts on a failed read: the door says so in the line
            // and tells the next caller again rather than recording a
            // silence as "nothing moved".
            Err(e) => (None, format!("Live now: the brief could not be read ({e}).")),
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
        let deferred = set_of(&self.deferred).clone();
        let loose = read_rows(session, LOOSE_SQL).await;
        if let Err(e) = &loose {
            println!("glossql ?? question-round: the loose derivation failed: {e}");
        }
        if let Ok(rows) = loose {
            for row in rows {
                let Some(q) = loose_from(&row) else {
                    continue;
                };
                if skip_deferred && deferred.contains(&q.id()) {
                    continue;
                }
                return Some(q);
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
        probe.retain(|q| q.id() == key);
        probe.pop()
    }

    async fn derive_all(&self, session: &Session) -> Vec<Question> {
        match read_rows(session, LOOSE_SQL).await {
            Ok(rows) => rows.iter().filter_map(loose_from).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Land what the human said — or defer, or hand a correction to
    /// the agent. The monitor note is the whole account.
    async fn digest_round(&self, key: &str, answer: ElicitResult, session: &Session) -> String {
        if answer.action != ElicitationAction::Accept {
            set_of(&self.deferred).insert(key.to_string());
            return format!("question-round: deferred ({:?})", answer.action);
        }
        let Some(content) = answer.content else {
            return "question-round: accepted without content".into();
        };
        let Some(question) = self.question_for_key(session, key).await else {
            return "question-round: the question no longer stands — nothing landed".into();
        };
        let Question {
            subject,
            aspect,
            key,
            dimension,
            assumption,
            sibling_stance,
            sibling_note,
            ..
        } = question;
        let entry = RulingEntry {
            subject: &subject,
            aspect: &aspect,
            dimension: &dimension,
            key: &key,
            assumption: &assumption,
            stance: "confirmed",
            note: None,
        };
        match content.get("stance").and_then(|v| v.as_str()) {
            Some("stands as stated") => self.land_ruling(session, entry).await,
            // The sibling ruling, replayed onto this aspect: the same
            // stance and the same words the human wrote next door, now
            // standing here in its own right. Nothing is inferred — the
            // human chose it.
            Some(said) if said.starts_with(SAME_AS) => {
                let Some(stance) = sibling_stance.as_deref() else {
                    return "question-round: nothing stands on that key to repeat".into();
                };
                let note = self
                    .land_ruling(
                        session,
                        RulingEntry {
                            stance,
                            note: sibling_note.clone(),
                            ..entry
                        },
                    )
                    .await;
                format!("{note} — repeated from the ruling already standing on that key")
            }
            Some("wrong") => {
                let correction = content
                    .get("correction")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(no correction text)");
                // The correction writes its own record — a message
                // alone left the question deriving forever (the
                // 2026-08-14 run). The re-grounding stays the agent's
                // work; the ruling holds the question closed while
                // they do it.
                let note = self
                    .land_ruling(
                        session,
                        RulingEntry {
                            stance: "corrected",
                            note: Some(correction.to_string()),
                            ..entry
                        },
                    )
                    .await;
                format!("{note} — the human's correction: {correction}")
            }
            _ => "question-round: the answer names no stance".into(),
        }
    }

    /// The human rules an assumption: the ruling lands as ITS OWN
    /// record — an entry appended to the human's `ruling` slot on the
    /// subject — and the ruled aspect's slots stay agent-authored.
    /// The entry names the claim by `key` (the agent's declared
    /// identity, the only thing joined on) and carries the prose the
    /// human actually read as a snapshot — display, never compared.
    /// The agent owes the fold-in (re-record the grounding citing the
    /// ruling); until then the brief counts the debt and the round
    /// holds the question closed. Ruled 2026-08-14: the earlier shape
    /// copied the winning body into the human slot, and the frozen
    /// copy outranked every later correction — the human slot now
    /// carries only what the human actually said.
    async fn land_ruling(&self, session: &Session, ruling: RulingEntry<'_>) -> String {
        let RulingEntry {
            subject,
            aspect,
            dimension,
            key,
            assumption,
            stance,
            note,
        } = ruling;
        // ONE KEY IS STILL RULED PER ASPECT, deliberately. Run 4 asked
        // about `days-in-period` three times (dso, dpo, dio) and
        // `goods-only` twice, and fanning one answer across every
        // aspect that discloses the key is the obvious cure — but it is
        // the wrong one: run 2's human confirmed `goods-only` on
        // `purchases` in the same session where they corrected it on
        // `dpo`, on purpose. A fan-out would have silently denied them
        // that. The key pairs the claims so the form can SAY what was
        // already ruled next door; it does not make them one claim.
        // The cheap answer (`params` below) is how the repeat stops
        // costing a re-read.
        //
        // The composing and the writing are `glossql_session::rulings`,
        // shared with the app door — the docket answers these same
        // questions when a person comes back to a page rather than to a
        // form. This door's part is to say WHO is speaking: the
        // anonymous human, on their own channel, witnessed here.
        let Some(dataset) = session.dataset() else {
            return "question-round: no dataset is bound — USE one first".into();
        };
        let human = match self
            .plane
            .channel(
                Actor {
                    kind: ActorKind::Human,
                    id: crate::HUMAN.into(),
                },
                Some(&dataset),
            )
            .await
        {
            Ok(session) => session,
            Err(e) => return format!("question-round: refused: {e}"),
        };
        match glossql_session::rulings::land(
            &human,
            glossql_session::rulings::Ruling {
                subject,
                aspect,
                dimension,
                key,
                assumption,
                stance,
                note,
            },
        )
        .await
        {
            Ok(said) => format!("question-round: {said}"),
            Err(e) => format!("question-round: refused: {e}"),
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

/// Claims the human ruled two ways — a read, not a round. Counted for
/// the brief so an agent hears about the tension; nothing asks about
/// it, and reconciling is the agent's act in its own groundings.
async fn conflict_count(plane: &Plane) -> Option<usize> {
    let mut names = plane.datasets().await.ok()?;
    names.sort();
    let dataset = names.into_iter().next()?;
    let actor = Actor {
        kind: ActorKind::Human,
        id: crate::HUMAN.into(),
    };
    let session = plane.channel(actor, Some(&dataset)).await.ok()?;
    Some(
        read_rows(&session, "SELECT * FROM ruling_conflicts")
            .await
            .ok()?
            .len(),
    )
}

/// The round's opaque state tag, echoed by MRTR retries. Untrusted —
/// landing rests on re-derivation, never on the echo.
const ROUND_STATE: &str = "question-round:v1";

/// The answer that replays the ruling already standing on this key
/// under another aspect. The form appends which one, so the option
/// reads "same as before (corrected on dpo)".
const SAME_AS: &str = "same as before";

/// How long the round waits for a person before treating the silence
/// as a decline. Long enough to read a form and answer it, short
/// enough that being away costs an agent a pause and not a coffee
/// break — run 4 waited 120s per read, repeatedly, for a human who was
/// simply not at the keyboard.
const ROUND_WAIT_SECS: u64 = 25;

/// One entry of the human's `ruling` slot, as the door composes it.
/// `key` names the claim (the join column); `assumption` is the prose
/// the human read, kept for the record and never matched; `note` is
/// display.
struct RulingEntry<'a> {
    subject: &'a str,
    aspect: &'a str,
    dimension: &'a str,
    key: &'a str,
    assumption: &'a str,
    stance: &'a str,
    note: Option<String>,
}

/// What still stands open for a human to judge, and the round's ONLY
/// derivation — unassessed witnessed claims (behavior, unit, role) are
/// the agent's measurement backlog, never human questions, and the
/// shipped functions settle them (ruled 2026-08-13). The derivation
/// itself is `crates/session/reads/open_questions.sql`, which carries
/// the gates and the reasons; the door only orders and serves it, and
/// the app's docket renders the same read. Least-confident first, and
/// the ordering rides here because an inner ORDER BY does not survive
/// a derived relation.
const LOOSE_SQL: &str =
    "SELECT * FROM open_questions ORDER BY conf ASC, subject, aspect, idx";

fn loose_from(row: &serde_json::Value) -> Option<Question> {
    Some(Question {
        subject: row["subject"].as_str()?.into(),
        aspect: row["aspect"].as_str()?.into(),
        key: row["key"].as_str()?.into(),
        dimension: row["dimension"].as_str().unwrap_or("-").into(),
        assumption: row["assumption"].as_str().unwrap_or("").into(),
        confidence: row["conf"].as_f64().unwrap_or(0.0),
        sibling: row["sibling"].as_str().map(str::to_string),
        sibling_stance: row["sibling_stance"].as_str().map(str::to_string),
        sibling_note: row["sibling_note"].as_str().map(str::to_string),
    })
}

/// One open question, derived — never stored. `key` is the agent's
/// declared identity for the claim; `assumption` is the prose the
/// human reads — two aspects may word one claim differently, and only
/// the key pairs them. `sibling` is what the human already ruled on
/// that same key under another aspect, carried by the read so the form
/// can say so while asking.
struct Question {
    subject: String,
    aspect: String,
    key: String,
    dimension: String,
    assumption: String,
    confidence: f64,
    sibling: Option<String>,
    /// The sibling ruling in parts, so the round can offer it back as
    /// an answer rather than only naming it.
    sibling_stance: Option<String>,
    sibling_note: Option<String>,
}

impl Question {
    /// The round's transport id — the MRTR map key and the deferred
    /// set's member. Built from identity (subject, aspect, the
    /// assumption's key), so it survives a re-record that reorders the
    /// assumptions array.
    fn id(&self) -> String {
        format!("loose:{}:{}:{}", self.subject, self.aspect, self.key)
    }

    fn params(&self) -> Result<ElicitRequestParams, String> {
        let Question {
            subject,
            aspect,
            dimension,
            assumption,
            confidence,
            sibling,
            ..
        } = self;
        // One decision spelled with one key across several groundings
        // is asked once per grounding, because a human may rule the
        // same key differently on two aspects and has. What the repeat
        // must not cost is a re-reading: where they already ruled this
        // key next door, that ruling is offered back as a third answer,
        // so agreeing is one choice and differing is still open.
        let mut stances = vec!["stands as stated".to_string(), "wrong".to_string()];
        if let Some(s) = sibling {
            stances.push(format!("{SAME_AS} ({s})"));
        }
        let schema = ElicitationSchema::builder()
            .required_enum_schema("stance", EnumSchema::builder(stances).build())
            .optional_string("correction")
            .build()
            .map_err(|e| e.to_string())?;
        // What they already ruled on this same claim elsewhere rides
        // the message, so the answer is given knowingly.
        let pointer = match sibling {
            Some(s) => format!(" Note: you ruled this same claim {s}."),
            None => String::new(),
        };
        Ok(ElicitRequestParams::FormElicitationParams {
            meta: None,
            message: format!(
                "{subject} · {aspect} — {dimension}: \"{assumption}\" \
                 (confidence {confidence}).{pointer} Does this stand? If wrong, \
                 say what is right. Decline to defer."
            ),
            requested_schema: schema,
        })
    }
}

impl ServerHandler for GlossqlMcp {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("glossql-serverd", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(format!("{INSTRUCTIONS}\n\n{}", self.brief.line()));
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
        // request on this call's own stream instead. Cadence (ruled
        // 2026-08-14, from the first live run): forms ride only calls
        // that read the record — a metadata read, no writes — so the
        // brief sweep and the stage read-backs carry the round while
        // landings and judging queries run uninterrupted. A writing
        // call re-opens what a decline deferred. One question per
        // call, only while the workspace derives open items; the
        // capability must come from the request's own stamp — the
        // transport's peer_info is synthetic on the sessionless path.
        let shape = glossql_session::call_shape(statements);
        if shape.writes {
            set_of(&self.deferred).clear();
        }
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
        } else if shape.reviews
            && set_of(&self.asking).is_empty()
            && context
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
                                question.id(),
                                InputRequest::Elicitation(ElicitRequest::new(params)),
                            );
                            println!("glossql ?? {id}: question-round: asking {}", question.id());
                            return Ok(InputRequiredResult::new(
                                Some(asks),
                                Some(ROUND_STATE.into()),
                            )
                            .into());
                        }
                        // Session lifecycle: the ask rides this call's
                        // own stream, the answer routes back through
                        // the transport session.
                        //
                        // The wait is bounded and it is spent once. On
                        // this lifecycle the ask travels on THIS call's
                        // stream, and the stream closes when the call
                        // returns — so the request cannot be fired and
                        // forgotten without losing it, and the handler
                        // has to stay for the answer.
                        //
                        // What can be fixed is the price of nobody
                        // being there. Run 4 waited two minutes per
                        // record read for a person who was away, over
                        // and over, because the next read asked the
                        // same question again. So: a wait short enough
                        // that a present human still answers it, and a
                        // silence treated as a decline — the question
                        // defers exactly like a "not now", and the
                        // round stays quiet until a writing call moves
                        // the workspace and re-opens it. One stall per
                        // question per move, never a stall per read.
                        let qid = question.id();
                        set_of(&self.asking).insert(qid.clone());
                        let asked = context
                            .peer
                            .create_elicitation_with_timeout(
                                params,
                                Some(std::time::Duration::from_secs(ROUND_WAIT_SECS)),
                            )
                            .await;
                        set_of(&self.asking).remove(&qid);
                        let note = match asked {
                            Ok(answer) => self.digest_round(&qid, answer, &session).await,
                            Err(e) => {
                                set_of(&self.deferred).insert(qid.clone());
                                format!(
                                    "question-round: no answer within {ROUND_WAIT_SECS}s \
                                     ({e}) — `{qid}` rests like a decline and will not \
                                     be asked again until a write moves the workspace"
                                )
                            }
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
        // The brief travels on the call that moved it (ruled
        // 2026-08-14, run 2's friction 11: initialize instructions are
        // fetched once per connection, so a long-lived session never
        // saw the counts change). Two rules hold it honest:
        // movement is decided on the COUNTS, never on the rendered
        // line (a rendered string is display, not identity); and the
        // The baseline is the facts as they stood before this call.
        // One shared baseline, so the mover hears it: a second agent
        // watching the same workspace does not, and that is the known
        // cost of not keeping per-actor state the run has never asked
        // for.
        let before = self.brief.facts.read().ok().and_then(|f| f.clone());
        Self::refresh_brief(&self.plane, &self.brief).await;
        let after = self.brief.facts.read().ok().and_then(|f| f.clone());
        let brief_moved = (after.is_some() && after != before)
            .then(|| format!("brief: {}", self.brief.line()));
        Ok(match rendered {
            Ok(body) => {
                let mut blocks = vec![ContentBlock::text(body.to_string())];
                if let Some(note) = probed {
                    blocks.push(ContentBlock::text(note));
                }
                if let Some(brief) = brief_moved {
                    blocks.push(ContentBlock::text(brief));
                }
                CallToolResult::success(blocks)
            }
            // A failed statement is the agent's business, not the
            // transport's: an error result, never a protocol error —
            // and the brief still travels: an input_responses ruling
            // may have landed even when the statement then refused.
            Err(e) => {
                println!("glossql !! {id}: {e}");
                let mut blocks = vec![ContentBlock::text(e)];
                if let Some(brief) = brief_moved {
                    blocks.push(ContentBlock::text(brief));
                }
                CallToolResult::error(blocks)
            }
        }
        .into())
    }
}
