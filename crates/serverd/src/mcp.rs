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
}

/// The brief's shared state, one per door process: the composed line
/// (served in the initialize instructions), the facts it renders, and
/// what each actor was last told. Delivery is per audience — each
/// agent hears about a move exactly once, on its own next call —
/// because the counts are workspace-wide while the listeners are not
/// (with one shared baseline, only the mover was ever told).
#[derive(Default)]
pub struct Brief {
    line: std::sync::RwLock<String>,
    facts: std::sync::RwLock<Option<BriefFacts>>,
    told: std::sync::RwLock<std::collections::HashMap<String, BriefFacts>>,
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
    /// content block to any tool result whose call finds the facts
    /// moved since this actor was last told. Shared across the
    /// per-session handler instances; refreshed after every tool call.
    brief: Arc<Brief>,
    /// Questions the human declined — transport state, never the
    /// store (no ledger, ruled 2026-08-13). A decline rests only
    /// until the workspace moves: any writing call clears the set,
    /// so "not now" never hardens into "never" (cadence ruling,
    /// 2026-08-14). A landed slot stops deriving on its own.
    deferred: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl GlossqlMcp {
    pub fn new(
        plane: Arc<Plane>,
        doors: crate::DoorConfig,
        brief: Arc<Brief>,
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
                line.push_str(
                    ". Start with the brief the glossql skill teaches — human slots, \
                     contested, red bands, the open queue — before acting.",
                );
                (Some(BriefFacts { counts, questions }), line)
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
        let deferred = self.deferred.lock().expect("deferred lock").clone();
        // Contradicting rulings first: a mis-click should be caught
        // before anything else piles on.
        if let Ok(rows) = read_rows(session, CONTRA_SQL).await {
            for row in rows {
                let Some(q) = contradiction_from(&row) else {
                    continue;
                };
                if skip_deferred && deferred.contains(&q.key()) {
                    continue;
                }
                return Some(q);
            }
        }
        let loose = read_rows(session, LOOSE_SQL).await;
        if let Err(e) = &loose {
            println!("glossql ?? question-round: the loose derivation failed: {e}");
        }
        if let Ok(rows) = loose {
            for row in rows {
                let Some(mut q) = loose_from(&row) else {
                    continue;
                };
                if skip_deferred && deferred.contains(&q.key()) {
                    continue;
                }
                // The (b) inform: the same KEY already ruled on a
                // sibling aspect rides the form's message — one lookup
                // for the one question actually served.
                if let Question::Loose {
                    subject,
                    aspect,
                    key,
                    sibling,
                    ..
                } = &mut q
                {
                    let escaped = key.replace('\'', "''");
                    let sql = format!(
                        "WITH entries AS ( \
                            SELECT r.subject AS subject, \
                                   json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect, \
                                   json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key, \
                                   json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'stance') AS stance \
                            FROM glossary r \
                            CROSS JOIN generate_series(0, 199) AS rj(j) \
                            WHERE r.aspect = 'ruling' AND r.actor_kind = 'human' \
                              AND NOT EXISTS (SELECT 1 FROM glossary r2 \
                                              WHERE r2.subject = r.subject AND r2.aspect = 'ruling' \
                                                AND r2.actor_kind = 'human' AND r2.written_at > r.written_at) \
                              AND rj.j < json_length(r.body, 'rulings')) \
                        SELECT aspect, stance FROM entries \
                        WHERE subject = '{subject}' AND key = '{escaped}' \
                          AND aspect <> '{aspect}' LIMIT 1"
                    );
                    if let Ok(rows) = read_rows(session, &sql).await
                        && let Some(r) = rows.first()
                        && let (Some(a), Some(s)) = (r["aspect"].as_str(), r["stance"].as_str())
                    {
                        *sibling = Some(format!("{s} on {a}"));
                    }
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
        probe.retain(|q| q.key() == key);
        probe.pop()
    }

    async fn derive_all(&self, session: &Session) -> Vec<Question> {
        let mut all = Vec::new();
        if let Ok(rows) = read_rows(session, CONTRA_SQL).await {
            all.extend(rows.iter().filter_map(contradiction_from));
        }
        if let Ok(rows) = read_rows(session, LOOSE_SQL).await {
            all.extend(rows.iter().filter_map(loose_from));
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
                key,
                dimension,
                assumption,
                ..
            } => {
                let entry = RulingEntry {
                    subject: &subject,
                    aspect: &aspect,
                    dimension: &dimension,
                    key: &key,
                    assumption: &assumption,
                    stance: "confirmed",
                    note: None,
                    settles_with: None,
                };
                match content.get("stance").and_then(|v| v.as_str()) {
                    Some("stands as stated") => self.land_ruling(session, entry).await,
                    Some("wrong") => {
                        let correction = content
                            .get("correction")
                            .and_then(|v| v.as_str())
                            .unwrap_or("(no correction text)");
                        // The correction writes its own record — a
                        // message alone left the question deriving
                        // forever (the 2026-08-14 run). The
                        // re-grounding stays the agent's work; the
                        // ruling holds the question closed while they
                        // do it.
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
            Question::Contradiction {
                subject,
                newer_aspect,
                older_aspect,
                dimension,
                key,
                assumption,
                newer_stance,
                older_stance,
            } => {
                let entry = RulingEntry {
                    subject: &subject,
                    aspect: &newer_aspect,
                    dimension: &dimension,
                    key: &key,
                    assumption: &assumption,
                    stance: &newer_stance,
                    note: None,
                    settles_with: None,
                };
                match content.get("resolution").and_then(|v| v.as_str()) {
                    Some(r) if r.starts_with("a slip") => {
                        self.land_ruling(
                            session,
                            RulingEntry {
                                stance: &older_stance,
                                note: Some(format!(
                                    "re-ruled: the earlier {newer_stance} was a slip \
                                     ({older_aspect} rules `{key}` too)"
                                )),
                                ..entry
                            },
                        )
                        .await
                    }
                    // The pair is settled STRUCTURALLY — the sibling's
                    // name joins this entry's `settles_with` list, and
                    // the derivation excludes the pair by an aspect-name
                    // match. The prose note is for the human faces and
                    // is never read back.
                    Some(r) if r.starts_with("deliberate") => {
                        self.land_ruling(
                            session,
                            RulingEntry {
                                note: Some(format!("differs from {older_aspect} by design")),
                                settles_with: Some(&older_aspect),
                                ..entry
                            },
                        )
                        .await
                    }
                    _ => "question-round: the answer names no resolution".into(),
                }
            }
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
            settles_with,
        } = ruling;
        if !ident_path(subject, 3) || !ident_path(aspect, 1) {
            return "question-round: refused: not an identifier path".into();
        }
        let sql = format!(
            "SELECT body FROM glossary \
             WHERE subject = '{subject}' AND aspect = 'ruling' \
               AND actor_kind = 'human' \
             ORDER BY written_at DESC LIMIT 1"
        );
        let mut body = match read_rows(session, &sql).await {
            Ok(rows) => rows
                .first()
                .and_then(|r| r["body"].as_str())
                .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok())
                .unwrap_or_else(|| serde_json::json!({ "rulings": [] })),
            Err(e) => return format!("question-round: the ruling slot read failed: {e}"),
        };
        let Some(rulings) = body.get_mut("rulings").and_then(|r| r.as_array_mut()) else {
            return "question-round: the standing ruling slot is not a ruling body".into();
        };
        // A re-ruling on the same (aspect, key) replaces its entry —
        // the slot is the standing judgment, not a transcript. The
        // pairs already settled as deliberate ride along: they are
        // the human's word too, and losing them would re-ask a
        // question they have answered.
        let mut settled: Vec<serde_json::Value> = rulings
            .iter()
            .filter(|r| r["aspect"] == *aspect && r["key"] == *key)
            .filter_map(|r| r["settles_with"].as_array().cloned())
            .next()
            .unwrap_or_default();
        rulings.retain(|r| !(r["aspect"] == *aspect && r["key"] == *key));
        let mut entry = serde_json::json!({
            "aspect": aspect,
            "dimension": dimension,
            "key": key,
            "assumption": assumption,
            "stance": stance,
        });
        if let Some(note) = note {
            entry["note"] = serde_json::json!(note);
        }
        if let Some(other) = settles_with {
            let other = serde_json::json!(other);
            if !settled.contains(&other) {
                settled.push(other);
            }
        }
        if !settled.is_empty() {
            entry["settles_with"] = serde_json::Value::Array(settled);
        }
        rulings.push(entry);
        match self
            .land_human_answer(session.dataset(), subject, "ruling", &body.to_string())
            .await
        {
            Ok(_) => format!(
                "question-round: ruled ({stance}) on `{key}` — the fold-in is yours: \
                 re-record `{aspect}` carrying that key, citing the ruling"
            ),
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
    let loose = read_rows(&session, LOOSE_SQL).await.ok()?.len();
    let contra = read_rows(&session, CONTRA_SQL)
        .await
        .map(|r| r.len())
        .unwrap_or(0);
    Some(loose + contra)
}

/// The round's opaque state tag, echoed by MRTR retries. Untrusted —
/// landing rests on re-derivation, never on the echo.
const ROUND_STATE: &str = "question-round:v1";

/// One entry of the human's `ruling` slot, as the door composes it.
/// `key` names the claim (the join column); `assumption` is the prose
/// the human read, kept for the record and never matched; `note` is
/// display; `settles_with` names a sibling ASPECT whose differing
/// stance on this key the human called deliberate — an identifier, so
/// the derivation can exclude the pair by name instead of hunting for
/// a phrase inside a sentence.
struct RulingEntry<'a> {
    subject: &'a str,
    aspect: &'a str,
    dimension: &'a str,
    key: &'a str,
    assumption: &'a str,
    stance: &'a str,
    note: Option<String>,
    settles_with: Option<&'a str>,
}

/// Judged assumptions below full confidence, winning slot only (the
/// same guard as the app's queue frame). This is the round's ONLY
/// derivation: unassessed witnessed claims (behavior, unit, role) are
/// the agent's measurement backlog, never human questions — the
/// shipped functions settle them (ruled 2026-08-13).
/// Contradicting rulings, asked before anything else (ruled
/// 2026-08-14): two standing entries on one subject rule THE SAME KEY
/// to different stances on different aspects. The key is the agent's
/// declared identity for the claim (`goods-only`), written at
/// disclosure and stable across rephrasing — assumption prose is
/// display and never a join column (STRING EQUALITY ON NON-KEYS IS
/// FORBIDDEN, ruled 2026-08-14). The live case was a mis-click; the
/// form catches it, or records the difference as deliberate — the
/// sibling ASPECT's name joins the newer entry's `settles_with` list,
/// and the `settled` anti-join then excludes that pair by name (an
/// identifier match, not a phrase hunt inside a sentence). Either
/// answer terminates. Known and accepted: the same claim disclosed
/// under two DIFFERENT keys is not paired here, and nothing pairs it —
/// the human's own reading of the ruling faces is the only net, by
/// ruling.
const CONTRA_SQL: &str = "WITH entries AS ( \
        SELECT r.subject AS subject, rj.j AS j, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect, \
               coalesce(json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'dimension'), '-') AS dimension, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key, \
               coalesce(json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'assumption'), '') AS assumption, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'stance') AS stance \
        FROM glossary r \
        CROSS JOIN generate_series(0, 199) AS rj(j) \
        WHERE r.aspect = 'ruling' AND r.actor_kind = 'human' \
          AND NOT EXISTS (SELECT 1 FROM glossary r2 \
                          WHERE r2.subject = r.subject AND r2.aspect = 'ruling' \
                            AND r2.actor_kind = 'human' AND r2.written_at > r.written_at) \
          AND rj.j < json_length(r.body, 'rulings')), \
    settled AS ( \
        SELECT r.subject AS subject, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key, \
               json_get_str(json_get(json_get(json_get(r.body, 'rulings'), rj.j), 'settles_with'), sk.k) AS other \
        FROM glossary r \
        CROSS JOIN generate_series(0, 199) AS rj(j) \
        CROSS JOIN generate_series(0, 9) AS sk(k) \
        WHERE r.aspect = 'ruling' AND r.actor_kind = 'human' \
          AND NOT EXISTS (SELECT 1 FROM glossary r2 \
                          WHERE r2.subject = r.subject AND r2.aspect = 'ruling' \
                            AND r2.actor_kind = 'human' AND r2.written_at > r.written_at) \
          AND rj.j < json_length(r.body, 'rulings') \
          AND sk.k < coalesce(json_length(json_get(json_get(r.body, 'rulings'), rj.j), 'settles_with'), 0)) \
    SELECT a.subject, a.aspect AS newer_aspect, b.aspect AS older_aspect, \
           a.dimension, a.key, a.assumption, \
           a.stance AS newer_stance, b.stance AS older_stance \
    FROM entries a \
    JOIN entries b ON a.subject = b.subject AND a.key = b.key \
      AND a.aspect <> b.aspect AND a.stance <> b.stance AND a.j > b.j \
    WHERE a.key IS NOT NULL \
      AND NOT EXISTS (SELECT 1 FROM settled s \
                      WHERE s.subject = a.subject AND s.aspect = a.aspect \
                        AND s.key = a.key AND s.other = b.aspect) \
    ORDER BY a.subject, a.aspect";

/// Open questions derive from the agent's CURRENT body — never a
/// frozen copy (the 2026-08-14 run: deriving from the winning human
/// slot re-asked every answered question, because the human copy kept
/// the stale confidences). Four gates beyond "below full confidence":
/// the aspect is a grounding (query kind); the assumption carries a
/// `key` (its declared identity — an unkeyed assumption cannot be
/// closed, so it is never asked; known and accepted); the dimension is
/// not one the function map owns (`behavior`, `sign`, `grain` are
/// statistics — ruled 2026-08-13, enforced, not just taught); and no
/// standing ruling entry names the same (aspect, key) — a ruling holds
/// the question closed until the agent's fold-in raises that key to
/// full confidence, at which point the row drops out on its own.
const LOOSE_SQL: &str = "WITH ruled AS ( \
        SELECT r.subject AS subject, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect, \
               json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key \
        FROM glossary r \
        CROSS JOIN generate_series(0, 199) AS rj(j) \
        WHERE r.aspect = 'ruling' AND r.actor_kind = 'human' \
          AND NOT EXISTS (SELECT 1 FROM glossary r2 \
                          WHERE r2.subject = r.subject AND r2.aspect = 'ruling' \
                            AND r2.actor_kind = 'human' AND r2.written_at > r.written_at) \
          AND rj.j < json_length(r.body, 'rulings')), \
    open_assumptions AS ( \
        SELECT g.subject, g.aspect, i.i AS idx, \
               json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dimension, \
               json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key, \
               json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS assumption, \
               json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf \
        FROM glossary g \
        JOIN aspects a ON a.name = g.aspect AND a.kind = 'query' \
        CROSS JOIN generate_series(0, 19) AS i(i) \
        WHERE g.actor_kind = 'agent' \
          AND NOT EXISTS (SELECT 1 FROM glossary g2 \
                          WHERE g2.subject = g.subject AND g2.aspect = g.aspect \
                            AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at) \
          AND i.i < json_length(g.body, 'assumptions')) \
    SELECT o.subject, o.aspect, o.idx, o.dimension, o.key, o.assumption, o.conf \
    FROM open_assumptions o \
    WHERE o.conf < 1.0 AND o.key IS NOT NULL \
      AND coalesce(o.dimension, '-') NOT IN ('behavior', 'sign', 'grain') \
      AND NOT EXISTS (SELECT 1 FROM ruled r \
                      WHERE r.subject = o.subject AND r.aspect = o.aspect \
                        AND r.key = o.key) \
    ORDER BY o.conf ASC, o.subject, o.aspect, o.idx";

fn loose_from(row: &serde_json::Value) -> Option<Question> {
    Some(Question::Loose {
        subject: row["subject"].as_str()?.into(),
        aspect: row["aspect"].as_str()?.into(),
        key: row["key"].as_str()?.into(),
        dimension: row["dimension"].as_str().unwrap_or("-").into(),
        assumption: row["assumption"].as_str().unwrap_or("").into(),
        confidence: row["conf"].as_f64().unwrap_or(0.0),
        sibling: None,
    })
}

fn contradiction_from(row: &serde_json::Value) -> Option<Question> {
    Some(Question::Contradiction {
        subject: row["subject"].as_str()?.into(),
        newer_aspect: row["newer_aspect"].as_str()?.into(),
        older_aspect: row["older_aspect"].as_str()?.into(),
        dimension: row["dimension"].as_str().unwrap_or("-").into(),
        key: row["key"].as_str()?.into(),
        assumption: row["assumption"].as_str().unwrap_or("").into(),
        newer_stance: row["newer_stance"].as_str()?.into(),
        older_stance: row["older_stance"].as_str()?.into(),
    })
}

/// One open question, derived — never stored. The key names it in
/// the MRTR map; the form is composed from it.
enum Question {
    /// A judged assumption below full confidence. `key` is the
    /// agent's declared identity for the claim; `assumption` is the
    /// prose the human reads — two aspects may word one claim
    /// differently, and only the key pairs them. `sibling` carries the
    /// (b) inform (ruled 2026-08-14): the same key already ruled on a
    /// sibling aspect, named in the form so the human answers
    /// knowingly.
    Loose {
        subject: String,
        aspect: String,
        key: String,
        dimension: String,
        assumption: String,
        confidence: f64,
        sibling: Option<String>,
    },
    /// Two standing rulings on one subject rule the same key to
    /// different stances (ruled 2026-08-14 — the live case was a
    /// mis-click: goods-only confirmed on purchases, corrected on
    /// dpo). Asked before anything else; either answer terminates —
    /// a realign flips the newer entry, a "deliberate" stamps its
    /// note with the sibling's name, which the derivation excludes.
    Contradiction {
        subject: String,
        newer_aspect: String,
        older_aspect: String,
        dimension: String,
        key: String,
        assumption: String,
        newer_stance: String,
        older_stance: String,
    },
}

impl Question {
    /// The round's transport id — the MRTR map key and the deferred
    /// set's member. Built from identity (subject, aspect, the
    /// assumption's key), so it survives a re-record that reorders the
    /// assumptions array.
    fn key(&self) -> String {
        match self {
            Question::Loose {
                subject,
                aspect,
                key,
                ..
            } => format!("loose:{subject}:{aspect}:{key}"),
            Question::Contradiction {
                subject,
                newer_aspect,
                key,
                ..
            } => format!("contra:{subject}:{newer_aspect}:{key}"),
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
                sibling,
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
            Question::Contradiction {
                subject,
                newer_aspect,
                older_aspect,
                key,
                assumption,
                newer_stance,
                older_stance,
                ..
            } => {
                let schema = ElicitationSchema::builder()
                    .required_enum_schema(
                        "resolution",
                        EnumSchema::builder(vec![
                            format!("a slip — {newer_aspect} should read {older_stance} too"),
                            "deliberate — both stand as ruled".into(),
                        ])
                        .build(),
                    )
                    .build()
                    .map_err(|e| e.to_string())?;
                Ok(ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: format!(
                        "{subject} · `{key}` — \"{assumption}\" — is ruled {older_stance} \
                         on {older_aspect} but {newer_stance} on {newer_aspect}. One \
                         claim, two stances: is the difference deliberate? Decline to \
                         defer."
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
            self.deferred.lock().expect("deferred lock").clear();
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
        // The brief travels on the call that moved it (ruled
        // 2026-08-14, run 2's friction 11: initialize instructions are
        // fetched once per connection, so a long-lived session never
        // saw the counts change). Two rules hold it honest:
        // movement is decided on the COUNTS, never on the rendered
        // line (a rendered string is display, not identity); and the
        // baseline is per audience — what THIS actor was last told. A
        // single shared baseline told only the mover, so a second
        // agent never heard what the first one changed.
        // An actor the door has not told yet is told: its first call
        // repeats what its connect instructions already carried, once
        // per actor per process. The alternative — assuming the
        // handshake told it — is what left the second agent silent,
        // and rmcp's `initialize` cannot be overridden to seed the
        // baseline without re-implementing version negotiation.
        Self::refresh_brief(&self.plane, &self.brief).await;
        let after = self.brief.facts.read().ok().and_then(|f| f.clone());
        let brief_moved = after.and_then(|after| {
            let told_before = self
                .brief
                .told
                .read()
                .ok()
                .and_then(|told| told.get(&id).cloned());
            if told_before.as_ref() == Some(&after) {
                return None;
            }
            if let Ok(mut told) = self.brief.told.write() {
                told.insert(id.clone(), after);
            }
            Some(format!("brief: {}", self.brief.line()))
        });
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
