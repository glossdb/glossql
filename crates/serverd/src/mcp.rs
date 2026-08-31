//! The MCP shim: one door, one tool. `glossql` takes statements and
//! returns outcomes — the door tells, skills teach. Everything an agent
//! must *learn* (grammar, function authoring, flows) ships as skills,
//! served from this same door as resources and prompts ([`crate::skills`]);
//! everything live (declared functions, the glossary, the tables) is
//! read through the language itself, where it is always current.

use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ElicitRequest,
    ElicitRequestParams, ElicitResult, ElicitationAction, ElicitationSchema,
    GetPromptRequestParams, GetPromptResponse, GetPromptResult, Implementation, InputRequest,
    InputRequests, InputRequiredResult, ListPromptsResult, ListResourcesResult, ListToolsResult,
    PaginatedRequestParams, Prompt, PromptMessage, ProtocolVersion, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, Role, ServerCapabilities,
    ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};

use glossql_glossary::{Actor, ActorKind};
use glossql_session::Caller;

use glossql_session::{Plane, Session, SessionError};

use crate::wire;

const INSTRUCTIONS: &str = "glossql workspace server — one SQL-shaped surface over a \
workspace's data and its context. The objects: a dataset holds tables, landed by recipes \
from sources; an aspect is a named JSON contract; a gloss speaks an aspect's value on a \
subject (a table, a column, the dataset itself), and a QUERY aspect's gloss is SQL, served \
back as `read.<name>()` — a metric, a current fact, or a derived relation, which the \
metrics skill tells apart; functions measure, and their measurements are the evidence; \
witnesses adjudicate the voices on a slot; a human's ruling outranks every agent gloss. \
One tool, `glossql`, runs statements and plain SQL — its description is the contract for \
every call — and live state is read through it, never assumed. Read \
`skill://glossql/SKILL.md` before the first statement and \
`skill://glossql-metrics/SKILL.md` before landing a source or grounding anything; each is \
one page and names its references (`skill://<name>/references/…`) for the moment they \
matter. The docs pages are served as `doc://docs/…`, the language as `doc://SPEC.md` and \
`doc://grammar.ebnf`. Start with SELECT * FROM datasets.";

/// What the brief is decided on: the store's counts plus the open
/// question count. Movement is a comparison of these FACTS — never of
/// rendered lines (string equality on non-keys is forbidden; a
/// rendered string is display, not identity).
#[derive(Clone, PartialEq, Eq)]
pub struct BriefFacts {
    counts: glossql_glossary::BriefCounts,
    questions: usize,
    datasets: usize,
}

/// The brief's shared state, one per door process: the composed line
/// (served in the initialize instructions and on the tool result that
/// moved it), the opening (how to begin here — served at initialize
/// only, since it is the same on every call and a tool result that
/// repeats it reads as an order), and the facts they render. One
/// shared baseline — the call that moved the facts carries the new
/// line back. A second agent on the same workspace hears nothing until
/// its own call moves something; that is a known gap, kept open rather
/// than paid for with per-actor state no run has needed.
#[derive(Default)]
pub struct Brief {
    line: std::sync::RwLock<String>,
    opening: std::sync::RwLock<String>,
    facts: std::sync::RwLock<Option<BriefFacts>>,
}

impl Brief {
    pub fn line(&self) -> String {
        self.line.read().map(|l| l.clone()).unwrap_or_default()
    }

    fn opening(&self) -> String {
        self.opening.read().map(|l| l.clone()).unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct GlossqlMcp {
    plane: Arc<Plane>,
    /// The door's knobs: the row cap.
    doors: crate::DoorConfig,
    /// The brief: one composed line over live counts, appended to
    /// the instructions every initialize/discover serves — and, since
    /// a client fetches those once per connection, ALSO appended as a
    /// content block to any tool result whose call moved the facts.
    /// Shared across the per-session handler instances; refreshed after
    /// every tool call.
    brief: Arc<Brief>,
}

impl GlossqlMcp {
    pub fn new(plane: Arc<Plane>, doors: crate::DoorConfig, brief: Arc<Brief>) -> Self {
        GlossqlMcp {
            plane,
            doors,
            brief,
        }
    }

    /// Recompose the brief from the store and the question derivation.
    /// Cheap (four bounded reads), awaited at the end of every tool
    /// call and once at boot.
    pub async fn refresh_brief(plane: &Plane, brief: &Brief) {
        let (facts, line, opening) = Self::compose_brief(plane).await;
        if let Ok(mut slot) = brief.facts.write() {
            *slot = facts;
        }
        if let Ok(mut slot) = brief.line.write() {
            *slot = line;
        }
        if let Ok(mut slot) = brief.opening.write() {
            *slot = opening;
        }
    }

    /// The facts, their rendering, and the opening, in one read pass.
    async fn compose_brief(plane: &Plane) -> (Option<BriefFacts>, String, String) {
        let questions = open_question_count(plane).await.unwrap_or(0);
        let datasets = plane.datasets().await.map(|d| d.len()).unwrap_or(0);
        // How to begin, decided by whether anything stands to read: a
        // workspace before its first dataset has no brief to sweep —
        // `owed`, `GLOSSARY(d)` and `ATTEST(d)` all need one — and
        // the metrics skill's landing page is where it starts.
        let opening = if datasets == 0 {
            "No dataset stands yet, so there is no brief to sweep: `SELECT * FROM \
             workspace_next` is the whole of the live state, and \
             `skill://glossql-metrics/SKILL.md` with its landing page is where to begin."
        } else {
            "Open with the brief the glossql skill teaches — `USE` a dataset, then human \
             slots, contested, red bands, `owed` — once, before the first write. It is a \
             read, not a gate: what it counts waits for the human while the work goes on."
        }
        .to_string();
        match plane.store().brief_counts().await {
            Ok(counts) => {
                // Debt first, presence last: what owes an act is what
                // the reader acts on, and a count of human writings is
                // the record's size, not a task. The owed acts are
                // exactly `owed`'s kinds that the whole workspace can
                // count; the questions are `open_questions`'s rows.
                let mut owed = Vec::new();
                if counts.rulings_owed > 0 {
                    owed.push(format!(
                        "{} ruling{} await{} the fold-in — re-record each ruled grounding \
                         citing its ruling",
                        counts.rulings_owed,
                        if counts.rulings_owed == 1 { "" } else { "s" },
                        if counts.rulings_owed == 1 { "s" } else { "" },
                    ));
                }
                if counts.approvals_pending > 0 {
                    owed.push(format!(
                        "{} approved recipe change{} await{} the re-declare",
                        counts.approvals_pending,
                        if counts.approvals_pending == 1 {
                            ""
                        } else {
                            "s"
                        },
                        if counts.approvals_pending == 1 {
                            "s"
                        } else {
                            ""
                        },
                    ));
                }
                if questions > 0 {
                    owed.push(format!(
                        "{} judgment question{} wait{} for the human (assumptions below \
                         full confidence — conventions and definitions, never \
                         statistics): each is served once as a form on a record read \
                         (glossary, GLOSSARY(), ATTEST()), or relayed in chat where \
                         forms are absent, and stays open until the human rules — the \
                         work goes on meanwhile",
                        questions,
                        if questions == 1 { "" } else { "s" },
                        if questions == 1 { "s" } else { "" },
                    ));
                }
                let mut line = if owed.is_empty() {
                    "Live now: nothing owed, the round is quiet".to_string()
                } else {
                    format!("Live now: {}", owed.join("; "))
                };
                line.push_str(&format!(
                    ". Record: {} human writing{}{}.",
                    counts.human_writings,
                    if counts.human_writings == 1 { "" } else { "s" },
                    match &counts.latest_human_at {
                        Some(at) => format!(", latest {at}"),
                        None => String::new(),
                    },
                ));
                (
                    Some(BriefFacts {
                        counts,
                        questions,
                        datasets,
                    }),
                    line,
                    opening,
                )
            }
            // No facts on a failed read: the door says so in the line
            // and tells the next caller again rather than recording a
            // silence as "nothing moved".
            Err(e) => (
                None,
                format!("Live now: the brief could not be read ({e})."),
                opening,
            ),
        }
    }

    /// The next open question the workspace derives — judged
    /// assumptions below full confidence on the winning slot, lowest
    /// confidence first. Judgment only, never statistics: a claim a
    /// measurement can settle — behavior, unit,
    /// role — is the agent's work through the shipped functions, and
    /// the door never asks the human for it. A workspace with no
    /// dataset bound (or nothing open) derives nothing, and the round
    /// stays silent.
    /// The round: every open claim on ONE grounding, and how many
    /// stand open in the whole workspace.
    ///
    /// The bound is the grounding, not a number. An aspect's
    /// assumptions were authored in one act against one query and are
    /// read together, so they are what one sitting is. The reason a
    /// bound is needed at all is mechanical: the client fulfils every
    /// entry of a round before it retries the call, so the agent's
    /// read does not return until the human has cleared the whole
    /// batch. A round is what someone can finish, and a grounding is
    /// where the record already draws that line.
    ///
    /// The total rides back so every dialog can say where it sits in
    /// it. A bounded round that did not would let someone answer three
    /// questions, see the queue end, and believe they were done while
    /// nine claims the numbers rest on stood untouched.
    async fn derive_round(&self, session: &Session) -> (Vec<(usize, Question)>, usize) {
        let loose = read_rows(session, LOOSE_SQL).await;
        if let Err(e) = &loose {
            tracing::warn!(error = %e, "question-round: the loose derivation failed");
        }
        let open: Vec<Question> = loose
            .map(|rows| rows.iter().filter_map(loose_from).collect())
            .unwrap_or_default();
        let total = open.len();
        // The read orders by confidence, so the least settled claim
        // picks which grounding is asked.
        let Some(first) = open.first().map(|q| q.aspect.clone()) else {
            return (Vec::new(), 0);
        };
        // The rank is against the whole, never against the round, so
        // the count means the same thing on every dialog.
        let round = open
            .into_iter()
            .enumerate()
            .filter(|(_, q)| q.aspect == first)
            .map(|(i, q)| (i + 1, q))
            .collect();
        (round, total)
    }

    /// The retry is stateless, so re-derivation is the only trust: an
    /// answer lands only if its question still derives — walked live
    /// for the key rather than trusting the echoed shape.
    async fn question_for_key(&self, session: &Session, key: &str) -> Option<Question> {
        read_rows(session, LOOSE_SQL)
            .await
            .ok()?
            .iter()
            .filter_map(loose_from)
            .find(|q| q.id() == key)
    }

    /// Land what the human said — or defer, or hand a correction to
    /// the agent. The monitor note is the whole account.
    ///
    /// The boxes are independent, so more than one can be ticked, and
    /// the order below is what a ruling means when they are. Unclear
    /// wins: it refuses the question, and a refused question has
    /// nothing left to confirm. Then the rival, then the prior ruling,
    /// then the plain confirmation — each a narrower claim than the
    /// one before. Words the human typed outrank every box, because
    /// they are the only part of the answer nobody wrote for them.
    async fn digest_round(&self, key: &str, answer: ElicitResult, session: &Session) -> String {
        // The person behind the agent: the token's subject, which the
        // agent's session carries. Their answer lands under their own
        // name with human standing — the server witnessed the act.
        let who = session.actor().id.clone();
        // Two outcomes, not three. Accepting with something said is
        // the save; every other way out of the dialog is a defer, and
        // a defer is not an opinion. Nothing is recorded, the claim
        // still derives, and the next round asks it again — answering
        // is owed, and a door that let someone opt out of being asked
        // would be a door that hides what the numbers rest on.
        //
        // Refusing the QUESTION is different, and it is an answer: the
        // unclear box with the words beside it, which lands and buys a
        // reformulation.
        if answer.action != ElicitationAction::Accept {
            return format!("question-round: `{key}` deferred — unanswered, it stands open");
        }
        let Some(content) = answer.content else {
            return format!("question-round: `{key}` deferred — nothing said, it stands open");
        };
        let Some(question) = self.question_for_key(session, key).await else {
            return "question-round: the question no longer stands — nothing landed".into();
        };
        let Question {
            dataset,
            subject,
            aspect,
            key,
            dimension,
            assumption,
            alternative,
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
        let ticked = |name: &str| content.get(name).and_then(|v| v.as_bool()).unwrap_or(false);
        let typed = content
            .get(CORRECTION)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        // The question itself missed — the human refuses it rather
        // than the claim. The entry holds this key closed; the agent
        // owes a reformulation under a new key, which derives its own
        // question — without this a sloppily worded question can only
        // be deferred.
        if ticked(UNCLEAR) {
            return self
                .land_ruling(
                    &dataset,
                    &who,
                    RulingEntry {
                        stance: "unclear",
                        note: typed,
                        ..entry
                    },
                )
                .await;
        }
        // The rival taken. Its words are the agent's own, read off the
        // same derivation that drew the question — so the note is
        // workspace prose, never something a form supplied. Words the
        // human typed replace it.
        if ticked(RATHER) {
            let Some(rival) = alternative else {
                return "question-round: no rival stands on that claim to take".into();
            };
            let note = typed.clone().unwrap_or(rival);
            let landed = self
                .land_ruling(
                    &dataset,
                    &who,
                    RulingEntry {
                        stance: "corrected",
                        note: Some(note.clone()),
                        ..entry
                    },
                )
                .await;
            return format!("{landed} — the rival taken: {note}");
        }
        // The sibling ruling, replayed onto this aspect: the same
        // stance and the same words the human wrote next door, now
        // standing here in its own right. Nothing is inferred — the
        // human chose it.
        if ticked(SAME_AS) {
            let Some(stance) = sibling_stance.as_deref() else {
                return "question-round: nothing stands on that key to repeat".into();
            };
            let landed = self
                .land_ruling(
                    &dataset,
                    &who,
                    RulingEntry {
                        stance,
                        note: typed.or(sibling_note),
                        ..entry
                    },
                )
                .await;
            return format!("{landed} — repeated from the ruling already standing on that key");
        }
        if ticked(STANDS) {
            return self
                .land_ruling(
                    &dataset,
                    &who,
                    RulingEntry {
                        note: typed,
                        ..entry
                    },
                )
                .await;
        }
        // No box, but words: a human who writes what is right instead
        // of ticking has still ruled, and the correction writes its own
        // record — a message alone would leave the question deriving
        // forever. The re-grounding stays the agent's work; the ruling
        // holds the question closed while they do it.
        if let Some(correction) = typed {
            let landed = self
                .land_ruling(
                    &dataset,
                    &who,
                    RulingEntry {
                        stance: "corrected",
                        note: Some(correction.clone()),
                        ..entry
                    },
                )
                .await;
            return format!("{landed} — the human's correction: {correction}");
        }
        // Saved with nothing ticked and nothing typed. An empty form
        // says as little as a dismissed one, so it is the same defer.
        format!(
            "question-round: `{subject}:{aspect}:{key}` deferred — nothing said, it stands open"
        )
    }

    /// The human rules an assumption: the ruling lands as ITS OWN
    /// record — an entry appended to the human's `ruling` slot on the
    /// subject — and the ruled aspect's slots stay agent-authored.
    /// The entry names the claim by `key` (the agent's declared
    /// identity, the only thing joined on) and carries the prose the
    /// human actually read as a snapshot — display, never compared.
    /// The agent owes the fold-in (re-record the grounding citing the
    /// ruling); until then the brief counts the debt and the round
    /// holds the question closed. Copying the winning body into the
    /// human slot is forbidden: the frozen
    /// copy would outrank every later correction — the human slot
    /// carries only what the human actually said.
    async fn land_ruling(&self, dataset: &str, who: &str, ruling: RulingEntry<'_>) -> String {
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
        // form. This door's part is to say WHO is speaking: the token's
        // subject, with human standing, on their own channel — the
        // server witnessed the act (SPEC.md §1).
        let human = match self
            .plane
            .channel(
                Actor {
                    kind: ActorKind::Human,
                    id: who.to_string(),
                },
                Some(dataset),
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
                "Execute glossql statements against the workspace; one outcome per statement. \
                 Every call opens unbound: begin any call that names a dataset's tables or \
                 columns with `USE <dataset>;` — a call without one is workspace-scoped. \
                 Outcomes: a read is `{{columns, rows, row_count, truncated}}`, rows capped at \
                 {} (GLOSSARY(), ATTEST() and the store relations sent as their own single \
                 statement are uncapped); a write is `{{done}}` or `{{affected}}`; a GLOSS on a \
                 QUERY aspect — a metric's grounding — answers with the metric's fact row in \
                 the `metric_axes()` shape: whether the SQL plans, its behavior verb and where \
                 that came from, the axes admitted, and every served column not admitted with \
                 the road back in — read it before the next write. A refused statement is an \
                 error naming its place; what landed before it stayed landed (`{{landed}}`). \
                 While the workspace holds open questions, a call that only reads the record \
                 (glossary, GLOSSARY(), ATTEST(), the store relations) carries the human's \
                 question forms; landings and data reads never do.",
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

/// How many questions the workspace would serve right now, across every
/// dataset — the same span the round itself asks over, since each row
/// carries its own dataset and a ruling lands where its question came
/// from. No dataset, no count: a workspace before its first landing has
/// nothing to ask.
///
/// One count, not one per dataset. `open_questions` derives from the
/// workspace-wide `glossary`, so every channel already answers for the
/// whole workspace; summing across channels multiplied the total by the
/// number of datasets. A channel is opened only because the read needs
/// one, which is why any dataset that admits one will do.
async fn open_question_count(plane: &Plane) -> Option<usize> {
    let actor = Actor {
        kind: ActorKind::Human,
        id: crate::BOOTSTRAP.into(),
    };
    for dataset in plane.datasets().await.ok()? {
        let Ok(session) = plane.channel(actor.clone(), Some(&dataset)).await else {
            continue;
        };
        if let Some(n) = read_rows(&session, "SELECT count(*) AS n FROM open_questions")
            .await
            .ok()
            .and_then(|rows| rows.first().and_then(|r| r["n"].as_u64()))
        {
            return Some(n as usize);
        }
    }
    None
}

/// The round's opaque state tag, echoed by MRTR retries. Untrusted —
/// landing rests on re-derivation, never on the echo.
const ROUND_STATE: &str = "question-round:v1";

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
/// shipped functions settle them. The derivation
/// itself is `crates/session/reads/open_questions.sql`, which carries
/// the gates and the reasons; the door only orders and serves it, and
/// the app's docket renders the same read. Least-confident first, and
/// the ordering rides here because an inner ORDER BY does not survive
/// a derived relation.
const LOOSE_SQL: &str = "SELECT * FROM open_questions ORDER BY conf ASC, subject, aspect, idx";

fn loose_from(row: &serde_json::Value) -> Option<Question> {
    Some(Question {
        dataset: row["dataset"].as_str()?.into(),
        subject: row["subject"].as_str()?.into(),
        aspect: row["aspect"].as_str()?.into(),
        key: row["key"].as_str()?.into(),
        dimension: row["dimension"].as_str().unwrap_or("-").into(),
        assumption: row["assumption"].as_str().unwrap_or("").into(),
        basis: row["basis"].as_str().unwrap_or("not stated").into(),
        alternative: row["alternative"].as_str().map(str::to_string),
        confidence: row["conf"].as_f64().unwrap_or(0.0),
        sibling: row["sibling"].as_str().map(str::to_string),
        sibling_stance: row["sibling_stance"].as_str().map(str::to_string),
        sibling_note: row["sibling_note"].as_str().map(str::to_string),
    })
}

/// One open question, derived — never stored. `key` is the agent's
/// declared identity for the claim; `assumption` is the prose the
/// human reads — two aspects may word one claim differently, and only
/// the key pairs them. `alternative` is the rival reading the agent
/// named beside it, and `sibling` is what the human already ruled on
/// that same key under another aspect; both are carried by the read so
/// the round can offer them as answers rather than only naming them.
struct Question {
    /// The dataset the claim belongs to, carried by the read. A
    /// workspace holds many datasets and the agent's `USE` is its own
    /// cursor, not the claim's address: a ruling lands where the
    /// question came from, whatever the caller happens to be pointed
    /// at — and lands even when the caller is pointed at nothing.
    dataset: String,
    subject: String,
    aspect: String,
    key: String,
    dimension: String,
    assumption: String,
    basis: String,
    /// The rival reading, when the agent named one. It is the only
    /// answer the record already holds that says something: a stance
    /// is a verdict on a sentence, a rival is a reading to take
    /// instead.
    alternative: Option<String>,
    confidence: f64,
    sibling: Option<String>,
    /// The sibling ruling in parts, so the round can offer it back as
    /// an answer rather than only naming it.
    sibling_stance: Option<String>,
    sibling_note: Option<String>,
}

/// The answers, as the schema names them.
///
/// Every one is a field the renderer draws without being opened: a
/// boolean puts its checkbox in the list, an enum hides its options
/// behind a keypress. That is the whole reason the stances are not one
/// enum.
///
/// The digits carry the order. `ElicitationSchema` holds its
/// properties in a `BTreeMap` and its `property_order` is
/// `#[serde(skip)]`, so the wire order is the sorted key order and the
/// order they were built in is lost. Sorted alphabetically the form
/// opens on the free-text box with the claim third; sorted by these
/// prefixes it opens on the claim, which is what the human came to
/// read. Nobody sees these names — the titles are what render.
const STANDS: &str = "1_stands";
const RATHER: &str = "2_rather";
const SAME_AS: &str = "3_same_as";
const UNCLEAR: &str = "4_unclear";
const CORRECTION: &str = "5_correction";

impl Question {
    /// The round's transport id — the MRTR map key and the deferred
    /// set's member. Built from identity (subject, aspect, the
    /// assumption's key), so it survives a re-record that reorders the
    /// assumptions array.
    fn id(&self) -> String {
        format!("loose:{}:{}:{}", self.subject, self.aspect, self.key)
    }

    /// The form: identity in the message, the answers in the fields.
    ///
    /// Where each piece of text may live is decided by the renderer,
    /// measured against 2.1.237. The message is clipped to three lines
    /// and each line truncated to the terminal width. A field's title
    /// is truncated too — it shares its line with the value, so a
    /// claim put there arrives as `an entry is a row in results —
    /// every car the ex…`. A field's *description* is the only surface
    /// that renders whole: it wraps under the field with a gutter and
    /// nothing caps its length.
    ///
    /// So the titles are short fixed labels and the substance rides
    /// the descriptions — the claim and its basis under the box that
    /// confirms it, the rival under the box that takes it, the prior
    /// words under the box that repeats them. The human reads a
    /// reading and picks one, rather than reading a sentence and
    /// voting on it.
    ///
    /// Every box carries `false` rather than nothing, so it draws as
    /// an empty checkbox instead of the words `not set`.
    ///
    /// Nothing is required. A required field holds the confirm button
    /// closed until it is set, which would make "I have no answer"
    /// unreachable except by leaving the form.
    fn params(&self, rank: usize, total: usize) -> Result<ElicitRequestParams, String> {
        let Question {
            subject,
            aspect,
            dimension,
            assumption,
            basis,
            alternative,
            confidence,
            sibling,
            sibling_note,
            ..
        } = self;
        let mut schema = ElicitationSchema::builder().optional_bool_with(STANDS, |b| {
            b.title("Stands as stated")
                .description(format!("{assumption}\nbasis: {basis}"))
                .with_default(false)
        });
        // The rival, offered as itself. Taking it is a correction
        // whose words the agent already wrote down.
        if let Some(rival) = alternative {
            schema = schema.optional_bool_with(RATHER, |b| {
                b.title("Rather, this reading")
                    .description(rival.clone())
                    .with_default(false)
            });
        }
        // What they already ruled on this same claim elsewhere, offered
        // back as an answer. One decision spelled with one key across
        // several groundings is asked once per grounding, because a
        // human may rule the same key differently on two aspects and
        // has. What the repeat must not cost is a re-reading.
        if let Some(prior) = sibling {
            let note = sibling_note.clone().unwrap_or_default();
            schema = schema.optional_bool_with(SAME_AS, |b| {
                b.title("Same as before")
                    .description(format!("{prior}\n{note}"))
                    .with_default(false)
            });
        }
        schema = schema
            .optional_bool_with(UNCLEAR, |b| {
                b.title("The question itself is unclear")
                    .description(
                        "Refuses the question, not the claim: it will be reformulated \
                         and asked again.",
                    )
                    .with_default(false)
            })
            .optional_string_with(CORRECTION, |s| {
                s.title("What is right instead")
                    .description("Your own words. They outrank every box above.")
            });
        Ok(ElicitRequestParams::FormElicitationParams {
            meta: None,
            // Identity, then where this sits in everything that stands
            // open. The position is the whole point of a bounded
            // round: nobody reaches the end of one and believes it was
            // the end of the work.
            message: format!(
                "{subject} · {aspect} — {dimension} · confidence {confidence} \
                 · {rank} of {total} open"
            ),
            requested_schema: schema.build().map_err(|e| e.to_string())?,
        })
    }
}

impl ServerHandler for GlossqlMcp {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        );
        info.server_info = Implementation::new("glossql-serverd", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(format!(
            "{INSTRUCTIONS}\n\n{} {}",
            self.brief.line(),
            self.brief.opening()
        ));
        info
    }

    /// One revision. An older client is refused with the list it could
    /// have used (`UnsupportedProtocolVersionError`) rather than served
    /// under semantics this door no longer implements — there is no
    /// session for it to be given, and no server-initiated request for
    /// it to receive.
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    /// The list is static per process — one tool, built from a constant
    /// schema — so it is cacheable for an hour. `private` because a
    /// workspace's door is not a shared intermediary's to hand on.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult {
            tools: vec![self.tool()],
            ttl_ms: Some(3_600_000),
            cache_scope: Some(rmcp::model::CacheScope::Private),
            ..Default::default()
        })
    }

    /// The teaching resources: the skills and the two normative
    /// artifacts they cite. Embedded at compile time, so static per
    /// process and cacheable like the tool list.
    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let mut resources: Vec<Resource> = crate::skills::SKILLS
            .iter()
            .map(|s| {
                Resource::new(s.uri(), s.name)
                    .with_description(s.description())
                    .with_mime_type("text/markdown")
                    .with_size(s.body.len() as u64)
            })
            .collect();
        resources.extend(crate::skills::DOCS.iter().map(|d| {
            Resource::new(d.uri(), d.name)
                .with_description(d.description)
                .with_mime_type(d.mime)
                .with_size(d.body.len() as u64)
        }));
        // The trees: a skill's references after its SKILL.md, then the
        // docs pages — each listed by its first heading, which is what
        // tells a reader when the page is worth its tokens.
        resources.extend(
            crate::skills::REFERENCES
                .iter()
                .chain(crate::skills::PAGES.iter())
                .map(|p| {
                    Resource::new(p.uri(), p.path)
                        .with_description(p.title())
                        .with_mime_type("text/markdown")
                        .with_size(p.body.len() as u64)
                }),
        );
        Ok(ListResourcesResult {
            resources,
            ttl_ms: Some(3_600_000),
            cache_scope: Some(rmcp::model::CacheScope::Private),
            ..Default::default()
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let (mime, body) = crate::skills::read(&request.uri).ok_or_else(|| {
            McpError::resource_not_found(format!("no resource at `{}`", request.uri), None)
        })?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(body, request.uri).with_mime_type(mime),
        ])
        .with_ttl_ms(3_600_000)
        .with_cache_scope(rmcp::model::CacheScope::Private)
        .into())
    }

    /// Each skill is also a prompt of the same name — the slash-command
    /// way in for a person, where resources wait on the client to offer
    /// them.
    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Ok(ListPromptsResult {
            prompts: crate::skills::SKILLS
                .iter()
                .map(|s| Prompt::new(s.name, Some(s.description()), None))
                .collect(),
            ttl_ms: Some(3_600_000),
            cache_scope: Some(rmcp::model::CacheScope::Private),
            ..Default::default()
        })
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        let skill = crate::skills::SKILLS
            .iter()
            .find(|s| s.name == request.name)
            .ok_or_else(|| {
                McpError::invalid_params(format!("unknown prompt `{}`", request.name), None)
            })?;
        Ok(
            GetPromptResult::new(vec![PromptMessage::new_text(Role::User, skill.body)])
                .with_description(skill.description())
                .into(),
        )
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

        // Actor rides the transport (SPEC.md §1). The gate verified a
        // token and left the caller in the HTTP extensions, which rmcp
        // forwards to this handler as `http::request::Parts`; this door
        // is the agent door, so the gate stamped agent standing. The
        // client's own `clientInfo` name is not used: it is a string the
        // caller picks for itself on each request, so recording it would
        // put an unproven name in the actor column of the record.
        let actor = context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<Caller>())
            .map(|caller| caller.0.clone())
            .ok_or_else(|| {
                McpError::internal_error("the door is not behind the gate: no caller", None)
            })?;
        let id = actor.id.clone();
        // The call opens unbound. There is no session to hold a dataset
        // and no path segment to carry one: the statements say where
        // they are, as `USE`, and `execute` moves with them. A call that
        // names none is workspace-scoped, which is what reading
        // `datasets` and writing a source-grain gloss both want.
        let session = self
            .plane
            .channel(actor.clone(), None)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        // The question round rides ahead of execution as an MRTR
        // `input_required` result (SEP-2322): the ask is the result,
        // and the answer arrives on the client's retry of this same
        // call. There is no other mechanism — the spec is explicit that
        // a server "MUST send server-to-client requests using the MRTR
        // pattern", and the older server-initiated request is gone.
        //
        // Cadence: forms ride only calls that read the record — a
        // metadata read, no writes — so the brief sweep and the stage
        // read-backs carry the round while landings and judging queries
        // run uninterrupted. A writing call re-opens what a decline
        // deferred. One question per call, only while the workspace
        // derives open items; the capability must come from the
        // request's own stamp, since the transport's peer_info is
        // synthetic when every request stands alone.
        let shape = glossql_session::call_shape(statements);
        let mut probed = None;
        if let Some(responses) = &request.input_responses {
            let note = if request.request_state.as_deref() != Some(ROUND_STATE) {
                "question-round: a retry without the echoed state".to_string()
            } else if !responses.is_empty() {
                // A round is a batch, so a retry carries a batch. Each
                // answer is digested against its own re-derived
                // question; one that no longer stands is said and
                // skipped, never taken as a verdict on another.
                let mut notes = Vec::new();
                for (key, raw) in responses.iter() {
                    notes.push(match serde_json::from_value::<ElicitResult>(raw.clone()) {
                        Ok(answer) => Box::pin(self.digest_round(key, answer, &session)).await,
                        Err(e) => format!("question-round: the answer does not parse: {e}"),
                    });
                }
                notes.join("\n")
            } else {
                "question-round: the retry carries no answer".into()
            };
            tracing::info!(subject = %id, note = %note, "question-round");
            probed = Some(note);
        } else if shape.reviews
            && context
                .client_capabilities()
                .and_then(|caps| caps.elicitation)
                .is_some()
        {
            // The whole batch travels in one result. The call ends
            // normally, the client fulfils every entry before it
            // retries, and nothing on the server waits: the round
            // costs the agent no time whether or not a person is
            // there.
            let (round, total) = self.derive_round(&session).await;
            let mut asks = InputRequests::new();
            let mut ids = Vec::new();
            for (rank, q) in &round {
                match q.params(*rank, total) {
                    Ok(params) => {
                        ids.push(q.id());
                        asks.insert(
                            q.id(),
                            InputRequest::Elicitation(ElicitRequest::new(params)),
                        );
                    }
                    Err(e) => {
                        tracing::warn!(subject = %id, error = %e, "question-round: form refused")
                    }
                }
            }
            if !asks.is_empty() {
                tracing::info!(
                    subject = %id,
                    asking = ids.len(),
                    open = total,
                    ids = %ids.join(", "),
                    "question-round"
                );
                return Ok(InputRequiredResult::new(Some(asks), Some(ROUND_STATE.into())).into());
            }
        }

        // A single query streams from the engine and stops at the cap —
        // what the agent won't see is never computed. Metadata reads
        // (GLOSSARY(), ATTEST(), the store relations) are exempt from
        // the cap: the map must be whole,
        // and the store bounds it. Everything else runs through execute.
        // What a refused sequence had already landed rides beside the
        // refusal, in the usual shape — the writes stood.
        let mut landed_json: Option<serde_json::Value> = None;
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
            // Statement sequences run at the plane: `USE` moves the
            // statements after it onto another channel for the rest of
            // this call, and never rebinds a session.
            Err(SessionError::NotOneRead) => {
                match self.plane.execute(actor, None, statements).await {
                    Ok(outcomes) => wire::outcomes_json(&outcomes, self.doors.row_cap),
                    Err(e) => {
                        if let SessionError::Sequence { landed, .. } = &e
                            && !landed.is_empty()
                        {
                            landed_json = wire::outcomes_json(landed, self.doors.row_cap).ok();
                        }
                        Err(e.to_string())
                    }
                }
            }
            Err(e) => Err(e.to_string()),
        };
        // The brief travels on the call that moved it: initialize
        // instructions are fetched once per connection, so a
        // long-lived session would never
        // see the counts change. Two rules hold it honest:
        // movement is decided on the COUNTS, never on the rendered
        // line (a rendered string is display, not identity); and the
        // The baseline is the facts as they stood before this call.
        // One shared baseline, so the mover hears it: a second agent
        // watching the same workspace does not, and that is the known
        // cost of not keeping per-actor state the run has never asked
        // for.
        // Scoped to the cause: only a write (or a landed ruling) can
        // move the counts, so a pure read never pays the recomposition.
        let brief_moved = if shape.writes || probed.is_some() {
            let before = self.brief.facts.read().ok().and_then(|f| f.clone());
            Self::refresh_brief(&self.plane, &self.brief).await;
            let after = self.brief.facts.read().ok().and_then(|f| f.clone());
            (after.is_some() && after != before).then(|| format!("brief: {}", self.brief.line()))
        } else {
            None
        };
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
                tracing::warn!(subject = %id, error = %e, "refused");
                let mut blocks = vec![ContentBlock::text(e)];
                if let Some(landed) = landed_json {
                    blocks.push(ContentBlock::text(
                        serde_json::json!({ "landed": landed }).to_string(),
                    ));
                }
                if let Some(brief) = brief_moved {
                    blocks.push(ContentBlock::text(brief));
                }
                CallToolResult::error(blocks)
            }
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn question() -> Question {
        Question {
            dataset: "f1".into(),
            subject: "f1".into(),
            aspect: "race_entries".into(),
            key: "entry-means-a-results-row".into(),
            dimension: "scope".into(),
            assumption: "an entry is a row in results, including cars that never started".into(),
            basis: "results carries no did-not-start marker beyond status_id".into(),
            alternative: None,
            confidence: 0.7,
            sibling: None,
            sibling_stance: None,
            sibling_note: None,
        }
    }

    fn form(q: &Question) -> (String, serde_json::Value) {
        match q.params(3, 12).expect("the form builds") {
            ElicitRequestParams::FormElicitationParams {
                message,
                requested_schema,
                ..
            } => (
                message,
                serde_json::to_value(&requested_schema).expect("schema serializes"),
            ),
            other => panic!("form mode expected, got {other:?}"),
        }
    }

    /// The message is the one surface the client clips, so it carries
    /// identity only — never the claim, which has to be read whole —
    /// and the position, so a bounded round never reads as the end of
    /// the work.
    #[test]
    fn the_message_is_identity_and_position() {
        let (message, _) = form(&question());
        assert_eq!(message.lines().count(), 1, "{message}");
        assert!(message.contains("race_entries"), "{message}");
        assert!(message.contains("3 of 12 open"), "{message}");
        assert!(!message.contains("an entry is a row"), "{message}");
    }

    /// Every answer is its own field: the renderer draws a checkbox in
    /// the list and hides an enum's options behind a keypress.
    #[test]
    fn every_answer_is_a_visible_field() {
        let (_, schema) = form(&question());
        let props = &schema["properties"];
        for name in [STANDS, UNCLEAR] {
            assert_eq!(props[name]["type"], "boolean", "{name} in {schema}");
        }
        assert_eq!(props[CORRECTION]["type"], "string", "{schema}");
        assert!(
            schema
                .get("required")
                .is_none_or(serde_json::Value::is_null),
            "nothing is required, or the confirm button never opens: {schema}"
        );
    }

    /// A title shares its line with the value and is truncated; a
    /// description renders whole. So the claim and its basis ride the
    /// description of the box that confirms them, and the title is a
    /// short fixed label.
    #[test]
    fn the_claim_rides_the_description_of_stands() {
        let q = question();
        let (_, schema) = form(&q);
        let stands = &schema["properties"][STANDS];
        assert_eq!(stands["title"], "Stands as stated");
        let said = stands["description"].as_str().expect("a description");
        assert!(said.contains(&q.assumption), "{schema}");
        assert!(said.contains(&q.basis), "{schema}");
    }

    /// The wire order is the sorted key order, so the names carry the
    /// order: the form must open on the claim, not on the text box.
    #[test]
    fn the_form_opens_on_the_claim() {
        let offered = Question {
            alternative: Some("starters only".into()),
            sibling: Some("corrected on finish_rate".into()),
            sibling_stance: Some("corrected".into()),
            sibling_note: Some("status_id = 1".into()),
            ..question()
        };
        let (_, schema) = form(&offered);
        let order: Vec<&str> = schema["properties"]
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            order,
            vec![STANDS, RATHER, SAME_AS, UNCLEAR, CORRECTION],
            "the claim first, the free text last"
        );
    }

    /// A box for a reading the record does not hold would be a box
    /// that cannot be answered, so neither appears unless it stands.
    #[test]
    fn the_rival_and_the_prior_ruling_appear_only_when_they_stand() {
        let (_, bare) = form(&question());
        assert!(bare["properties"].get(RATHER).is_none(), "{bare}");
        assert!(bare["properties"].get(SAME_AS).is_none(), "{bare}");

        let offered = Question {
            alternative: Some("starters only, excluding rows whose grid slot is 0".into()),
            sibling: Some("corrected on finish_rate".into()),
            sibling_stance: Some("corrected".into()),
            sibling_note: Some("status_id = 1 is the finish marker".into()),
            ..question()
        };
        let (_, schema) = form(&offered);
        assert_eq!(
            schema["properties"][RATHER]["description"],
            offered.alternative.as_deref().unwrap()
        );
        assert!(
            schema["properties"][SAME_AS]["description"]
                .as_str()
                .is_some_and(|d| d.contains("corrected on finish_rate")),
            "{schema}"
        );
    }
}
