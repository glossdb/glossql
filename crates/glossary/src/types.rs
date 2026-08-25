//! Row and actor types served by the store.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A backend could not serve or accept rows. The store's rules never
    /// produce this — only the IO behind the metadata seam does.
    #[error("store backend: {0}")]
    Backend(String),
    #[error("unknown {what} `{name}` — declare it first")]
    Unknown { what: &'static str, name: String },
    #[error("aspect `{0}` is MEASUREMENT — measurements are computed by functions, never glossed")]
    MeasurementGloss(String),
    #[error("body rejected by the {which} schema: {detail}")]
    BodyRejected { which: String, detail: String },
    #[error("no witness on aspect `{aspect}` admits {kind} glosses")]
    SpeakerNotAdmitted { aspect: String, kind: ActorKind },
    #[error("aspect `{name}`: WITH is not a usable JSON Schema: {detail}")]
    BadAspectSchema { name: String, detail: String },
    #[error("aspect `{name}`: {detail}")]
    BadCondition { name: String, detail: String },
    #[error(
        "aspect `{name}` has {glosses} gloss(es) — delete them before re-declaring it differently"
    )]
    AspectInUse { name: String, glosses: i64 },
    #[error(
        "witness on MEASUREMENT aspect `{0}` cannot name BY — measurements are never glossed; only a DETECTOR applies"
    )]
    MeasurementWitnessSpeakers(String),
    #[error(
        "function `{function}` is not eligible as detector — it RETURNS an aspect; a detector is a function without RETURNS"
    )]
    DetectorNotEligible { function: String },
    #[error(
        "aspect `{aspect}` is already returned by `{existing}` — a MEASUREMENT aspect has one producing function"
    )]
    MeasurementProducerTaken { aspect: String, existing: String },
    #[error("witness `{0}` names neither BY nor DETECTOR — nothing to declare")]
    WitnessNamesNothing(String),
    #[error(
        "function `{function}` RETURNS `{aspect}`, a QUERY aspect — metrics run as their SQL, functions never fill them"
    )]
    ReturnsQueryAspect { function: String, aspect: String },
    #[error("aspect `{aspect}` is declared ON {declared} — `{subject}` is a {grain} subject")]
    GrainRefused {
        aspect: String,
        subject: String,
        grain: &'static str,
        declared: String,
    },
    #[error("statement targets `{0}` — only the glossary relation accepts forwarded SQL")]
    ForwardRejected(String),
    #[error(
        "the strike is parked: the substrate cannot remove rows until iceberg-rust lands the delete write path — supersede the slot, or rebuild the workspace"
    )]
    StrikeParked,
    #[error(
        "`{0}` is a store relation — a table cannot take its name, it would shadow the relation"
    )]
    ReservedTableName(String),
    #[error("stored JSON is corrupt: {0}")]
    Corrupt(String),
}

impl From<glossql_catalog::Error> for Error {
    fn from(e: glossql_catalog::Error) -> Self {
        Error::Backend(e.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorKind {
    Agent,
    Human,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::Agent => "agent",
            ActorKind::Human => "human",
        }
    }
}

impl std::fmt::Display for ActorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The connection's actor (SPEC.md §1): every write is stamped with it; there
/// is no BY clause anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub kind: ActorKind,
    pub id: String,
}

/// One row of `GLOSSARY(subject, all => true)` (SPEC.md §5.3): one current
/// value per (subject, aspect, kind, witness), precedence the reader's
/// business. `kind` is the aspect's kind (fact | query | measurement); who
/// spoke is `actor` (an actor id, or the function name for the measurement
/// slot) under `witness`.
#[derive(Debug, Clone)]
pub struct RawRow {
    pub subject: String,
    pub aspect: String,
    pub kind: String,
    pub witness: Option<String>,
    pub actor: String,
    pub body: String,
    pub written_at: String,
    /// Which slot spoke — `human` | `agent` | `function`. Not part of the
    /// §5.3 shape; detectors receive it in their slots document.
    pub speaker: String,
    /// Whether the slot stands at the read's pin — false for a function
    /// voice landed at an earlier one, which still serves (§7).
    pub current: bool,
}

/// One row of the collapsed `GLOSSARY(subject)` read (SPEC.md §5.3).
/// `value` is the precedence pick (human > agent > function) unless the
/// detector's score exceeds the witness threshold; `state` says what the
/// value's absence or presence means — the read never hides a gap:
/// `unassessed` (a witness exists, nobody spoke — the row still appears),
/// `contested` (entropy over threshold, value withheld), `current`, or
/// `stale` (served and marked: the table's snapshot moved on, or the
/// column's type decision postdates the write).
#[derive(Debug, Clone)]
pub struct CollapsedRow {
    pub subject: String,
    pub aspect: String,
    pub value: Option<String>,
    pub band: Option<String>,
    pub score: Option<f64>,
    pub state: String,
    /// The serving voice's rank — 0 human, 1 agent, 2 function — where a
    /// value is served; None on a contested or unassessed row. Not a
    /// served column: what a read policy needs to know whose word it is
    /// acting on.
    pub rank: Option<u8>,
}

/// One row of `ATTEST(...)` — the fixed attest shape (SPEC.md §7.2).
#[derive(Debug, Clone)]
pub struct AttestRow {
    pub subject: String,
    pub aspect: String,
    pub witness: String,
    pub band: String,
    pub score: f64,
    pub computed_at: String,
    /// Whether every function voice the verdict read stands at the
    /// read's pin — false when a voice was landed at an earlier one.
    pub current: bool,
}

/// A measurement served from the `measurements` relation: one function's
/// output at one pin.
#[derive(Debug, Clone)]
pub struct MeasurementRow {
    pub subject: String,
    pub function: String,
    pub body: String,
    pub computed_at: String,
}

/// One glossary row, with the format's write order — the store's read
/// rules run over these.
#[derive(Debug, Clone)]
pub struct GlossRow {
    pub dataset: String,
    pub subject: String,
    pub aspect: String,
    pub actor_kind: String,
    pub actor_id: String,
    pub body: String,
    pub written_at: String,
    pub snapshot_id: Option<i64>,
    pub seq: (i64, i64),
}

/// A declared aspect, parsed once.
#[derive(Debug, Clone)]
pub struct AspectRow {
    pub name: String,
    pub kind: String,
    pub grains: Option<String>,
    pub condition: Option<(String, String)>,
    pub schema: String,
}

impl AspectRow {
    pub fn source_grain(&self) -> bool {
        self.grains
            .as_deref()
            .is_some_and(|g| g.split(',').any(|g| g == "source"))
    }
}

/// One witness's verdict over one subject, computed at read and never
/// stored. The collapse withholds when a score
/// crosses its own witness's threshold — never a neighbour's.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub witness: String,
    pub band: String,
    pub score: f64,
    pub threshold: Option<f64>,
    pub computed_at: String,
    /// Whether every function voice in the subject's slots stands at
    /// the read's pin.
    pub current: bool,
}

/// Verdicts keyed (subject, aspect) — the session computes them (it
/// holds the script runtime), the collapse consumes them.
pub type Verdicts = std::collections::HashMap<(String, String), Vec<Verdict>>;

/// A declared function (SPEC.md §6), as the session's extraction executor
/// needs it.
#[derive(Debug, Clone)]
pub struct FunctionRow {
    pub name: String,
    /// `None` = GLOBAL.
    pub scope_dataset: Option<String>,
    pub script: String,
    /// `RETURNS aspect` — the aspect the output fills; output validates
    /// against that aspect's schema. `None` = detector.
    pub returns: Option<String>,
}

/// What a `DECLARE RECIPE` amounted to (SPEC.md §3): the session
/// materializes on `Created`, leaves `Unchanged` alone, and on
/// `Replaced` drops the old landing before re-materializing
/// (supersede-and-reland: a post-landing defect needs a way through
/// the refusal wall).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeAdmission {
    Created,
    Unchanged,
    Replaced,
}

/// A stored recipe, as materialization needs it.
#[derive(Debug, Clone)]
pub struct RecipeRow {
    pub source: String,
    pub sql: String,
}

/// A declared witness (SPEC.md §7.1). Function voices are not here — they
/// arrive through `RETURNS`; `BY` gates actors only.
#[derive(Debug, Clone)]
pub struct WitnessRow {
    pub name: String,
    pub aspect: String,
    pub admits_agent: bool,
    pub admits_human: bool,
    pub detector: Option<String>,
    pub threshold: Option<f64>,
}
