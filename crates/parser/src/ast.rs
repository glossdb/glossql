//! AST for the glossql statement forms (`grammar.ebnf`).
//!
//! Names are sqlparser [`Ident`]s (quoting and spans ride along). Substrate
//! SQL is not modelled here — it stays a DataFusion
//! [`DFStatement`](datafusion_sql::parser::Statement).

use datafusion_sql::parser::Statement as DFStatement;
use datafusion_sql::sqlparser::ast::Ident;
use serde_json::Value;

/// A JSON object body: the parsed value plus the verbatim dollar-quoted
/// content (byte-exact, useful for hashing and display).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonBody {
    pub value: Value,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Declare(Box<Declaration>),
    Use(Use),
    Gloss(Gloss),
    Extract(Extract),
    Probe(Probe),
    /// Host SQL, parsed by `DFParser` — includes `GLOSSARY()` / `ATTEST()`
    /// reads and glossary/cache DELETEs.
    Substrate(Box<DFStatement>),
}

/// `PROBE source AS $$sql$$` — a recipe rehearsal (ruled 2026-08-04): the
/// same SQL surface as a recipe, executed at the source, landing nothing.
/// The result carries the schema the recipe would land, so `LIMIT 0`
/// rehearses the identity a `DECLARE RECIPE` would stamp.
#[derive(Debug, Clone, PartialEq)]
pub struct Probe {
    pub source: Ident,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Declaration {
    Source(SourceDecl),
    Recipe(RecipeDecl),
    Dataset(DatasetDecl),
    Relationship(RelationshipDecl),
    Aspect(AspectDecl),
    Function(FunctionDecl),
    Witness(WitnessDecl),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDecl {
    pub name: Ident,
    pub settings: Vec<Setting>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetDecl {
    pub name: Ident,
    pub settings: Vec<Setting>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Setting {
    pub key: Ident,
    pub value: SettingValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SettingValue {
    Name(Ident),
    String(String),
    /// Decimal literal, kept verbatim.
    Number(String),
}

/// `DECLARE RECIPE r ON dataset FROM source AS $$ <sql> $$` — the SQL runs
/// at the source, in the source's dialect; it is dollar-quoted so
/// foreign-dialect text never meets the host tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub struct RecipeDecl {
    pub table: Ident,
    pub dataset: Ident,
    pub source: Ident,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationshipDecl {
    pub left: RelSide,
    pub op: RelOp,
    pub right: RelSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOp {
    /// `->` — many-to-one, the FK direction.
    ManyToOne,
    /// `<->` — one-to-one.
    OneToOne,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AspectDecl {
    pub name: Ident,
    pub schema: JsonBody,
    pub kind: AspectKind,
    /// `ON grain, …` — the subject classes glosses may attach to (ruled
    /// 2026-08-05); empty = all grains.
    pub grains: Vec<Grain>,
    /// `WHEN aspect = 'value'` — conditional relevance (ruled
    /// 2026-08-14): the aspect is owed on a subject only while the
    /// named sibling aspect's collapsed body carries `value` = the
    /// literal. Bounds `unassessed` disclosure only; writes stay gated
    /// by grain, and a spoken slot outside its condition serves.
    pub condition: Option<(Ident, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grain {
    Dataset,
    Table,
    Column,
    Relationship,
    /// A declared source's name as subject (ruled 2026-08-12): the
    /// deposit the next dataset reads — source-grain slots collapse
    /// workspace-wide.
    Source,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AspectKind {
    Measurement,
    Fact,
    Query,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionDecl {
    pub name: Ident,
    pub scope: FunctionScope,
    /// The script reference named by `FROM`.
    pub script: String,
    /// `ACCEPTS (aspect, …)` — aspects whose current values the server
    /// hands the script as its context document; empty = no context.
    pub accepts: Vec<Ident>,
    /// `RETURNS aspect` — the aspect this function's output fills,
    /// mirroring `ACCEPTS` (project lead, 2026-08-04). Absent, the
    /// function is a detector: role is declared by shape.
    pub returns: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionScope {
    Dataset(Ident),
    Global,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WitnessDecl {
    pub name: Ident,
    pub aspect: Ident,
    pub speakers: Vec<Speaker>,
    pub detector: Option<Ident>,
    /// Decimal literal in 0..1, kept verbatim; range is checked at
    /// admission, not in the grammar.
    pub threshold: Option<String>,
}

/// A speaker in a witness's `BY` list — actor kinds only. A function's
/// voice comes from its `RETURNS` binding, never from the gate (project
/// lead, 2026-08-04).
#[derive(Debug, Clone, PartialEq)]
pub enum Speaker {
    Agent,
    Human,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Use {
    pub dataset: Ident,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Gloss {
    pub aspect: Ident,
    pub subject: Subject,
    pub body: JsonBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Subject {
    Path(Path),
    Pair(Box<PairPath>),
}

/// `dataset | dataset.table | dataset.table.column` — one to three
/// segments; unprefixed paths resolve against the USE'd dataset, which is
/// not the parser's business.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub segments: Vec<Ident>,
}

/// A relationship endpoint: `[dataset.]table.column` or, composite,
/// `[dataset.]table.(a, b)` — the tuple is the key (ruled 2026-08-05;
/// there is no derived-column cure).
#[derive(Debug, Clone, PartialEq)]
pub struct RelSide {
    pub dataset: Option<Ident>,
    pub table: Ident,
    /// One column, or ≥ 2 for a composite endpoint.
    pub columns: Vec<Ident>,
}

/// A declared relationship, addressed by its pair path (relationships have
/// no names).
#[derive(Debug, Clone, PartialEq)]
pub struct PairPath {
    pub left: RelSide,
    pub op: RelOp,
    pub right: RelSide,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Extract {
    /// Bare function names — calls carry no arguments; settings are context.
    pub calls: Vec<Ident>,
    pub subject: Subject,
}
