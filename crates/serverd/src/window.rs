//! The window — the mechanical layer of the procedural graph, served
//! on every door call's result, localized to the act the call landed
//! on. The shape is the localized two-hop view of arXiv 2609.09153;
//! what that paper cannot have, and this door does, is the record as
//! the localizer's second input.
//!
//! The graph is `window.json` at the repository root: embedded here,
//! served as `doc://window.json`, held to its vocabulary by the suite
//! (`tests/suite/window.rs`). A node names an act: a statement kind
//! (`USE`, `PROBE`, `EXTRACT`, `SQL`, `DECLARE <what>`, `DECLARE ASPECT
//! <kind>`, `GLOSS <aspect>` for an aspect the graph names, `GLOSS
//! <kind>` for any other), a function, a read, `Start` or `End`. An
//! edge says `to` is admissible after `from`: `when` under what
//! condition, `guidance` how, `pitfalls` what goes wrong, `source` the
//! page that states it. A `key` makes the condition a fact of the
//! record — `{read, where, none}`: some row of `read` matches every
//! `where` predicate (a value, `"empty"`, `"nonempty"`, `"nonzero"`),
//! or no row does when `none` is true. An edge without a key is
//! admissible whenever its node is.
//!
//! Served: the hop-1 edges out of the node whose key holds or that
//! carry none, keyed first, capped, then the hop-2 names on one line.
//! A key is evaluated only for edges out of the current node, one read
//! per relation per call, and only when the call bound a dataset — an
//! edge whose key cannot be evaluated is not served. No order: the
//! block says what the record admits next, and judgment stays the
//! agent's. No model, no store row, no procedural edge: those the loop
//! learns in the eval harness, and nothing learned rides here until
//! the gate accepted it.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::sync::OnceLock;

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::visit_relations;
use glossql_parser::{AspectKind, Declaration, GlossqlParser, Statement};
use serde::Deserialize;
use serde_json::Value;

/// The graph, verbatim — `doc://window.json` serves the same bytes.
pub const GRAPH_JSON: &str = include_str!("../../../window.json");

/// How many hop-1 edges a window shows before it counts the rest.
pub const CAP: usize = 10;

#[derive(Deserialize)]
pub struct Graph {
    pub nodes: Vec<String>,
    pub edges: Vec<Edge>,
}

#[derive(Deserialize, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub relation: String,
    pub when: When,
    #[serde(default)]
    pub guidance: String,
    #[serde(default)]
    pub pitfalls: String,
    #[serde(default)]
    pub source: String,
}

#[derive(Deserialize, Clone, Default)]
pub struct When {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub key: Option<Keys>,
}

#[derive(Deserialize, Clone)]
pub struct Key {
    pub read: String,
    #[serde(rename = "where", default)]
    pub conditions: BTreeMap<String, Value>,
    #[serde(default)]
    pub none: bool,
}

/// One key, or several that must all hold — a condition over two
/// reads, such as a metric applicable and no app standing.
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum Keys {
    One(Key),
    All(Vec<Key>),
}

impl Keys {
    pub fn each(&self) -> impl Iterator<Item = &Key> {
        match self {
            Keys::One(key) => std::slice::from_ref(key).iter(),
            Keys::All(keys) => keys.iter(),
        }
    }
}

/// The embedded graph, parsed once.
pub fn graph() -> &'static Graph {
    static GRAPH: OnceLock<Graph> = OnceLock::new();
    GRAPH.get_or_init(|| {
        serde_json::from_str(GRAPH_JSON).expect("window.json parses; the suite holds it")
    })
}

impl Graph {
    /// The node spelled as the graph spells it, matched without case —
    /// `attest` in SQL is the graph's `ATTEST`.
    pub fn node_named(&self, name: &str) -> Option<&str> {
        self.nodes
            .iter()
            .map(String::as_str)
            .find(|n| n.eq_ignore_ascii_case(name))
    }

    /// The edges out of a node, in file order.
    pub fn out(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node).collect()
    }
}

/// Where a call left the agent: the dataset its statements bound, and
/// the act its last statement was.
pub struct Locus {
    pub dataset: Option<String>,
    pub act: Act,
}

/// An act: a node, or a gloss whose node needs the aspect's kind from
/// the record — the graph names some aspects (`GLOSS app`, `GLOSS
/// formulas`) and the rest localize by kind (`GLOSS query`).
pub enum Act {
    Node(String),
    Gloss(String),
}

/// The locus of a call: its statements, and how many of them ran —
/// every one when the call landed, the refused one's place when it did
/// not (`refused`), unknown when the parser refused the call whole.
pub fn locate(graph: &Graph, statements: &str, ran: Option<usize>, refused: bool) -> Locus {
    match GlossqlParser::parse_sql(statements) {
        Ok(parsed) => {
            let n = ran.unwrap_or(parsed.len()).min(parsed.len());
            // A refused statement bound nothing: the dataset is the
            // last USE that landed.
            let landed = if refused { n.saturating_sub(1) } else { n };
            let ran = &parsed[..n];
            let dataset = parsed[..landed].iter().rev().find_map(|s| match s {
                Statement::Use(u) => Some(u.dataset.value.clone()),
                _ => None,
            });
            let act = ran
                .iter()
                .rev()
                .find(|s| !matches!(s, Statement::Use(_)))
                .map(|s| act_of(graph, s))
                .or_else(|| ran.last().map(|_| Act::Node("USE".into())))
                .unwrap_or(Act::Node("SQL".into()));
            Locus { dataset, act }
        }
        Err(_) => locate_by_text(graph, statements, ran, refused),
    }
}

/// The aspect kind as the graph spells it.
pub fn kind_word(kind: &AspectKind) -> &'static str {
    match kind {
        AspectKind::Query => "query",
        AspectKind::Fact => "fact",
        AspectKind::Measurement => "measurement",
    }
}

fn act_of(graph: &Graph, statement: &Statement) -> Act {
    match statement {
        Statement::Use(_) => Act::Node("USE".into()),
        Statement::Probe(_) => Act::Node("PROBE".into()),
        Statement::Declare(d) => Act::Node(match d.as_ref() {
            Declaration::Aspect(a) => format!("DECLARE ASPECT {}", kind_word(&a.kind)),
            Declaration::Source(_) => "DECLARE SOURCE".into(),
            Declaration::Recipe(_) => "DECLARE RECIPE".into(),
            Declaration::Dataset(_) => "DECLARE DATASET".into(),
            Declaration::Relationship(_) => "DECLARE RELATIONSHIP".into(),
            Declaration::Function(_) => "DECLARE FUNCTION".into(),
            Declaration::Witness(_) => "DECLARE WITNESS".into(),
        }),
        Statement::Gloss(g) => gloss_act(graph, &g.aspect.value),
        // An extraction localizes to the function it calls when the
        // graph knows it, else to the statement kind.
        Statement::Extract(e) => Act::Node(
            e.calls
                .iter()
                .find_map(|c| graph.node_named(&c.value).map(str::to_string))
                .unwrap_or_else(|| "EXTRACT".into()),
        ),
        Statement::Substrate(df) => Act::Node(substrate_node(graph, df)),
    }
}

/// A gloss on an aspect the graph names is that node; any other waits
/// for the aspect's kind from the record.
fn gloss_act(graph: &Graph, aspect: &str) -> Act {
    match graph.node_named(&format!("GLOSS {aspect}")) {
        Some(node) => Act::Node(node.to_string()),
        None => Act::Gloss(aspect.to_string()),
    }
}

/// SQL localizes to the first relation it names that the graph knows —
/// a door such as `metric_axes()`, a read such as `owed`, a family
/// such as `read.<name>` — else to `SQL`.
fn substrate_node(graph: &Graph, df: &DFStatement) -> String {
    let DFStatement::Statement(inner) = df else {
        return "SQL".into();
    };
    let mut found: Option<String> = None;
    let _ = visit_relations(inner.as_ref(), |name| {
        if found.is_none() {
            let parts: Vec<&str> = name
                .0
                .iter()
                .filter_map(|p| p.as_ident())
                .map(|i| i.value.as_str())
                .collect();
            let candidate = match parts.as_slice() {
                [one] => (*one).to_string(),
                [family, _] => format!("{family}.<name>"),
                _ => String::new(),
            };
            found = graph.node_named(&candidate).map(str::to_string);
        }
        ControlFlow::<()>::Continue(())
    });
    found.unwrap_or_else(|| "SQL".into())
}

/// The locus of a call the parser refused: the same rules over the
/// text, statement by statement, so a refusal still localizes.
fn locate_by_text(graph: &Graph, statements: &str, ran: Option<usize>, refused: bool) -> Locus {
    let parts = split_statements(statements);
    let n = ran.unwrap_or(parts.len()).min(parts.len());
    let ran = &parts[..n];
    let mut dataset = None;
    let mut acts = Vec::new();
    for (i, s) in ran.iter().enumerate() {
        let words = words_of(s);
        let head = words
            .first()
            .map(|(w, _)| w.to_ascii_uppercase())
            .unwrap_or_default();
        acts.push(match head.as_str() {
            "USE" => {
                if !(refused && i + 1 == n) {
                    dataset = words.get(1).map(|(w, _)| (*w).to_string());
                }
                Act::Node("USE".into())
            }
            "PROBE" => Act::Node("PROBE".into()),
            "DECLARE" => {
                let second = words
                    .get(1)
                    .map(|(w, _)| w.to_ascii_uppercase())
                    .unwrap_or_default();
                if second == "ASPECT" {
                    let kind = words
                        .windows(2)
                        .find(|w| w[0].0.eq_ignore_ascii_case("AS"))
                        .map(|w| w[1].0.to_ascii_lowercase())
                        .filter(|k| matches!(k.as_str(), "query" | "fact" | "measurement"));
                    Act::Node(match kind {
                        Some(k) => format!("DECLARE ASPECT {k}"),
                        None => "DECLARE ASPECT".into(),
                    })
                } else {
                    Act::Node(format!("DECLARE {second}"))
                }
            }
            "GLOSS" => match words.get(1) {
                Some((aspect, _)) => gloss_act(graph, aspect),
                None => Act::Node("GLOSS".into()),
            },
            _ => {
                let mut node = None;
                for (i, (word, called)) in words.iter().enumerate() {
                    let after_from = i > 0 && words[i - 1].0.eq_ignore_ascii_case("FROM");
                    if !(*called || after_from) {
                        continue;
                    }
                    let candidate = match word.split_once('.') {
                        Some((family, _)) => format!("{family}.<name>"),
                        None => (*word).to_string(),
                    };
                    if let Some(n) = graph.node_named(&candidate) {
                        node = Some(n.to_string());
                        break;
                    }
                }
                Act::Node(node.unwrap_or_else(|| "SQL".into()))
            }
        });
    }
    let act = acts
        .into_iter()
        .rev()
        .fold((None, None), |(non_use, last), a| {
            let last = last.or_else(|| Some(a.clone_kind()));
            match (&non_use, &a) {
                (None, Act::Node(n)) if n != "USE" => (Some(a), last),
                (None, Act::Gloss(_)) => (Some(a), last),
                _ => (non_use, last),
            }
        });
    Locus {
        dataset,
        act: act.0.or(act.1).unwrap_or(Act::Node("SQL".into())),
    }
}

impl Act {
    fn clone_kind(&self) -> Act {
        match self {
            Act::Node(n) => Act::Node(n.clone()),
            Act::Gloss(a) => Act::Gloss(a.clone()),
        }
    }
}

/// `;`-separated statements, a `;` inside `$$…$$`, `'…'` or `"…"` kept.
pub fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut quote: Option<&str> = None;
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if let Some(q) = quote {
            buf.push(c);
            if q == "$$" && text[i..].starts_with("$$") {
                buf.push('$');
                chars.next();
                quote = None;
            } else if q.len() == 1 && q.starts_with(c) {
                quote = None;
            }
            continue;
        }
        if text[i..].starts_with("$$") {
            buf.push_str("$$");
            chars.next();
            quote = Some("$$");
        } else if c == '\'' {
            buf.push(c);
            quote = Some("'");
        } else if c == '"' {
            buf.push(c);
            quote = Some("\"");
        } else if c == ';' {
            let s = buf.trim();
            if !s.is_empty() {
                out.push(s.to_string());
            }
            buf.clear();
        } else {
            buf.push(c);
        }
    }
    let s = buf.trim();
    if !s.is_empty() {
        out.push(s.to_string());
    }
    out
}

/// The words of a statement — identifiers, dotted names included —
/// each with whether a `(` follows it.
fn words_of(statement: &str) -> Vec<(&str, bool)> {
    let bytes = statement.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'.'))
            {
                i += 1;
            }
            let mut j = i;
            while j < bytes.len() && (bytes[j] as char).is_ascii_whitespace() {
                j += 1;
            }
            out.push((&statement[start..i], j < bytes.len() && bytes[j] == b'('));
        } else {
            i += 1;
        }
    }
    out
}

/// The SQL that reads a key's relation on the bound dataset: a door by
/// its call, a relation or a shipped read by its name.
pub fn read_sql(read: &str, dataset: &str) -> String {
    match read {
        "GLOSSARY" => "SELECT * FROM GLOSSARY()".to_string(),
        "ATTEST" => format!("SELECT * FROM ATTEST({dataset})"),
        name => {
            let lower = name.to_ascii_lowercase();
            // A door called with no arguments is called; a relation or
            // a shipped read is named. A door that takes an argument
            // cannot be a key's read — the suite plans every key.
            let call = glossql_session::DOORS
                .iter()
                .any(|(door, syntax)| *door == lower && *syntax == format!("{door}()"));
            if call {
                format!("SELECT * FROM {lower}()")
            } else {
                format!("SELECT * FROM {lower}")
            }
        }
    }
}

/// Whether a keyed condition holds on the rows its read served: some
/// row matches every predicate, or none does when the key says so.
pub fn holds(key: &Key, rows: &[Value]) -> bool {
    let any = rows.iter().any(|row| {
        key.conditions
            .iter()
            .all(|(field, want)| predicate(row.get(field), want))
    });
    if key.none { !any } else { any }
}

fn predicate(have: Option<&Value>, want: &Value) -> bool {
    match want.as_str() {
        Some("empty") => is_empty(have),
        Some("nonempty") => !is_empty(have),
        Some("nonzero") => have.and_then(Value::as_f64).is_some_and(|n| n != 0.0),
        _ => match have {
            None => false,
            Some(h) => {
                h == want
                    || match (h.as_str(), want.as_str()) {
                        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                        _ => false,
                    }
            }
        },
    }
}

fn is_empty(have: Option<&Value>) -> bool {
    match have {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty() || s == "[]",
        Some(Value::Array(a)) => a.is_empty(),
        Some(Value::Object(o)) => o.is_empty(),
        _ => false,
    }
}

/// The window at a node: the hop-1 edges whose key `held` says holds or
/// that carry none, keyed first, capped; then the hop-2 names. `None`
/// when nothing is admissible — an unknown node, or every keyed edge
/// off. The text is what glossval's mining parses; keep the shape.
pub fn render(graph: &Graph, node: &str, held: &dyn Fn(&Key) -> Option<bool>) -> Option<String> {
    let hop1 = graph.out(node);
    if hop1.is_empty() {
        return None;
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    seen.insert(node);
    seen.extend(hop1.iter().map(|e| e.to.as_str()));
    let mut horizon: BTreeSet<&str> = BTreeSet::new();
    for e in &hop1 {
        for e2 in graph.out(&e.to) {
            if !seen.contains(e2.to.as_str()) {
                horizon.insert(e2.to.as_str());
            }
        }
    }
    let mut keyed = Vec::new();
    let mut open = Vec::new();
    for e in hop1 {
        match &e.when.key {
            None => open.push(e),
            Some(keys) if keys.each().all(|k| held(k) == Some(true)) => keyed.push(e),
            Some(_) => {}
        }
    }
    let ordered: Vec<&Edge> = keyed.into_iter().chain(open).collect();
    if ordered.is_empty() {
        return None;
    }
    let mut lines = vec![
        format!("[procedural graph] you are at: {node}"),
        "admissible next:".to_string(),
    ];
    for e in ordered.iter().take(CAP) {
        let mut line = format!("- {}", e.to);
        if !e.when.text.is_empty() {
            line.push_str(&format!(" — when {}", e.when.text));
        }
        if !e.guidance.is_empty() {
            line.push_str(&format!(": {}", e.guidance));
        }
        if !e.pitfalls.is_empty() {
            line.push_str(&format!(" (pitfall: {})", e.pitfalls));
        }
        lines.push(line);
    }
    if ordered.len() > CAP {
        lines.push(format!("- … and {} more from {node}", ordered.len() - CAP));
    }
    if !horizon.is_empty() {
        lines.push(format!(
            "then: {}",
            horizon.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    Some(lines.join("\n"))
}
