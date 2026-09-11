//! Where a call left the agent, on its result: the act its last
//! statement was (`situation:`), and one act per goal the record
//! affords from there (`next:`), each a link into `next://` and the
//! `next()` read. The representation carries the links; the client
//! holds the goal. Nothing rides the instructions or the stable
//! prefix, and nothing is an order: the skills say how, the record
//! says what is admissible now.
//!
//! The localizer names the act from the parsed statement — a gloss by
//! the aspect the graph names, otherwise by the aspect's kind from the
//! record; SQL by the door or read it names, else `SQL`; a refused
//! call by the statement that refused, and the dataset by the last
//! `USE` that landed. The graph's nodes are the vocabulary
//! (`glossql_session::next`).

use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::visit_relations;
use glossql_parser::{AspectKind, Declaration, GlossqlParser, Statement};
use glossql_session::next::Graph;
use serde_json::Value;

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
    let mut last_non_use = None;
    let mut last = None;
    for a in acts.into_iter().rev() {
        if last.is_none() {
            last = Some(a.clone_kind());
        }
        if last_non_use.is_none() && !matches!(&a, Act::Node(n) if n == "USE") {
            last_non_use = Some(a);
        }
    }
    Locus {
        dataset,
        act: last_non_use.or(last).unwrap_or(Act::Node("SQL".into())),
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

/// The `situation:` line: the act, landed or refused, and for a
/// grounding what its fact row said.
pub fn situation(node: &str, refusal: Option<&str>, outcome: Option<&Value>) -> String {
    if let Some(text) = refusal {
        let first = text.lines().next().unwrap_or(text);
        return format!("situation: refused at {node} — {first}");
    }
    let Some(fact) = outcome.filter(|o| o.get("metric").is_some()) else {
        return format!("situation: {node} landed");
    };
    let list = |field: &str| -> String {
        fact.get(field)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    let metric = fact.get("metric").and_then(Value::as_str).unwrap_or("");
    let mut line = format!("situation: {node} landed — {metric}: ");
    if fact.get("applicable") == Some(&Value::Bool(true)) {
        line.push_str(&format!(
            "applicable; axes [{}]; unadmitted [{}]; wanted [{}]",
            list("dims"),
            list("unadmitted"),
            list("wanted")
        ));
    } else {
        let reason = fact.get("reason").and_then(Value::as_str).unwrap_or("");
        let first = reason.split(". ").next().unwrap_or(reason);
        line.push_str(&format!("not applicable — {first}"));
    }
    // A re-record says what it changed against the writing it
    // supersedes — the totals over their shared months.
    if let Some(drift) = fact
        .get("superseded_divergence")
        .and_then(Value::as_str)
        .filter(|d| !d.is_empty())
    {
        line.push_str(&format!("; against the other writing: {drift}"));
    }
    line
}

fn field(row: &Value, name: &str) -> String {
    row.get(name)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// The `next:` line: one act per goal, the link beside it; a blocked
/// goal says what blocks it; a done goal says so.
pub fn next_line(dataset: &str, rows: &[Value]) -> String {
    let parts: Vec<String> = rows
        .iter()
        .map(|row| {
            let surface = field(row, "surface");
            match field(row, "state").as_str() {
                "next" => format!(
                    "{surface} → {} (next://{dataset}/{surface})",
                    field(row, "say")
                ),
                "blocked" => format!("{surface} → blocked: {}", field(row, "why")),
                _ => format!("{surface}: done"),
            }
        })
        .collect();
    format!("next: {}", parts.join(" · "))
}

/// The `next://<dataset>[/<surface>]` page: every answer with its form.
pub fn next_page(dataset: &str, rows: &[Value]) -> String {
    let mut out = format!("# next on {dataset}\n\n");
    for row in rows {
        let surface = field(row, "surface");
        let state = field(row, "state");
        out.push_str(&format!("## {surface}: {state}\n\n"));
        if state == "next" {
            out.push_str(&format!(
                "**act:** {} — {}\n\n**why:** {}\n",
                field(row, "act"),
                field(row, "say"),
                field(row, "why"),
            ));
            // A step the record cannot fill hands no statement — the
            // names are the author's.
            let statement = field(row, "statement");
            if !statement.is_empty() {
                out.push_str(&format!("\n```glossql\n{statement}\n```\n"));
            }
            let then = field(row, "then");
            if !then.is_empty() {
                out.push_str(&format!("\nthen:\n\n```glossql\n{then}\n```\n"));
            }
            out.push('\n');
        } else {
            out.push_str(&format!("{}\n\n", field(row, "why")));
        }
    }
    out
}
