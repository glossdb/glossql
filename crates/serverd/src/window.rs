//! Where a call left the agent, on its result: the act its last
//! statement was (`situation:`), and one act per goal the record
//! affords from there (`next:`), each a link into `next://` and the
//! `next()` read. The representation carries the links; the client
//! holds the goal. Nothing rides the instructions or the stable
//! prefix, and nothing is an order: the skills say how, the record
//! says what is admissible now.
//!
//! The localizer names the act from the statements the door parsed
//! once and ran — a gloss by the aspect the graph names, otherwise by
//! the aspect's kind from the record; SQL by the door or read it
//! names, else `SQL`; a refused call by the statement that refused,
//! and the dataset by the last `USE` that landed. A call the parser
//! refused ran nothing and localizes nowhere. The graph's nodes are
//! the vocabulary (`glossql_session::next`).

use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::visit_relations;
use glossql_parser::{AspectKind, Declaration, Statement};
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

/// The locus of a call: its statements as parsed, and how many of
/// them ran — every one when the call landed, the refused one's place
/// when it did not (`refused`).
pub fn locate(graph: &Graph, statements: &[Statement], ran: Option<usize>, refused: bool) -> Locus {
    let n = ran.unwrap_or(statements.len()).min(statements.len());
    // A refused statement bound nothing: the dataset is the last USE
    // that landed.
    let landed = if refused { n.saturating_sub(1) } else { n };
    let ran = &statements[..n];
    let dataset = statements[..landed].iter().rev().find_map(|s| match s {
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

/// The `situation:` line: the act, landed or refused — refused with
/// no act when the call did not parse — and for a grounding what its
/// fact row said.
pub fn situation(node: &str, refusal: Option<&str>, outcome: Option<&Value>) -> String {
    if let Some(text) = refusal {
        let first = text.lines().next().unwrap_or(text);
        return if node.is_empty() {
            format!("situation: refused — {first}")
        } else {
            format!("situation: refused at {node} — {first}")
        };
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
        // The grounding's word on its axes, where it spoke: what the
        // list closed and what a verdict or a gloss would have
        // admitted, or that the empty list did not hold.
        let word = match fact.get("axes_basis").and_then(Value::as_str) {
            Some("authored") => {
                let closed = tagged(
                    fact,
                    &["closed", "closed over verdict", "closed over gloss"],
                );
                let by_verdict = tagged(fact, &["closed over verdict"]);
                let by_gloss = tagged(fact, &["closed over gloss"]);
                let mut word = String::from(" (the grounding's word");
                if !closed.is_empty() {
                    word.push_str(&format!(" — closes {}", closed.join(", ")));
                }
                if !by_verdict.is_empty() {
                    word.push_str(&format!("; a verdict admits {}", by_verdict.join(", ")));
                }
                if !by_gloss.is_empty() {
                    word.push_str(&format!("; a gloss admits {}", by_gloss.join(", ")));
                }
                word.push(')');
                word
            }
            Some("measured over authored") => format!(
                " (measured over the authored empty list — a {} keeps its verdicts; the empty \
                 list closes a distinct count or a ratio)",
                fact.get("behavior")
                    .and_then(Value::as_str)
                    .unwrap_or("flow")
            ),
            _ => String::new(),
        };
        line.push_str(&format!(
            "applicable; axes [{}]{word}; unadmitted [{}]; wanted [{}]",
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

/// The unadmitted columns of a fact row whose act is one of `tags`,
/// in the row's order.
fn tagged(fact: &Value, tags: &[&str]) -> Vec<String> {
    let columns = fact.get("unadmitted").and_then(Value::as_array);
    let acts = fact.get("unadmitted_act").and_then(Value::as_array);
    let (Some(columns), Some(acts)) = (columns, acts) else {
        return Vec::new();
    };
    columns
        .iter()
        .zip(acts.iter())
        .filter(|(_, act)| act.as_str().is_some_and(|t| tags.contains(&t)))
        .filter_map(|(column, _)| column.as_str().map(str::to_string))
        .collect()
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
