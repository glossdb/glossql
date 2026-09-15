//! The two lines on every result: where the call left the agent, on
//! its last outcome (`situation:`), and one act per goal the record
//! affords from there (`next:`), each a link into `next://` and the
//! `next` read. The representation carries the links; the client
//! holds the goal. Nothing rides the instructions or the stable
//! prefix, and nothing is an order: the skills say how, the record
//! says what is admissible now.

use serde_json::Value;

/// The `situation:` line: refused, with the refusal's first line, or
/// landed — and for a grounding what its fact row said.
pub fn situation(refusal: Option<&str>, outcome: Option<&Value>) -> String {
    if let Some(text) = refusal {
        let first = text.lines().next().unwrap_or(text);
        return format!("situation: refused — {first}");
    }
    let Some(fact) = outcome.filter(|o| o.get("metric").is_some()) else {
        return "situation: landed".to_string();
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
    let mut line = format!("situation: landed — {metric}: ");
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
