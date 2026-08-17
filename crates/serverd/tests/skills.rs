//! The skills, under the standing invariant.
//!
//! Run 2's first friction was five skills teaching `SELECT value FROM
//! GLOSSARY(…)` — a column that does not exist and never did. Prose
//! drifts from a moving surface because nothing checks it, so the
//! examples are checked here instead of proofread.
//!
//! Two rules, over every fenced ` ```glossql ` and ` ```sql ` block in
//! `.claude/skills/*/SKILL.md`:
//!
//!   1. it parses;
//!   2. if it is a single read, it PLANS against a bootstrapped
//!      workspace — with the one exemption that a missing TABLE is
//!      fine, because an example may name a customer's data. A missing
//!      or misspelled COLUMN is not fine: those are ours, and that is
//!      exactly the class of error that shipped.
//!
//! Nothing here executes a write. A block that declares or glosses is
//! held to rule 1 alone.

use std::sync::Arc;

use glossql_glossary::{Actor, ActorKind, Store};
use glossql_serverd::{Plane, bootstrap};
use glossql_session::NoRuntime;

fn human() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: glossql_serverd::HUMAN.into(),
    }
}

struct Block {
    skill: String,
    line: usize,
    sql: String,
}

/// Every fenced glossql/sql block in the shipped skills.
fn blocks() -> Vec<Block> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.claude/skills")
        .canonicalize()
        .expect("the skills directory ships with the repo");
    let mut out = Vec::new();
    let mut skills: Vec<_> = std::fs::read_dir(&dir)
        .expect("readable skills directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    skills.sort();
    for skill in skills {
        let path = skill.join("SKILL.md");
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let name = skill
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();
        let mut open: Option<(usize, String)> = None;
        for (i, line) in text.lines().enumerate() {
            match &mut open {
                None => {
                    if line.trim() == "```glossql" || line.trim() == "```sql" {
                        open = Some((i + 2, String::new()));
                    }
                }
                Some((start, body)) => {
                    if line.trim() == "```" {
                        out.push(Block {
                            skill: name.clone(),
                            line: *start,
                            sql: std::mem::take(body),
                        });
                        open = None;
                    } else {
                        body.push_str(line);
                        body.push('\n');
                    }
                }
            }
        }
    }
    assert!(!out.is_empty(), "no fenced examples found in the skills");
    out
}

/// A refusal that only says "this example names data this workspace
/// does not have". Everything else is the skill's fault.
///
/// "No dataset in use" is deliberately NOT here. It was, and it
/// swallowed the very bug this file exists for: `SELECT value FROM
/// GLOSSARY()` fails on the missing dataset before it ever reaches the
/// missing column, so the test passed on the exact example that
/// shipped. The harness declares and USEs a dataset instead, and a
/// read that cannot plan for any other reason is a defect.
fn missing_table(error: &str) -> bool {
    // Any qualified table this workspace does not have, not just the
    // default schema: an extract example names its subject as
    // `orders.amount`, which resolves to `datafusion.orders.amount`
    // (widened 2026-08-15). A shipped read that does not exist lands
    // here too — as `datafusion.public.<name>` — and always did.
    error.contains("table 'datafusion.")
        || error.contains("No table named")
        || error.contains("table not found")
        // `read.<metric>()` / `whatif.<lever>()` name a workspace's own
        // groundings, which a fresh one has none of. Narrow on purpose:
        // only an undeclared aspect behind a serve door, never a
        // missing column inside one.
        || (error.contains("not a subject:") && error.contains("is declared"))
}

#[tokio::test(flavor = "multi_thread")]
async fn every_skill_example_parses() {
    let mut broken = Vec::new();
    for b in blocks() {
        if let Err(e) = glossql_parser::GlossqlParser::parse_sql(&b.sql) {
            broken.push(format!("{}:{} — {e}", b.skill, b.line));
        }
    }
    assert!(
        broken.is_empty(),
        "skill examples that do not parse:\n{}",
        broken.join("\n")
    );
}

/// Every `DECLARE FUNCTION … AS $$…$$` body in the skills compiles.
///
/// Rule 1 cannot see this: a function's body is opaque text to the
/// glossql parser, so an example whose rhai the engine rejects parses
/// perfectly and ships as teaching. Found in a run on 2026-08-15 — the
/// functions skill's worked example carried a multi-line double-quoted
/// string, which rhai does not allow (SQL over several lines needs
/// backticks). An agent copying it got "Open string is not terminated".
#[tokio::test(flavor = "multi_thread")]
async fn every_skill_function_body_compiles() {
    let runtime = glossql_scripts::RhaiRuntime::new(std::env::temp_dir());
    let mut broken = Vec::new();
    let mut checked = 0usize;
    for b in blocks() {
        // Anchored on DECLARE FUNCTION: PROBE, RECIPE and GLOSS bodies
        // ride the same dollar quoting and are SQL or JSON, so a bare
        // scan for `AS $$` compiles the wrong thing. Within a function
        // statement, `AS $$` opens the body and the next `$$` closes
        // it — bodies cannot contain `$$`, which is what makes this
        // sound.
        for part in b.sql.split("DECLARE FUNCTION").skip(1) {
            let Some(open) = part.find("AS $$") else {
                continue;
            };
            let after = &part[open + 5..];
            let Some(close) = after.find("$$") else { continue };
            checked += 1;
            let body = &after[..close];
            // Role by shape (§7e): a body that parses as one SQL query
            // is a measurement the engine runs; anything else must
            // compile as a script.
            let is_sql = glossql_parser::GlossqlParser::parse_sql(body)
                .is_ok_and(|statements| statements.len() == 1);
            if !is_sql && let Err(e) = runtime.compiles(body) {
                broken.push(format!("{}:{} — {e}", b.skill, b.line));
            }
        }
    }
    assert!(
        broken.is_empty(),
        "skill function bodies that do not compile:\n{}",
        broken.join("\n")
    );
    assert!(
        checked >= 2,
        "only {checked} function bodies were compiled — the scan has stopped finding its subject"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_skill_read_names_columns_that_exist() {
    let store = Store::open_memory().await.unwrap();
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));
    bootstrap(&store, &plane, human())
        .await
        .unwrap();

    // One session for the run, with a dataset in use — the names the
    // examples actually spell, so a read reaches its columns instead of
    // stopping at the binding.
    let session = plane.session(human()).await.unwrap();
    for name in ["fin", "orders", "erp_export"] {
        session
            .execute(&format!(
                "DECLARE DATASET {name} SET (purpose: 'the skills harness');"
            ))
            .await
            .unwrap();
    }
    session.execute("USE fin;").await.unwrap();

    let mut broken = Vec::new();
    let mut planned = 0usize;
    for b in blocks() {
        let trimmed: String = b
            .sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let head = trimmed.trim_start();
        // Reads only: a write example is held to parsing alone.
        if !head.to_lowercase().starts_with("select") {
            continue;
        }
        // One statement per block, or we cannot bound it.
        if trimmed.trim_end().trim_end_matches(';').contains(';') {
            continue;
        }
        let sql = format!(
            "SELECT * FROM ({}) LIMIT 0",
            trimmed.trim().trim_end_matches(';')
        );
        planned += 1;
        if let Err(e) = session.execute(&sql).await {
            let text = e.to_string();
            if !missing_table(&text) {
                broken.push(format!("{}:{} — {text}", b.skill, b.line));
            }
        }
    }
    assert!(
        broken.is_empty(),
        "skill examples naming something the reads do not serve:\n{}",
        broken.join("\n")
    );
    // A test that silently checks nothing passes forever. The skills
    // teach reads; if this floor is not met, the classification above
    // has started skipping what it should be holding.
    assert!(
        planned >= 5,
        "only {planned} skill reads were planned — the test is skipping its own subject"
    );
}
