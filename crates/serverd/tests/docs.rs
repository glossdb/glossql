//! The docs, under the same standing invariant as the skills.
//!
//! Prose drifts from a moving surface because nothing checks it, so
//! doc examples are checked instead of proofread — the same two rules
//! `skills.rs` holds over the skills, here over every fenced
//! ` ```glossql ` and ` ```sql ` block in `docs/**/*.md`:
//!
//!   1. it parses;
//!   2. if it is a single read, it PLANS against a bootstrapped
//!      workspace — with the one exemption that a missing TABLE is
//!      fine, because an example may name a customer's data. A missing
//!      or misspelled COLUMN is not fine: those are ours.
//!
//! Nothing here executes a write; a block that declares or glosses is
//! held to rule 1 alone. Unlike `skills.rs` there is no floor on how
//! many blocks exist: the docs tree grows page by page, and a page
//! with no examples is legitimate. The skills file keeps the floors
//! that prove the harness still finds its subject.

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
    page: String,
    line: usize,
    sql: String,
}

/// Every markdown file under `docs/`, recursively, in a stable order.
fn pages() -> Vec<std::path::PathBuf> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs")
        .canonicalize()
        .expect("the docs directory ships with the repo");
    let mut stack = vec![root];
    let mut out = Vec::new();
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                out.push(path);
            }
        }
    }
    out.sort();
    assert!(!out.is_empty(), "no markdown pages found under docs/");
    out
}

/// Every fenced glossql/sql block in the docs.
fn blocks() -> Vec<Block> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs")
        .canonicalize()
        .expect("the docs directory ships with the repo");
    let mut out = Vec::new();
    for path in pages() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let page = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
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
                            page: page.clone(),
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
    out
}

/// A refusal that only says "this example names data this workspace
/// does not have". Everything else is the page's fault.
///
/// "No dataset in use" is deliberately NOT here, for the reason
/// recorded in `skills.rs`: it swallows the missing-column class this
/// invariant exists to catch. The harness declares and USEs a dataset
/// instead.
fn missing_table(error: &str) -> bool {
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
async fn every_doc_example_parses() {
    let mut broken = Vec::new();
    for b in blocks() {
        if let Err(e) = glossql_parser::GlossqlParser::parse_sql(&b.sql) {
            broken.push(format!("{}:{} — {e}", b.page, b.line));
        }
    }
    assert!(
        broken.is_empty(),
        "doc examples that do not parse:\n{}",
        broken.join("\n")
    );
}

/// Every `DECLARE FUNCTION … AS $$…$$` body in the docs is one SQL
/// query — the parser cannot see inside a dollar-quoted body, so a
/// broken example would parse perfectly and ship as documentation.
#[tokio::test(flavor = "multi_thread")]
async fn every_doc_function_body_compiles() {
    let mut broken = Vec::new();
    for b in blocks() {
        // Anchored on DECLARE FUNCTION, as in `skills.rs`: within a
        // function statement `AS $$` opens the body and the next `$$`
        // closes it — bodies cannot contain `$$`.
        for part in b.sql.split("DECLARE FUNCTION").skip(1) {
            let Some(open) = part.find("AS $$") else {
                continue;
            };
            let after = &part[open + 5..];
            let Some(close) = after.find("$$") else {
                continue;
            };
            let body = &after[..close];
            // A body is SQL — a measurement over data, a detector over
            // `slots` (§6) — so the check is the session's shape rule.
            let one_query = glossql_parser::GlossqlParser::parse_sql(body)
                .is_ok_and(|statements| statements.len() == 1);
            if !one_query {
                broken.push(format!(
                    "{}:{} — a function body is one SQL query",
                    b.page, b.line
                ));
            }
        }
    }
    assert!(
        broken.is_empty(),
        "doc function bodies that do not compile:\n{}",
        broken.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_doc_read_names_columns_that_exist() {
    let (_dir, store) = scratch_store().await;
    let plane = Arc::new(Plane::new(store.clone(), Arc::new(NoRuntime)));
    bootstrap(&plane, human()).await.unwrap();

    // One session for the run, with a dataset in use — the names the
    // examples actually spell, so a read reaches its columns instead
    // of stopping at the binding.
    let session = plane.channel(human(), None).await.unwrap();
    for name in ["ops", "orders", "erp_export"] {
        session
            .execute(&format!(
                "DECLARE DATASET {name} SET (purpose: 'the docs harness');"
            ))
            .await
            .unwrap();
    }
    session.execute("USE ops;").await.unwrap();

    let mut broken = Vec::new();
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
        if let Err(e) = session.execute(&sql).await {
            let text = e.to_string();
            if !missing_table(&text) {
                broken.push(format!("{}:{} — {text}", b.page, b.line));
            }
        }
    }
    assert!(
        broken.is_empty(),
        "doc examples naming something the reads do not serve:\n{}",
        broken.join("\n")
    );
}

/// A store over its own throwaway lake; hold the dir for the test's life.
async fn scratch_store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let lake = glossql_catalog::Lake::open(
        &dir.path().join("catalog.sqlite"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let store = Store::open(lake).await.unwrap();
    (dir, store)
}
