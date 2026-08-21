//! The corpus is the acceptance suite — the standing invariant: every
//! ```glossql block in tests/corpus/*.md parses, every ```glossql-gap block
//! fails to parse (one that parses means the gap closed — flip its tag), and
//! every ```sql block in SPEC.md parses. Other fences (yaml, corpus-quoted
//! sql, json) quote source artifacts and are skipped.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use glossql_parser::GlossqlParser;

#[derive(Clone, Copy, PartialEq)]
enum Expect {
    Parses,
    Fails,
}

struct Block {
    file: String,
    line: usize,
    expect: Expect,
    text: String,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at crates/parser")
        .to_path_buf()
}

fn blocks(path: &Path, rel: &str, tags: &[(&str, Expect)]) -> Vec<Block> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {rel}: {e}"));
    let mut out = Vec::new();
    let mut open: Option<(Expect, usize, String)> = None;
    for (idx, line) in text.lines().enumerate() {
        match open.as_mut() {
            Some((expect, start, body)) => {
                if line.trim_end() == "```" {
                    out.push(Block {
                        file: rel.to_string(),
                        line: *start,
                        expect: *expect,
                        text: std::mem::take(body),
                    });
                    open = None;
                } else {
                    body.push_str(line);
                    body.push('\n');
                }
            }
            None => {
                if let Some(tag) = line.trim_end().strip_prefix("```")
                    && let Some((_, expect)) = tags.iter().find(|(t, _)| *t == tag)
                {
                    open = Some((*expect, idx + 1, String::new()));
                }
            }
        }
    }
    assert!(open.is_none(), "{rel}: unclosed code fence");
    out
}

#[test]
fn corpus_and_spec_behave_as_tagged() {
    let corpus_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&corpus_dir)
        .expect("tests/corpus/ directory")
        .map(|e| e.expect("corpus entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "md"))
        .collect();
    files.sort();

    let root = repo_root();
    let mut all = Vec::new();
    for file in &files {
        let rel = format!(
            "tests/corpus/{}",
            file.file_name().unwrap().to_string_lossy()
        );
        all.extend(blocks(
            file,
            &rel,
            &[("glossql", Expect::Parses), ("glossql-gap", Expect::Fails)],
        ));
    }
    let corpus_count = all.len();
    all.extend(blocks(
        &root.join("SPEC.md"),
        "SPEC.md",
        &[("sql", Expect::Parses)],
    ));

    assert!(
        corpus_count > 0,
        "no tagged corpus blocks found — path rot?"
    );
    assert!(
        all.len() > corpus_count,
        "no ```sql blocks found in SPEC.md — path rot?"
    );

    let mut failures = String::new();
    for b in &all {
        match (b.expect, GlossqlParser::parse_sql(&b.text)) {
            (Expect::Parses, Err(e)) => {
                // Errors carry Line/Column relative to the block, whose body
                // starts one line below the fence at b.line.
                let _ = writeln!(
                    failures,
                    "{} (fence at line {}): must parse: {e}",
                    b.file, b.line
                );
            }
            (Expect::Fails, Ok(_)) => {
                let _ = writeln!(
                    failures,
                    "{}:{}: gap closed — the block parses now; flip ```glossql-gap to ```glossql",
                    b.file, b.line
                );
            }
            _ => {}
        }
    }
    assert!(failures.is_empty(), "corpus violations:\n{failures}");
}
