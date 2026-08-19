//! The conformance ratchet.
//!
//! Four patterns mark places where we built beside DataFusion instead of
//! inside it: blocking a planner thread, bridging sync to async by hand,
//! keeping planner state in a thread-local, and scheduling our own tasks.
//! Each has a count today, and every remaining count has a named owner.
//! This test fails if any count goes UP.
//!
//! It is not a style rule. It is how we notice we have started building
//! around the framework again — see `.claude/skills/glossql-substrate`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Today's counts, and what owns them.
///
/// `block_in_place` / `block_on` — sync code bridging to async. One
/// named owner: `glossql-import` bridges a genuinely sync ADBC driver,
/// argued in place.
///
/// `thread_local` — at zero, and it stays there. Nothing re-plans
/// through the same context, so there is no door-expansion stack to
/// guard.
///
/// `tokio::spawn` — hand-scheduled work. The engine schedules by
/// partition; what remains is fire-and-forget serving work in the doors
/// crate (the brief refresh, the response stream driver) and the apps
/// frame stream — not engine-work scheduling.
const CEILING: [(&str, usize); 4] = [
    ("block_in_place", 3),
    ("block_on", 1),
    ("thread_local!", 0),
    ("tokio::spawn", 3),
];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates")
        .canonicalize()
        .expect("the crates directory ships with the repo")
}

/// Every `.rs` under `crates/*/src` — the code we own. Tests and examples
/// are excluded: a test may legitimately drive a runtime by hand.
fn sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    for e in std::fs::read_dir(crates_dir())
        .expect("readable crates directory")
        .filter_map(|e| e.ok())
    {
        let src = e.path().join("src");
        if src.is_dir() {
            walk(&src, &mut out);
        }
    }
    out.sort();
    out
}

fn counts() -> BTreeMap<&'static str, (usize, Vec<String>)> {
    let root = crates_dir();
    let mut found: BTreeMap<&str, (usize, Vec<String>)> =
        CEILING.iter().map(|(p, _)| (*p, (0, Vec::new()))).collect();
    for path in sources() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
            .to_string();
        for (i, line) in text.lines().enumerate() {
            // Comments describe the patterns without being them — this
            // very file would otherwise trip its own ratchet.
            if line.trim_start().starts_with("//") {
                continue;
            }
            for (pattern, _) in CEILING {
                if line.contains(pattern) {
                    let entry = found.get_mut(pattern).expect("seeded");
                    entry.0 += 1;
                    entry.1.push(format!("{rel}:{}", i + 1));
                }
            }
        }
    }
    found
}

#[test]
fn the_substrate_ratchet_only_turns_down() {
    let found = counts();
    let mut risen = Vec::new();
    for (pattern, ceiling) in CEILING {
        let (n, sites) = &found[pattern];
        if *n > ceiling {
            risen.push(format!(
                "`{pattern}`: {n} sites, ceiling {ceiling}\n    {}",
                sites.join("\n    ")
            ));
        }
    }
    assert!(
        risen.is_empty(),
        "a substrate workaround was added rather than removed.\n\n{}\n\n\
         Each of these is a place the engine already has an answer — see \
         `.claude/skills/glossql-substrate`. If the new one is genuinely \
         right, raise the ceiling in this file WITH the reason.",
        risen.join("\n\n")
    );
}

/// The counts are the roadmap's gate: stage 2 takes the session's
/// blocking and the thread-local to zero. This records where they stand
/// so a drop is visible in the diff rather than only in a passing test.
#[test]
fn the_ratchet_reports_where_it_stands() {
    let found = counts();
    let mut lines = Vec::new();
    for (pattern, ceiling) in CEILING {
        let (n, _) = &found[pattern];
        lines.push(format!("{pattern}: {n}/{ceiling}"));
    }
    println!("substrate ratchet — {}", lines.join("  "));
    // Nothing to assert: the first test is the gate. This one exists so
    // `cargo test -- --nocapture` shows the number without reading code.
}
