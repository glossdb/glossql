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
/// named owner: `glossql-import`'s file reader infers a schema inside
/// the engine's synchronous table-function call, argued in place.
///
/// `thread_local` — at zero, and it stays there. Nothing re-plans
/// through the same context, so there is no door-expansion stack to
/// guard.
///
/// `tokio::spawn` — hand-scheduled work. The engine schedules by
/// partition; what remains is fire-and-forget serving work in the doors
/// crate (the brief refresh, the response stream driver) and the apps
/// crate's two response-stream drivers (a frame as Arrow IPC, a table
/// or metric as a CSV or Parquet download) — not engine-work
/// scheduling.
const CEILING: [(&str, usize); 4] = [
    ("block_in_place", 1),
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

/// The period calendar is the engine's: `date_trunc` and interval
/// arithmetic in a plan say where a bucket starts and what the next
/// one is, and a period travels in the lake's unit (`crates/session/
/// src/period.rs`). Calendar arithmetic by hand in the session crate —
/// a month added, a week stepped, a bucket floored in Rust — is a
/// second calendar, and a nanosecond period is a cast between a landed
/// column and a cell. Each stays at zero.
const CALENDAR: [&str; 12] = [
    "checked_add_months",
    "checked_sub_months",
    "checked_add_days",
    "checked_sub_days",
    "Months::new",
    "Days::new",
    "TimeDelta::",
    "pred_opt",
    "succ_opt",
    "from_timestamp_nanos",
    "timestamp_nanos_opt",
    "TimestampNanosecond",
];

#[test]
fn the_period_calendar_is_the_engines() {
    let root = crates_dir();
    let mut sites = Vec::new();
    for path in sources() {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
            .to_string();
        if !rel.starts_with("session/src/") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for pattern in CALENDAR {
                if line.contains(pattern) {
                    sites.push(format!("{rel}:{}: `{pattern}`", i + 1));
                }
            }
        }
    }
    assert!(
        sites.is_empty(),
        "a second period calendar: the engine's date_trunc and interval arithmetic \
         define a period (`crates/session/src/period.rs`), and a period travels in \
         the lake's unit.\n    {}",
        sites.join("\n    ")
    );
}

/// The session crate's modules depend on each other in one direction,
/// with these exceptions: pairs that import each other, counted from
/// the `crate::` paths each module's source names. `shared.rs` is the
/// leaf every module reads — the refusal, the state, the served shape,
/// the resolution — so a module that only refuses or reads state
/// depends on nothing else, and `reads.rs` plans the record while
/// `doors.rs` answers the doors by name. What remains is the cube and
/// the three readers over it, the two search doors that read each
/// other, the shipped library and the pre-pass that parses it, and
/// the plane that builds sessions. A pair that closes lowers the
/// ceiling; a new one is a module that should have read the leaf.
const MUTUAL_PAIRS: usize = 7;

#[test]
fn the_session_modules_depend_one_way() {
    let root = crates_dir().join("session").join("src");
    let mut edges: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();
    let modules: std::collections::BTreeSet<String> = sources()
        .into_iter()
        .filter_map(|p| p.strip_prefix(&root).ok().map(|rel| rel.to_path_buf()))
        .filter(|rel| rel.as_os_str() != "lib.rs")
        .filter_map(|rel| {
            rel.iter()
                .next()
                .map(|m| m.to_string_lossy().trim_end_matches(".rs").to_string())
        })
        .collect();
    for path in sources() {
        let Ok(rel) = path.strip_prefix(&root) else {
            continue;
        };
        let Some(module) = rel
            .iter()
            .next()
            .map(|m| m.to_string_lossy().trim_end_matches(".rs").to_string())
        else {
            continue;
        };
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            for (i, _) in line.match_indices("crate::") {
                let rest = &line[i + "crate::".len()..];
                let target: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if modules.contains(&target) && target != module {
                    edges.entry(module.clone()).or_default().insert(target);
                }
            }
        }
    }
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (a, targets) in &edges {
        for b in targets {
            if a < b && edges.get(b).is_some_and(|t| t.contains(a)) {
                pairs.push((a.clone(), b.clone()));
            }
        }
    }
    assert!(
        edges.get("shared").is_none_or(|t| t.is_empty()),
        "shared.rs is the leaf and imports no module: {:?}",
        edges.get("shared")
    );
    assert!(
        pairs.len() <= MUTUAL_PAIRS,
        "the session crate's mutual imports rose to {} pairs, ceiling {MUTUAL_PAIRS}:\n    {}\n\n\
         A module that refuses, reads state or serves a shape imports `shared`, never the \
         module that happens to hold the caller. If the new pair is genuinely right, raise the \
         ceiling in this file WITH the reason.",
        pairs.len(),
        pairs
            .iter()
            .map(|(a, b)| format!("{a} <-> {b}"))
            .collect::<Vec<_>>()
            .join("\n    ")
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

/// The lockstep rule as a check: arrow, datafusion and parquet are one
/// version each in the lock. adbc's own arrow requirement spans two
/// majors, so a bare `cargo update` can split the tree (the workspace
/// `Cargo.toml` says how to converge it); a split does not compile at
/// the import seam, but only on the target that builds it.
#[test]
fn one_arrow_one_datafusion_one_parquet_in_the_lock() {
    let lock = std::fs::read_to_string(crates_dir().join("../Cargo.lock")).expect("Cargo.lock");
    let mut versions: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut name = None;
    for line in lock.lines() {
        if let Some(n) = line.strip_prefix("name = ") {
            name = Some(n.trim_matches('"'));
        } else if let Some(v) = line.strip_prefix("version = ")
            && let Some(n) = name.take()
            && (n.starts_with("arrow") || n.starts_with("datafusion") || n == "parquet")
        {
            versions.entry(n).or_default().push(v.trim_matches('"'));
        }
    }
    assert!(!versions.is_empty(), "the lock names the engine");
    let split: Vec<String> = versions
        .iter()
        .filter(|(_, v)| v.len() > 1)
        .map(|(n, v)| format!("{n}: {}", v.join(", ")))
        .collect();
    assert!(
        split.is_empty(),
        "the arrow tree split: {}",
        split.join("; ")
    );
}
