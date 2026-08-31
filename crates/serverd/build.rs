//! Embeds the teaching trees beside the hand-written skill table in
//! `skills.rs`: every `.md` under `skills/` other than a `SKILL.md`
//! (a skill's references, read on demand), and every `.md` under
//! `docs/` (the pages an agent working in this repository reads). The
//! door serves them as resources, so a connected agent holds what a
//! checkout holds — and what a build serves is what its suite tested,
//! since both trees sit under the docs and skills harnesses.

use std::path::{Path, PathBuf};

/// Every `.md` under `root`, recursively, hidden entries skipped.
fn walk(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
        {
            continue;
        }
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets it"));
    let repo = manifest
        .join("../..")
        .canonicalize()
        .expect("the crate sits two levels under the repo root");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets it"));
    let mut src = String::new();
    for (root, table, references_only) in [("skills", "REFERENCES", true), ("docs", "PAGES", false)]
    {
        let dir = repo.join(root);
        println!("cargo:rerun-if-changed={}", dir.display());
        let mut files = Vec::new();
        walk(&dir, &mut files);
        files.sort();
        src.push_str(&format!("pub const {table}: &[Page] = &[\n"));
        for file in files {
            if references_only && file.file_name().is_some_and(|n| n == "SKILL.md") {
                continue;
            }
            let rel = file
                .strip_prefix(&repo)
                .expect("under the repo")
                .to_string_lossy()
                .replace('\\', "/");
            src.push_str(&format!(
                "    Page {{ path: {rel:?}, body: include_str!({:?}) }},\n",
                file.display()
            ));
        }
        src.push_str("];\n");
    }
    std::fs::write(out_dir.join("embedded.rs"), src).expect("OUT_DIR is writable");
}
