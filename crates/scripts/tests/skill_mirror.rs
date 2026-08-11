//! The teaching layer cannot drift: the kernel list in the
//! glossql-functions skill must mirror what the engine actually registers
//! (rhai `gen_fn_signatures`, global namespace only — the standard
//! packages stay out). Add a kernel and forget the skill, or teach a
//! kernel that doesn't exist, and this test fails — the same invariant
//! that keeps SPEC.md's sql blocks parsing.

use std::collections::BTreeSet;

use glossql_scripts::RhaiRuntime;

fn skill_text() -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.claude/skills/glossql-functions/SKILL.md"
    );
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn registered_names() -> BTreeSet<String> {
    RhaiRuntime::new("unused")
        .kernel_signatures()
        .iter()
        .filter_map(|sig| sig.split('(').next().map(str::to_string))
        .collect()
}

#[test]
fn every_registered_kernel_is_taught() {
    let text = skill_text();
    let missing: Vec<_> = registered_names()
        .into_iter()
        .filter(|name| !text.contains(&format!("{name}(")))
        .collect();
    assert!(
        missing.is_empty(),
        "kernels the engine registers but the skill never mentions: {missing:?}"
    );
}

#[test]
fn every_taught_kernel_is_registered() {
    let text = skill_text();
    let section = text
        .split("## Kernels")
        .nth(1)
        .expect("the skill has a Kernels section")
        .split("\n## ")
        .next()
        .unwrap()
        .to_string();
    // A backticked call-shaped token: `name(` — dotted forms (`db.query`,
    // rhai stdlib like `.parse_int()`) deliberately don't match.
    let taught: BTreeSet<String> = regex::Regex::new(r"`([a-z_]+)\(")
        .unwrap()
        .captures_iter(&section)
        .map(|c| c[1].to_string())
        .collect();
    assert!(
        !taught.is_empty(),
        "no kernels found in the Kernels section"
    );
    let registered = registered_names();
    let phantom: Vec<_> = taught.difference(&registered).collect();
    assert!(
        phantom.is_empty(),
        "kernels the skill teaches but the engine never registers: {phantom:?}"
    );
}
