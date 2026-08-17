//! The reference library as data.
//!
//! A function's body is carried by its declaration (ruled 2026-08-15,
//! fixture 24), so the shipped library is fifteen ordinary statements
//! and the workspace keeps no `functions/` directory. The bodies still
//! live as files *in this repo* — an operator edits a script with an
//! editor and the binary embeds it at build time — but nothing reads a
//! file at run time.
//!
//! This lives beside the scripts rather than in `serverd` so that the
//! test suites which run library functions compose the same
//! declarations the door does, from the same text.

/// Every reference body the binary carries: file name, text. `.sql` is
/// a measurement the engine runs, `.rhai` a script the runtime runs —
/// role by shape (§7e), visible in the extension.
pub const SCRIPTS: &[(&str, &str)] = &[
    ("profile.sql", include_str!("../functions/profile.sql")),
    ("outliers.sql", include_str!("../functions/outliers.sql")),
    ("temporal.sql", include_str!("../functions/temporal.sql")),
    (
        "relationships.rhai",
        include_str!("../functions/relationships.rhai"),
    ),
    (
        "behavior_evidence.rhai",
        include_str!("../functions/behavior_evidence.rhai"),
    ),
    (
        "dimension_relevance.rhai",
        include_str!("../functions/dimension_relevance.rhai"),
    ),
    (
        "hierarchies.sql",
        include_str!("../functions/hierarchies.sql"),
    ),
    (
        "grounding_collisions.rhai",
        include_str!("../functions/grounding_collisions.rhai"),
    ),
    (
        "derivations.sql",
        include_str!("../functions/derivations.sql"),
    ),
    ("coherence.rhai", include_str!("../functions/coherence.rhai")),
    (
        "slot_entropy.rhai",
        include_str!("../functions/slot_entropy.rhai"),
    ),
    (
        "metric_bands.rhai",
        include_str!("../functions/metric_bands.rhai"),
    ),
    (
        "metric_cube.rhai",
        include_str!("../functions/metric_cube.rhai"),
    ),
    (
        "band_breach.rhai",
        include_str!("../functions/band_breach.rhai"),
    ),
    (
        "rate_tolerance.rhai",
        include_str!("../functions/rate_tolerance.rhai"),
    ),
];

/// The library's declarations, bodies not yet spliced in.
const CONTRACTS: &str = include_str!("../functions/bootstrap.glossql");

/// The KPI kit — the semantic vocabulary and its witnesses.
pub const KIT: &str = include_str!("../functions/kpi_kit.glossql");

/// One reference script's body, by file name.
pub fn script(file: &str) -> Option<&'static str> {
    SCRIPTS
        .iter()
        .find(|(name, _)| *name == file)
        .map(|(_, text)| *text)
}

/// The library's declarations with each body spliced in.
///
/// `bootstrap.glossql` names its bodies rather than carrying them —
/// `AS $$profile.rhai$$` — so the file stays one readable declaration
/// per function and remains valid glossql on its own. Joining the two
/// has to happen here: a `.glossql` file cannot `include_str!`
/// another.
///
/// Strict on purpose. An unreplaced marker would declare a function
/// whose body is the string `profile.rhai`, and that fails at the first
/// invocation with a rhai parse error nobody could trace back here.
pub fn declarations() -> Result<String, String> {
    splice(CONTRACTS)
}

/// Replace every `$$<file>.rhai$$` marker with that script's body.
///
/// Exposed because the suites that run library functions declare them
/// themselves, and a test writing a retired form is how a fixture comes
/// to teach a shape that no longer exists. They write the marker and
/// splice it exactly as the door does.
pub fn splice(statements: &str) -> Result<String, String> {
    let mut out = statements.to_string();
    for (name, text) in SCRIPTS {
        // The body rides a dollar-quoted statement; a `$$` inside one
        // would close it early and parse the rest as further statements.
        if text.contains("$$") {
            return Err(format!("reference script `{name}` carries `$$`"));
        }
        out = out.replace(&format!("$${name}$$"), &format!("$${text}$$"));
    }
    for marker in [".rhai$$", ".sql$$"] {
        if let Some(at) = out.find(marker) {
            return Err(format!(
                "a script marker had no embedded body: …{}",
                &out[at.saturating_sub(40)..at + marker.len()]
            ));
        }
    }
    Ok(out)
}
