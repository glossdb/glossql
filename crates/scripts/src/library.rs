//! The reference library as data.
//!
//! A function's body is carried by its declaration
//! (fixture 24), so the shipped library is fourteen ordinary statements
//! and the workspace keeps no `functions/` directory. The bodies still
//! live as files *in this repo* — an operator edits a script with an
//! editor and the binary embeds it at build time — but nothing reads a
//! file at run time.
//!
//! This lives beside the scripts rather than in `serverd` so that the
//! test suites which run library functions compose the same
//! declarations the door does, from the same text.

/// What keeps the shipped library from declaring: a body that would
/// close its own dollar quote, or a marker no body replaced.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reference script `{0}` carries `$$`")]
    DollarQuoted(&'static str),
    #[error("a script marker had no embedded body: …{0}")]
    Unspliced(String),
}

/// Every reference body the binary carries: file name, text. Every body
/// is one SQL query the engine plans — a measurement over data, a
/// detector over its witness's `slots`; role by shape (`RETURNS` or
/// not, §6) lives in the declaration, not here.
pub const SCRIPTS: &[(&str, &str)] = &[
    ("profile.sql", include_str!("../functions/profile.sql")),
    ("outliers.sql", include_str!("../functions/outliers.sql")),
    ("temporal.sql", include_str!("../functions/temporal.sql")),
    (
        "relationships.sql",
        include_str!("../functions/relationships.sql"),
    ),
    (
        "behavior_evidence.sql",
        include_str!("../functions/behavior_evidence.sql"),
    ),
    (
        "dimension_relevance.sql",
        include_str!("../functions/dimension_relevance.sql"),
    ),
    (
        "hierarchies.sql",
        include_str!("../functions/hierarchies.sql"),
    ),
    (
        "grounding_collisions.sql",
        include_str!("../functions/grounding_collisions.sql"),
    ),
    (
        "derivations.sql",
        include_str!("../functions/derivations.sql"),
    ),
    ("coherence.sql", include_str!("../functions/coherence.sql")),
    (
        "slot_entropy.sql",
        include_str!("../functions/slot_entropy.sql"),
    ),
    (
        "metric_bands.sql",
        include_str!("../functions/metric_bands.sql"),
    ),
    (
        "band_breach.sql",
        include_str!("../functions/band_breach.sql"),
    ),
    (
        "rate_tolerance.sql",
        include_str!("../functions/rate_tolerance.sql"),
    ),
];

/// The library's declarations, bodies not yet spliced in.
const CONTRACTS: &str = include_str!("../functions/bootstrap.glossql");

/// The KPI kit — the semantic vocabulary and its witnesses.
pub const KIT: &str = include_str!("../functions/kpi_kit.glossql");

/// The library's declarations with each body spliced in.
///
/// `bootstrap.glossql` names its bodies rather than carrying them —
/// `AS $$profile.sql$$` — so the file stays one readable declaration
/// per function and remains valid glossql on its own. Joining the two
/// has to happen here: a `.glossql` file cannot `include_str!`
/// another.
///
/// Strict on purpose. An unreplaced marker would declare a function
/// whose body is the string `profile.sql`, and that fails at the first
/// invocation with a message nobody could trace back here.
pub fn declarations() -> Result<String, Error> {
    splice(CONTRACTS)
}

/// Replace every `$$<file>$$` marker with that script's body.
///
/// Exposed because the suites that run library functions declare them
/// themselves, and a test writing a retired form is how a fixture comes
/// to teach a shape that no longer exists. They write the marker and
/// splice it exactly as the door does.
pub fn splice(statements: &str) -> Result<String, Error> {
    let mut out = statements.to_string();
    for (name, text) in SCRIPTS {
        // The body rides a dollar-quoted statement; a `$$` inside one
        // would close it early and parse the rest as further statements.
        if text.contains("$$") {
            return Err(Error::DollarQuoted(name));
        }
        out = out.replace(&format!("$${name}$$"), &format!("$${text}$$"));
    }
    if let Some(at) = out.find(".sql$$") {
        return Err(Error::Unspliced(
            out[at.saturating_sub(40)..at + ".sql$$".len()].to_string(),
        ));
    }
    Ok(out)
}
