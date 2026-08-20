//! The shipped system, embedded at compile time — the same lockstep as
//! the teaching resources: what this build ships is what this build
//! declares. A fresh workspace receives the global measurement library
//! and the KPI kit (the semantic vocabulary and its witnesses) before
//! any agent connects; what stays the agent's work is the company's
//! own vocabulary — metrics, validations, scenarios.


use glossql_glossary::Actor;

use crate::Plane;

/// Declare the shipped system into a workspace. Idempotent — every
/// boot calls it.
///
/// Nothing lands on disk: a function's body is data
/// (fixture 24), so the reference library arrives as
/// fourteen ordinary declarations and reads back through the `functions`
/// relation like anything else an agent wrote.
pub async fn bootstrap(
    plane: &Plane,
    actor: Actor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Every boot declares: the store is idempotent on identical
    // re-declares, and a changed shipped body supersedes like any
    // re-declare — so what this build ships is what this build declares,
    // on fresh and existing workspaces alike.
    let session = plane.session(actor).await?;
    session
        .execute(&glossql_scripts::library::declarations()?)
        .await?;
    session.execute(glossql_scripts::library::KIT).await?;
    Ok(())
}
