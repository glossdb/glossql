//! The shipped system, embedded at compile time — the same lockstep as
//! the teaching resources: what this build ships is what this build
//! declares. A fresh workspace receives the KPI kit (the semantic
//! vocabulary), the global measurement library, and the shipped
//! witness plane — in that order: the library's column evidence
//! conditions on the kit's `role`, and a witness names an aspect and
//! a detector that must already stand. What stays the agent's work is
//! the company's own vocabulary — metrics, validations, scenarios.

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
    // The shipped system is the workspace's, not a dataset's: it
    // declares on the unbound channel.
    let session = plane.channel(actor, None).await?;
    let shipped = async {
        // The standing relations are walked concurrently up front, so
        // the sequence's checks find them held instead of walking one
        // relation per first-touching statement.
        plane.store().warm().await?;
        // One sequence, so one flush: a sequence lands batched, one
        // append per relation it touches, and a remote catalog charges
        // a round trip per commit. The order inside it stands — the
        // library's column evidence conditions on the kit's `role`,
        // and a witness names an aspect and a detector that must
        // already stand.
        let sequence = format!(
            "{};\n{};\n{}",
            glossql_scripts::library::KIT,
            glossql_scripts::library::declarations()?,
            glossql_scripts::library::WITNESSES
        );
        session.execute(&sequence).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    tracing::Instrument::instrument(Box::pin(shipped), tracing::info_span!("bootstrap")).await
}
