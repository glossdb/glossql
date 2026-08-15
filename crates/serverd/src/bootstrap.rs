//! The shipped system, embedded at compile time — the same lockstep as
//! the teaching resources: what this build ships is what this build
//! declares. A fresh workspace receives the global measurement library
//! and the KPI kit (the semantic vocabulary and its witnesses) before
//! any agent connects; what stays the agent's work is the company's
//! own vocabulary — metrics, validations, scenarios.


use glossql_glossary::{Actor, Store};

use crate::Plane;

/// Declare the shipped system into a workspace, once, while none of it
/// stands. Idempotent — every boot calls it.
///
/// Nothing lands on disk any more: a function's body is data (ruled
/// 2026-08-15, fixture 24), so the reference library arrives as
/// fifteen ordinary declarations and reads back through the `functions`
/// relation like anything else an agent wrote.
pub async fn bootstrap(
    store: &Store,
    plane: &Plane,
    actor: Actor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if store.relation_rows("functions").await?.is_empty() {
        let session = plane.session(actor).await?;
        session
            .execute(&glossql_scripts::library::declarations()?)
            .await?;
        session.execute(glossql_scripts::library::KIT).await?;
    }
    Ok(())
}
