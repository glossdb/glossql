//! The shipped system, embedded at compile time — the same lockstep as
//! the teaching resources: what this build ships is what this build
//! declares. A fresh workspace receives the global measurement library
//! before any agent connects; the vertical (semantic aspects, their
//! witnesses) is deliberately not here — framing it is the agent's work.

use std::path::Path;

use glossql_glossary::{Actor, Store};

use crate::Plane;

const SCRIPTS: &[(&str, &str)] = &[
    ("profile.rhai", include_str!("../../scripts/functions/profile.rhai")),
    (
        "outliers.rhai",
        include_str!("../../scripts/functions/outliers.rhai"),
    ),
    (
        "temporal.rhai",
        include_str!("../../scripts/functions/temporal.rhai"),
    ),
    (
        "relationships.rhai",
        include_str!("../../scripts/functions/relationships.rhai"),
    ),
    (
        "behavior_evidence.rhai",
        include_str!("../../scripts/functions/behavior_evidence.rhai"),
    ),
    (
        "dimension_relevance.rhai",
        include_str!("../../scripts/functions/dimension_relevance.rhai"),
    ),
    (
        "hierarchies.rhai",
        include_str!("../../scripts/functions/hierarchies.rhai"),
    ),
    (
        "grounding_collisions.rhai",
        include_str!("../../scripts/functions/grounding_collisions.rhai"),
    ),
    (
        "derivations.rhai",
        include_str!("../../scripts/functions/derivations.rhai"),
    ),
    (
        "coherence.rhai",
        include_str!("../../scripts/functions/coherence.rhai"),
    ),
    (
        "slot_entropy.rhai",
        include_str!("../../scripts/functions/slot_entropy.rhai"),
    ),
    (
        "metric_bands.rhai",
        include_str!("../../scripts/functions/metric_bands.rhai"),
    ),
    (
        "band_breach.rhai",
        include_str!("../../scripts/functions/band_breach.rhai"),
    ),
];

/// The measurement library's declarations.
const BOOTSTRAP: &str = include_str!("../../scripts/functions/bootstrap.glossql");

/// Materialize the shipped system into a workspace: the reference
/// scripts land under `functions/` when absent (an operator's edit is
/// never clobbered), the declarations run once while none exist.
/// Idempotent — every boot calls it.
pub async fn bootstrap(
    store: &Store,
    plane: &Plane,
    workspace: &Path,
    actor: Actor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = workspace.join("functions");
    std::fs::create_dir_all(&dir)?;
    for (name, text) in SCRIPTS {
        let path = dir.join(name);
        if !path.exists() {
            std::fs::write(&path, text)?;
        }
    }
    if store.relation_rows("functions").await?.is_empty() {
        plane.session(actor).await?.execute(BOOTSTRAP).await?;
    }
    Ok(())
}
