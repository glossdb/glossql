//! The glossary store — declarations, gloss rows, pin-keyed measurements, and
//! the read shapes behind `GLOSSARY()` / `ATTEST()` (SPEC.md §5–§7).
//!
//! One store (`reports/2026-08-17-the-foundation.md` §4): every relation
//! is an Iceberg table in the workspace's lake, ordered by the format's
//! own row lineage. Supersession is a read — latest row per (subject,
//! aspect, actor kind) — never an update.
//!
//! Subjects are stored dataset-relative, exactly as statements spell them
//! (`orders.amount`, `orders.customer_id -> customers.id`); the session
//! resolves dataset prefixes before the store sees anything.

pub mod rules;
pub mod schemas;
mod store;
mod types;

pub use schemas::{GROUNDING_SCHEMA, grounding_schema};
pub use glossql_catalog::{Lake, MetadataBackend, Row};
pub use rules::{admit_grain, grain_of};
pub use store::{
    BriefCounts, LANDING_CASTS_PROP, LANDING_DROPPED_PROP, LANDING_SCANS_PROP, ReadContext,
    Pin, Relation, Scope, Store, relation_columns,
};
pub use types::{
    Actor, ActorKind, AttestRow, CollapsedRow, Error, FunctionRow, GlossRow, MeasurementRow,
    RawRow, Verdict, Verdicts,
    RecipeAdmission, RecipeRow, Result, WitnessRow,
};
