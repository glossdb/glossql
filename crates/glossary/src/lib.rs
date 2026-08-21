//! The glossary store — declarations, gloss rows, pin-keyed measurements, and
//! the read shapes behind `GLOSSARY()` / `ATTEST()` (SPEC.md §5–§7).
//!
//! One store: every relation
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

pub use glossql_catalog::{Lake, Row};
pub use rules::admit_grain;
pub use store::{
    BriefCounts, LANDING_CASTS_PROP, LANDING_DROPPED_PROP, LANDING_SCANS_PROP, Pin, ReadContext,
    Relation, Scope, Store, relation_columns,
};
pub use types::{
    Actor, ActorKind, AttestRow, CollapsedRow, Error, FunctionRow, GlossRow, MeasurementRow,
    RawRow, RecipeAdmission, RecipeRow, Result, Verdict, Verdicts, WitnessRow,
};
