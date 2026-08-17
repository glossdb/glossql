//! The glossary store — declarations, gloss rows, the extraction cache, and
//! the read shapes behind `GLOSSARY()` / `ATTEST()` (SPEC.md §5–§7).
//!
//! Relational by decision (`reports/2026-08-03-poc-substrate.md`): SQLite in
//! the workspace, Postgres by connection string, via sqlx. Supersession is a
//! read — latest row per (subject, aspect, actor kind) — never an update.
//!
//! Subjects are stored dataset-relative, exactly as statements spell them
//! (`orders.amount`, `orders.customer_id -> customers.id`); the session
//! resolves dataset prefixes before the store sees anything.

pub mod rules;
pub mod schemas;
mod store;
mod types;

pub use schemas::{GROUNDING_SCHEMA, grounding_schema};
pub use rules::{admit_grain, grain_of};
pub use store::{
    BriefCounts, ReadContext, Relation, Scope, Store, accepts_relation, relation_columns,
};
pub use types::{
    Actor, ActorKind, AttestRow, CacheRow, CollapsedRow, Error, FunctionRow, RawRow,
    RecipeAdmission, RecipeRow, Result, WitnessRow,
};
