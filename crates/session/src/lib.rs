//! The statement router: one `Session` per connection, holding the actor,
//! the `USE`'d dataset, the store, and a DataFusion `SessionContext`.
//!
//! glossql statements (`DECLARE`, `USE`, `GLOSS`, extraction) execute against
//! the store; everything else is substrate SQL handed to DataFusion — with
//! `GLOSSARY()` / `ATTEST()` and the store's relations planned
//! by a registered [`RelationPlanner`], DataFusion's seam for custom FROM
//! elements (`datafusion-expr-53.1.0/src/planner.rs:379`). That seam sees the
//! raw `TableFactor` before default planning, which is what makes
//! `GLOSSARY(subject, all => true)` plannable at all: the default table
//! function path rejects named arguments
//! (`datafusion-sql-53.1.0/src/relation/mod.rs:163`).
//!
//! [`RelationPlanner`]: datafusion::logical_expr::planner::RelationPlanner

mod behavior;
mod cube;
mod library;
mod measure;
mod misfit;
mod plane;
mod prepass;
mod provenance;
mod reads;
pub mod rulings;
mod search;
mod session;
mod subject;
mod whatif;

pub use cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
pub use plane::Plane;
pub use session::{
    CallShape, FunctionRuntime, NoRuntime, Outcome, Session, SessionError, call_shape,
};
