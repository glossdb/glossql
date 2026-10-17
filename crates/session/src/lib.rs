//! The statement router: one `Session` per connection, holding the actor,
//! the `USE`'d dataset, the store, and a DataFusion `SessionContext`.
//!
//! glossql statements (`DECLARE`, `USE`, `GLOSS`, extraction) execute against
//! the store; everything else is substrate SQL handed to DataFusion — with
//! `GLOSSARY()` / `ATTEST()` and the store's relations planned
//! by a registered [`RelationPlanner`], DataFusion's seam for custom FROM
//! elements (datafusion-expr `planner.rs`). That seam sees the
//! raw `TableFactor` before default planning, which is what makes
//! `GLOSSARY(subject, all => true)` plannable at all: the default table
//! function path rejects named arguments
//! (datafusion-sql `relation/mod.rs`, `create_relation`).
//!
//! [`RelationPlanner`]: datafusion::logical_expr::planner::RelationPlanner
//!
//! The modules read one way: `shared` is the leaf — the refusal, the
//! outcome, the runtime seam, the session's state, the served shape and
//! a statement's resolution — and imports none of the others; `reads`
//! plans the record and `doors` answers the doors by name; `prepass`
//! resolves a statement ahead of planning; `cube`, `search`, `scenario`,
//! `plan_world`, `misfit`, `behavior` and `period` are the doors' work;
//! `session` routes statements and `plane` builds sessions. The
//! conformance suite in serverd holds the mutual imports at a ceiling.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

mod behavior;
mod cube;
mod doors;
mod execution;
mod library;
mod measure;
mod memo;
mod misfit;
mod period;
mod plan;
mod plan_world;
mod plane;
mod prepass;
mod provenance;
mod reads;
pub mod replay;
pub mod rulings;
mod scenario;
mod search;
mod session;
mod shared;
mod subject;

/// The attribute a [`FunctionRuntime`] implementation carries — the
/// table call is an async trait method.
pub use async_trait::async_trait;
pub use cube::{CubeCache, DEFAULT_CUBE_CACHE_MB};
pub use doors::DOORS;
pub use memo::ShippedCache;
pub use plane::{Caller, DEFAULT_MEMORY_LIMIT_MB, Plane};
pub use session::{
    CallShape, Registered, Session, call_shape, detector_functions, reader_functions,
};
pub use shared::{
    Answered, DoorPage, FORECAST, FunctionRuntime, MISFIT, NoRuntime, Outcome, POINTS,
    SessionError, WHATIF, no_model, point_variable,
};
