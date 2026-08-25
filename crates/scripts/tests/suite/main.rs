//! One test binary for the crate: every file beside this one is a
//! module of it. A binary per file would link datafusion once more
//! each — half a gigabyte and a link step per file — and run its
//! tests in a process of its own, one process after another; one
//! binary links once and runs them all concurrently.

mod bands;
mod behavior_document_hop;
mod behavior_evidence;
mod behavior_oracle;
mod collisions;
mod cube;
mod dimensions;
mod dimensions_oracle;
mod extract_subquery;
mod fixture11;
mod quality;
mod relationships;
mod temporal;
