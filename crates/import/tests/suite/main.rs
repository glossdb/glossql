//! One test binary for the crate: every file beside this one is a
//! module of it. A binary per file would link datafusion once more
//! each — half a gigabyte and a link step per file — and run its
//! tests in a process of its own, one process after another; one
//! binary links once and runs them all concurrently.

mod recipes;
