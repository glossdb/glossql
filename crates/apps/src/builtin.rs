//! The app shipped in the binary. It serves at its plain name unless
//! the workspace carries `apps/<name>/` — the workspace shadows the
//! built-in, so forking is copying the directory out and editing. A
//! workspace can equally author apps as glosses (`glossed.rs`), which
//! is the shape an agent over MCP can write.
//!
//! One ships: the docket — what stands open for a human to judge, what
//! has been settled, what waits on an act, with the metric surfaces
//! and the record behind it (ruled 2026-08-15, replacing the separate
//! model and metrics apps: they were two faces of one workspace, and
//! keeping them apart meant deriving the same counts twice).
//! Built-ins move in lockstep with the binary instead of going stale
//! in a workspace copy.

#[derive(Debug)]
pub struct BuiltinApp {
    pub name: &'static str,
    /// Paths relative to the app root (`app.toml`, `index.html`,
    /// `frames/<name>.sql`), content verbatim.
    pub files: &'static [(&'static str, &'static str)],
}

macro_rules! docket {
    ($path:literal) => {
        ($path, include_str!(concat!("../builtin/docket/", $path)))
    };
}

pub const BUILTINS: &[BuiltinApp] = &[BuiltinApp {
    name: "docket",
    files: &[
        docket!("app.toml"),
        docket!("index.html"),
        docket!("metrics.html"),
        docket!("record.html"),
        docket!("frames/assumptions.sql"),
        docket!("frames/census.sql"),
        docket!("frames/checks.sql"),
        docket!("frames/conflicts.sql"),
        docket!("frames/coverage.sql"),
        docket!("frames/dims.sql"),
        docket!("frames/front.sql"),
        docket!("frames/metric.sql"),
        docket!("frames/open.sql"),
        docket!("frames/owed.sql"),
        docket!("frames/pulse.sql"),
        docket!("frames/settled.sql"),
        docket!("frames/slices.sql"),
        docket!("frames/stale.sql"),
        docket!("frames/trend.sql"),
        docket!("specs/series.vl.json"),
    ],
}];

pub fn builtin(name: &str) -> Option<&'static BuiltinApp> {
    BUILTINS.iter().find(|b| b.name == name)
}
