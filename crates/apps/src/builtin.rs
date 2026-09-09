//! The app shipped in the binary. It serves at its plain name; a
//! workspace authors its own apps as glosses (`glossed.rs`), under
//! their own names — a glossed part under a built-in's name is refused
//! rather than shadowing it. Forking the built-in happens where its
//! source is, in this repository.
//!
//! One ships: the docket — what stands open for a human to judge, what
//! has been settled, what waits on an act, with the metric surfaces,
//! the record behind it and the column lineage (one app, not a model app beside a
//! metrics app: they were two faces of one workspace, and
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
        docket!("lineage.html"),
        docket!("frames/assumptions.sql"),
        docket!("frames/axes.sql"),
        docket!("frames/census.sql"),
        docket!("frames/checks.sql"),
        docket!("frames/coverage.sql"),
        docket!("frames/dims.sql"),
        docket!("frames/drivers.sql"),
        docket!("frames/fact.sql"),
        docket!("frames/front.sql"),
        docket!("frames/latest.sql"),
        docket!("frames/lineage_edges.sql"),
        docket!("frames/lineage_nodes.sql"),
        docket!("frames/metric.sql"),
        docket!("frames/open.sql"),
        docket!("frames/owed.sql"),
        docket!("frames/pulse.sql"),
        docket!("frames/remeasure.sql"),
        docket!("frames/settled.sql"),
        docket!("frames/slices.sql"),
        docket!("frames/trend.sql"),
        docket!("specs/series.vl.json"),
    ],
}];

pub fn builtin(name: &str) -> Option<&'static BuiltinApp> {
    BUILTINS.iter().find(|b| b.name == name)
}
