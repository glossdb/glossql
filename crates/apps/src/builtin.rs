//! The app shipped in the binary. It serves at its plain name; a
//! workspace authors its own apps as glosses (`glossed.rs`), under
//! their own names — a glossed part under a built-in's name is refused
//! rather than shadowing it. Forking the built-in happens where its
//! source is, in this repository.
//!
//! One ships: the docket — the questions a human is asked, the metric
//! surfaces, how well the data is held, the dataset's words, the
//! column lineage; then every table and read as a file, and a pane for
//! a read of one's own (one app, not a model app beside a metrics app:
//! they were two faces of one workspace, and keeping them apart meant
//! deriving the same counts twice). It is the dataset's own page:
//! `/<dataset>/app` opens it. Built-ins move in lockstep with the
//! binary instead of going stale in a workspace copy.

#[derive(Debug)]
pub struct BuiltinApp {
    pub name: &'static str,
    /// Paths relative to the app root (`app.toml`, `index.html`,
    /// `frames/<name>.sql`), content verbatim.
    pub files: &'static [(&'static str, &'static str)],
}

/// The app `/<dataset>/app` opens: the dataset's own page.
pub const DATASET_PAGE: &str = "docket";

macro_rules! docket {
    ($path:literal) => {
        ($path, include_str!(concat!("../builtin/docket/", $path)))
    };
}

pub const BUILTINS: &[BuiltinApp] = &[BuiltinApp {
    name: DATASET_PAGE,
    files: &[
        docket!("app.toml"),
        docket!("index.html"),
        docket!("metrics.html"),
        docket!("quality.html"),
        docket!("lineage.html"),
        docket!("glossary.html"),
        docket!("export.html"),
        docket!("query.html"),
        docket!("frames/assumptions.sql"),
        docket!("frames/axes.sql"),
        docket!("frames/breach.sql"),
        docket!("frames/census.sql"),
        docket!("frames/check.sql"),
        docket!("frames/check_points.sql"),
        docket!("frames/check_rates.sql"),
        docket!("frames/check_slots.sql"),
        docket!("frames/check_verdicts.sql"),
        docket!("frames/checks.sql"),
        docket!("frames/columns.sql"),
        docket!("frames/conventions.sql"),
        docket!("frames/coverage.sql"),
        docket!("frames/definitions.sql"),
        docket!("frames/dims.sql"),
        docket!("frames/drivers.sql"),
        docket!("frames/fact.sql"),
        docket!("frames/facts.sql"),
        docket!("frames/front.sql"),
        docket!("frames/latest.sql"),
        docket!("frames/lineage_edges.sql"),
        docket!("frames/landing.sql"),
        docket!("frames/lineage_nodes.sql"),
        docket!("frames/metric.sql"),
        docket!("frames/open.sql"),
        docket!("frames/owed.sql"),
        docket!("frames/plan.sql"),
        docket!("frames/pulse.sql"),
        docket!("frames/reads.sql"),
        docket!("frames/relations.sql"),
        docket!("frames/remeasure.sql"),
        docket!("frames/settled.sql"),
        docket!("frames/slices.sql"),
        docket!("frames/tables.sql"),
        docket!("frames/trend.sql"),
        docket!("frames/views.sql"),
        docket!("specs/series.vl.json"),
    ],
}];

pub fn builtin(name: &str) -> Option<&'static BuiltinApp> {
    BUILTINS.iter().find(|b| b.name == name)
}
