//! Apps shipped in the binary. A built-in serves at its plain name
//! unless the workspace carries `apps/<name>/` — the workspace shadows
//! the built-in, so forking is copying the directory out and editing.
//! Two ship: the model app, the verification surface over the read
//! machinery (ruled 2026-08-11), and the metrics app, the business
//! surface over the declared metric surfaces (2026-08-13) — the metric
//! dossier lives there, the model app keeps the glossary's own faces.
//! Built-ins move in lockstep with the binary instead of going stale
//! in a workspace copy.

#[derive(Debug)]
pub struct BuiltinApp {
    pub name: &'static str,
    /// Paths relative to the app root (`app.toml`, `index.html`,
    /// `frames/<name>.sql`), content verbatim.
    pub files: &'static [(&'static str, &'static str)],
}

macro_rules! model {
    ($path:literal) => {
        ($path, include_str!(concat!("../builtin/model/", $path)))
    };
}

macro_rules! metrics {
    ($path:literal) => {
        ($path, include_str!(concat!("../builtin/metrics/", $path)))
    };
}

pub const BUILTINS: &[BuiltinApp] = &[
    BuiltinApp {
        name: "model",
        files: &[
            model!("app.toml"),
            model!("index.html"),
            model!("frames/aspects_list.sql"),
            model!("frames/census.sql"),
            model!("frames/checks.sql"),
            model!("frames/checks_backlog.sql"),
            model!("frames/claims.sql"),
            model!("frames/coverage.sql"),
            model!("frames/drivers.sql"),
            model!("frames/facts_list.sql"),
            model!("frames/inside.sql"),
            model!("frames/joins.sql"),
            model!("frames/measured.sql"),
            model!("frames/measurements_list.sql"),
            model!("frames/brief.sql"),
            model!("frames/queue.sql"),
            model!("frames/scenarios.sql"),
            model!("frames/subjects_list.sql"),
            model!("frames/surfaces.sql"),
            model!("frames/travels.sql"),
            model!("frames/verdicts.sql"),
            model!("frames/witnesses_list.sql"),
        ],
    },
    BuiltinApp {
        name: "metrics",
        files: &[
            metrics!("app.toml"),
            metrics!("index.html"),
            metrics!("frames/assumptions.sql"),
            metrics!("frames/bands.sql"),
            metrics!("frames/dims.sql"),
            metrics!("frames/front.sql"),
            metrics!("frames/graph.sql"),
            metrics!("frames/metric.sql"),
            metrics!("frames/metric_questions.sql"),
            metrics!("frames/pulse.sql"),
            metrics!("frames/slices.sql"),
            metrics!("frames/trend.sql"),
            metrics!("specs/bands.vl.json"),
            metrics!("specs/series.vl.json"),
        ],
    },
];

pub fn builtin(name: &str) -> Option<&'static BuiltinApp> {
    BUILTINS.iter().find(|b| b.name == name)
}
