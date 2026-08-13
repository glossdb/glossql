//! Apps shipped in the binary. A built-in serves at its plain name
//! unless the workspace carries `apps/<name>/` — the workspace shadows
//! the built-in, so forking is copying the directory out and editing.
//! The model app is the verification surface over the read machinery
//! and ships here so it moves in lockstep with the binary instead of
//! going stale in a workspace copy (ruled 2026-08-11).

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

pub const BUILTINS: &[BuiltinApp] = &[BuiltinApp {
    name: "model",
    files: &[
        model!("app.toml"),
        model!("index.html"),
        model!("frames/aspects_list.sql"),
        model!("frames/assumptions.sql"),
        model!("frames/bands.sql"),
        model!("frames/census.sql"),
        model!("frames/claims.sql"),
        model!("frames/coverage.sql"),
        model!("frames/drivers.sql"),
        model!("frames/facts_list.sql"),
        model!("frames/inside.sql"),
        model!("frames/joins.sql"),
        model!("frames/measured.sql"),
        model!("frames/measurements_list.sql"),
        model!("frames/metric.sql"),
        model!("frames/brief.sql"),
        model!("frames/queue.sql"),
        model!("frames/scenarios.sql"),
        model!("frames/subjects_list.sql"),
        model!("frames/surfaces.sql"),
        model!("frames/travels.sql"),
        model!("frames/verdicts.sql"),
        model!("frames/witnesses_list.sql"),
        model!("specs/bands.vl.json"),
    ],
}];

pub fn builtin(name: &str) -> Option<&'static BuiltinApp> {
    BUILTINS.iter().find(|b| b.name == name)
}
