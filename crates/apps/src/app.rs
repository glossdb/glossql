//! An app is a directory: `apps/<name>/app.toml` names its title and
//! may pin the dataset its frames read; without the pin, the app binds
//! to the workspace's sole dataset at request time (the
//! one-dataset-per-workspace binding, SPEC.md §1 — a selector becomes
//! a concern only when a second dataset does). Everything else in the
//! directory *is* the app — pages (`*.html`, tera), `frames/*.sql`,
//! `specs/*.vl.json` — read fresh per request, so an author saves a
//! file and reloads. Apps shipped in the binary (`builtin.rs`) resolve
//! the same way, workspace directory first: the workspace shadows the
//! built-in, and forking is copying the directory out.

use std::path::{Path, PathBuf};

use crate::builtin::{self, BuiltinApp};

#[derive(Debug)]
pub struct AppDef {
    pub name: String,
    pub title: String,
    /// `None` binds by convention at request time: the workspace's sole
    /// dataset.
    pub dataset: Option<String>,
    source: Source,
}

#[derive(Debug)]
enum Source {
    Dir(PathBuf),
    Builtin(&'static BuiltinApp),
}

/// URL segments walk into the filesystem: one flat name, no
/// separators, no dot-walking, nothing hidden.
pub fn safe_segment(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// Fit to ride a `USE` statement verbatim.
fn identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The manifest fields both sources share. `dataset` is optional —
/// absent means bind at request time.
fn manifest(origin: &str, text: &str) -> Result<(Option<String>, Option<String>), String> {
    let value: toml::Value = toml::from_str(text).map_err(|e| format!("{origin}: {e}"))?;
    let field = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    let dataset = field("dataset");
    if let Some(dataset) = &dataset
        && !identifier(dataset)
    {
        return Err(format!(
            "{origin}: `dataset` must be a plain identifier, got `{dataset}`"
        ));
    }
    Ok((field("title"), dataset))
}

impl AppDef {
    /// The app's definition, or why there is none: `Ok(None)` is "no
    /// such app" (a 404), `Err` is an app that exists but cannot serve.
    /// The workspace directory wins; a built-in answers for the name
    /// only when the workspace carries nothing under it.
    pub fn load(workspace: &Path, name: &str) -> Result<Option<AppDef>, String> {
        if !safe_segment(name) {
            return Ok(None);
        }
        let dir = workspace.join("apps").join(name);
        let path = dir.join("app.toml");
        if path.is_file() {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("reading {}: {e}", path.display()))?;
            let (title, dataset) = manifest(&path.display().to_string(), &text)?;
            return Ok(Some(AppDef {
                title: title.unwrap_or_else(|| name.to_string()),
                name: name.to_string(),
                dataset,
                source: Source::Dir(dir),
            }));
        }
        let Some(app) = builtin::builtin(name) else {
            return Ok(None);
        };
        let toml = app
            .files
            .iter()
            .find(|(p, _)| *p == "app.toml")
            .map(|(_, text)| *text)
            .unwrap_or("");
        let (title, dataset) = manifest(&format!("builtin `{name}`"), toml)?;
        Ok(Some(AppDef {
            title: title.unwrap_or_else(|| name.to_string()),
            name: name.to_string(),
            dataset,
            source: Source::Builtin(app),
        }))
    }

    /// Every servable app: the workspace's directories plus the
    /// built-ins the workspace does not shadow. Broken manifests are
    /// skipped here — their own pages say what is wrong.
    pub fn list(workspace: &Path) -> Vec<AppDef> {
        let mut names: Vec<String> = std::fs::read_dir(workspace.join("apps"))
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        for b in builtin::BUILTINS {
            if !names.iter().any(|n| n == b.name) {
                names.push(b.name.to_string());
            }
        }
        let mut apps: Vec<AppDef> = names
            .into_iter()
            .filter_map(|name| AppDef::load(workspace, &name).ok().flatten())
            .collect();
        apps.sort_by(|a, b| a.name.cmp(&b.name));
        apps
    }

    /// A file inside the app, by root-relative location, guarded
    /// against escaping it.
    pub fn read(&self, sub: &str, name: &str) -> Option<String> {
        if !safe_segment(name) {
            return None;
        }
        match &self.source {
            Source::Dir(dir) => {
                let path = if sub.is_empty() {
                    dir.join(name)
                } else {
                    dir.join(sub).join(name)
                };
                path.is_file()
                    .then(|| std::fs::read_to_string(&path).ok())
                    .flatten()
            }
            Source::Builtin(app) => {
                let key = if sub.is_empty() {
                    name.to_string()
                } else {
                    format!("{sub}/{name}")
                };
                app.files
                    .iter()
                    .find(|(p, _)| *p == key)
                    .map(|(_, text)| (*text).to_string())
            }
        }
    }

    /// Every page of the app, so pages can include each other.
    pub fn html_pages(&self) -> Vec<(String, String)> {
        match &self.source {
            Source::Dir(dir) => std::fs::read_dir(dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    if !name.ends_with(".html") {
                        return None;
                    }
                    let text = std::fs::read_to_string(e.path()).ok()?;
                    Some((name, text))
                })
                .collect(),
            Source::Builtin(app) => app
                .files
                .iter()
                .filter(|(p, _)| p.ends_with(".html") && !p.contains('/'))
                .map(|(p, text)| ((*p).to_string(), (*text).to_string()))
                .collect(),
        }
    }
}
