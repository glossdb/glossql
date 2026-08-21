//! An app is a directory: `apps/<name>/app.toml` names its title.
//! It names no dataset — the URL does (`/<dataset>/app/<name>`), so one
//! app serves every dataset in the workspace and the header's picker is
//! a link rather than a feature. Everything else in the
//! directory *is* the app — pages (`*.html`, tera), `frames/*.sql`,
//! `specs/*.vl.json` — read fresh per request, so an author saves a
//! file and reloads. Apps shipped in the binary (`builtin.rs`) resolve
//! the same way, workspace directory first: the workspace shadows the
//! built-in, and forking is copying the directory out.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::builtin::{self, BuiltinApp};

#[derive(Debug)]
pub struct AppDef {
    pub name: String,
    pub title: String,
    source: Source,
}

#[derive(Debug)]
enum Source {
    Dir(PathBuf),
    /// The app's files as the glosses spelled them, keyed like a
    /// directory: `index.html`, `frames/open.sql`.
    Glossed(BTreeMap<String, String>),
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

/// The one manifest field: the title a page and the nav print. A
/// `dataset` key is accepted and ignored — the URL binds now.
fn manifest(origin: &str, text: &str) -> Result<Option<String>, String> {
    let value: toml::Value = toml::from_str(text).map_err(|e| format!("{origin}: {e}"))?;
    Ok(value
        .get("title")
        .and_then(|v| v.as_str())
        .map(str::to_string))
}

/// The manifest of a glossed app: the same field, arriving as the
/// `app` aspect's body rather than as TOML text.
fn manifest_json(origin: &str, text: &str) -> Result<Option<String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("{origin}: {e}"))?;
    Ok(value
        .get("title")
        .and_then(|v| v.as_str())
        .map(str::to_string))
}

impl AppDef {
    /// The app's definition, or why there is none: `Ok(None)` is "no
    /// such app" (a 404), `Err` is an app that exists but cannot serve.
    /// The workspace directory wins; a built-in answers for the name
    /// only when the workspace carries nothing under it.
    /// `glossed` is every app part the workspace has authored, loaded
    /// once by the caller — apps are small and one read serves the
    /// whole door.
    pub fn load(
        workspace: &Path,
        name: &str,
        glossed: &[crate::glossed::Part],
    ) -> Result<Option<AppDef>, String> {
        if !safe_segment(name) {
            return Ok(None);
        }
        let dir = workspace.join("apps").join(name);
        let path = dir.join("app.toml");
        if path.is_file() {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("reading {}: {e}", path.display()))?;
            let title = manifest(&path.display().to_string(), &text)?;
            return Ok(Some(AppDef {
                title: title.unwrap_or_else(|| name.to_string()),
                name: name.to_string(),
                source: Source::Dir(dir),
            }));
        }
        // A workspace directory without a manifest is an authored app
        // that cannot serve — never a silent fall-through to a built-in
        // it half-shadows.
        if dir.is_dir() {
            return Err(format!(
                "{} exists but has no app.toml — the workspace directory shadows \
                 any built-in `{name}` whole; add the manifest or remove the directory",
                dir.display()
            ));
        }
        let files = crate::glossed::files_of(glossed, name);
        // Add an app, don't fork the built-in. A
        // glossed part carries no manifest requirement, so a single
        // `GLOSS app_frame ON docket.mine` would resolve the whole app
        // to that one file and 404 every page the built-in ships. The
        // directory branch above refuses a half-shadow;
        // the glossed branch is the same hazard reached by
        // the route an MCP-only agent actually takes.
        if !files.is_empty() && builtin::builtin(name).is_some() {
            return Err(format!(
                "`{name}` ships in the binary and a glossed part shadows it whole — \
                 the built-in's other pages would stop serving. Author your app \
                 under its own name; the door serves as many as the workspace writes"
            ));
        }
        if !files.is_empty() {
            let title = match files.get("app") {
                Some(body) => manifest_json(&format!("glossed app `{name}`"), body)?,
                // Parts without a manifest still serve: the app is named
                // by its subject and bound by the URL like any other.
                None => None,
            };
            return Ok(Some(AppDef {
                title: title.unwrap_or_else(|| name.to_string()),
                name: name.to_string(),
                source: Source::Glossed(files),
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
        let title = manifest(&format!("builtin `{name}`"), toml)?;
        Ok(Some(AppDef {
            title: title.unwrap_or_else(|| name.to_string()),
            name: name.to_string(),
            source: Source::Builtin(app),
        }))
    }

    /// Every servable app: the workspace's directories plus the
    /// built-ins the workspace does not shadow. Broken manifests are
    /// skipped here — their own pages say what is wrong.
    pub fn list(workspace: &Path, glossed: &[crate::glossed::Part]) -> Vec<AppDef> {
        let mut names: Vec<String> = std::fs::read_dir(workspace.join("apps"))
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        for part in glossed {
            if !names.contains(&part.app) {
                names.push(part.app.clone());
            }
        }
        for b in builtin::BUILTINS {
            if !names.iter().any(|n| n == b.name) {
                names.push(b.name.to_string());
            }
        }
        let mut apps: Vec<AppDef> = names
            .into_iter()
            .filter_map(|name| AppDef::load(workspace, &name, glossed).ok().flatten())
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
            Source::Glossed(files) => {
                let key = if sub.is_empty() {
                    name.to_string()
                } else {
                    format!("{sub}/{name}")
                };
                files.get(&key).cloned()
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
            Source::Glossed(files) => files
                .iter()
                .filter(|(p, _)| p.ends_with(".html") && !p.contains('/'))
                .map(|(p, text)| (p.clone(), text.clone()))
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
