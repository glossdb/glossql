//! An app is a named set of parts — pages (`*.html`, tera),
//! `frames/*.sql`, `specs/*.vl.json`, a manifest — authored as glosses
//! into the record (`glossed.rs`: one gloss per part, the shape an
//! agent over MCP writes and a human supersedes) or shipped in the
//! binary (`builtin.rs`). Nothing is read from disk: the record and
//! the binary are the two sources, in that order. An app names no
//! dataset — the URL does (`/<dataset>/app/<name>`), so one app serves
//! every dataset in the workspace and the bar's pickers are links
//! rather than a feature.

use std::collections::BTreeMap;

use crate::builtin::{self, BuiltinApp};

#[derive(Debug)]
pub struct AppDef {
    pub name: String,
    pub title: String,
    /// The app's pages as the bar lists them: `(name, title)` in the
    /// manifest's order, else every page by its file name, index first.
    pub pages: Vec<(String, String)>,
    source: Source,
}

#[derive(Debug)]
enum Source {
    /// The app's files as the glosses spelled them, keyed like a
    /// directory: `index.html`, `frames/open.sql`.
    Glossed(BTreeMap<String, String>),
    Builtin(&'static BuiltinApp),
}

/// URL segments name files inside an app: one flat name, no
/// separators, no dot-walking, nothing hidden.
pub fn safe_segment(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// What a manifest says: the title the picker prints, and the pages
/// the bar shows as tabs, in the author's order. A `dataset` key is
/// accepted and ignored — the URL binds.
#[derive(Default)]
struct Manifest {
    title: Option<String>,
    pages: Vec<(String, String)>,
}

/// The built-in's manifest is TOML (`app.toml` beside its pages).
fn manifest_toml(origin: &str, text: &str) -> Result<Manifest, String> {
    let value: toml::Value = toml::from_str(text).map_err(|e| format!("{origin}: {e}"))?;
    let value = serde_json::to_value(value).map_err(|e| format!("{origin}: {e}"))?;
    Ok(manifest(&value))
}

/// A glossed app's manifest is the `app` aspect's JSON body.
fn manifest_json(origin: &str, text: &str) -> Result<Manifest, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("{origin}: {e}"))?;
    Ok(manifest(&value))
}

fn manifest(value: &serde_json::Value) -> Manifest {
    let pages = value
        .get("pages")
        .and_then(serde_json::Value::as_array)
        .map(|pages| {
            pages
                .iter()
                .filter_map(|p| {
                    Some((
                        p.get("name")?.as_str()?.to_string(),
                        p.get("title")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    Manifest {
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        pages,
    }
}

/// The pages a manifest did not list: every page by its file name,
/// `index` first, the rest in name order.
fn pages_by_name<'a>(files: impl Iterator<Item = &'a str>) -> Vec<(String, String)> {
    let mut names: Vec<String> = files
        .filter(|p| p.ends_with(".html") && !p.contains('/'))
        .map(|p| p.trim_end_matches(".html").to_string())
        .collect();
    names.sort_by_key(|n| (n != "index", n.clone()));
    names.into_iter().map(|n| (n.clone(), n)).collect()
}

impl AppDef {
    /// The app's definition, or why there is none: `Ok(None)` is "no
    /// such app" (a 404), `Err` is an app that exists but cannot serve.
    /// The record wins; a built-in answers for the name only when the
    /// record carries nothing under it. `glossed` is every app part
    /// the dataset carries, loaded once by the caller — apps are small
    /// and one read serves the whole door.
    pub fn load(name: &str, glossed: &[crate::glossed::Part]) -> Result<Option<AppDef>, String> {
        if !safe_segment(name) {
            return Ok(None);
        }
        let files = crate::glossed::files_of(glossed, name);
        // Add an app, don't fork the built-in. A glossed part carries no
        // manifest requirement, so a single `GLOSS app_frame ON
        // docket.mine` would resolve the whole app to that one file and
        // 404 every page the built-in ships — the hazard reached by the
        // route an MCP-only agent actually takes.
        if !files.is_empty() && builtin::builtin(name).is_some() {
            return Err(format!(
                "`{name}` ships in the binary and a glossed part shadows it whole — \
                 the built-in's other pages would stop serving. Author your app \
                 under its own name; the door serves as many as the workspace writes"
            ));
        }
        if !files.is_empty() {
            let manifest = match files.get("app") {
                Some(body) => manifest_json(&format!("glossed app `{name}`"), body)?,
                // Parts without a manifest still serve: the app is named
                // by its subject and bound by the URL like any other.
                None => Manifest::default(),
            };
            let pages = if manifest.pages.is_empty() {
                pages_by_name(files.keys().map(String::as_str))
            } else {
                manifest.pages
            };
            return Ok(Some(AppDef {
                title: manifest.title.unwrap_or_else(|| name.to_string()),
                name: name.to_string(),
                pages,
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
        let manifest = manifest_toml(&format!("builtin `{name}`"), toml)?;
        let pages = if manifest.pages.is_empty() {
            pages_by_name(app.files.iter().map(|(p, _)| *p))
        } else {
            manifest.pages
        };
        Ok(Some(AppDef {
            title: manifest.title.unwrap_or_else(|| name.to_string()),
            name: name.to_string(),
            pages,
            source: Source::Builtin(app),
        }))
    }

    /// Every servable app: the glossed apps, and the built-ins nothing
    /// shadows. Broken manifests are skipped here — their own pages say
    /// what is wrong.
    pub fn list(glossed: &[crate::glossed::Part]) -> Vec<AppDef> {
        let mut names: Vec<String> = Vec::new();
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
            .filter_map(|name| AppDef::load(&name, glossed).ok().flatten())
            .collect();
        apps.sort_by(|a, b| a.name.cmp(&b.name));
        apps
    }

    /// Where the app comes from, for a listing: the binary, or glosses.
    pub fn origin(&self) -> &'static str {
        match &self.source {
            Source::Glossed(_) => "glossed",
            Source::Builtin(_) => "built in",
        }
    }

    /// A file inside the app, by root-relative location.
    pub fn read(&self, sub: &str, name: &str) -> Option<String> {
        if !safe_segment(name) {
            return None;
        }
        let key = if sub.is_empty() {
            name.to_string()
        } else {
            format!("{sub}/{name}")
        };
        match &self.source {
            Source::Glossed(files) => files.get(&key).cloned(),
            Source::Builtin(app) => app
                .files
                .iter()
                .find(|(p, _)| *p == key)
                .map(|(_, text)| (*text).to_string()),
        }
    }

    /// Every page of the app, so pages can include each other.
    pub fn html_pages(&self) -> Vec<(String, String)> {
        match &self.source {
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
