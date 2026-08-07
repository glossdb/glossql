//! An app is a directory: `apps/<name>/app.toml` names its title and
//! binds the one dataset its frames read (the one-dataset-per-workspace
//! binding lives in the app, SPEC.md §1). Everything else in the
//! directory *is* the app — pages (`*.html`, tera), `frames/*.sql`,
//! `specs/*.vl.json` — read fresh per request, so an author saves a
//! file and reloads.

use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct AppDef {
    pub name: String,
    pub title: String,
    pub dataset: String,
    pub dir: PathBuf,
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

impl AppDef {
    /// The app's definition, or why there is none: `Ok(None)` is "no
    /// such app" (a 404), `Err` is an app that exists but cannot serve.
    pub fn load(workspace: &Path, name: &str) -> Result<Option<AppDef>, String> {
        if !safe_segment(name) {
            return Ok(None);
        }
        let dir = workspace.join("apps").join(name);
        let manifest = dir.join("app.toml");
        if !manifest.is_file() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&manifest)
            .map_err(|e| format!("reading {}: {e}", manifest.display()))?;
        let value: toml::Value =
            toml::from_str(&text).map_err(|e| format!("{}: {e}", manifest.display()))?;
        let field = |key: &str| {
            value
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        let dataset = field("dataset")
            .ok_or_else(|| format!("{}: `dataset` is required", manifest.display()))?;
        if !identifier(&dataset) {
            return Err(format!(
                "{}: `dataset` must be a plain identifier, got `{dataset}`",
                manifest.display()
            ));
        }
        Ok(Some(AppDef {
            title: field("title").unwrap_or_else(|| name.to_string()),
            name: name.to_string(),
            dataset,
            dir,
        }))
    }

    /// Every servable app in the workspace, for the shell nav and the
    /// home page. Broken manifests are skipped here — their own pages
    /// say what is wrong.
    pub fn list(workspace: &Path) -> Vec<AppDef> {
        let Ok(entries) = std::fs::read_dir(workspace.join("apps")) else {
            return Vec::new();
        };
        let mut apps: Vec<AppDef> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter_map(|name| AppDef::load(workspace, &name).ok().flatten())
            .collect();
        apps.sort_by(|a, b| a.name.cmp(&b.name));
        apps
    }

    /// A file inside the app directory, guarded against escaping it.
    pub fn file(&self, sub: &str, name: &str) -> Option<PathBuf> {
        if !safe_segment(name) {
            return None;
        }
        let path = if sub.is_empty() {
            self.dir.join(name)
        } else {
            self.dir.join(sub).join(name)
        };
        path.is_file().then_some(path)
    }
}
