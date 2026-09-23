//! Apps the workspace authored as glosses, loaded from the record.
//!
//! This is what lets an agent author an app at all: over MCP it has
//! statements and no filesystem, so an app that only exists as a
//! directory is an app only a human with a shell can write. As glosses
//! the parts travel the way every other writing does.
//!
//! What the door serves is the app at its release: a gloss of the
//! `app_release` aspect on the app's name, and each part as it stood
//! when that gloss was written — or at the earlier release its `at`
//! names. Every part written since is the draft, served under `?draft`
//! on the same URLs; an app nobody released serves its draft. The
//! collapses are the record's usual: the newest writing per (subject,
//! aspect, actor kind), then the human's over the agent's. Loaded
//! whole and cut in Rust — an app is a handful of small strings, and
//! its history not many more.

use std::collections::BTreeMap;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;

/// The aspects an app is made of, and where each part goes.
const PARTS: [&str; 4] = ["app", "app_page", "app_frame", "app_spec"];
const RELEASE: &str = "app_release";

/// One glossed part: which app, where it goes, what it holds.
pub struct Part {
    /// The dataset the part was glossed in — the one the app serves.
    pub dataset: String,
    pub app: String,
    pub path: String,
    pub text: String,
}

/// The dataset's glossed apps as the door serves them: the parts, and
/// for each app served at a release, the time its parts are cut at.
pub(crate) struct Loaded {
    pub parts: Vec<Part>,
    pub releases: BTreeMap<String, String>,
}

/// One writing on an app subject, as the record holds it.
struct Row {
    subject: String,
    aspect: String,
    actor_kind: String,
    body: String,
    written_at: String,
}

/// Every glossed app the dataset carries, at its release — or, with
/// `draft`, as its newest parts stand. A dataset that does not exist,
/// holds no glossed apps, or a store that will not read serves none
/// — the built-ins still answer.
pub(crate) async fn parts(door: &AppDoor, dataset: &str, draft: bool) -> Loaded {
    let rows = rows(door, dataset).await;
    // The cut per app: the serving release's own time, or the earlier
    // release its body names.
    let mut releases: BTreeMap<String, String> = BTreeMap::new();
    if !draft {
        let apps: Vec<&str> = rows
            .iter()
            .filter(|r| r.aspect == RELEASE)
            .map(|r| r.subject.as_str())
            .collect();
        for app in apps {
            if releases.contains_key(app) {
                continue;
            }
            let Some(release) = serving(
                rows.iter()
                    .filter(|r| r.aspect == RELEASE && r.subject == app),
            ) else {
                continue;
            };
            let at = serde_json::from_str::<serde_json::Value>(&release.body)
                .ok()
                .and_then(|v| v.get("at")?.as_str().map(str::to_string))
                .unwrap_or_else(|| release.written_at.clone());
            releases.insert(app.to_string(), at);
        }
    }
    let mut keys: Vec<(&str, &str)> = rows
        .iter()
        .filter(|r| PARTS.contains(&r.aspect.as_str()))
        .map(|r| (r.subject.as_str(), r.aspect.as_str()))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let mut out = Vec::new();
    for (subject, aspect) in keys {
        let app = subject.split('.').next().unwrap_or(subject);
        let part = subject.split_once('.').map(|(_, p)| p).unwrap_or("");
        if aspect != "app" && part.is_empty() {
            continue;
        }
        let cut = releases.get(app);
        let Some(row) = serving(rows.iter().filter(|r| {
            r.subject == subject
                && r.aspect == aspect
                && cut.is_none_or(|at| r.written_at.as_str() <= at.as_str())
        })) else {
            continue;
        };
        let (path, key) = match aspect {
            "app" => ("app".to_string(), None),
            "app_page" => (format!("{part}.html"), Some("html")),
            "app_frame" => (format!("frames/{part}.sql"), Some("sql")),
            _ => (format!("specs/{part}.vl.json"), Some("spec")),
        };
        let text = match key {
            None => row.body.clone(),
            Some(key) => match serde_json::from_str::<serde_json::Value>(&row.body)
                .ok()
                .and_then(|v| v.get(key)?.as_str().map(str::to_string))
            {
                Some(text) => text,
                None => continue,
            },
        };
        out.push(Part {
            dataset: dataset.to_string(),
            app: app.to_string(),
            path,
            text,
        });
    }
    Loaded {
        parts: out,
        releases,
    }
}

/// The writing that serves among `rows` of one slot: the newest per
/// actor kind, the human's over the agent's.
fn serving<'a>(rows: impl Iterator<Item = &'a Row>) -> Option<&'a Row> {
    let mut human: Option<&Row> = None;
    let mut agent: Option<&Row> = None;
    for row in rows {
        let slot = if row.actor_kind == "human" {
            &mut human
        } else {
            &mut agent
        };
        if slot.is_none_or(|have| row.written_at > have.written_at) {
            *slot = Some(row);
        }
    }
    human.or(agent)
}

/// Every writing on an app subject in the dataset.
async fn rows(door: &AppDoor, dataset: &str) -> Vec<Row> {
    // The loader reads as a Human — the same standing every frame
    // takes, and the read collapses human over agent anyway.
    let actor = Actor {
        kind: ActorKind::Human,
        id: "app:loader".into(),
    };
    let Ok(session) = door.plane.channel(actor, Some(dataset)).await else {
        return Vec::new();
    };
    let aspects = PARTS
        .iter()
        .chain([RELEASE].iter())
        .map(|a| format!("'{a}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let Ok(query) = session
        .query_stream(&format!(
            "SELECT g.subject, g.aspect, g.actor_kind, g.body, g.written_at FROM glossary g \
             JOIN current_dataset d ON d.dataset = g.dataset WHERE g.aspect IN ({aspects})"
        ))
        .await
    else {
        return Vec::new();
    };
    let mut stream = query.stream;
    let mut out = Vec::new();
    while let Some(Ok(batch)) = stream.next().await {
        out.extend(decode(&batch));
    }
    out
}

/// The app's files, keyed the way a directory would spell them
/// (`index.html`, `frames/open.sql`, `app` for the manifest).
pub(crate) fn files_of(parts: &[Part], app: &str) -> BTreeMap<String, String> {
    parts
        .iter()
        .filter(|p| p.app == app)
        .map(|p| (p.path.clone(), p.text.clone()))
        .collect()
}

fn decode(batch: &RecordBatch) -> Vec<Row> {
    let column = |name: &str| -> Option<&StringArray> {
        batch.column_by_name(name)?.as_any().downcast_ref()
    };
    let (Some(subject), Some(aspect), Some(kind), Some(body), Some(at)) = (
        column("subject"),
        column("aspect"),
        column("actor_kind"),
        column("body"),
        column("written_at"),
    ) else {
        return Vec::new();
    };
    (0..batch.num_rows())
        .filter(|&i| {
            !subject.is_null(i)
                && !aspect.is_null(i)
                && !kind.is_null(i)
                && !body.is_null(i)
                && !at.is_null(i)
        })
        .map(|i| Row {
            subject: subject.value(i).to_string(),
            aspect: aspect.value(i).to_string(),
            actor_kind: kind.value(i).to_string(),
            body: body.value(i).to_string(),
            written_at: at.value(i).to_string(),
        })
        .collect()
}
