//! Apps the workspace authored as glosses, loaded through the shipped
//! `app_parts` read.
//!
//! This is what lets an agent author an app at all: over MCP it has
//! statements and no filesystem, so an app that only exists as a
//! directory is an app only a human with a shell can write. As glosses
//! the parts travel the way every other writing does.
//!
//! Loaded whole and filtered in Rust — an app is a handful of small
//! strings, and reading all of them costs less than composing a
//! filtered query would.

use std::collections::BTreeMap;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use glossql_glossary::{Actor, ActorKind};

use crate::AppDoor;

/// One glossed part: which app, where it goes, what it holds.
pub struct Part {
    pub app: String,
    pub path: String,
    pub text: String,
}

/// Every glossed app part the dataset carries. A dataset that does not
/// exist, holds no glossed apps, or a store that will not read serves
/// none — the built-ins and the workspace directories still answer.
pub(crate) async fn parts(door: &AppDoor, dataset: &str) -> Vec<Part> {
    // The loader reads as a Human — the same standing every frame
    // takes, and the read collapses human over agent anyway.
    let actor = Actor {
        kind: ActorKind::Human,
        id: "app:loader".into(),
    };
    let Ok(session) = door.plane.channel(actor, Some(dataset)).await else {
        return Vec::new();
    };
    let Ok(query) = session
        .query_stream("SELECT app, path, text FROM app_parts")
        .await
    else {
        return Vec::new();
    };
    let mut stream = query.stream;
    let mut out = Vec::new();
    while let Some(Ok(batch)) = stream.next().await {
        out.extend(rows(&batch));
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

fn rows(batch: &RecordBatch) -> Vec<Part> {
    let column = |name: &str| -> Option<&StringArray> {
        batch.column_by_name(name)?.as_any().downcast_ref()
    };
    let (Some(app), Some(path), Some(text)) = (column("app"), column("path"), column("text"))
    else {
        return Vec::new();
    };
    (0..batch.num_rows())
        .filter(|&i| !app.is_null(i) && !path.is_null(i) && !text.is_null(i))
        .map(|i| Part {
            app: app.value(i).to_string(),
            path: path.value(i).to_string(),
            text: text.value(i).to_string(),
        })
        .collect()
}
