//! The function surface per execution context, as pages the door
//! serves: `doc://functions/door.md`, `doc://functions/recipe.md`,
//! `doc://functions/detector.md`. Three contexts register different
//! functions, and nothing on the door said which — an agent learned
//! the surface by refusal. Each page lists what its context registers,
//! read from that context's own registry at boot (after the shipped
//! system landed), so a page cannot drift from the registration; the
//! suite compares them (`tests/suite/next.rs`).

use std::fmt::Write as _;

use glossql_glossary::{Actor, ActorKind};
use glossql_session::{DOORS, DoorPage, Plane, Registered, SessionError};

use crate::wire;

/// The three pages, built on the plane's own registries.
pub async fn pages(plane: &Plane) -> Result<Vec<DoorPage>, SessionError> {
    let actor = Actor {
        kind: ActorKind::Human,
        id: crate::BOOTSTRAP.into(),
    };
    let session = plane.channel(actor, None).await?;
    let mut door = session.registered_functions();
    door.extend(DOORS.iter().map(|(name, syntax)| Registered {
        kind: "door",
        name: (*name).to_string(),
        syntax: Some((*syntax).to_string()),
    }));
    // The functions declared when the door opened — the kit's, and any
    // a workspace declared before this boot. `functions` says what
    // stands now.
    let declared = session
        .execute("SELECT name FROM functions ORDER BY name")
        .await?;
    if let Ok(rendered) = wire::outcomes_json(&declared)
        && let Some(rows) = rendered
            .get(0)
            .and_then(|o| o.get("rows"))
            .and_then(|r| r.as_array())
    {
        door.extend(
            rows.iter()
                .filter_map(|r| r.get("name")?.as_str())
                .map(|name| Registered {
                    kind: "extract",
                    name: name.to_string(),
                    syntax: Some(format!("SELECT {name}() FROM <subject>")),
                }),
        );
    }
    door.sort();
    Ok(vec![
        page(
            "door",
            "Functions on the door",
            "What SQL sent through the door can call — a statement's SQL, a \
             grounding's, an app frame's. The engine's functions, the JSON \
             functions, the try-casts and the shipped aggregates plan here \
             (`scalar`, `aggregate`, `window`, `table`); the reads the planner \
             answers by name are the doors over the record (`door`); a \
             declared function runs as an extraction, `SELECT <name>() FROM \
             <subject>`, and `SELECT name FROM functions` lists what stands \
             now (`extract`, as declared when the door opened). \
             a grounding serves under its name, `misfit.<name>()` and \
             `whatif.<name>()` serve what the workspace glossed.",
            &door,
        ),
        page(
            "recipe",
            "Functions in a recipe or a probe",
            "What the SQL of `DECLARE RECIPE` and `PROBE` can call: the \
             engine's defaults, the try-casts, and the three readers over \
             the source's files. Nothing else — no JSON functions, no \
             shipped aggregates, no declared function.",
            &glossql_session::reader_functions(),
        ),
        page(
            "detector",
            "Functions in a detector",
            "What a witness's detector query can call over its `slots` \
             relation: the engine's defaults, nothing more.",
            &glossql_session::detector_functions(),
        ),
    ])
}

fn page(name: &str, title: &str, intro: &str, list: &[Registered]) -> DoorPage {
    let mut body = format!("# {title}\n\n{intro}\n\n| function | kind | syntax |\n|---|---|---|\n");
    for f in list {
        let syntax = f.syntax.as_deref().unwrap_or("").replace('|', "\\|");
        let _ = writeln!(body, "| `{}` | {} | {} |", f.name, f.kind, syntax);
    }
    DoorPage {
        uri: format!("doc://functions/{name}.md"),
        name: format!("functions/{name}.md"),
        title: title.to_string(),
        description: title.to_string(),
        mime: "text/markdown",
        body,
    }
}
