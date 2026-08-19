//! Pages: tera over two template tiers. The shell and the module
//! macros ship embedded in the binary; an app's own pages load fresh
//! from its directory on every request — save and reload, no rebuild.
//! A page's context is the app, the workspace's app list (for the
//! nav), and the URL's query params as `state` — the URL is the only
//! state there is.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use serde_json::{Map, Value, json};
use tera::Tera;

use crate::AppDoor;
use crate::app::AppDef;

const SHELL: &str = include_str!("../templates/shell.html");
const HOME: &str = include_str!("../templates/home.html");
const TILES: &str = include_str!("../templates/modules/tiles.html");

fn base_tera() -> Result<Tera, tera::Error> {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        ("shell.html", SHELL),
        ("home.html", HOME),
        ("modules/tiles.html", TILES),
    ])?;
    tera.register_filter("urlencode", urlencode);
    Ok(tera)
}

/// Query-component percent-encoding for template-built hrefs (state
/// values carry customer names and dates). Tera's own `urlencode`
/// lives behind the builtins feature, which pulls chrono/rand/slug —
/// this is the one builtin the templates need.
fn urlencode(value: &Value, _: &std::collections::HashMap<String, Value>) -> tera::Result<Value> {
    let s = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    Ok(Value::String(out))
}

fn state_map(params: Vec<(String, String)>) -> Value {
    let mut map = Map::new();
    for (k, v) in params {
        map.insert(k, Value::String(v));
    }
    Value::Object(map)
}

fn apps_json(workspace: &std::path::Path, glossed: &[crate::glossed::Part]) -> Value {
    Value::Array(
        AppDef::list(workspace, glossed)
            .iter()
            .map(|a| {
                json!({
                    "name": a.name,
                    "title": a.title,
                    "dataset": a.dataset.clone().unwrap_or_default(),
                })
            })
            .collect(),
    )
}

pub async fn home(
    State(door): State<AppDoor>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let glossed = crate::glossed::parts(&door).await;
    let mut ctx = tera::Context::new();
    ctx.insert("apps", &apps_json(&door.workspace, &glossed));
    ctx.insert("state", &state_map(params));
    render("home.html", ctx, base_tera())
}

pub async fn index(
    State(door): State<AppDoor>,
    Path(app): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    page_response(&door, &app, "index", params).await
}

pub async fn page(
    State(door): State<AppDoor>,
    Path((app, page)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    page_response(&door, &app, &page, params).await
}

async fn page_response(
    door: &AppDoor,
    app: &str,
    page: &str,
    params: Vec<(String, String)>,
) -> Response {
    let glossed = crate::glossed::parts(door).await;
    let def = match AppDef::load(&door.workspace, app, &glossed) {
        Ok(Some(def)) => def,
        Ok(None) => return plain(StatusCode::NOT_FOUND, format!("no app `{app}`")),
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    if def.read("", &format!("{page}.html")).is_none() {
        return plain(
            StatusCode::NOT_FOUND,
            format!("no page `{page}` in `{app}`"),
        );
    }
    let tera = base_tera().and_then(|mut tera| {
        // Every page of the app loads, so pages can include each other.
        for (name, text) in def.html_pages() {
            tera.add_raw_template(&format!("pages/{name}"), &text)?;
        }
        Ok(tera)
    });
    let mut ctx = tera::Context::new();
    // The bound dataset, not the manifest's pin: the docket pins none
    // and binds to the workspace's sole dataset, and the bar names what
    // the frames will actually read.
    let bound = crate::frames::bound_dataset(door, &def).await;
    ctx.insert(
        "app",
        &json!({
            "name": def.name,
            "title": def.title,
            "dataset": bound.unwrap_or_default(),
        }),
    );
    ctx.insert("apps", &apps_json(&door.workspace, &glossed));
    ctx.insert("state", &state_map(params));
    render(&format!("pages/{page}.html"), ctx, tera)
}

/// Sidecar vega-lite specs, served as they were authored.
pub async fn spec(
    State(door): State<AppDoor>,
    Path((app, spec)): Path<(String, String)>,
) -> Response {
    let glossed = crate::glossed::parts(&door).await;
    let def = match AppDef::load(&door.workspace, &app, &glossed) {
        Ok(Some(def)) => def,
        Ok(None) => return plain(StatusCode::NOT_FOUND, format!("no app `{app}`")),
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let Some(text) = def.read("specs", &spec) else {
        return plain(
            StatusCode::NOT_FOUND,
            format!("no spec `{spec}` in `{app}`"),
        );
    };
    ([(header::CONTENT_TYPE, "application/json")], text).into_response()
}

/// Render errors answer as readable text with the whole tera error
/// chain — the author is looking at their own template.
fn render(name: &str, ctx: tera::Context, tera: Result<Tera, tera::Error>) -> Response {
    let rendered = tera.and_then(|tera| tera.render(name, &ctx));
    match rendered {
        // Never cached. A page is a live view of a mutable record, and
        // with no Cache-Control at all the browser applies heuristic
        // freshness — so after a ruling POST redirected back here, the
        // browser would serve the pre-ruling copy from cache and the
        // change would only appear on a manual reload.
        // The redirect was correct; the caching was the bug.
        Ok(html) => (
            [(header::CACHE_CONTROL, "no-store")],
            Html(html),
        )
            .into_response(),
        Err(e) => {
            let mut lines = vec![e.to_string()];
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                lines.push(cause.to_string());
                source = cause.source();
            }
            plain(StatusCode::INTERNAL_SERVER_ERROR, lines.join("\n"))
        }
    }
}

fn plain(status: StatusCode, text: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], text).into_response()
}
