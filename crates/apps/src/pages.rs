//! Pages: tera over two template tiers. The shell and the module
//! macros ship embedded in the binary; an app's own pages load fresh
//! from its directory on every request — save and reload, no rebuild.
//! A page's context is the app, the workspace's app list (for the
//! nav), the dataset the URL bound and every dataset the workspace
//! holds (for the picker), and the URL's query params as `state` — the
//! URL is the only state there is.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use serde_json::{Map, Value, json};
use tera::Tera;

use crate::AppDoor;
use crate::app::AppDef;

const SHELL: &str = include_str!("../templates/shell.html");
const HOME: &str = include_str!("../templates/home.html");
const DATASETS: &str = include_str!("../templates/datasets.html");
const TILES: &str = include_str!("../templates/modules/tiles.html");

fn base_tera() -> Result<Tera, tera::Error> {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        ("shell.html", SHELL),
        ("home.html", HOME),
        ("datasets.html", DATASETS),
        ("modules/tiles.html", TILES),
    ])?;
    Ok(tera)
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
            .map(|a| json!({ "name": a.name, "title": a.title }))
            .collect(),
    )
}

/// The workspace's datasets, and whether the URL named one of them.
async fn admit(door: &AppDoor, dataset: &str) -> Result<Vec<String>, Response> {
    let names = crate::known(door).await;
    if names.iter().any(|n| n == dataset) {
        return Ok(names);
    }
    Err(plain(
        StatusCode::NOT_FOUND,
        crate::no_such_dataset(dataset, &names),
    ))
}

/// The workspace root: every dataset, as a way in.
pub async fn datasets(State(door): State<AppDoor>) -> Response {
    let names = door.plane.datasets().await.unwrap_or_default();
    let mut ctx = tera::Context::new();
    ctx.insert("datasets", &names);
    render("datasets.html", ctx, base_tera())
}

pub async fn home(
    State(door): State<AppDoor>,
    Path(dataset): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let datasets = match admit(&door, &dataset).await {
        Ok(names) => names,
        Err(response) => return response,
    };
    let glossed = crate::glossed::parts(&door, &dataset).await;
    let mut ctx = tera::Context::new();
    ctx.insert("apps", &apps_json(&door.workspace, &glossed));
    ctx.insert("dataset", &dataset);
    ctx.insert("datasets", &datasets);
    ctx.insert("state", &state_map(params));
    render("home.html", ctx, base_tera())
}

pub async fn index(
    State(door): State<AppDoor>,
    Path((dataset, app)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    page_response(&door, &dataset, &app, "index", params).await
}

pub async fn page(
    State(door): State<AppDoor>,
    Path((dataset, app, page)): Path<(String, String, String)>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    page_response(&door, &dataset, &app, &page, params).await
}

async fn page_response(
    door: &AppDoor,
    dataset: &str,
    app: &str,
    page: &str,
    params: Vec<(String, String)>,
) -> Response {
    let datasets = match admit(door, dataset).await {
        Ok(names) => names,
        Err(response) => return response,
    };
    let glossed = crate::glossed::parts(door, dataset).await;
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
    ctx.insert("app", &json!({ "name": def.name, "title": def.title }));
    ctx.insert("apps", &apps_json(&door.workspace, &glossed));
    ctx.insert("dataset", dataset);
    ctx.insert("datasets", &datasets);
    ctx.insert("state", &state_map(params));
    render(&format!("pages/{page}.html"), ctx, tera)
}

/// Sidecar vega-lite specs, served as they were authored.
pub async fn spec(
    State(door): State<AppDoor>,
    Path((dataset, app, spec)): Path<(String, String, String)>,
) -> Response {
    let glossed = crate::glossed::parts(&door, &dataset).await;
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
        Ok(html) => ([(header::CACHE_CONTROL, "no-store")], Html(html)).into_response(),
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
