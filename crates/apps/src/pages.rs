//! Pages: tera over two template tiers. The shell and the module
//! macros ship embedded in the binary; an app's own pages come from
//! the record or the binary on every request. A page's context is the
//! app with its pages (the bar's tabs) and which of them this is, the
//! workspace's app list and its datasets (the bar's two pickers), the
//! dataset the URL bound, the server's own address, and the URL's
//! query params as `state` — the URL is the only state there is.

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde_json::{Map, Value, json};
use tera::Tera;

use crate::AppDoor;
use crate::app::AppDef;
use crate::overview;

const SHELL: &str = include_str!("../templates/shell.html");
const DATASETS: &str = include_str!("../templates/datasets.html");
const TILES: &str = include_str!("../templates/modules/tiles.html");

fn base_tera() -> Result<Tera, tera::Error> {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        ("shell.html", SHELL),
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

fn apps_json(glossed: &[crate::glossed::Part]) -> Value {
    Value::Array(
        AppDef::list(glossed)
            .iter()
            .map(|a| json!({ "name": a.name, "title": a.title }))
            .collect(),
    )
}

/// The workspace's datasets, and whether the URL named one of them.
/// The `Err` is the 404 to send, built once on the miss — boxing it
/// would buy nothing on this path.
#[allow(clippy::result_large_err)]
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

/// The workspace root: every dataset at a glance and the way into
/// each, the apps, the doors, and how an agent connects. Read from
/// the record at each visit — one workspace read for the datasets,
/// one bound read per dataset for its counts.
pub async fn datasets(State(door): State<AppDoor>) -> Response {
    let (mut datasets, error) = match overview::rows(&door, None, overview::DATASETS, &[]).await {
        Ok(rows) => (rows, String::new()),
        Err(e) => (Vec::new(), e),
    };
    let names: Vec<String> = datasets
        .iter()
        .filter_map(|d| d.get("name").and_then(Value::as_str).map(str::to_string))
        .collect();
    let mut glossed = Vec::new();
    for (row, name) in datasets.iter_mut().zip(&names) {
        match overview::rows(&door, Some(name), overview::COUNTS, &[]).await {
            Ok(counts) => {
                if let (Some(into), Some(Value::Object(from))) =
                    (row.as_object_mut(), counts.first())
                {
                    into.extend(from.clone());
                }
            }
            Err(e) => {
                if let Some(into) = row.as_object_mut() {
                    into.insert("error".into(), Value::String(e));
                }
            }
        }
        glossed.extend(crate::glossed::parts(&door, name).await);
    }
    overview::grouped(
        &mut datasets,
        &["tables", "rows", "served", "stopped", "open"],
    );
    overview::minute(&mut datasets, "landed");
    // An app names no dataset, so a directory or built-in app serves
    // every one; a glossed app serves the dataset it was glossed in.
    let apps: Vec<Value> = AppDef::list(&glossed)
        .iter()
        .map(|a| {
            let serves: Vec<&String> = if a.origin() == "glossed" {
                let mut in_datasets: Vec<&String> = glossed
                    .iter()
                    .filter(|p| p.app == a.name)
                    .map(|p| &p.dataset)
                    .collect();
                in_datasets.sort();
                in_datasets.dedup();
                in_datasets
            } else {
                names.iter().collect()
            };
            json!({ "name": a.name, "title": a.title, "origin": a.origin(), "datasets": serves })
        })
        .collect();
    let mut ctx = tera::Context::new();
    ctx.insert("datasets", &datasets);
    ctx.insert("error", &error);
    ctx.insert("apps", &apps);
    ctx.insert("origin", &*door.origin);
    render("datasets.html", ctx, base_tera())
}

/// The dataset's page is the built-in: `/<dataset>/app` opens the
/// docket, and the URL says so.
pub async fn home(State(door): State<AppDoor>, Path(dataset): Path<String>) -> Response {
    if let Some(missing) = crate::missing(&door, &dataset).await {
        return plain(StatusCode::NOT_FOUND, missing);
    }
    Redirect::to(&format!("/{dataset}/app/{}", crate::builtin::DATASET_PAGE)).into_response()
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
    let def = match AppDef::load(app, &glossed) {
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
    let pages: Vec<Value> = def
        .pages
        .iter()
        .map(|(name, title)| json!({ "name": name, "title": title }))
        .collect();
    let mut ctx = tera::Context::new();
    ctx.insert(
        "app",
        &json!({ "name": def.name, "title": def.title, "origin": def.origin() }),
    );
    ctx.insert("apps", &apps_json(&glossed));
    ctx.insert("pages", &pages);
    ctx.insert("page", page);
    ctx.insert("dataset", dataset);
    ctx.insert("datasets", &datasets);
    ctx.insert("state", &state_map(params));
    ctx.insert("origin", &*door.origin);
    render(&format!("pages/{page}.html"), ctx, tera)
}

/// Sidecar vega-lite specs, served as they were authored.
pub async fn spec(
    State(door): State<AppDoor>,
    Path((dataset, app, spec)): Path<(String, String, String)>,
) -> Response {
    let glossed = crate::glossed::parts(&door, &dataset).await;
    let def = match AppDef::load(&app, &glossed) {
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
