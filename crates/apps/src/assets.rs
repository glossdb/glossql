//! Embedded static assets: the wrapper's own JS and CSS plus the
//! vendored libraries (see assets/vendor/README.md). Everything the
//! browser runs ships in the binary — the door works offline.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::LazyLock;

use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

macro_rules! asset {
    ($name:literal, $mime:literal) => {
        (
            $name,
            include_bytes!(concat!("../assets/", $name)).as_slice(),
            $mime,
        )
    };
}

const ASSETS: &[(&str, &[u8], &str)] = &[
    asset!("app.css", "text/css"),
    asset!("store.js", "text/javascript"),
    asset!("gl-chart.js", "text/javascript"),
    asset!("gl-table.js", "text/javascript"),
    asset!("gl-value.js", "text/javascript"),
    asset!("gl-rows.js", "text/javascript"),
    asset!("gl-window.js", "text/javascript"),
    asset!("gl-graph.js", "text/javascript"),
    asset!("vendor/htmx.min.js", "text/javascript"),
    asset!("vendor/vega.min.js", "text/javascript"),
    asset!("vendor/vega-lite.min.js", "text/javascript"),
    asset!("vendor/vega-embed.min.js", "text/javascript"),
    asset!("vendor/arrow.min.js", "text/javascript"),
    asset!("vendor/dagre.min.js", "text/javascript"),
];

/// One validator per asset, from its bytes: a new binary's changed
/// asset has a new tag, an unchanged one keeps its own.
static TAGS: LazyLock<Vec<String>> = LazyLock::new(|| {
    ASSETS
        .iter()
        .map(|(_, bytes, _)| {
            let mut hasher = DefaultHasher::new();
            bytes.hash(&mut hasher);
            format!("\"{:016x}\"", hasher.finish())
        })
        .collect()
});

pub async fn asset(Path(file): Path<String>, headers: HeaderMap) -> Response {
    let Some(at) = ASSETS.iter().position(|(name, ..)| *name == file) else {
        return (StatusCode::NOT_FOUND, format!("no asset `{file}`")).into_response();
    };
    let (_, bytes, mime) = ASSETS[at];
    let tag = TAGS[at].as_str();
    // no-cache: the browser revalidates on every load, so a new
    // binary's assets land immediately and a stale island never
    // renders. The tag is what makes the revalidation cheap — an
    // unchanged asset answers 304 with no body.
    let caching = [(header::ETAG, tag), (header::CACHE_CONTROL, "no-cache")];
    let unchanged = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|sent| sent.split(',').any(|t| t.trim() == tag));
    if unchanged {
        return (StatusCode::NOT_MODIFIED, caching).into_response();
    }
    (caching, [(header::CONTENT_TYPE, mime)], bytes).into_response()
}
