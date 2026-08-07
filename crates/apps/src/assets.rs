//! Embedded static assets: the wrapper's own JS and CSS plus the
//! vendored libraries (see assets/vendor/README.md). Everything the
//! browser runs ships in the binary — the door works offline.

use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

macro_rules! asset {
    ($name:literal, $mime:literal) => {
        ($name, include_bytes!(concat!("../assets/", $name)).as_slice(), $mime)
    };
}

const ASSETS: &[(&str, &[u8], &str)] = &[
    asset!("app.css", "text/css"),
    asset!("store.js", "text/javascript"),
    asset!("gl-chart.js", "text/javascript"),
    asset!("gl-table.js", "text/javascript"),
    asset!("vendor/htmx.min.js", "text/javascript"),
    asset!("vendor/vega.min.js", "text/javascript"),
    asset!("vendor/vega-lite.min.js", "text/javascript"),
    asset!("vendor/vega-embed.min.js", "text/javascript"),
    asset!("vendor/arrow.min.js", "text/javascript"),
];

pub async fn asset(Path(file): Path<String>) -> Response {
    match ASSETS.iter().find(|(name, ..)| *name == file) {
        Some((_, bytes, mime)) => (
            [
                (header::CONTENT_TYPE, *mime),
                (header::CACHE_CONTROL, "max-age=3600"),
            ],
            *bytes,
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, format!("no asset `{file}`")).into_response(),
    }
}
