//! The sign-in simulation (ruled 2026-08-12): a button sets a JWT
//! cookie — no real login, any name, the slot where real auth lands
//! later. The token is HS256 over a per-boot secret, so the name has
//! a spine (a stale or hand-rolled cookie verifies to nothing) while
//! remaining what it always was: provenance, not permission. The pin
//! door prefers the cookie's subject over anything a form carries.

use axum::Form;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

const COOKIE: &str = "gl_actor";

/// One secret per server process — sign-ins do not survive a restart,
/// which is the right lifetime for a simulation.
pub fn boot_secret() -> [u8; 32] {
    let mut secret = [0u8; 32];
    getrandom::getrandom(&mut secret).expect("os entropy");
    secret
}

/// A human actor name the doors accept: trimmed, bounded, plain
/// characters. Shared by the sign-in and the pin fallback.
pub fn actor_name(raw: &str) -> Option<&str> {
    let name = raw.trim();
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || " ._@-".contains(c));
    ok.then_some(name)
}

fn mac(secret: &[u8], message: &str) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac takes any key length");
    mac.update(message.as_bytes());
    mac
}

/// A compact JWT: header.payload.signature, HS256.
pub fn issue(secret: &[u8], name: &str) -> String {
    let header = B64.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let payload = B64.encode(
        serde_json::json!({"sub": name, "kind": "human", "iat": issued_at}).to_string(),
    );
    let message = format!("{header}.{payload}");
    let signature = B64.encode(mac(secret, &message).finalize().into_bytes());
    format!("{message}.{signature}")
}

/// The verified subject riding the request, if any. A token that does
/// not verify is no token — the caller falls back, never errors.
pub fn subject(secret: &[u8], headers: &HeaderMap) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    let token = cookies
        .split(';')
        .map(str::trim)
        .find_map(|c| c.strip_prefix("gl_actor="))?;
    let (message, signature) = token.rsplit_once('.')?;
    let signature = B64.decode(signature).ok()?;
    mac(secret, message).verify_slice(&signature).ok()?;
    let (_header, payload) = message.split_once('.')?;
    let payload: serde_json::Value = serde_json::from_slice(&B64.decode(payload).ok()?).ok()?;
    payload.get("sub")?.as_str().map(str::to_string)
}

#[derive(Deserialize)]
pub struct SignIn {
    pub name: String,
    /// Where to return; the Referer covers the plain-form case.
    #[serde(default)]
    pub back: String,
}

pub async fn sign_in(
    axum::extract::State(door): axum::extract::State<crate::AppDoor>,
    headers: HeaderMap,
    Form(form): Form<SignIn>,
) -> Response {
    let Some(name) = actor_name(&form.name) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            "a name signs pins: letters, digits, spaces, . _ @ -, at most 64",
        )
            .into_response();
    };
    let token = issue(door.secret.as_ref(), name);
    let cookie = format!("{COOKIE}={token}; Path=/app; HttpOnly; SameSite=Lax");
    respond(headers, form.back, cookie)
}

pub async fn sign_out(headers: HeaderMap, Form(form): Form<Back>) -> Response {
    let cookie = format!("{COOKIE}=; Path=/app; HttpOnly; SameSite=Lax; Max-Age=0");
    respond(headers, form.back, cookie)
}

#[derive(Deserialize)]
pub struct Back {
    #[serde(default)]
    pub back: String,
}

/// Back to where the form was: an explicit local `back`, else the
/// same-origin Referer path, else the app door's home.
fn respond(headers: HeaderMap, back: String, cookie: String) -> Response {
    let local = |s: &str| s.starts_with('/') && !s.starts_with("//");
    let referer = headers
        .get(header::REFERER)
        .and_then(|v| v.to_str().ok())
        .and_then(|r| {
            // Absolute referers reduce to their path + query.
            let path = r.find("://").map_or(r, |i| {
                let rest = &r[i + 3..];
                rest.find('/').map_or("/", |j| &rest[j..])
            });
            local(path).then(|| path.to_string())
        });
    let target = if local(&back) {
        back
    } else {
        referer.unwrap_or_else(|| "/app".to_string())
    };
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, cookie),
            (header::LOCATION, target),
        ],
    )
        .into_response()
}
