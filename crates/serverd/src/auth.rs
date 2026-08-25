//! Who is speaking, proved rather than declared.
//!
//! SPEC.md §1 says the actor rides the transport. This module is the
//! transport's half of that: every door sits behind one gate, and the
//! gate reads a bearer token the same way for all of them.
//!
//! The server is an OAuth 2.1 **resource server** (MCP authorization,
//! 2026-07-28) and never an authorization server. It publishes RFC 9728
//! metadata, answers a missing or bad token with 401 and the discovery
//! pointer, and verifies tokens against the keys the issuer publishes.
//! It issues nothing: the login, the consent, the client registration
//! are the issuer's.
//!
//! **Identity is the token's; standing is the door's.** A token names a
//! subject (`sub`) — the person who consented. Whether that person is
//! speaking as themselves or through an agent is not a claim anyone
//! signs; it is which door the request came through. `/mcp` is the
//! agent door, everything else is a human door, and the gate stamps the
//! door's kind on the caller. That is the supersession key's third leg
//! (subject, aspect, actor kind), settled by the transport as §1 says.
//!
//! **What binds a token to this server.** RFC 8707, which the MCP
//! authorization spec makes a MUST: the token's `aud` names this
//! server's canonical URI. MCP clients ask for that with the `resource`
//! parameter; an issuer that fills `aud` only from a parameter of its
//! own, which no MCP client sends, mints `aud: []`. For those the gate
//! binds on the other claim such an issuer does stamp: the application
//! the token was minted for (`azp`, or `client_id` per RFC 9068), which
//! must be this server's registered client. A token for another
//! application, or for another resource, is refused either way.
//!
//! Standing the server *witnesses* is a separate matter and is not
//! governed here: an answer elicited mid-call lands with human standing
//! under the same subject, because the server saw the act.

use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use glossql_glossary::{Actor, ActorKind};
use glossql_session::Caller;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode_header};
use serde::Deserialize;

/// The cookie a browser carries its token in. `HttpOnly` keeps it away
/// from injected script, `SameSite=Lax` is the CSRF defence, and
/// `Path=/` covers every dataset — the dataset is a path segment, not a
/// separate credential.
pub const COOKIE: &str = "glossql_token";

/// How soon the key set may be fetched again after an unknown `kid`. A
/// rotation is rare; a flood of forged `kid`s must not become a flood
/// of requests at the issuer.
const REFRESH_FLOOR: Duration = Duration::from_secs(10);

/// The issuer's two user-facing endpoints, read from its discovery
/// document: where a browser is sent to sign in, and where a code is
/// exchanged for a token ([`crate::login`]).
#[derive(Clone, Debug)]
pub struct Endpoints {
    pub authorization: String,
    pub token: String,
}

/// The verifying half, and only the verifying half.
pub struct Gate {
    issuer: String,
    /// This server's canonical URI, the token's audience (RFC 8707 §2).
    resource: String,
    /// The application registered at the issuer for this server — what
    /// a token minted for it carries as `azp`.
    client_id: String,
    endpoints: Endpoints,
    keys: RwLock<JwkSet>,
    /// Where fresh keys come from. `None` when the set is fixed, which is
    /// the test arrangement — then an unknown `kid` is a plain refusal.
    jwks_uri: Option<String>,
    refreshed: Mutex<Instant>,
    http: reqwest::Client,
}

/// The fields of the issuer's discovery document this server reads.
#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: String,
    authorization_endpoint: String,
    token_endpoint: String,
}

/// `aud` as RFC 7519 allows it: one string, or an array of them.
#[derive(Deserialize, Default, Debug)]
#[serde(untagged)]
enum Audience {
    One(String),
    #[default]
    None,
    Many(Vec<String>),
}

impl Audience {
    fn names(&self, resource: &str) -> bool {
        match self {
            Audience::One(a) => a == resource,
            Audience::Many(all) => all.iter().any(|a| a == resource),
            Audience::None => false,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Audience::One(_) => false,
            Audience::Many(all) => all.is_empty(),
            Audience::None => true,
        }
    }
}

impl std::fmt::Display for Audience {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Audience::One(a) => write!(f, "{a}"),
            Audience::Many(all) => write!(f, "[{}]", all.join(", ")),
            Audience::None => write!(f, "[]"),
        }
    }
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    aud: Audience,
    /// The application the token was minted for, under either name it
    /// goes by: `azp` (the OpenID Connect convention many issuers carry
    /// onto access tokens) or `client_id` (RFC 9068 §2.2, the JWT
    /// access-token profile).
    azp: Option<String>,
    client_id: Option<String>,
}

impl Claims {
    fn application(&self) -> Option<&str> {
        self.azp.as_deref().or(self.client_id.as_deref())
    }
}

impl Gate {
    /// An issuer, by discovery: its OpenID configuration names the key
    /// set, and the key set is fetched once here and again only when a
    /// token names a key that is not in it.
    ///
    /// Fails when the issuer cannot be read. There is nothing to verify
    /// against without it, and a server that cannot verify does not
    /// serve.
    pub async fn discover(issuer: &str, resource: &str, client_id: &str) -> Result<Gate, String> {
        let issuer = issuer.trim_end_matches('/');
        let http = reqwest::Client::new();
        let url = format!("{issuer}/.well-known/openid-configuration");
        let doc: Discovery = fetch(&http, &url).await?;
        if doc.issuer.trim_end_matches('/') != issuer {
            return Err(format!(
                "{url} names issuer `{}`, not `{issuer}` — the document must be the issuer's own",
                doc.issuer
            ));
        }
        let keys = fetch(&http, &doc.jwks_uri).await?;
        Ok(Gate {
            issuer: issuer.to_string(),
            resource: resource.to_string(),
            client_id: client_id.to_string(),
            endpoints: Endpoints {
                authorization: doc.authorization_endpoint,
                token: doc.token_endpoint,
            },
            keys: RwLock::new(keys),
            jwks_uri: Some(doc.jwks_uri),
            refreshed: Mutex::new(Instant::now()),
            http,
        })
    }

    /// A fixed key set, for tests: whoever holds the private halves mints.
    pub fn with_keys(
        issuer: &str,
        resource: &str,
        client_id: &str,
        keys: JwkSet,
        endpoints: Endpoints,
    ) -> Gate {
        Gate {
            issuer: issuer.trim_end_matches('/').to_string(),
            resource: resource.to_string(),
            client_id: client_id.to_string(),
            endpoints,
            keys: RwLock::new(keys),
            jwks_uri: None,
            refreshed: Mutex::new(Instant::now()),
            http: reqwest::Client::new(),
        }
    }

    /// Signature, issuer, expiry, then the binding to this server, then
    /// the subject.
    ///
    /// The key is the one the token names (`kid`); the algorithm is the
    /// one that key's family admits, read off the key rather than off
    /// the token, which is what keeps algorithm confusion out.
    pub async fn verify(&self, token: &str) -> Result<String, String> {
        let header = decode_header(token).map_err(|e| e.to_string())?;
        let kid = header.kid.ok_or("the token names no key (`kid`)")?;
        let key = match self.key(&kid) {
            Some(key) => key,
            None => {
                self.refresh().await?;
                self.key(&kid)
                    .ok_or_else(|| format!("no key `{kid}` at {}", self.issuer))?
            }
        };
        let (decoding, algorithm) = key;
        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&[&self.issuer]);
        // The audience is checked below, where an empty one has a
        // second reading; the library's own check knows only the first.
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        let claims = jsonwebtoken::decode::<Claims>(token, &decoding, &validation)
            .map_err(|e| e.to_string())?
            .claims;
        let bound = claims.aud.names(&self.resource)
            || (claims.aud.is_empty() && claims.application() == Some(&self.client_id));
        if !bound {
            return Err(format!(
                "the token is for {} (application {}), this server is {} (application {})",
                claims.aud,
                claims.application().unwrap_or("unnamed"),
                self.resource,
                self.client_id
            ));
        }
        if claims.sub.trim().is_empty() {
            return Err("the token names no subject".into());
        }
        Ok(claims.sub)
    }

    fn key(&self, kid: &str) -> Option<(DecodingKey, Algorithm)> {
        let keys = self.keys.read().expect("jwks lock");
        let jwk = keys.find(kid)?;
        let algorithm = algorithm_of(jwk)?;
        let decoding = DecodingKey::from_jwk(jwk).ok()?;
        Some((decoding, algorithm))
    }

    async fn refresh(&self) -> Result<(), String> {
        let Some(uri) = &self.jwks_uri else {
            return Ok(());
        };
        {
            let mut at = self.refreshed.lock().expect("refresh lock");
            if at.elapsed() < REFRESH_FLOOR {
                return Ok(());
            }
            *at = Instant::now();
        }
        let keys = fetch(&self.http, uri).await?;
        *self.keys.write().expect("jwks lock") = keys;
        Ok(())
    }

    /// Who mints the tokens this gate verifies.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// This server's canonical URI.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// The application this server is registered as at the issuer.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }

    /// Whether the browser's cookie must be marked `Secure` — it must
    /// whenever the server is reached over TLS, and cannot be on plain
    /// loopback HTTP, where a browser would drop it.
    pub fn secure(&self) -> bool {
        self.resource.starts_with("https://")
    }

    /// RFC 9728 protected resource metadata, which the MCP authorization
    /// spec makes a MUST: where a client learns which authorization
    /// server to go to for this resource.
    pub fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "resource": self.resource,
            "authorization_servers": [self.issuer],
            "bearer_methods_supported": ["header"],
        })
    }
}

async fn fetch<T: serde::de::DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
) -> Result<T, String> {
    http.get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("{url}: {e}"))?
        .json()
        .await
        .map_err(|e| format!("{url}: {e}"))
}

/// The algorithm a published key admits. The key's own `alg` when it
/// names one; otherwise the one its family implies. A family this
/// server does not verify with (symmetric keys, P-521) yields none, and
/// a token naming such a key is refused.
fn algorithm_of(jwk: &Jwk) -> Option<Algorithm> {
    if let Some(named) = &jwk.common.key_algorithm {
        return named.to_string().parse().ok();
    }
    match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::OctetKeyPair(_) => Some(Algorithm::EdDSA),
        AlgorithmParameters::EllipticCurve(params) => match params.curve {
            EllipticCurve::P256 => Some(Algorithm::ES256),
            EllipticCurve::P384 => Some(Algorithm::ES384),
            _ => None,
        },
        // Symmetric keys and whatever a later jsonwebtoken adds: a
        // family this server does not verify with.
        _ => None,
    }
}

/// The gate, one layer above a door, carrying that door's standing.
///
/// Every door is behind one of these, so identity is read the same way
/// for all of them and no handler can forget to. The kind is the
/// door's: `/mcp` says agent, the others say human.
pub async fn gate(
    State((gate, kind)): State<(std::sync::Arc<Gate>, ActorKind)>,
    mut req: Request,
    next: Next,
) -> Response {
    let at = format!("{} {}", req.method(), req.uri().path());
    let Some(token) = bearer(&req).or_else(|| cookie(&req)) else {
        return refused(&gate, &req, &at, "no bearer token");
    };
    match gate.verify(&token).await {
        Ok(id) => {
            req.extensions_mut().insert(Caller(Actor { kind, id }));
            next.run(req).await
        }
        Err(e) => refused(&gate, &req, &at, &e),
    }
}

/// A refusal, said out loud minus the token: the client sees only a
/// 401, and whoever runs the server would otherwise see nothing.
///
/// A person arriving in a browser is not answered with a 401 to read
/// but sent to sign in, and brought back to where they were going.
fn refused(gate: &Gate, req: &Request, at: &str, why: &str) -> Response {
    println!("glossql refused {at}: {why}");
    if navigates(req) {
        let back = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        return see_other(&format!("/auth/login?{}", query(&[("next", back)])));
    }
    challenge(gate, why)
}

/// Whether the request is a person's browser going somewhere, as
/// opposed to a machine's call: a GET that asks for HTML.
fn navigates(req: &Request) -> bool {
    req.method() == axum::http::Method::GET
        && req
            .headers()
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|accept| accept.contains("text/html"))
}

/// 401 with the discovery pointer the MCP spec requires, so a client
/// that can authorize knows where to go.
fn challenge(gate: &Gate, why: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            format!(
                "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource\", \
                 error=\"invalid_token\", error_description=\"{}\"",
                gate.resource,
                why.replace('"', "'")
            ),
        )],
        format!("{why}\n"),
    )
        .into_response()
}

fn bearer(req: &Request) -> Option<String> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
}

fn cookie(req: &Request) -> Option<String> {
    cookie_named(req.headers(), COOKIE)
}

/// One cookie's value out of the request, by name.
pub(crate) fn cookie_named(headers: &header::HeaderMap, name: &str) -> Option<String> {
    for header in headers.get_all(header::COOKIE) {
        for pair in header.to_str().ok()?.split(';') {
            if let Some((k, value)) = pair.split_once('=')
                && k.trim() == name
            {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

/// A `Set-Cookie` value in the shape every cookie of this server takes:
/// `HttpOnly`, `SameSite=Lax`, scoped to `path`, `Secure` behind TLS,
/// and either good for `max_age` seconds or, at zero, cleared.
pub(crate) fn set_cookie(
    name: &str,
    value: &str,
    path: &str,
    max_age: Option<u64>,
    secure: bool,
) -> HeaderValue {
    let mut cookie = format!("{name}={value}; Path={path}; HttpOnly; SameSite=Lax");
    if let Some(seconds) = max_age {
        cookie.push_str(&format!("; Max-Age={seconds}"));
    }
    if secure {
        cookie.push_str("; Secure");
    }
    HeaderValue::from_str(&cookie).expect("a cookie is ASCII")
}

/// 303: the browser goes there with a GET, whatever it just sent.
pub(crate) fn see_other(location: &str) -> Response {
    (
        StatusCode::SEE_OTHER,
        [(header::LOCATION, location.to_string())],
    )
        .into_response()
}

/// A query string, encoded.
pub(crate) fn query(pairs: &[(&str, &str)]) -> String {
    let mut out = oauth2::url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        out.append_pair(k, v);
    }
    out.finish()
}
