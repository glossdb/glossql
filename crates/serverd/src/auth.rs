//! Who is speaking, proved rather than declared.
//!
//! SPEC.md §1 says the actor rides the transport. Until now no door
//! asked the transport for anything: `/mcp` took the client's own name
//! for its id, `/query` and `/app` wrote as an anonymous human, and the
//! supersession precedence — human outranks agent, key `(subject,
//! aspect, actor kind)` — rested on every caller being well behaved.
//! A signed token closes that: `kind` is a claim the issuer signs, so
//! an agent cannot mint human standing, because it cannot sign.
//!
//! The server is an OAuth 2.1 **resource server** and never an
//! authorization server. It verifies a bearer token against a public
//! key and maps its claims to an [`Actor`]. There is no login flow, no
//! user table, and nothing to administer inside a workspace.
//!
//! Two configurations:
//!
//! * **Configured issuer** — `--issuer-key` (a public key in PEM),
//!   `--issuer`, `--audience`. The company's IdP mints; this verifies.
//! * **Local** — the workspace holds its own Ed25519 key, generated on
//!   first boot, and the server mints one human and one agent token
//!   from it. Machines carry the token in `Authorization: Bearer`;
//!   a browser carries the same string in a cookie, which is what the
//!   htmx essay's advice comes to.
//!
//! Standing that the server *witnesses* is a separate thing and is not
//! governed here: an answer elicited mid-call (the MCP form, a `ui://`
//! page's click) lands with human standing over an agent's token,
//! because the server saw the act (SPEC.md §1). The token governs
//! standing that is *claimed*.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use glossql_glossary::{Actor, ActorKind};
use glossql_session::Caller;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// The cookie a browser carries the same token in. `HttpOnly` keeps it
/// away from injected script, `SameSite=Lax` is the CSRF defence the
/// htmx essay settles on, and `Path=/` covers every dataset — the
/// dataset is a path segment, not a separate credential.
pub const COOKIE: &str = "glossql_token";

/// The query parameter a startup link carries once. The door swaps it
/// into the cookie and redirects to the bare path, so the token does
/// not stay in the address bar, the history, or a shared screenshot.
pub const TOKEN_PARAM: &str = "token";

/// The dataset the URL names, for doors that cannot use an axum
/// extractor. `/mcp` is served by a tower service through rmcp, which
/// hands the tool handler the HTTP parts and nothing else, so the
/// segment travels in the extensions beside the caller.
#[derive(Clone, Debug)]
pub struct Dataset(pub String);

#[derive(Serialize, Deserialize)]
struct Claims {
    iss: String,
    aud: String,
    sub: String,
    /// `human` | `agent` — the actor kind, which is the third leg of
    /// the supersession key and the reason the token exists.
    kind: String,
    exp: u64,
    iat: u64,
}

/// The verifying half, plus the minting half when the workspace owns
/// its key.
pub struct Gate {
    decoding: DecodingKey,
    validation: Validation,
    /// Local mode only: a configured issuer's private key is not ours
    /// and never will be.
    encoding: Option<EncodingKey>,
    algorithm: Algorithm,
    issuer: String,
    /// The canonical URI of this server, the token's audience. RFC 8707
    /// §2, which the MCP authorization spec makes a MUST for a resource
    /// server: a token minted for somewhere else must not open this.
    pub resource: String,
    /// With no token at all: refuse (`true`), or serve as the door's
    /// own default (`false`). False is how a fresh workspace is opened
    /// and tested before anyone has a token in hand.
    pub require_token: bool,
}

impl Gate {
    /// The workspace's own key, generated on first boot and kept under
    /// `keys/`. The private half never leaves the directory; the public
    /// half is written beside it so another process can verify what
    /// this one signed.
    pub fn local(workspace: &Path, resource: &str, require_token: bool) -> Result<Gate, String> {
        let dir = workspace.join("keys");
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let private = dir.join("signing.pem");
        let signing = if private.exists() {
            let pem = std::fs::read_to_string(&private)
                .map_err(|e| format!("{}: {e}", private.display()))?;
            SigningKey::from_pkcs8_pem(&pem).map_err(|e| format!("{}: {e}", private.display()))?
        } else {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed).map_err(|e| format!("no randomness: {e}"))?;
            let signing = SigningKey::from_bytes(&seed);
            let pem = signing
                .to_pkcs8_pem(LineEnding::LF)
                .map_err(|e| e.to_string())?;
            write_private(&private, pem.as_str())?;
            signing
        };
        let public = signing
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .map_err(|e| e.to_string())?;
        std::fs::write(dir.join("public.pem"), &public)
            .map_err(|e| format!("{}: {e}", dir.join("public.pem").display()))?;
        let private_pem = signing
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| e.to_string())?;
        Ok(Gate {
            decoding: DecodingKey::from_ed_pem(public.as_bytes()).map_err(|e| e.to_string())?,
            validation: validation(Algorithm::EdDSA, LOCAL_ISSUER, resource),
            encoding: Some(
                EncodingKey::from_ed_pem(private_pem.as_bytes()).map_err(|e| e.to_string())?,
            ),
            algorithm: Algorithm::EdDSA,
            issuer: LOCAL_ISSUER.into(),
            resource: resource.into(),
            require_token,
        })
    }

    /// A configured issuer's public key, in PEM. The family is read
    /// from the key itself rather than named by a flag — an Ed25519,
    /// EC, or RSA public key each admits exactly one set of algorithms,
    /// and accepting more than the key can carry is how algorithm
    /// confusion gets in.
    pub fn issuer(
        key_pem: &Path,
        issuer: &str,
        resource: &str,
        require_token: bool,
    ) -> Result<Gate, String> {
        let pem = std::fs::read(key_pem).map_err(|e| format!("{}: {e}", key_pem.display()))?;
        let (decoding, algorithm) = if let Ok(key) = DecodingKey::from_ed_pem(&pem) {
            (key, Algorithm::EdDSA)
        } else if let Ok(key) = DecodingKey::from_ec_pem(&pem) {
            (key, Algorithm::ES256)
        } else {
            (
                DecodingKey::from_rsa_pem(&pem).map_err(|e| {
                    format!(
                        "{}: not an Ed25519, EC, or RSA public key in PEM ({e}) — \
                         a certificate is not a key here; extract its public key first",
                        key_pem.display()
                    )
                })?,
                Algorithm::RS256,
            )
        };
        Ok(Gate {
            decoding,
            validation: validation(algorithm, issuer, resource),
            encoding: None,
            algorithm,
            issuer: issuer.into(),
            resource: resource.into(),
            require_token,
        })
    }

    /// A token for an actor. Local mode only — with a configured issuer
    /// the private key is theirs, and minting is their business.
    pub fn mint(&self, kind: ActorKind, id: &str, days: u64) -> Result<String, String> {
        let key = self
            .encoding
            .as_ref()
            .ok_or("this workspace does not hold a signing key — its issuer mints")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        let claims = Claims {
            iss: self.issuer.clone(),
            aud: self.resource.clone(),
            sub: id.into(),
            kind: kind.as_str().into(),
            iat: now,
            exp: now + Duration::from_secs(days * 86_400).as_secs(),
        };
        jsonwebtoken::encode(&Header::new(self.algorithm), &claims, key).map_err(|e| e.to_string())
    }

    /// Signature, issuer, audience, and expiry, then the claims that
    /// make an actor. An unknown `kind` is refused rather than defaulted
    /// — defaulting it would pick a standing on the holder's behalf.
    pub fn verify(&self, token: &str) -> Result<Actor, String> {
        let data = jsonwebtoken::decode::<Claims>(token, &self.decoding, &self.validation)
            .map_err(|e| e.to_string())?;
        let kind = match data.claims.kind.as_str() {
            "human" => ActorKind::Human,
            "agent" => ActorKind::Agent,
            other => return Err(format!("unknown actor kind `{other}`")),
        };
        if data.claims.sub.trim().is_empty() {
            return Err("the token names no subject".into());
        }
        Ok(Actor {
            kind,
            id: data.claims.sub,
        })
    }

    /// RFC 9728 protected resource metadata, which the MCP
    /// authorization spec makes a MUST for a resource server. In local
    /// mode the workspace is its own issuer and says so.
    pub fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "resource": self.resource,
            "authorization_servers": [self.issuer],
            "bearer_methods_supported": ["header", "cookie"],
        })
    }
}

const LOCAL_ISSUER: &str = "glossql-workspace";

fn validation(algorithm: Algorithm, issuer: &str, resource: &str) -> Validation {
    let mut validation = Validation::new(algorithm);
    if algorithm == Algorithm::ES256 {
        validation.algorithms = vec![Algorithm::ES256, Algorithm::ES384];
    }
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[resource]);
    validation.validate_exp = true;
    validation
}

#[cfg(unix)]
fn write_private(path: &Path, pem: &str) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    file.write_all(pem.as_bytes())
        .map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(not(unix))]
fn write_private(path: &Path, pem: &str) -> Result<(), String> {
    std::fs::write(path, pem).map_err(|e| format!("{}: {e}", path.display()))
}

/// The tokens a local workspace hands out, written beside the key.
/// Both are long-lived on purpose: they are a workspace's own
/// credentials, carried in an MCP client's config and a browser cookie,
/// and a daily expiry would only teach people to disable the check.
pub struct Handout {
    pub human: String,
    pub agent: String,
    pub dir: PathBuf,
}

pub fn hand_out(
    gate: &Gate,
    workspace: &Path,
    human: &str,
    agent: &str,
) -> Result<Handout, String> {
    let dir = workspace.join("tokens");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let human = gate.mint(ActorKind::Human, human, 365)?;
    let agent = gate.mint(ActorKind::Agent, agent, 365)?;
    // Replaced rather than overwritten: `write_private` creates with the
    // mode, so a stale file would keep whatever mode it was born with.
    for (name, token) in [("human.jwt", &human), ("agent.jwt", &agent)] {
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        write_private(&path, token)?;
    }
    Ok(Handout { human, agent, dir })
}

/// The one gate every door is behind.
///
/// It runs at the top of the router, where the URI is still the one the
/// client sent, so it reads the dataset segment as well as the token —
/// `/mcp` is a tower service and has no other way to learn it.
pub async fn gate(State(gate): State<Arc<Gate>>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    // A startup link carries the token once. Swap it into the cookie and
    // send the browser to the bare path: the address bar, the history,
    // and anything pasted from them stay clean.
    if let Some(token) = query_token(req.uri().query()) {
        return match gate.verify(&token) {
            Ok(_) => (
                StatusCode::SEE_OTHER,
                [
                    (header::LOCATION, path.clone()),
                    (
                        header::SET_COOKIE,
                        format!("{COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/"),
                    ),
                ],
            )
                .into_response(),
            Err(e) => challenge(&gate, &e),
        };
    }
    if let Some(dataset) = dataset_segment(&path) {
        req.extensions_mut().insert(Dataset(dataset));
    }
    match bearer(&req).or_else(|| cookie(&req)) {
        Some(token) => match gate.verify(&token) {
            Ok(actor) => {
                req.extensions_mut().insert(Caller(actor));
            }
            Err(e) => return challenge(&gate, &e),
        },
        // No token. Either the server insists, or the doors fall back to
        // the identity they carried before tokens existed.
        None if gate.require_token => return challenge(&gate, "no bearer token"),
        None => {}
    }
    next.run(req).await
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

/// The dataset a URL names: the first segment, when the path is one a
/// dataset owns. The workspace's own surfaces own the rest.
fn dataset_segment(path: &str) -> Option<String> {
    let rest = path.strip_prefix('/')?;
    let (first, tail) = rest.split_once('/')?;
    if first.is_empty() || first.starts_with('.') || first == "assets" {
        return None;
    }
    (tail == "mcp" || tail == "query" || tail == "app" || tail.starts_with("app/"))
        .then(|| first.to_string())
}

fn bearer(req: &Request) -> Option<String> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
}

fn cookie(req: &Request) -> Option<String> {
    for header in req.headers().get_all(header::COOKIE) {
        for pair in header.to_str().ok()?.split(';') {
            if let Some((name, value)) = pair.split_once('=')
                && name.trim() == COOKIE
            {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn query_token(query: Option<&str>) -> Option<String> {
    for pair in query?.split('&') {
        if let Some((name, value)) = pair.split_once('=')
            && name == TOKEN_PARAM
        {
            return Some(value.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(dir: &Path) -> Gate {
        Gate::local(dir, "http://127.0.0.1:8080", false).unwrap()
    }

    #[test]
    fn a_minted_token_names_its_actor() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path());
        let token = gate.mint(ActorKind::Human, "ada", 1).unwrap();
        let actor = gate.verify(&token).unwrap();
        assert_eq!(actor.kind, ActorKind::Human);
        assert_eq!(actor.id, "ada");
    }

    #[test]
    fn a_token_for_another_audience_does_not_open_this_one() {
        let dir = tempfile::tempdir().unwrap();
        let mine = gate(dir.path());
        let elsewhere = Gate::local(dir.path(), "https://elsewhere.example", false).unwrap();
        let token = elsewhere.mint(ActorKind::Human, "ada", 1).unwrap();
        // Same key, same issuer, different audience: RFC 8707 binding is
        // the whole point of the claim.
        assert!(mine.verify(&token).is_err());
    }

    #[test]
    fn the_key_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let token = gate(dir.path())
            .mint(ActorKind::Agent, "claude", 1)
            .unwrap();
        // A second boot reads the key it wrote rather than minting a new
        // one — otherwise every restart would invalidate every handout.
        assert!(gate(dir.path()).verify(&token).is_ok());
    }

    #[test]
    fn an_agent_cannot_carry_human_standing() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path());
        let token = gate.mint(ActorKind::Agent, "claude", 1).unwrap();
        assert_eq!(gate.verify(&token).unwrap().kind, ActorKind::Agent);
        // Rewriting the claim breaks the signature, which is the only
        // thing standing between an agent and the human's slot.
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = r#"{"iss":"glossql-workspace","aud":"http://127.0.0.1:8080","sub":"claude","kind":"human","exp":9999999999,"iat":1}"#;
        let encoded = base64_url(forged.as_bytes());
        parts[1] = &encoded;
        assert!(gate.verify(&parts.join(".")).is_err());
    }

    #[test]
    fn the_dataset_is_the_first_segment_of_a_door() {
        assert_eq!(dataset_segment("/f1/mcp").as_deref(), Some("f1"));
        assert_eq!(dataset_segment("/f1/query").as_deref(), Some("f1"));
        assert_eq!(dataset_segment("/f1/app/docket").as_deref(), Some("f1"));
        assert_eq!(dataset_segment("/assets/app.css"), None);
        assert_eq!(dataset_segment("/.well-known/x"), None);
        assert_eq!(dataset_segment("/"), None);
    }

    fn base64_url(bytes: &[u8]) -> String {
        const SET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            for i in 0..chunk.len() + 1 {
                out.push(SET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
        out
    }
}
