//! Who is speaking, proved rather than declared.
//!
//! SPEC.md §1 says the actor rides the transport. Until now no door
//! asked the transport for anything: `/mcp` took the client's own name
//! for its id, `/query` and `/app` wrote as an anonymous human, and the
//! supersession precedence — human outranks agent, key `(subject,
//! aspect, actor kind)` — rested on every caller being well behaved.
//! A signed token closes that: `kind` is a claim the issuer signs, so
//! an agent cannot claim human standing, because it cannot sign.
//!
//! The server is an OAuth 2.1 **resource server** and never an
//! authorization server. It verifies a bearer token against a public
//! key and maps its claims to an [`Actor`]. There is no login flow, no
//! user table, and nothing to administer inside a workspace.
//!
//! **No private key comes near this process.** There is one
//! configuration: `--public-key` (PEM), `--issuer`, `--audience`, and
//! whoever holds the matching private half does the minting — an IdP in
//! a deployment, and for development a keypair that was used once and
//! discarded, leaving `dev/public.pem` and two long-lived tokens in the
//! repository. Machines carry the token in `Authorization: Bearer`; a
//! browser carries the same string in a cookie, which is what the htmx
//! essay's advice comes to.
//!
//! Standing that the server *witnesses* is a separate thing and is not
//! governed here: an answer elicited mid-call (the MCP form, a `ui://`
//! page's click) lands with human standing over an agent's token,
//! because the server saw the act (SPEC.md §1). The token governs
//! standing that is *claimed*.

use std::path::Path;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use glossql_glossary::{Actor, ActorKind};
use glossql_session::Caller;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

/// The cookie a browser carries the same token in. `HttpOnly` keeps it
/// away from injected script, `SameSite=Lax` is the CSRF defence the
/// htmx essay settles on, and `Path=/` covers every dataset — the
/// dataset is a path segment, not a separate credential.
pub const COOKIE: &str = "glossql_token";

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

/// The verifying half, and only the verifying half.
pub struct Gate {
    decoding: DecodingKey,
    validation: Validation,
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
    /// A configured issuer's public key, in PEM. The family is read
    /// from the key itself rather than named by a flag — an Ed25519,
    /// EC, or RSA public key each admits exactly one set of algorithms,
    /// and accepting more than the key can carry is how algorithm
    /// confusion gets in.
    pub fn issuer(
        public_key: &Path,
        issuer: &str,
        resource: &str,
        require_token: bool,
    ) -> Result<Gate, String> {
        let pem =
            std::fs::read(public_key).map_err(|e| format!("{}: {e}", public_key.display()))?;
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
                        public_key.display()
                    )
                })?,
                Algorithm::RS256,
            )
        };
        Ok(Gate {
            decoding,
            validation: validation(algorithm, issuer, resource),
            issuer: issuer.into(),
            resource: resource.into(),
            require_token,
        })
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
    /// Who mints the tokens this gate verifies.
    pub fn minted_by(&self) -> &str {
        &self.issuer
    }

    pub fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "resource": self.resource,
            "authorization_servers": [self.issuer],
            "bearer_methods_supported": ["header", "cookie"],
        })
    }
}

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

/// The one gate every door is behind.
///
/// One layer above every door, so identity is read the same way for all
/// of them and no handler can forget to.
pub async fn gate(State(gate): State<Arc<Gate>>, mut req: Request, next: Next) -> Response {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The checked-in development credential: a public key and two
    /// tokens whose private half was used once and thrown away. Nothing
    /// in the tree can mint another, which is the point — these assert
    /// what verification does, not that we can sign.
    fn dev(file: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../dev")
            .join(file)
    }

    fn gate(resource: &str) -> Gate {
        Gate::issuer(&dev("public.pem"), "glossql-dev", resource, false).unwrap()
    }

    fn token(kind: &str) -> String {
        std::fs::read_to_string(dev(format!("{kind}.jwt").as_str()))
            .unwrap()
            .trim()
            .to_string()
    }

    #[test]
    fn a_token_names_its_actor_and_its_kind() {
        let gate = gate("http://127.0.0.1:8080");
        let human = gate.verify(&token("human")).unwrap();
        assert_eq!(human.kind, ActorKind::Human);
        assert_eq!(human.id, "dev-human");
        assert_eq!(gate.verify(&token("agent")).unwrap().kind, ActorKind::Agent);
    }

    #[test]
    fn a_token_for_another_audience_does_not_open_this_one() {
        // Same key, same issuer, different audience: the RFC 8707
        // binding is the whole point of the claim.
        assert!(
            gate("https://elsewhere.example")
                .verify(&token("human"))
                .is_err()
        );
    }

    #[test]
    fn an_agent_cannot_carry_human_standing() {
        let gate = gate("http://127.0.0.1:8080");
        let agent = token("agent");
        assert_eq!(gate.verify(&agent).unwrap().kind, ActorKind::Agent);
        // Rewriting the claim breaks the signature, which is the only
        // thing standing between an agent and the human's slot.
        let mut parts: Vec<&str> = agent.split('.').collect();
        let forged = r#"{"iss":"glossql-dev","aud":"http://127.0.0.1:8080","sub":"dev-agent","kind":"human","exp":9999999999,"iat":1}"#;
        let encoded = base64_url(forged.as_bytes());
        parts[1] = &encoded;
        assert!(gate.verify(&parts.join(".")).is_err());
    }

    #[test]
    fn a_private_key_never_enters_the_tree() {
        // The guard on the whole arrangement: a resource server that can
        // sign is an authorization server, whatever it is called.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dev");
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let body = std::fs::read_to_string(&path).unwrap_or_default();
            assert!(
                !body.contains("PRIVATE KEY"),
                "{} carries a private key",
                path.display()
            );
        }
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
