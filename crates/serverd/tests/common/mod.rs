//! A test issuer: a key minted when the test starts, a gate that holds
//! its public half, and tokens signed with the private half. The server
//! only ever verifies — this is the one place in the tree that signs,
//! and it exists so the tests can say what verification does.

#![allow(dead_code)]

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use glossql_serverd::Gate;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};

pub const ISSUER: &str = "https://issuer.test";
pub const RESOURCE: &str = "http://127.0.0.1:8080";
/// The application registered at the test issuer for this server.
pub const CLIENT_ID: &str = "glossql-app";
const KID: &str = "test-key";

/// One fixed seed: the key is the same in every test, which is what
/// lets a token minted in one helper be verified by a gate built in
/// another. It never leaves the test binary.
fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7u8; 32])
}

/// The public half, as the issuer would publish it.
pub fn jwks() -> JwkSet {
    let public = signing_key().verifying_key().to_bytes();
    serde_json::from_value(json!({
        "keys": [{
            "kty": "OKP",
            "crv": "Ed25519",
            "kid": KID,
            "use": "sig",
            "x": URL_SAFE_NO_PAD.encode(public),
        }]
    }))
    .unwrap()
}

pub fn gate() -> Arc<Gate> {
    Arc::new(Gate::with_keys(ISSUER, RESOURCE, CLIENT_ID, jwks()))
}

/// A token for `sub`, valid for this issuer and this resource.
pub fn token(sub: &str) -> String {
    token_for(sub, ISSUER, RESOURCE, far_future())
}

pub fn token_for(sub: &str, iss: &str, aud: &str, exp: u64) -> String {
    sign(
        KID,
        &json!({ "iss": iss, "aud": aud, "sub": sub, "exp": exp, "iat": 1 }),
    )
}

/// What an issuer that ignores RFC 8707 mints: `aud` empty, `azp` the
/// application the token was issued to.
pub fn token_from_app(sub: &str, azp: &str) -> String {
    sign(
        KID,
        &json!({ "iss": ISSUER, "aud": [], "azp": azp, "sub": sub, "exp": far_future(), "iat": 1 }),
    )
}

/// Any claims at all, signed by the test issuer.
pub fn token_with(claims: Value) -> String {
    sign(KID, &claims)
}

/// A valid token whose header names a key the issuer does not publish —
/// what a token signed before a rotation looks like afterwards.
pub fn token_under_kid(sub: &str, kid: &str) -> String {
    sign(
        kid,
        &json!({ "iss": ISSUER, "aud": RESOURCE, "sub": sub, "exp": far_future(), "iat": 1 }),
    )
}

/// `token` with its payload swapped for one naming `sub`: the signature
/// no longer covers what the token says.
pub fn forged(token: &str, sub: &str) -> String {
    let mut parts: Vec<&str> = token.split('.').collect();
    let payload = json!({
        "iss": ISSUER, "aud": RESOURCE, "sub": sub, "exp": far_future(), "iat": 1
    })
    .to_string();
    let encoded = URL_SAFE_NO_PAD.encode(payload);
    parts[1] = &encoded;
    parts.join(".")
}

fn sign(kid: &str, claims: &Value) -> String {
    let der = signing_key().to_pkcs8_der().unwrap();
    let key = EncodingKey::from_ed_der(der.as_bytes());
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.into());
    jsonwebtoken::encode(&header, claims, &key).unwrap()
}

pub fn bearer(sub: &str) -> String {
    format!("Bearer {}", token(sub))
}

pub fn far_future() -> u64 {
    9_999_999_999
}
