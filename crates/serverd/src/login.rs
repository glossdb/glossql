//! The browser's way in: this server as an OAuth client of the issuer,
//! for the one door a browser uses.
//!
//! A machine obtains its token itself and carries it. A browser cannot,
//! so `/auth/login` sends it to the issuer's sign-in (authorization code
//! with PKCE, RFC 7636), `/auth/callback` exchanges the code for the
//! token — verified by the same gate every door verifies with — and
//! hands it to the browser as the `glossql_token` cookie. From then on
//! the browser is any other caller. The issuer does the signing in; this
//! server never sees a password, and holds no session: what the browser
//! carries is the token, and the token is what the gate reads.
//!
//! The one thing held between the two requests — the CSRF state, the
//! PKCE verifier, and where the person was going — rides in a short
//! cookie scoped to `/auth`, so nothing is kept on the server and a
//! restart mid-login costs one more click.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use oauth2::basic::BasicClient;
use oauth2::{
    AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet,
    EndpointSet, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};

use crate::auth::{COOKIE, Gate, cookie_named, see_other, set_cookie};

/// The cookie that carries a login in progress, from `/auth/login` to
/// `/auth/callback` and no further.
const PENDING: &str = "glossql_login";

/// How long a sign-in may take before the state expires with it.
const PENDING_SECONDS: u64 = 600;

/// The registered application, ready to send a browser to the issuer
/// and to exchange what comes back.
pub struct Login {
    gate: Arc<Gate>,
    client: BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>,
    /// Redirects are not followed: the token endpoint answers, it does
    /// not send us on, and a redirect there is something to refuse.
    http: reqwest::Client,
}

/// What a login in progress carries.
#[derive(Serialize, Deserialize)]
struct Pending {
    state: String,
    verifier: String,
    next: String,
}

#[derive(Deserialize)]
pub struct Next {
    next: Option<String>,
}

#[derive(Deserialize)]
pub struct Callback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

impl Login {
    /// The application at the gate's issuer. The redirect the issuer
    /// must know is `<resource>/auth/callback`.
    pub fn new(gate: Arc<Gate>, client_secret: &str) -> Result<Login, String> {
        let endpoints = gate.endpoints();
        let client = BasicClient::new(ClientId::new(gate.client_id().to_string()))
            .set_client_secret(ClientSecret::new(client_secret.to_string()))
            // The secret in the request body (`client_secret_post`): the
            // one method every issuer's discovery lists; the header form
            // is not universal.
            .set_auth_type(AuthType::RequestBody)
            .set_auth_uri(AuthUrl::new(endpoints.authorization.clone()).map_err(|e| e.to_string())?)
            .set_token_uri(TokenUrl::new(endpoints.token.clone()).map_err(|e| e.to_string())?)
            .set_redirect_uri(
                RedirectUrl::new(format!("{}/auth/callback", gate.resource()))
                    .map_err(|e| e.to_string())?,
            );
        let http = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Login { gate, client, http })
    }

    pub fn gate(&self) -> &Arc<Gate> {
        &self.gate
    }
}

/// The three routes, outside the gate: a browser arrives here precisely
/// because it holds no token yet.
pub fn router(login: Arc<Login>) -> Router {
    Router::new()
        .route("/auth/login", get(start))
        .route("/auth/callback", get(finish))
        .route("/auth/logout", get(logout))
        .with_state(login)
}

/// Off to the issuer. `next` is where to come back to — a path on this
/// server and nothing else, so the login cannot be used to send a
/// person elsewhere.
async fn start(State(login): State<Arc<Login>>, Query(q): Query<Next>) -> Response {
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let (url, state) = login
        .client
        .authorize_url(CsrfToken::new_random)
        // RFC 8707: the token is asked for this resource. An issuer
        // that does not read the parameter mints `aud: []`, and the gate
        // binds that token by the application instead.
        .add_extra_param("resource", login.gate.resource())
        .set_pkce_challenge(challenge)
        .url();
    let pending = Pending {
        state: state.secret().clone(),
        verifier: verifier.secret().clone(),
        next: local_path(q.next.as_deref()),
    };
    let sealed = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&pending).expect("plain fields"));
    let mut response = see_other(url.as_str());
    response.headers_mut().insert(
        header::SET_COOKIE,
        set_cookie(
            PENDING,
            &sealed,
            "/auth",
            Some(PENDING_SECONDS),
            login.gate.secure(),
        ),
    );
    response
}

/// Back from the issuer with a code, or with its refusal.
async fn finish(
    State(login): State<Arc<Login>>,
    Query(back): Query<Callback>,
    headers: HeaderMap,
) -> Response {
    let Some(pending) = cookie_named(&headers, PENDING)
        .and_then(|sealed| URL_SAFE_NO_PAD.decode(sealed).ok())
        .and_then(|bytes| serde_json::from_slice::<Pending>(&bytes).ok())
    else {
        return plain(
            StatusCode::BAD_REQUEST,
            "no sign-in in progress — start again at /auth/login",
        );
    };
    if let Some(error) = back.error {
        return plain(
            StatusCode::BAD_REQUEST,
            &format!(
                "the issuer refused the sign-in: {error}{}",
                back.error_description
                    .map(|d| format!(" — {d}"))
                    .unwrap_or_default()
            ),
        );
    }
    if back.state.as_deref() != Some(pending.state.as_str()) {
        return plain(
            StatusCode::BAD_REQUEST,
            "the sign-in that came back is not the one that was started — start again at /auth/login",
        );
    }
    let Some(code) = back.code else {
        return plain(StatusCode::BAD_REQUEST, "the issuer sent no code");
    };
    let token = match login
        .client
        .exchange_code(AuthorizationCode::new(code))
        .set_pkce_verifier(PkceCodeVerifier::new(pending.verifier))
        .add_extra_param("resource", login.gate.resource())
        .request_async(&login.http)
        .await
    {
        Ok(token) => token,
        Err(e) => {
            tracing::warn!(error = %e, "login: the code exchange failed");
            return plain(
                StatusCode::BAD_GATEWAY,
                &format!("the issuer did not exchange the code: {e}"),
            );
        }
    };
    // The same verification every door applies, before the browser is
    // handed anything: a token the doors would refuse is not a login.
    let access = token.access_token().secret();
    if let Err(e) = login.gate.verify(access).await {
        tracing::warn!(error = %e, "login: the issuer's token is refused");
        return plain(
            StatusCode::UNAUTHORIZED,
            &format!("the issuer's token does not open this server: {e}"),
        );
    }
    let mut response = see_other(&pending.next);
    let headers = response.headers_mut();
    headers.append(
        header::SET_COOKIE,
        set_cookie(
            COOKIE,
            access,
            "/",
            token.expires_in().map(|d| d.as_secs()),
            login.gate.secure(),
        ),
    );
    headers.append(
        header::SET_COOKIE,
        set_cookie(PENDING, "", "/auth", Some(0), login.gate.secure()),
    );
    response
}

/// The cookie goes; the issuer's own session is the issuer's.
async fn logout(State(login): State<Arc<Login>>) -> Response {
    let mut response = see_other("/");
    response.headers_mut().insert(
        header::SET_COOKIE,
        set_cookie(COOKIE, "", "/", Some(0), login.gate.secure()),
    );
    response
}

/// A path on this server: it starts with one slash and no more, so
/// `//evil.example` (a scheme-relative URL) is not a path.
fn local_path(next: Option<&str>) -> String {
    match next {
        Some(p) if p.starts_with('/') && !p.starts_with("//") => p.to_string(),
        _ => "/".to_string(),
    }
}

fn plain(status: StatusCode, text: &str) -> Response {
    (status, format!("{text}\n")).into_response()
}
