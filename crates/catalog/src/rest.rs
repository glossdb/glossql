//! The same [`Lake`], reached over an Iceberg REST catalog.
//!
//! A REST backend attaches storage on its own side: every table load
//! answers with the storage properties (and, when the connection asks
//! for delegation, the credentials) that table's FileIO needs, so a
//! connection here configures nothing about storage — only where the
//! catalog is, which warehouse, and how to authenticate. What executes
//! those properties is `storage`: the engine's own `object_store`,
//! behind iceberg's `Storage` seam.
//!
//! Authentication is the one piece on top. A static token rides the
//! client's own OAuth2 machinery untouched. Client credentials go
//! through [`ClientCredentials`] instead of the client's built-in
//! manager, because that one exchanges once and caches the token for
//! the process lifetime (its own refresh TODO, iceberg-catalog-rest
//! `auth/oauth2.rs`), while the authorization servers behind the
//! catalogs issue tokens that expire within the hour.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use iceberg::CatalogBuilder as _;
use iceberg::sensitive::SensitiveString;
use iceberg_catalog_rest::{
    AUTH_TYPE_OAUTH2, AuthManager, AuthSession, HttpClient, HttpRequest,
    REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
    RestCatalogBuilder,
};

use crate::{Lake, Result};

mod storage;
pub use storage::S3StorageFactory;

/// A REST catalog connection: where, which warehouse, how to
/// authenticate — and nothing about storage, which is the catalog's.
/// (A dev store that vends nothing gets its keys through
/// `object_store`'s own environment conventions — see `storage`.)
pub struct Connection {
    pub uri: String,
    pub warehouse: String,
    pub auth: Auth,
}

/// How the workspace authenticates to the catalog.
pub enum Auth {
    /// A bearer token used as-is — minted out of band and not expiring
    /// (an object-store platform's API token).
    Token(String),
    /// OAuth2 client credentials — `client_id:client_secret` exchanged
    /// for a bearer token at an authorization server's token endpoint,
    /// and exchanged again when that token nears its expiry.
    ClientCredentials {
        credential: String,
        token_endpoint: String,
        scope: Option<String>,
    },
}

impl Lake {
    /// Connect the workspace data plane to a REST catalog. The
    /// delegation header is always sent: a backend that vends storage
    /// credentials answers each table load with them, and one that
    /// does not ignores it.
    pub async fn connect(connection: Connection) -> Result<Self> {
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), connection.uri),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                connection.warehouse,
            ),
            (
                "header.X-Iceberg-Access-Delegation".to_string(),
                "vended-credentials".to_string(),
            ),
        ]);
        let mut builder =
            RestCatalogBuilder::default().with_storage_factory(Arc::new(S3StorageFactory));
        match connection.auth {
            Auth::Token(token) => {
                // Stated, not left for the client to infer from the
                // token's presence (it warns otherwise).
                props.insert(
                    REST_CATALOG_PROP_AUTH_TYPE.to_string(),
                    AUTH_TYPE_OAUTH2.to_string(),
                );
                props.insert("token".to_string(), token);
            }
            Auth::ClientCredentials {
                credential,
                token_endpoint,
                scope,
            } => {
                builder = builder.with_auth_manager(ClientCredentials::new(
                    &credential,
                    token_endpoint,
                    scope,
                ));
            }
        }
        let catalog = builder.load("glossql", props).await?;
        // Over REST a new table's format version rides the reserved
        // create property — see the field on [`Lake`].
        Ok(Lake::over(Arc::new(catalog), true))
    }
}

/// How long before its stated expiry a token stops being served: the
/// token has to outlive the request it opens, not just its own attach.
const EXPIRY_LEEWAY: Duration = Duration::from_secs(30);

/// A token and when it stops being fresh; `None` when the endpoint
/// stated no `expires_in`, which makes it as good as static.
struct Minted {
    token: SensitiveString,
    stale_at: Option<Instant>,
}

impl Minted {
    fn fresh(&self) -> bool {
        self.stale_at.is_none_or(|at| Instant::now() < at)
    }
}

/// [`AuthManager`] exchanging OAuth2 client credentials for a bearer
/// token and exchanging again once it nears expiry.
///
/// The connection is explicit, so the properties the catalog's
/// `/v1/config` merges back never re-point the exchange — unlike the
/// built-in manager, which recomputes its endpoint from them.
///
/// The minted slot is shared by every session the manager builds and
/// read outside any lock across the exchange, so two requests hitting
/// an expiry together may both exchange; either result is a valid
/// token and the last one wins the slot.
pub struct ClientCredentials {
    client_id: Option<String>,
    client_secret: SensitiveString,
    token_endpoint: String,
    scope: Option<String>,
    minted: Arc<Mutex<Option<Arc<Minted>>>>,
}

impl ClientCredentials {
    /// `credential` is `client_id:client_secret`, or a bare secret.
    pub fn new(credential: &str, token_endpoint: String, scope: Option<String>) -> Self {
        let (client_id, client_secret) = match credential.split_once(':') {
            Some((id, secret)) => (Some(id.to_string()), secret.to_string()),
            None => (None, credential.to_string()),
        };
        ClientCredentials {
            client_id,
            client_secret: client_secret.into(),
            token_endpoint,
            scope,
            minted: Arc::new(Mutex::new(None)),
        }
    }

    fn session(&self, client: &HttpClient) -> Session {
        Session {
            // Stripped of its auth session: `post_form` authenticates
            // through the client it runs on (iceberg-catalog-rest
            // client.rs), and a session exchanging through a client that
            // authenticates through the session would recurse.
            client: client.without_auth_session(),
            manager: ManagerState {
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
                token_endpoint: self.token_endpoint.clone(),
                scope: self.scope.clone(),
                minted: Arc::clone(&self.minted),
            },
        }
    }
}

impl std::fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("token_endpoint", &self.token_endpoint)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthManager for ClientCredentials {
    async fn init_session(
        &self,
        client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Box<dyn AuthSession>> {
        Ok(Box::new(self.session(client)))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(self.session(client)))
    }
}

/// The manager's configuration as a session carries it.
struct ManagerState {
    client_id: Option<String>,
    client_secret: SensitiveString,
    token_endpoint: String,
    scope: Option<String>,
    minted: Arc<Mutex<Option<Arc<Minted>>>>,
}

/// What a token endpoint answers with; further fields exist and are
/// not read.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

/// [`AuthSession`] attaching the freshest exchanged token.
struct Session {
    client: HttpClient,
    manager: ManagerState,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("token_endpoint", &self.manager.token_endpoint)
            .finish_non_exhaustive()
    }
}

impl Session {
    async fn exchange(&self) -> iceberg::Result<Arc<Minted>> {
        let m = &self.manager;
        let mut params = HashMap::from([
            ("grant_type", "client_credentials"),
            ("client_secret", m.client_secret.expose()),
        ]);
        if let Some(id) = &m.client_id {
            params.insert("client_id", id);
        }
        if let Some(scope) = &m.scope {
            params.insert("scope", scope);
        }
        let response = self
            .client
            .post_form(&m.token_endpoint, &Default::default(), &params)
            .await?;
        if response.status() != http::StatusCode::OK {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "the token endpoint refused the credential exchange",
            )
            .with_context("code", response.status().to_string())
            .with_context("url", m.token_endpoint.clone()));
        }
        let token: TokenResponse = serde_json::from_slice(response.body()).map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "the token endpoint's answer is not a token response",
            )
            .with_context("url", m.token_endpoint.clone())
            .with_source(e)
        })?;
        Ok(Arc::new(Minted {
            token: token.access_token.into(),
            stale_at: token
                .expires_in
                .map(|s| Instant::now() + Duration::from_secs(s).saturating_sub(EXPIRY_LEEWAY)),
        }))
    }
}

#[async_trait]
impl AuthSession for Session {
    async fn authenticate(&self, request: &mut HttpRequest) -> iceberg::Result<()> {
        let held = self
            .manager
            .minted
            .lock()
            .expect("minted lock")
            .as_ref()
            .filter(|m| m.fresh())
            .map(Arc::clone);
        let minted = match held {
            Some(minted) => minted,
            None => {
                let minted = self.exchange().await?;
                *self.manager.minted.lock().expect("minted lock") = Some(Arc::clone(&minted));
                minted
            }
        };
        let mut value: http::HeaderValue = format!("Bearer {}", minted.token.expose())
            .parse()
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "the exchanged token does not fit an Authorization header",
                )
                .with_source(e)
            })?;
        value.set_sensitive(true);
        request
            .headers_mut()
            .insert(http::header::AUTHORIZATION, value);
        Ok(())
    }
}
