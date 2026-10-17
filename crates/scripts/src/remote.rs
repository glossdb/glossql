//! A service point over HTTP: one table and a specification in, one
//! table and an answer out, as Arrow IPC streams — column names and
//! types travel with the rows, the specification as JSON in the
//! schema's metadata under `spec`, the answer object under `answer`.
//! A refusal comes back as JSON under a 4xx — `{"error": …}`, and
//! where the specification is at fault `errors` naming every field to
//! repair — and is reported with its text and its fields; a full queue
//! (429) and a stopping instance (503) are waited out for as long as
//! they say, and a connection that drops mid-flight is tried again a
//! few times, all within the call's bound — the reads are pure, so a
//! call sent twice costs only time; a service that does not answer is
//! reported by its address at once. The bearer is a key used as-is, or
//! an ID token the platform mints for the service and this client
//! refreshes before it expires. Nothing here knows what the numbers
//! mean.

use std::collections::HashMap;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use base64::Engine;
use datafusion::arrow::array::RecordBatch;
use glossql_session::{Answered, RuntimeRefusal};
use serde_json::Value;

/// The media type of an Arrow IPC stream, both ways.
const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// The schema metadata keys the wire uses: the specification on the
/// way in, the answer object on the way out.
const SPEC: &str = "spec";
const ANSWER: &str = "answer";

/// How long the service has to accept a connection.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long one call may take, whole: the bound the service puts on
/// a request of its own, so the client gives up where the service
/// would have.
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// The interval of the TCP keepalive on a call in flight.
const KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(30);

/// The longest wait between tries on a `503` that names none; the
/// waits double from one second up to it.
const BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(30);

/// How many times a call whose connection dropped mid-flight is sent
/// again (the waits doubling from a second) before that is the answer.
const DROPPED_TRIES: u32 = 3;

/// Google's metadata server, which mints an ID token for the attached
/// service account on Cloud Run, GKE and Compute Engine.
pub const GOOGLE_METADATA: &str = "http://metadata.google.internal";

/// How long before an ID token's stated expiry it is minted anew.
const TOKEN_MARGIN: std::time::Duration = std::time::Duration::from_secs(120);

/// What a minted token is good for when it does not say.
const TOKEN_ASSUMED: std::time::Duration = std::time::Duration::from_secs(1800);

/// The bearer on every call to the service.
#[derive(Clone, Debug)]
pub enum Bearer {
    /// An open service (a laptop).
    None,
    /// A key used as-is (`GLOSSQL_<POINT>_TOKEN`).
    Key(String),
    /// A Google-signed ID token for `audience` — the service's URL, which
    /// it verifies — minted by the metadata server at `metadata` for the
    /// service account this process runs as (`GLOSSQL_<POINT>_AUDIENCE`).
    /// No key anywhere; the token is refreshed before it expires.
    Identity { audience: String, metadata: String },
}

impl Bearer {
    /// From the environment: an audience wins over a key; neither is open.
    pub fn from_env(token: Option<&str>, audience: Option<&str>) -> Bearer {
        fn trimmed(v: Option<&str>) -> Option<&str> {
            v.map(str::trim).filter(|v| !v.is_empty())
        }
        match (trimmed(token), trimmed(audience)) {
            (_, Some(audience)) => Bearer::Identity {
                audience: audience.to_string(),
                metadata: GOOGLE_METADATA.to_string(),
            },
            (Some(token), None) => Bearer::Key(token.to_string()),
            (None, None) => Bearer::None,
        }
    }
}

/// A service behind one point: where, and the bearer it expects.
pub struct Remote {
    client: reqwest::Client,
    base: String,
    bearer: Bearer,
    /// The ID token last minted, and when it expires.
    minted: tokio::sync::Mutex<Option<(String, std::time::Instant)>>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.base)
            .finish_non_exhaustive()
    }
}

impl Remote {
    /// `url` is the service's address (`GLOSSQL_<POINT>_URL`); `bearer`
    /// what rides every call.
    pub fn new(url: &str, bearer: Bearer) -> Result<Self, String> {
        let base = url.trim().trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(format!("`{url}` is not a service's http(s) address"));
        }
        // An answer takes as long as the model does, up to the
        // service's own bound; reaching the service is bounded apart,
        // and a peer that has gone away is noticed by the keepalive
        // rather than waited out.
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(KEEPALIVE)
            .build()
            .map_err(|e| format!("the service client does not build: {e}"))?;
        Ok(Remote {
            client,
            base,
            bearer,
            minted: tokio::sync::Mutex::new(None),
        })
    }

    pub fn url(&self) -> &str {
        &self.base
    }

    /// The bearer for the next call: the key, or an ID token minted when
    /// none is kept or the kept one is near its expiry. `fresh` drops
    /// the kept token first (the service refused it).
    async fn bearer(&self, fresh: bool) -> Result<Option<String>, String> {
        let Bearer::Identity { audience, metadata } = &self.bearer else {
            return Ok(match &self.bearer {
                Bearer::Key(key) => Some(key.clone()),
                _ => None,
            });
        };
        let mut kept = self.minted.lock().await;
        if fresh {
            *kept = None;
        }
        if let Some((token, until)) = &*kept
            && std::time::Instant::now() + TOKEN_MARGIN < *until
        {
            return Ok(Some(token.clone()));
        }
        let url =
            format!("{metadata}/computeMetadata/v1/instance/service-accounts/default/identity");
        let response = self
            .client
            .get(&url)
            .query(&[("audience", audience.as_str()), ("format", "full")])
            .header("Metadata-Flavor", "Google")
            .timeout(CONNECT_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                format!("the metadata server did not mint a token for the service: {e}")
            })?;
        if !response.status().is_success() {
            return Err(format!(
                "the metadata server refused to mint a token for the service ({}) — is a service account attached?",
                response.status()
            ));
        }
        let token = response
            .text()
            .await
            .map_err(|e| format!("the metadata server's token did not arrive whole: {e}"))?
            .trim()
            .to_string();
        let until = std::time::Instant::now() + expires_in(&token).unwrap_or(TOKEN_ASSUMED);
        *kept = Some((token.clone(), until));
        Ok(Some(token))
    }

    /// The table call on `route`: the table with `spec` in its schema's
    /// metadata, as one IPC stream; the answer's rows as one batch and
    /// its answer object. A refusal is reported with the service's text
    /// and the fields it names.
    pub async fn table(
        &self,
        route: &str,
        table: RecordBatch,
        spec: Value,
    ) -> Result<Answered, RuntimeRefusal> {
        let body = encode(table, &spec).map_err(|e| {
            RuntimeRefusal::Unanswered(format!("the {route} table does not encode: {e}"))
        })?;
        let bytes = self.post(route, body).await?;
        decode(&bytes).map_err(|e| {
            RuntimeRefusal::Unanswered(format!("the {route} service's answer does not read: {e}"))
        })
    }

    async fn post(&self, route: &str, body: Vec<u8>) -> Result<bytes::Bytes, RuntimeRefusal> {
        let url = format!("{}/{route}", self.base);
        let deadline = std::time::Instant::now() + CALL_TIMEOUT;
        let mut backoff = std::time::Duration::from_secs(1);
        let mut reminted = false;
        let mut dropped = 0;
        // Wait `wait` within the call's bound, or say why the call is over.
        let pause = |wait: std::time::Duration, why: &str| {
            let why = why.to_string();
            async move {
                if std::time::Instant::now() + wait >= deadline {
                    return Err(format!(
                        "the {route} service {why} for {}s",
                        CALL_TIMEOUT.as_secs()
                    ));
                }
                tokio::time::sleep(wait).await;
                Ok(())
            }
        };
        loop {
            let mut request = self
                .client
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, ARROW_STREAM)
                .header(reqwest::header::ACCEPT, ARROW_STREAM)
                .body(body.clone());
            if let Some(token) = self
                .bearer(false)
                .await
                .map_err(RuntimeRefusal::Unanswered)?
            {
                request = request.bearer_auth(token);
            }
            let response = match request.send().await {
                Ok(response) => response,
                // A connection that dropped mid-flight (an instance taken
                // away under the call): sent again, a few times. A service
                // that cannot be reached at all, or a call that ran out its
                // bound, is the answer.
                Err(e)
                    if !e.is_connect()
                        && !e.is_timeout()
                        && !e.is_builder()
                        && dropped < DROPPED_TRIES =>
                {
                    dropped += 1;
                    pause(backoff, "kept dropping the connection")
                        .await
                        .map_err(|why| RuntimeRefusal::Unanswered(format!("{why}: {e}")))?;
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                    continue;
                }
                Err(e) => {
                    return Err(RuntimeRefusal::Unanswered(format!(
                        "the {route} service at {} did not answer: {e}",
                        self.base
                    )));
                }
            };
            let status = response.status();
            // The service's queue is full for this caller, or the instance
            // is stopping: it says how long to wait, and the call waits
            // that long, within its bound.
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                let said = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(|s| std::time::Duration::from_secs(s.clamp(1, 60)));
                let why = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    "'s queue stayed full"
                } else {
                    " kept stopping"
                };
                pause(said.unwrap_or(backoff), why)
                    .await
                    .map_err(RuntimeRefusal::Unanswered)?;
                if said.is_none() {
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
                continue;
            }
            // A minted token the service no longer takes is minted anew, once.
            if status == reqwest::StatusCode::UNAUTHORIZED
                && matches!(self.bearer, Bearer::Identity { .. })
                && !reminted
            {
                reminted = true;
                self.bearer(true)
                    .await
                    .map_err(RuntimeRefusal::Unanswered)?;
                continue;
            }
            let bytes = response.bytes().await.map_err(|e| {
                RuntimeRefusal::Unanswered(format!(
                    "the {route} service's answer did not arrive whole: {e}"
                ))
            })?;
            if !status.is_success() {
                return Err(RuntimeRefusal::Refused(format!(
                    "the {route} service refused ({status}): {}",
                    refusal(&bytes)
                )));
            }
            return Ok(bytes);
        }
    }
}

/// The table as one IPC stream, `spec` in the schema's metadata.
fn encode(table: RecordBatch, spec: &Value) -> Result<Vec<u8>, String> {
    let mut metadata = table.schema().metadata().clone();
    metadata.insert(SPEC.to_string(), spec.to_string());
    let schema = table.schema().as_ref().clone().with_metadata(metadata);
    let table = table
        .with_schema(std::sync::Arc::new(schema.clone()))
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
    writer.write(&table).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    Ok(out)
}

/// The answer's IPC stream as one batch and its answer object. A
/// stream of no batches is the empty table in the stream's schema.
fn decode(bytes: &[u8]) -> Result<Answered, String> {
    let reader =
        StreamReader::try_new(std::io::Cursor::new(bytes), None).map_err(|e| e.to_string())?;
    let schema = reader.schema();
    let answer: Value = match schema.metadata().get(ANSWER) {
        Some(text) => {
            serde_json::from_str(text).map_err(|e| format!("`answer` is not JSON: {e}"))?
        }
        None => return Err("the schema's metadata holds no `answer`".into()),
    };
    let batches = reader
        .collect::<Result<Vec<RecordBatch>, _>>()
        .map_err(|e| e.to_string())?;
    let bare = std::sync::Arc::new(schema.as_ref().clone().with_metadata(HashMap::new()));
    let table = if batches.is_empty() {
        RecordBatch::new_empty(bare)
    } else {
        datafusion::arrow::compute::concat_batches(&bare, &batches).map_err(|e| e.to_string())?
    };
    Ok(Answered { table, answer })
}

/// A refusal's text: the service's `error`, and the fields its `errors`
/// name, each with its own text; the raw body where it is not the
/// JSON the contract promises.
fn refusal(bytes: &[u8]) -> String {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return String::from_utf8_lossy(bytes).into_owned();
    };
    let mut said = value["error"]
        .as_str()
        .map_or_else(|| value.to_string(), str::to_string);
    let fields: Vec<String> = value["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|e| match (e["field"].as_str(), e["error"].as_str()) {
            (Some(field), Some(error)) => format!("{field}: {error}"),
            _ => e.to_string(),
        })
        .collect();
    if !fields.is_empty() {
        said.push_str(" — ");
        said.push_str(&fields.join("; "));
    }
    said
}

/// How long a JWT says it is good for, from its `exp` claim — read
/// without verifying (the service verifies; here it only times the refresh).
fn expires_in(token: &str) -> Option<std::time::Duration> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_u64()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some(std::time::Duration::from_secs(exp.saturating_sub(now)))
}
