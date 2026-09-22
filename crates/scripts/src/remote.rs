//! The kernel service over HTTP — glosskernels: two routes, `/bands`
//! (many reads in one request, each its training rows and the rows to
//! call) and `/misfit`, as JSON bodies. Matrices travel as nested
//! lists, NaN as null both ways. A refusal comes back as `{"error": …}`
//! under a 4xx and is reported by its text; a full queue (429) and a
//! stopping instance (503) are waited out for as long as they say, and
//! a connection that drops mid-flight is tried again a few times, all
//! within the call's bound — the reads are pure, so a call sent twice
//! costs only time; a service that does not answer is reported by its
//! address at once. The bearer is a key
//! used as-is, or an ID token the platform mints for the service and
//! this client refreshes before it expires. Nothing here knows what the
//! numbers mean.

use base64::Engine;
use glossql_session::{BandRead, Matrix};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// How long the service has to accept a connection.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long one call may take, whole: the bound the kernel service
/// puts on a request of its own, so the client gives up where the
/// service would have.
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// The interval of the TCP keepalive on a call in flight.
const KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(30);

/// Walk points per `/bands` request: bounds a request's body where a
/// workspace holds thousands of metrics; the service answers each
/// request's reads together.
const READS_PER_REQUEST: usize = 512;

/// The service's ensemble size behind the replay grid read — the
/// package's default, the regime the read was ruled in for.
const GRID_MEMBERS: u32 = 8;

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
    /// A key used as-is (`GLOSSQL_TABICL_TOKEN`).
    Key(String),
    /// A Google-signed ID token for `audience` — the service's URL, which
    /// it verifies — minted by the metadata server at `metadata` for the
    /// service account this process runs as (`GLOSSQL_TABICL_AUDIENCE`).
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

/// A kernel service: where, and the bearer it expects.
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
    /// `url` is the service's address (`GLOSSQL_TABICL_URL`); `bearer`
    /// what rides every call.
    pub fn new(url: &str, bearer: Bearer) -> Result<Self, String> {
        let base = url.trim().trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(format!(
                "GLOSSQL_TABICL_URL is `{url}` — the kernel service's http(s) address"
            ));
        }
        // A kernel's answer takes as long as its model does, up to the
        // service's own bound; reaching the service is bounded apart,
        // and a peer that has gone away is noticed by the keepalive
        // rather than waited out.
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(KEEPALIVE)
            .build()
            .map_err(|e| format!("the kernel service client does not build: {e}"))?;
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
                format!("the metadata server did not mint a token for the kernel service: {e}")
            })?;
        if !response.status().is_success() {
            return Err(format!(
                "the metadata server refused to mint a token for the kernel service ({}) — is a service account attached?",
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

    async fn post(&self, route: &str, body: Value) -> Result<Value, String> {
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
                        "the kernel service {why} for {}s",
                        CALL_TIMEOUT.as_secs()
                    ));
                }
                tokio::time::sleep(wait).await;
                Ok(())
            }
        };
        loop {
            let mut request = self.client.post(&url).json(&body);
            if let Some(token) = self.bearer(false).await? {
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
                        .map_err(|why| format!("{why}: {e}"))?;
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "the kernel service at {} did not answer: {e}",
                        self.base
                    ));
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
                pause(said.unwrap_or(backoff), why).await?;
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
                self.bearer(true).await?;
                continue;
            }
            let text = response
                .text()
                .await
                .map_err(|e| format!("the kernel service's answer did not arrive whole: {e}"))?;
            let value: Value = serde_json::from_str(&text).map_err(|_| {
                format!("the kernel service answered {status} with something that is not JSON")
            })?;
            if !status.is_success() {
                let reason = value
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or(text.as_str());
                return Err(format!("the kernel service refused ({status}): {reason}"));
            }
            return Ok(value);
        }
    }

    /// `/bands`: the reads' answers, one object per read, in order.
    async fn bands(
        &self,
        reads: Vec<Value>,
        alphas: &[f64],
        members: u32,
        pit_history: Option<&[f64]>,
    ) -> Result<Vec<Value>, String> {
        #[derive(serde::Deserialize)]
        struct Answer {
            reads: Vec<Value>,
        }
        let asked = reads.len();
        let mut body = json!({ "reads": reads, "alphas": alphas, "members": members });
        if let Some(history) = pit_history {
            body["pit_history"] = json!(history);
        }
        let answer: Answer = decode(self.post("bands", body).await?)?;
        if answer.reads.len() != asked {
            return Err(format!(
                "the kernel service answered {} reads for {asked}",
                answer.reads.len()
            ));
        }
        Ok(answer.reads)
    }

    /// Walk points, `READS_PER_REQUEST` to a request: each read one
    /// test row with its actual, the pinned member (`members` 1), the
    /// PIT read by the service against its raw grid. With a
    /// `pit_history` the service reads the bands through it and its
    /// default record (the raw bands ride back beside them, unread here).
    pub async fn band_points(
        &self,
        reads: &[BandRead],
        alphas: &[f64],
        pit_history: Option<&[f64]>,
    ) -> Result<Vec<(Vec<f64>, f64)>, String> {
        #[derive(serde::Deserialize)]
        struct Answer {
            quantiles: Vec<Vec<Option<f64>>>,
            pit: Option<Vec<Option<f64>>>,
        }
        let mut out = Vec::with_capacity(reads.len());
        for chunk in reads.chunks(READS_PER_REQUEST) {
            let bodies = chunk
                .iter()
                .map(|r| {
                    let train = Matrix {
                        data: &r.train_x,
                        rows: r.train_y.len(),
                        cols: r.test_x.len(),
                    };
                    json!({
                        "train_x": rows(train),
                        "train_y": vector(&r.train_y),
                        "test_x": [vector(&r.test_x)],
                        "actual": [r.actual],
                    })
                })
                .collect();
            for answered in self.bands(bodies, alphas, 1, pit_history).await? {
                let mut answer: Answer = decode(answered)?;
                if answer.quantiles.len() != 1 || answer.quantiles[0].len() != alphas.len() {
                    return Err(format!(
                        "the kernel service answered {} rows of {} quantiles for one row at {} alphas",
                        answer.quantiles.len(),
                        answer.quantiles.first().map_or(0, Vec::len),
                        alphas.len()
                    ));
                }
                let pit = answer
                    .pit
                    .as_ref()
                    .and_then(|p| p.first().copied().flatten())
                    .unwrap_or(f64::NAN);
                out.push((floats(answer.quantiles.remove(0)), pit));
            }
        }
        Ok(out)
    }

    pub async fn band_grid(
        &self,
        train: Matrix<'_>,
        train_y: &[f64],
        test: Matrix<'_>,
        alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        #[derive(serde::Deserialize)]
        struct Answer {
            quantiles: Vec<Vec<Option<f64>>>,
        }
        let read = json!({
            "train_x": rows(train),
            "train_y": vector(train_y),
            "test_x": rows(test),
        });
        let mut answered = self.bands(vec![read], alphas, GRID_MEMBERS, None).await?;
        let answer: Answer = decode(answered.remove(0))?;
        if answer.quantiles.len() != test.rows
            || answer.quantiles.iter().any(|r| r.len() != alphas.len())
        {
            return Err(format!(
                "the kernel service answered {} rows for {} test rows and {} alphas",
                answer.quantiles.len(),
                test.rows,
                alphas.len()
            ));
        }
        Ok(answer.quantiles.into_iter().flat_map(floats).collect())
    }

    pub async fn misfit(&self, x: Matrix<'_>) -> Result<Vec<f64>, String> {
        #[derive(serde::Deserialize)]
        struct Answer {
            scores: Vec<Option<f64>>,
        }
        let answer: Answer = decode(self.post("misfit", json!({ "x": rows(x) })).await?)?;
        if answer.scores.len() != x.rows {
            return Err(format!(
                "the kernel service answered {} scores for {} rows",
                answer.scores.len(),
                x.rows
            ));
        }
        Ok(floats(answer.scores))
    }
}

/// A row-major matrix as nested lists, non-finite values as null.
fn rows(m: Matrix<'_>) -> Vec<Vec<Value>> {
    m.data.chunks(m.cols.max(1)).map(vector).collect()
}

fn vector(v: &[f64]) -> Vec<Value> {
    v.iter()
        .map(|f| if f.is_finite() { json!(f) } else { Value::Null })
        .collect()
}

/// Nulls back to NaN — the contract the doors read.
fn floats(v: Vec<Option<f64>>) -> Vec<f64> {
    v.into_iter().map(|f| f.unwrap_or(f64::NAN)).collect()
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value)
        .map_err(|e| format!("the kernel service's answer has the wrong shape: {e}"))
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
