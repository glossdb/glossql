//! The kernel service over HTTP — glosskernels: two routes, `/bands`
//! (many reads in one request, each its training rows and the rows to
//! call) and `/misfit`, as JSON bodies. Matrices travel as nested
//! lists, NaN as null both ways. A refusal comes back as `{"error": …}`
//! under a 4xx and is reported by its text; a full queue (429) is
//! waited out for as long as it says, within the call's bound; a
//! service that does not answer is reported by its address. Nothing
//! here knows what the numbers mean.

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

/// A kernel service: where, and the bearer it expects.
pub struct Remote {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("url", &self.base)
            .finish_non_exhaustive()
    }
}

impl Remote {
    /// `url` is the service's address (`GLOSSQL_TABICL_URL`); `token`
    /// the bearer (`GLOSSQL_TABICL_TOKEN`), none on an open service.
    pub fn new(url: &str, token: Option<&str>) -> Result<Self, String> {
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
            token: token
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty()),
        })
    }

    pub fn url(&self) -> &str {
        &self.base
    }

    async fn post(&self, route: &str, body: Value) -> Result<Value, String> {
        let url = format!("{}/{route}", self.base);
        let deadline = std::time::Instant::now() + CALL_TIMEOUT;
        loop {
            let mut request = self.client.post(&url).json(&body);
            if let Some(token) = &self.token {
                request = request.bearer_auth(token);
            }
            let response = request
                .send()
                .await
                .map_err(|e| format!("the kernel service at {} did not answer: {e}", self.base))?;
            let status = response.status();
            // The service's queue is full for this caller: it says how
            // long to wait, and the call waits that long, within its bound.
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let wait = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(1)
                    .clamp(1, 60);
                let wait = std::time::Duration::from_secs(wait);
                if std::time::Instant::now() + wait >= deadline {
                    return Err(format!(
                        "the kernel service's queue stayed full for {}s",
                        CALL_TIMEOUT.as_secs()
                    ));
                }
                tokio::time::sleep(wait).await;
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
