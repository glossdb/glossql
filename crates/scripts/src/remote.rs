//! The kernel service over HTTP — glosskernels, wire `v1`: the three
//! model reads as JSON bodies. Matrices travel as nested lists, NaN as
//! null both ways. A refusal comes back as `{"error": …}` under a 4xx
//! and is reported by its text; a service that does not answer is
//! reported by its address. Nothing here knows what the numbers mean.

use glossql_session::Matrix;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// How long the service has to accept a connection.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The interval of the TCP keepalive on a call in flight.
const KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(30);

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
        // A kernel's answer takes as long as its model does, so the call
        // itself is unbounded; what is bounded is reaching the service,
        // and a peer that has gone away is noticed by the keepalive
        // rather than waited on.
        let client = reqwest::Client::builder()
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
        let url = format!("{}/v1/{route}", self.base);
        let mut request = self.client.post(&url).json(&body);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("the kernel service at {} did not answer: {e}", self.base))?;
        let status = response.status();
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
        Ok(value)
    }

    pub async fn band_point(
        &self,
        train: Matrix<'_>,
        train_y: &[f64],
        test_x: &[f64],
        alphas: &[f64],
        actual: f64,
    ) -> Result<(Vec<f64>, f64), String> {
        #[derive(serde::Deserialize)]
        struct Answer {
            quantiles: Vec<Option<f64>>,
            pit: Option<f64>,
        }
        let answer: Answer = decode(
            self.post(
                "band_point",
                json!({
                    "train_x": rows(train),
                    "train_y": vector(train_y),
                    "test_x": vector(test_x),
                    "alphas": alphas,
                    "actual": actual,
                }),
            )
            .await?,
        )?;
        if answer.quantiles.len() != alphas.len() {
            return Err(format!(
                "the kernel service answered {} quantiles for {} alphas",
                answer.quantiles.len(),
                alphas.len()
            ));
        }
        Ok((floats(answer.quantiles), answer.pit.unwrap_or(f64::NAN)))
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
        let answer: Answer = decode(
            self.post(
                "band_grid",
                json!({
                    "train_x": rows(train),
                    "train_y": vector(train_y),
                    "test_x": rows(test),
                    "alphas": alphas,
                }),
            )
            .await?,
        )?;
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
