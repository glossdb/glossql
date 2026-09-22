//! The kernel runtime behind [`FunctionRuntime`] (SPEC.md §6). Function
//! bodies are SQL the engine plans — a measurement over data, a detector
//! over its witness's `slots` — so nothing here evaluates a body. What
//! remains is what SQL cannot express: the statistical kernels in Rust,
//! behind the engine's aggregate registrations and the runtime's typed
//! methods — and the three model reads, which the server never computes
//! itself. They go to the kernel service ([`remote`]) when the
//! environment names one, and refuse by name when none is.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

pub mod library;
mod remote;
mod statistics;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::compute::{CastOptions, cast_with_options, partition};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use glossql_session::{BandRead, FunctionRuntime, Matrix, PIT_BINS};
use serde_json::{Value, json};

pub use remote::Remote;

/// The native kernels, and — when the server names one — the kernel
/// service behind the three model reads.
pub struct KernelRuntime {
    remote: Option<Remote>,
}

impl std::fmt::Debug for KernelRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KernelRuntime")
            .field("kernel", &self.remote.as_ref().map(Remote::url))
            .finish()
    }
}

type ScriptResult<T> = Result<T, String>;

fn fail<T>(message: impl Into<String>) -> ScriptResult<T> {
    Err(message.into())
}

impl KernelRuntime {
    /// The native kernels alone — the profile aggregates and reconcile.
    /// The three model reads refuse by name.
    pub fn native() -> Self {
        KernelRuntime { remote: None }
    }

    /// The native kernels, and the model reads served by the kernel
    /// service at `url` (`GLOSSQL_TABICL_URL`), `token` as the bearer on
    /// every call.
    pub fn with_remote(url: &str, token: Option<&str>) -> Result<Self, String> {
        Ok(KernelRuntime {
            remote: Some(Remote::new(url, token)?),
        })
    }

    /// Where the model reads go, when they go anywhere.
    pub fn kernel_url(&self) -> Option<&str> {
        self.remote.as_ref().map(Remote::url)
    }

    fn model(&self) -> Result<&Remote, String> {
        self.remote
            .as_ref()
            .ok_or_else(|| "this runtime carries no model".to_string())
    }
}

#[glossql_session::async_trait]
impl FunctionRuntime for KernelRuntime {
    fn carries_model(&self) -> bool {
        self.remote.is_some()
    }

    /// The shipped statistics (`profile`; `mad` and `entropy` ride
    /// inside its struct), registered when the runtime attaches — a
    /// measurement body and an agent's own SQL name the same
    /// aggregates.
    fn udafs(&self) -> Vec<datafusion::logical_expr::AggregateUDF> {
        statistics::udafs()
    }

    /// The `whatif.` door's kernel: the regressor ensemble over the
    /// replayed worlds — a replay grid is a handful of worlds, the
    /// sparse-support regime the ensemble was ruled in for. The shape
    /// is checked here; the service checks that something varies.
    async fn band_grid(
        &self,
        train: Matrix<'_>,
        train_y: &[f64],
        test: Matrix<'_>,
        alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        let Matrix {
            data: train_x,
            rows,
            cols,
        } = train;
        let Matrix {
            data: test_x,
            rows: test_rows,
            cols: test_cols,
        } = test;
        if rows < 2
            || train_x.len() != rows * cols
            || test_cols != cols
            || test_x.len() != test_rows * cols
        {
            return Err(format!(
                "band_grid: {rows} train rows x {cols} features against {} train values \
                 and {} test values",
                train_y.len(),
                test_x.len()
            ));
        }
        self.model()?.band_grid(train, train_y, test, alphas).await
    }

    /// The behavior-evidence door's kernel: the stock/flow
    /// discriminator, over batches — native, never remote.
    fn reconcile(
        &self,
        aligned: &[RecordBatch],
        n_common: i64,
        terms: &[String],
    ) -> Result<Value, String> {
        reconcile_kernel(aligned, n_common, terms.to_vec())
    }

    /// The walk's points together: shapes checked here, one request
    /// to the service per chunk of them.
    async fn band_points(
        &self,
        reads: &[BandRead],
        alphas: &[f64],
        pit_history: Option<&[f64]>,
    ) -> Result<Vec<(Vec<f64>, f64)>, String> {
        if let Some(history) = pit_history
            && (history.len() != PIT_BINS || history.iter().any(|c| c.is_nan() || *c < 0.0))
        {
            return Err(format!(
                "band_points: a PIT history is {PIT_BINS} non-negative counts, got {}",
                history.len()
            ));
        }
        for (i, read) in reads.iter().enumerate() {
            let (rows, cols) = (read.train_y.len(), read.test_x.len());
            if rows < 2 || read.train_x.len() != rows * cols {
                return Err(format!(
                    "band_points: read {i}: {rows} rows x {cols} features against {} values",
                    read.train_x.len()
                ));
            }
        }
        self.model()?.band_points(reads, alphas, pit_history).await
    }

    /// The `misfit.` door's kernel (fixture 20): the chain-rule density
    /// read, fit on the frame and scored on the same frame, log space
    /// end to end — the service runs the two orderings.
    async fn misfit_scores(&self, x: Matrix<'_>) -> Result<Vec<f64>, String> {
        let Matrix { data, rows, cols } = x;
        if rows < 2 || cols < 2 || data.len() != rows * cols {
            return Err(format!(
                "misfit_scores: {rows} rows x {cols} features against {} values",
                data.len()
            ));
        }
        self.model()?.misfit(x).await
    }
}

/// What the float kernels may read as numbers: numeric types themselves,
/// booleans, and strings (the safe-cast reading on a raw column). Temporal
/// columns are deliberately out — a date has an order but no mean.
fn numeric_like(dt: &DataType) -> bool {
    dt.is_numeric()
        || matches!(
            dt,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Null
        )
}

/// A column's smallest or largest value under the scripts' type rules.
pub(crate) enum Extremum {
    Num(f64),
    Text(String),
}

/// min/max with the scripts' type rules: strings compare as strings,
/// numeric-readable columns as floats, everything else by its display
/// form — ISO spellings sort chronologically, so min/max stay truthful.
pub(crate) fn extremum_of(a: &ArrayRef, min: bool) -> ScriptResult<Option<Extremum>> {
    if let Some(values) = a.as_any().downcast_ref::<StringArray>() {
        let v = if min {
            aggregate::min_string(values)
        } else {
            aggregate::max_string(values)
        };
        return Ok(v.map(|s| Extremum::Text(s.to_string())));
    }
    if numeric_like(a.data_type()) {
        let floats = as_floats(a)?;
        let v = if min {
            aggregate::min(&floats)
        } else {
            aggregate::max(&floats)
        };
        return Ok(v.map(Extremum::Num));
    }
    let mut best: Option<String> = None;
    for i in 0..a.len() {
        if a.is_null(i) {
            continue;
        }
        let value = array_value_to_string(a, i).map_err(|e| e.to_string())?;
        best = Some(match best {
            None => value,
            Some(b) => {
                if (value < b) == min {
                    value
                } else {
                    b
                }
            }
        });
    }
    Ok(best.map(Extremum::Text))
}

/// Non-null values counted by display form — the reading a human would
/// count. One pass serves both the distinct count (the map's size) and
/// the top-k display buckets.
pub(crate) fn display_counts(a: &ArrayRef) -> ScriptResult<HashMap<String, i64>> {
    let mut counts: HashMap<String, i64> = HashMap::new();
    for i in 0..a.len() {
        if a.is_null(i) {
            continue;
        }
        let value = array_value_to_string(a, i).map_err(|e| e.to_string())?;
        *counts.entry(value).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Shannon entropy (nats) of the non-null value distribution, exact —
/// one pass over typed cell keys, never display buckets. `top_k` stays
/// a display cap; this scalar is what a score may read (a display
/// cap must not become a statistics cap).
pub(crate) fn entropy_of(a: &ArrayRef) -> ScriptResult<f64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for key in cell_keys(a)?.into_iter().flatten() {
        *counts.entry(key).or_insert(0) += 1;
    }
    let n: i64 = counts.values().sum();
    if n == 0 {
        return Ok(0.0);
    }
    let n = n as f64;
    Ok(counts
        .values()
        .map(|&count| {
            let p = count as f64 / n;
            -p * p.ln()
        })
        .sum())
}

/// The k most frequent display values, count descending then value
/// ascending — deterministic buckets for a judge to read.
pub(crate) fn top_k(counts: &HashMap<String, i64>, k: usize) -> Vec<(String, i64)> {
    let mut pairs: Vec<(String, i64)> = counts.iter().map(|(v, c)| (v.clone(), *c)).collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    pairs.truncate(k);
    pairs
}

/// (min, max, avg) of string lengths in characters; `None` off strings
/// or when nothing is there.
pub(crate) fn len_stats_of(a: &ArrayRef) -> Option<(i64, i64, f64)> {
    let values = a.as_any().downcast_ref::<StringArray>()?;
    let (mut min, mut max, mut total, mut n) = (i64::MAX, 0i64, 0i64, 0i64);
    for i in 0..values.len() {
        if values.is_null(i) {
            continue;
        }
        let len = values.value(i).chars().count() as i64;
        min = min.min(len);
        max = max.max(len);
        total += len;
        n += 1;
    }
    (n > 0).then(|| (min, max, total as f64 / n as f64))
}

pub(crate) fn mean_of(v: &[f64]) -> Option<f64> {
    (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
}

/// Sample standard deviation, matching SQL STDDEV.
pub(crate) fn stddev_of(v: &[f64]) -> Option<f64> {
    if v.len() < 2 {
        return None;
    }
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (v.len() as f64 - 1.0);
    Some(var.sqrt())
}

/// Median absolute deviation — the robust spread the modified Z-score
/// fences ride on.
pub(crate) fn mad_of(v: &[f64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    let median = interpolate(&v, 0.5);
    let mut deviations: Vec<f64> = v.iter().map(|x| (x - median).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    Some(interpolate(&deviations, 0.5))
}

/// The parseable values as floats — safe-cast semantics, so on a raw
/// VARCHAR column this is "every value that reads as a number". Empty for
/// column types with no numeric reading (dates, timestamps): the kernels
/// answer UNIT there instead of arithmetic on an epoch encoding.
fn valid_floats(array: &ArrayRef) -> ScriptResult<Vec<f64>> {
    if !numeric_like(array.data_type()) {
        return Ok(Vec::new());
    }
    let floats = as_floats(array)?;
    Ok((0..floats.len())
        .filter(|&i| !floats.is_null(i))
        .map(|i| floats.value(i))
        .collect())
}

/// PERCENTILE_CONT over an already-sorted slice.
fn interpolate(sorted: &[f64], p: f64) -> f64 {
    let rank = p * (sorted.len() - 1) as f64;
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    sorted[low] + (sorted[high] - sorted[low]) * (rank - low as f64)
}

fn as_floats(array: &ArrayRef) -> ScriptResult<Float64Array> {
    let cast = cast_with_options(
        array,
        &DataType::Float64,
        &CastOptions {
            safe: true,
            ..Default::default()
        },
    )
    .map_err(|e| e.to_string())?;
    Ok(cast
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to Float64 yields Float64")
        .clone())
}

// ---- statistical kernels ---------------------------------------------
//
// The compute-heavy halves of the measurement scripts, in Rust where
// they belong (the crate contract above: scripts orchestrate, they
// never iterate rows). Two families:
//
// - Key vectors: the SPIDER/SINDY substrate for inclusion-dependency
//   discovery (Bauckmann et al. 2006; Kruse et al. 2015) — a column's
//   distinct values as one sorted `Vec<u64>`, containment between two
//   columns as a linear merge. Exact while Σ distinct fits memory; the
//   named ladder past that is BINDER-style hash-range partitioning
//   (Papenbrock et al., VLDB 2015) and bottom-k/KMV sketches (Bar-Yossef
//   et al. 2002; Beyer et al. 2007) — not built until a dataset needs
//   them.
// - `reconcile`: the stock/flow discriminator (its constants sit
//   with the arithmetic they govern) — convention
//   evaluation as one matrix product over stacked entity series, then
//   segmented L1 residual reductions.

/// FNV-1a over bytes — fixed keys, so runs reproduce; std's hasher
/// randomizes per process and would break the determinism discipline.
fn fnv1a(init: u64, bytes: &[u8]) -> u64 {
    let mut h = init;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// Per-row u64 key for a column, `None` for NULL. Equality-faithful
/// within one dtype (the scripts gate pairs by dtype): integer-like
/// values keep their identity, byte-backed values hash deterministically.
/// No display strings, no per-value allocation.
fn cell_keys(array: &ArrayRef) -> ScriptResult<Vec<Option<u64>>> {
    use DataType::*;
    let keys = match array.data_type() {
        Int8
        | Int16
        | Int32
        | Int64
        | Date32
        | Date64
        | Timestamp(_, _)
        | Time32(_)
        | Time64(_)
        | Duration(_) => {
            let cast = cast_with_options(
                array,
                &Int64,
                &CastOptions {
                    safe: true,
                    ..Default::default()
                },
            )
            .map_err(|e| e.to_string())?;
            let a = cast
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cast to Int64 yields Int64");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i) as u64))
                .collect()
        }
        UInt8 | UInt16 | UInt32 | UInt64 => {
            let cast = cast_with_options(
                array,
                &UInt64,
                &CastOptions {
                    safe: true,
                    ..Default::default()
                },
            )
            .map_err(|e| e.to_string())?;
            let a = cast
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("cast to UInt64 yields UInt64");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i)))
                .collect()
        }
        Float32 | Float64 => {
            let a = as_floats(array)?;
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i).to_bits()))
                .collect()
        }
        Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean downcasts");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| u64::from(a.value(i))))
                .collect()
        }
        Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Decimal128 downcasts");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| fnv1a(FNV_SEED, &a.value(i).to_le_bytes())))
                .collect()
        }
        Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 downcasts");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| fnv1a(FNV_SEED, a.value(i).as_bytes())))
                .collect()
        }
        LargeUtf8 => {
            let a = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeUtf8 downcasts");
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| fnv1a(FNV_SEED, a.value(i).as_bytes())))
                .collect()
        }
        // Anything exotic falls back to the display form — correctness
        // over speed for types no measurement has met yet.
        _ => {
            let mut out = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                if array.is_null(i) {
                    out.push(None);
                } else {
                    let s = array_value_to_string(array, i).map_err(|e| e.to_string())?;
                    out.push(Some(fnv1a(FNV_SEED, s.as_bytes())));
                }
            }
            out
        }
    };
    Ok(keys)
}

/// A named column across the batches, concatenated once.
fn column_of(t: &[RecordBatch], name: &str) -> ScriptResult<ArrayRef> {
    let Some(first) = t.first() else {
        return fail(format!("no rows carry a column `{name}`"));
    };
    let Some((index, _)) = first.schema().column_with_name(name) else {
        return fail(format!("no column `{name}` in the result"));
    };
    if t.len() == 1 {
        return Ok(Arc::clone(first.column(index)));
    }
    let arrays: Vec<&dyn Array> = t.iter().map(|b| b.column(index).as_ref()).collect();
    datafusion::arrow::compute::concat(&arrays).map_err(|e| e.to_string())
}

// ---- the reconcile kernel: the stock/flow discriminator -------------
//
// The gates. An entity votes when the winning residual is under
// FIRE_RESIDUAL_MAX and the loser stands far enough from it to make
// the vote a choice rather than a tie. The residual gate is the null
// model: for two unrelated positive series of similar size the flow
// residual Σ|y − m| / Σ|m| sits near 0.4 and the delta residual of the
// same pair near 1.0, so a gate above 0.4 hands `flow` to any positive
// column of the movement's size. A true reconciliation sits under
// 0.01; 0.05 leaves room for dirt. The separation gate is
// (loser − winner) / (loser + winner) at one third.
const MIN_PERIODS: usize = 4;
const FIRE_RESIDUAL_MAX: f64 = 0.05;
const MIN_SEPARATION: f64 = 1.0 / 3.0;
const MIN_ENTITIES_FIRED: usize = 2;
const AGREEMENT_MIN: f64 = 0.8;

/// One entity's vote: `None` = abstained. A measure that never moves
/// is a dead value and says nothing about what a movement would do to
/// it; a movement that is all zero has no denominator, while a
/// constant nonzero one is a movement. A wrong anchor leaves both
/// residuals large; a near-tie converts the last significant digit
/// into no verdict at all.
fn classify_series(y: &[f64], m: &[f64]) -> (Option<bool>, f64, f64) {
    const INF: f64 = f64::INFINITY;
    if y.len() < MIN_PERIODS
        || y.iter().all(|v| *v == y[0])
        || !m.iter().any(|v| *v != 0.0)
        // A NaN anywhere in the series abstains: every comparison against
        // it is false, so it would slip past the residual gate and the
        // separation gate alike, and land NaN in the voters (a real column
        // reaches here as NaN through a float source).
        || y.iter().chain(m).any(|v| v.is_nan())
    {
        return (None, INF, INF);
    }
    let denom_flow: f64 = m.iter().map(|v| v.abs()).sum();
    let r_flow = if denom_flow > 0.0 {
        y.iter().zip(m).map(|(a, b)| (a - b).abs()).sum::<f64>() / denom_flow
    } else {
        INF
    };
    let denom_stock: f64 = m[1..].iter().map(|v| v.abs()).sum();
    let r_stock = if denom_stock > 0.0 {
        (1..y.len())
            .map(|t| ((y[t] - y[t - 1]) - m[t]).abs())
            .sum::<f64>()
            / denom_stock
    } else {
        INF
    };
    if r_flow.min(r_stock) > FIRE_RESIDUAL_MAX {
        return (None, r_flow, r_stock);
    }
    let (rw, rl) = if r_flow < r_stock {
        (r_flow, r_stock)
    } else {
        (r_stock, r_flow)
    };
    let sep = if rw.is_infinite() {
        0.0
    } else if rl.is_infinite() {
        1.0
    } else if rw + rl == 0.0 {
        0.0
    } else {
        (rl - rw) / (rl + rw)
    };
    if sep < MIN_SEPARATION {
        return (None, r_flow, r_stock);
    }
    (Some(r_stock < r_flow), r_flow, r_stock)
}

fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    })
}

/// The discriminator over two grouped query results (both
/// `ORDER BY e, b`): y rows `(e, b, yv)`, m rows `(e, b, s_<term>…)`.
/// Alignment is a hash join on typed cell keys; conventions (each term,
/// and every ordered pair difference) evaluate as one matrix product
/// over the stacked entity series; residuals reduce per (entity,
/// convention) under the gates above. Returns per-convention
/// summaries; support policy (Wilson, winner, alternatives) stays in
/// the door.
fn reconcile_kernel(
    aligned: &[RecordBatch],
    n_common: i64,
    terms: Vec<String>,
) -> ScriptResult<Value> {
    let k = terms.len();
    if k == 0 {
        return fail("reconcile needs at least one movement term");
    }
    if k > 64 {
        return fail("more than 64 movement terms — the validity mask is a u64");
    }
    let entity = column_of(aligned, "e")?;
    let yv = as_floats(&column_of(aligned, "yv")?)?;
    let mut mcols = Vec::with_capacity(k);
    for t in &terms {
        mcols.push(as_floats(&column_of(aligned, &format!("s_{t}"))?)?);
    }

    // Contiguous entity segments. The two sides arrive aligned and in
    // entity order — the pairing is the door's join, and cells missing
    // on either side never reach here — so a segment is a run of equal
    // keys, which arrow's own partition finds by comparing the values
    // rather than a hash of them.
    let ranges = if entity.is_empty() {
        Vec::new()
    } else {
        partition(std::slice::from_ref(&entity))
            .map_err(|e| e.to_string())?
            .ranges()
    };

    // Stack the cells: M (cells × k, NULL as 0.0 with a validity bit)
    // and the y vector, segment bounds kept.
    let mut mmat: Vec<f64> = Vec::with_capacity(entity.len() * k);
    let mut valid: Vec<u64> = Vec::with_capacity(entity.len());
    let mut yvec: Vec<f64> = Vec::with_capacity(entity.len());
    let mut bounds = Vec::with_capacity(ranges.len());
    for r in ranges {
        let start = yvec.len();
        for row in r {
            if yv.is_null(row) {
                continue;
            }
            yvec.push(yv.value(row));
            let mut bits = 0u64;
            for (t, col) in mcols.iter().enumerate() {
                if col.is_null(row) {
                    mmat.push(0.0);
                } else {
                    mmat.push(col.value(row));
                    bits |= 1u64 << t;
                }
            }
            valid.push(bits);
        }
        bounds.push((start, yvec.len() - start));
    }
    let ncells = yvec.len();

    // Conventions: each term, then every ordered pair difference,
    // evaluated as M · W in one product.
    let mut conv_terms: Vec<(usize, Option<usize>)> = Vec::new();
    let mut conv_names: Vec<String> = Vec::new();
    for (i, t) in terms.iter().enumerate() {
        conv_terms.push((i, None));
        conv_names.push(t.clone());
    }
    for i1 in 0..k {
        for i2 in 0..k {
            if i1 != i2 {
                conv_terms.push((i1, Some(i2)));
                conv_names.push(format!("{} - {}", terms[i1], terms[i2]));
            }
        }
    }
    let cc = conv_terms.len();
    let mmatf = faer::Mat::from_fn(ncells, k, |i, j| mmat[i * k + j]);
    let w = faer::Mat::from_fn(k, cc, |i, j| {
        let (t1, t2) = conv_terms[j];
        if i == t1 {
            1.0
        } else if Some(i) == t2 {
            -1.0
        } else {
            0.0
        }
    });
    let mw = &mmatf * &w;

    let mut summaries = Vec::with_capacity(cc);
    let mut ys_buf: Vec<f64> = Vec::new();
    let mut ms_buf: Vec<f64> = Vec::new();
    for (c, &(t1, t2)) in conv_terms.iter().enumerate() {
        let mask = (1u64 << t1) | t2.map_or(0, |t| 1u64 << t);
        let mut flow_votes = 0usize;
        let mut stock_votes = 0usize;
        let mut rf_flow = Vec::new();
        let mut rs_flow = Vec::new();
        let mut rf_stock = Vec::new();
        let mut rs_stock = Vec::new();
        for &(start, len) in &bounds {
            ys_buf.clear();
            ms_buf.clear();
            for cell in start..start + len {
                if valid[cell] & mask == mask {
                    ys_buf.push(yvec[cell]);
                    ms_buf.push(mw[(cell, c)]);
                }
            }
            let (label, rf, rs) = classify_series(&ys_buf, &ms_buf);
            match label {
                Some(true) => {
                    stock_votes += 1;
                    rf_stock.push(rf);
                    rs_stock.push(rs);
                }
                Some(false) => {
                    flow_votes += 1;
                    rf_flow.push(rf);
                    rs_flow.push(rs);
                }
                None => {}
            }
        }
        let voted = flow_votes + stock_votes;
        let stock_wins = stock_votes > flow_votes;
        let winners = if stock_wins { stock_votes } else { flow_votes };
        let agreement = if voted > 0 {
            winners as f64 / voted as f64
        } else {
            0.0
        };
        let verdict = if voted >= MIN_ENTITIES_FIRED && agreement >= AGREEMENT_MIN {
            if stock_wins { "stock" } else { "flow" }
        } else {
            "abstain"
        };
        let mut s = serde_json::Map::new();
        s.insert("convention".into(), json!(conv_names[c]));
        s.insert(
            "terms".into(),
            json!(if t2.is_some() { 2i64 } else { 1i64 }),
        );
        s.insert("verdict".into(), json!(verdict));
        s.insert("voted".into(), json!(voted as i64));
        s.insert("winners".into(), json!(winners as i64));
        s.insert("agreement".into(), json!(agreement));
        if verdict != "abstain" {
            // Medians over the winning-label voters only — a dissenting
            // minority's residuals would contaminate the diagnostics.
            let (rf, rs) = if stock_wins {
                (rf_stock.clone(), rs_stock.clone())
            } else {
                (rf_flow.clone(), rs_flow.clone())
            };
            if let Some(v) = median(rf) {
                s.insert("r_flow".into(), json!(v));
            }
            if let Some(v) = median(rs) {
                s.insert("r_stock".into(), json!(v));
            }
            // The sign partition: every entity
            // re-classified against the negated anchor. A voter firing
            // the winning pattern only under negation stores the mirror
            // convention — ledger-signed data reads this way. Diagnostic
            // only: selection stays on original-sign support.
            let mut primary = 0i64;
            let mut mirror = 0i64;
            let mut both = 0i64;
            let mut rss = 0.0f64;
            let mut voters = 0usize;
            let mut neg_buf: Vec<f64> = Vec::new();
            for &(start, len) in &bounds {
                ys_buf.clear();
                ms_buf.clear();
                for cell in start..start + len {
                    if valid[cell] & mask == mask {
                        ys_buf.push(yvec[cell]);
                        ms_buf.push(mw[(cell, c)]);
                    }
                }
                let (label, rf, rs) = classify_series(&ys_buf, &ms_buf);
                let fires = label == Some(stock_wins);
                neg_buf.clear();
                neg_buf.extend(ms_buf.iter().map(|v| -v));
                let (mlabel, _, _) = classify_series(&ys_buf, &neg_buf);
                let mirrored = mlabel == Some(stock_wins);
                match (fires, mirrored) {
                    (true, true) => both += 1,
                    (true, false) => primary += 1,
                    (false, true) => mirror += 1,
                    (false, false) => {}
                }
                if fires {
                    let r = rf.min(rs);
                    rss += r * r;
                    voters += 1;
                }
            }
            s.insert("sign_primary".into(), json!(primary));
            s.insert("sign_mirror".into(), json!(mirror));
            s.insert("sign_both".into(), json!(both));
            // BIC over the winning voters' best residuals:
            // n·ln(RSS/n) + arity·ln(n), RSS floored so an
            // exact fit stays finite. The ΔBIC>10 arity tiebreak in the
            // script reads this.
            if voters > 0 {
                let n = voters as f64;
                let arity = if t2.is_some() { 2.0 } else { 1.0 };
                let bic = n * (rss.max(1e-12) / n).ln() + arity * n.ln();
                s.insert("bic".into(), json!(bic));
            }
        }
        summaries.push(Value::Object(s));
    }

    Ok(json!({
        "n_common": n_common,
        "summaries": summaries,
    }))
}
