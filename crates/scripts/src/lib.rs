//! The kernel runtime behind [`FunctionRuntime`] (SPEC.md §6). Function
//! bodies are SQL the engine plans — a measurement over data, a detector
//! over its witness's `slots` — so nothing here evaluates a body. What
//! remains is what SQL cannot express: the statistical kernels in Rust,
//! behind the engine's aggregate registrations and the runtime's typed
//! methods.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

pub mod library;
mod statistics;
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use glossql_session::FunctionRuntime;
use serde_json::{Value, json};

/// The native kernels and their one lazily loaded model.
pub struct KernelRuntime {
    band_model: Arc<BandModel>,
}

impl std::fmt::Debug for KernelRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KernelRuntime").finish_non_exhaustive()
    }
}

type ScriptResult<T> = Result<T, String>;

fn fail<T>(message: impl Into<String>) -> ScriptResult<T> {
    Err(message.into())
}

/// The TabICL regressor behind the band kernel: loaded once per runtime
/// from the workspace's weights directory on first call, shared across
/// scripts and threads (the forward takes `&self`). A failed load is
/// never cached — weights provisioned after the first refused read are
/// picked up by the next call, no restart (a caching OnceLock would
/// hold the error for the process lifetime).
struct BandModel {
    dir: PathBuf,
    model: RwLock<Option<Arc<tabicl_candle::tabicl::TabIcl>>>,
    /// Chosen once: Metal when the machine has it (only the candle
    /// compute rides the GPU), CPU otherwise.
    device: tabicl_candle::Device,
}

/// The pool the candle CPU work runs on — capped so the model never
/// takes the machine (`GLOSSQL_CANDLE_THREADS`, default 4). Only candle
/// calls run inside it; the rest of the server is untouched.
fn compute_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let n = std::env::var("GLOSSQL_CANDLE_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            // rayon reads 0 as "all logical CPUs" — the opposite of the
            // cap's point; 0 and an unparseable value both fall back.
            .filter(|n| *n > 0)
            .unwrap_or(4);
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|i| format!("candle-{i}"))
            .build()
            .expect("candle thread pool")
    })
}

fn pick_device() -> tabicl_candle::Device {
    tabicl_candle::Device::new_metal(0).unwrap_or(tabicl_candle::Device::Cpu)
}

impl BandModel {
    /// Where the model lives: the workspace's own `weights/` when it is
    /// complete (the operator's override), else the directory the build
    /// staged beside the binaries (`build.rs` copies safetensors, config,
    /// and the pinned DIGESTS from the tabicl-candle checkout at compile
    /// time — weights ride the build, never a runtime copy).
    fn resolve_dir(&self) -> Result<PathBuf, String> {
        let complete = |d: &std::path::Path| {
            d.join("DIGESTS").exists()
                && d.join("tabicl-regressor.safetensors").exists()
                && d.join("tabicl-regressor.config.json").exists()
        };
        let mut candidates = vec![self.dir.clone()];
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            candidates.push(dir.join("weights"));
            candidates.push(dir.join("../weights"));
        }
        candidates
            .iter()
            .find(|d| complete(d))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "no complete weights (safetensors + config + DIGESTS) at any of: {} — \
                     the build stages them beside the binary from ../tabicl-candle; a \
                     workspace weights/ overrides",
                    candidates
                        .iter()
                        .map(|d| d.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }

    fn device(&self) -> &tabicl_candle::Device {
        &self.device
    }

    fn get(&self) -> Result<Arc<tabicl_candle::tabicl::TabIcl>, String> {
        if let Some(model) = self.model.read().expect("band model lock").as_ref() {
            return Ok(Arc::clone(model));
        }
        let dir = self.resolve_dir()?;
        let ckpt = tabicl_candle::weights::load_dir(&dir, "regressor", &self.device)
            .map_err(|e| format!("tabicl weights at {}: {e}", dir.display()))?;
        let loaded = Arc::new(
            tabicl_candle::tabicl::TabIcl::from_checkpoint(ckpt).map_err(|e| e.to_string())?,
        );
        let mut slot = self.model.write().expect("band model lock");
        // Two readers racing both load; the first write wins, the loads
        // are identical (digest-verified), nothing is poisoned.
        Ok(Arc::clone(slot.get_or_insert(loaded)))
    }

    /// The band kernel — one fit, quantiles at the
    /// levels, and the PIT read off the monotone quantile grid. Ordinal
    /// by construction; raw densities never leave here. Serves the
    /// metric-bands walk door through
    /// [`FunctionRuntime::band_point`].
    fn bands_core(
        &self,
        x: &[f64],
        rows: usize,
        cols: usize,
        y: &[f64],
        test: &[f64],
        levels: &[f64],
        actual: f64,
    ) -> Result<(Vec<f64>, f64), String> {
        if rows < 2 || x.len() != rows * cols || y.len() != rows || test.len() != cols {
            return Err(format!(
                "band_point: {rows} rows x {cols} features against {} values and {} test features",
                y.len(),
                test.len()
            ));
        }
        let model = self.get()?;
        let pred =
            compute_pool()
                .install(|| {
                    tabicl_candle::regressor::TabIclRegressor::fit(&model, x, rows, cols, y)
                        .predict(test, 1, &self.device)
                })
                .map_err(|e| e.to_string())?;
        let q: Vec<f32> = pred
            .quantiles(levels)
            .and_then(|t| Ok(t.flatten_all()?.to_vec1()?))
            .map_err(|e| e.to_string())?;
        let grid: Vec<f32> = pred
            .raw_quantiles()
            .and_then(|t| Ok(t.flatten_all()?.to_vec1()?))
            .map_err(|e| e.to_string())?;
        let below = grid.iter().filter(|v| f64::from(**v) <= actual).count();
        Ok((
            q.into_iter().map(f64::from).collect(),
            below as f64 / (grid.len() + 1) as f64,
        ))
    }
}

impl KernelRuntime {
    /// `root` is the workspace directory. Bodies live in declarations
    /// (fixture 24), never under it — only the band model's weights do.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        // Weights load lazily, digest-verified, from the workspace's
        // weights/ directory (flat layout: tabicl-regressor.safetensors,
        // its config json, DIGESTS); a missing directory fails the
        // calling door with the loader's message.
        let band_model = Arc::new(BandModel {
            dir: root.join("weights"),
            model: RwLock::new(None),
            device: pick_device(),
        });
        KernelRuntime { band_model }
    }
}

impl FunctionRuntime for KernelRuntime {
    /// The shipped statistics (`profile`; `mad` and `entropy` ride
    /// inside its struct), registered when the runtime attaches — a
    /// measurement body and an agent's own SQL name the same
    /// aggregates (stage 5, §7e).
    fn udafs(&self) -> Vec<datafusion::logical_expr::AggregateUDF> {
        statistics::udafs()
    }

    /// The `whatif.` door's kernel: the regressor
    /// ensemble over the replayed worlds — a replay grid is a handful
    /// of worlds, exactly the sparse-support regime the ensemble was
    /// ruled in for (tabicl-candle README, stage 3). Members from the
    /// crate's own generator, seed pinned; quantiles averaged across
    /// members in the original y space.
    fn band_grid(
        &self,
        train_x: &[f64],
        rows: usize,
        cols: usize,
        train_y: &[f64],
        test_x: &[f64],
        test_rows: usize,
        alphas: &[f64],
    ) -> Result<Vec<f64>, String> {
        if rows < 2 || train_x.len() != rows * cols || test_x.len() != test_rows * cols {
            return Err(format!(
                "band_grid: {rows} train rows x {cols} features against {} train values \
                 and {} test values",
                train_y.len(),
                test_x.len()
            ));
        }
        // The kernel's preprocessor imputes NaN to the column mean and
        // then drops constant columns, and the ensemble asserts each
        // member's shuffle against the kept count — members generated
        // over the raw count would kill the read on a pool thread
        // whenever a feature is constant (one post month, a lever whose
        // bracketed worlds were all skipped). Mirror the filter exactly
        // (tabicl-candle regressor.rs; an all-NaN column stays, since
        // NaN != NaN) and generate over the kept count.
        let kept = (0..cols)
            .filter(|&c| {
                let column = || train_x.iter().skip(c).step_by(cols).copied();
                let (sum, n) = column()
                    .filter(|v| !v.is_nan())
                    .fold((0.0, 0usize), |(s, n), v| (s + v, n + 1));
                let mean = sum / n as f64;
                let impute = move |v: f64| if v.is_nan() { mean } else { v };
                column().map(impute).any(|v| v != impute(train_x[c]))
            })
            .count();
        if kept == 0 {
            return Err(
                "band_grid: every feature column is constant over the training rows — \
                 nothing varies to band on"
                    .into(),
            );
        }
        let model = self.band_model.get()?;
        let members = tabicl_candle::ensemble::EnsembleMember::generate(kept, 8, 0);
        compute_pool()
            .install(|| {
                let est = tabicl_candle::ensemble::TabIclEnsemble::fit(
                    &model, train_x, rows, cols, train_y, members,
                );
                est.predict_quantiles(test_x, test_rows, alphas, self.band_model.device())
            })
            .map_err(|e| e.to_string())
    }

    /// The behavior-evidence door's kernel (stage 5): the stock/flow
    /// discriminator, over batches.
    fn reconcile(
        &self,
        y: &[RecordBatch],
        m: &[RecordBatch],
        terms: &[String],
    ) -> Result<Value, String> {
        reconcile_kernel(y, m, terms.to_vec())
    }

    /// The metric-bands walk's kernel (stage 5): one fit and one read,
    /// typed.
    fn band_point(
        &self,
        train_x: &[f64],
        rows: usize,
        cols: usize,
        train_y: &[f64],
        test_x: &[f64],
        alphas: &[f64],
        actual: f64,
    ) -> Result<(Vec<f64>, f64), String> {
        self.band_model
            .bands_core(train_x, rows, cols, train_y, test_x, alphas, actual)
    }

    /// The `misfit.` door's kernel (fixture 20): the
    /// chain-rule density read, fit on the frame and scored on the same
    /// frame (self-fit — measured protocol-robust for this model).
    /// Numeric features only, through the regressor checkpoint. Two
    /// deterministic orderings — identity and reverse — so every
    /// feature conditions both early and late; nothing semantic rides
    /// the ordering stream (the port's own note). Log space end to end.
    fn misfit_scores(&self, x: &[f64], rows: usize, cols: usize) -> Result<Vec<f64>, String> {
        if rows < 2 || cols < 2 || x.len() != rows * cols {
            return Err(format!(
                "misfit_scores: {rows} rows x {cols} features against {} values",
                x.len()
            ));
        }
        let model = self.band_model.get()?;
        let xf: Vec<f32> = x.iter().map(|v| *v as f32).collect();
        let unsup = tabicl_candle::unsupervised::Unsupervised::fit(
            &model,
            None,
            xf.clone(),
            rows,
            cols,
            vec![],
        );
        let perms: Vec<Vec<usize>> = vec![(0..cols).collect(), (0..cols).rev().collect()];
        // The dummy column for empty conditionings: a fixed-seed normal
        // stream — deterministic, so the read reproduces.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut noise = move |n: usize| -> Vec<f32> {
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 11) as f64 / (1u64 << 53) as f64
            };
            (0..n)
                .map(|_| {
                    let (u1, u2) = (next().max(1e-12), next());
                    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                    z as f32
                })
                .collect()
        };
        // The feature conditionals run in parallel on the capped candle
        // pool (CPU) or in order on the accelerator's one queue.
        compute_pool()
            .install(|| {
                unsup.score_log_mean(&xf, rows, &perms, &mut noise, self.band_model.device())
            })
            .map_err(|e| e.to_string())
    }
}

/// What the float kernels may read as numbers: numeric types themselves,
/// booleans, and strings (the safe-cast reading on a raw column). Temporal
/// columns are deliberately out — a date has an order but no mean, exactly
/// v0.3's gate (numeric stats behind `is_numeric(resolved_type)`).
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
// - `reconcile`: v0.3's stock/flow discriminator (its constants and
//   provenance move here with the arithmetic they govern) — convention
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

// ---- the reconcile kernel: v0.3's stock/flow discriminator ----------
//
// Constants ported verbatim with their provenance; the two gates are
// COUPLED (see behavior_evidence.sql's header for the derivation).
const MIN_PERIODS: usize = 4;
const FIRE_RESIDUAL_MAX: f64 = 0.5;
const MIN_ENTITIES_FIRED: usize = 2;
const AGREEMENT_MIN: f64 = 0.8;

fn min_separation() -> f64 {
    (1.0 - FIRE_RESIDUAL_MAX) / (1.0 + FIRE_RESIDUAL_MAX)
}

/// One entity's vote: `None` = abstained. A dead measure abstains
/// symmetrically with a dead anchor; a wrong anchor leaves both
/// residuals large; a near-tie converts the last significant digit
/// into no verdict at all.
fn classify_series(y: &[f64], m: &[f64]) -> (Option<bool>, f64, f64) {
    const INF: f64 = f64::INFINITY;
    if y.len() < MIN_PERIODS
        || !y.iter().any(|v| *v != 0.0)
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
    if sep < min_separation() {
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
/// convention) under the ported gates. Returns per-convention
/// summaries; support policy (Wilson, winner, alternatives) stays in
/// the door.
fn reconcile_kernel(
    y: &[RecordBatch],
    m: &[RecordBatch],
    terms: Vec<String>,
) -> ScriptResult<Value> {
    let k = terms.len();
    if k == 0 {
        return fail("reconcile needs at least one movement term");
    }
    if k > 64 {
        return fail("more than 64 movement terms — the validity mask is a u64");
    }
    let ye = cell_keys(&column_of(y, "e")?)?;
    let yb = cell_keys(&column_of(y, "b")?)?;
    let yv = as_floats(&column_of(y, "yv")?)?;
    let me = cell_keys(&column_of(m, "e")?)?;
    let mb = cell_keys(&column_of(m, "b")?)?;
    let mut mcols = Vec::with_capacity(k);
    for t in &terms {
        mcols.push(as_floats(&column_of(m, &format!("s_{t}"))?)?);
    }

    let mut mrows: HashMap<(u64, u64), usize> = HashMap::with_capacity(me.len());
    let mut m_entities: HashSet<u64> = HashSet::new();
    for i in 0..me.len() {
        if let (Some(e), Some(b)) = (me[i], mb[i]) {
            m_entities.insert(e);
            mrows.insert((e, b), i);
        }
    }

    // Contiguous entity segments in y order; a cell pairs a y value
    // with its matching m row. Cells missing on the m side drop —
    // intersection pairing, as recorded in the script's header.
    let mut segments: Vec<Vec<(f64, usize)>> = Vec::new();
    let mut y_entities: HashSet<u64> = HashSet::new();
    let mut current: Option<u64> = None;
    for i in 0..ye.len() {
        let (Some(e), Some(b)) = (ye[i], yb[i]) else {
            continue;
        };
        if yv.is_null(i) {
            continue;
        }
        y_entities.insert(e);
        if current != Some(e) {
            segments.push(Vec::new());
            current = Some(e);
        }
        if let Some(&row) = mrows.get(&(e, b)) {
            segments
                .last_mut()
                .expect("segment exists")
                .push((yv.value(i), row));
        }
    }
    let n_common = y_entities.intersection(&m_entities).count();

    // Stack the cells: M (cells × k, NULL as 0.0 with a validity bit)
    // and the y vector, segment bounds kept.
    let ncells: usize = segments.iter().map(Vec::len).sum();
    let mut mmat = vec![0.0f64; ncells * k];
    let mut valid = vec![0u64; ncells];
    let mut yvec = vec![0.0f64; ncells];
    let mut bounds = Vec::with_capacity(segments.len());
    let mut at = 0usize;
    for seg in &segments {
        let start = at;
        for &(yval, row) in seg {
            yvec[at] = yval;
            for (t, col) in mcols.iter().enumerate() {
                if !col.is_null(row) {
                    mmat[at * k + t] = col.value(row);
                    valid[at] |= 1u64 << t;
                }
            }
            at += 1;
        }
        bounds.push((start, at - start));
    }

    // Conventions: each term, then every ordered pair difference —
    // v0.3's enumeration, unchanged. Evaluated as M · W in one product.
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
            // BIC over the winning voters' best residuals (v0.3's
            // formula): n·ln(RSS/n) + arity·ln(n), RSS floored so an
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
        "n_common": n_common as i64,
        "summaries": summaries,
    }))
}

#[cfg(test)]
mod band_grid_filter {
    //! The constant-column mirror in `band_grid`: the door
    //! mechanics live in glossql-session's
    //! suites; here the seam itself — refuse cleanly when nothing
    //! varies, and survive the single-post-month frame whose constant
    //! month index used to fire the kernel's shuffle assert.

    use super::*;

    #[test]
    fn band_grid_refuses_when_no_feature_varies() {
        // Both columns constant over the training rows: refused before
        // the model would load, so no weights are needed here.
        let rt = KernelRuntime::new("no-workspace-here");
        let train_x = vec![1.0, 3.0, 1.0, 3.0, 1.0, 3.0];
        let train_y = vec![10.0, 11.0, 12.0];
        let e = rt
            .band_grid(&train_x, 3, 2, &train_y, &[1.0, 3.0], 1, &[0.5])
            .unwrap_err();
        assert!(e.contains("constant"), "{e}");
    }

    #[test]
    fn band_grid_survives_a_constant_month_index() {
        // Finding 1's live trigger: one post month leaves the month
        // index constant, the preprocessor drops it, and members must
        // span the kept column alone. Panicked before the mirror.
        let sibling = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../tabicl-candle"
        ));
        if !sibling
            .join("weights/tabicl-regressor.safetensors")
            .exists()
        {
            eprintln!("skipping: no converted weights in the sibling checkout");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let weights = dir.path().join("weights");
        std::fs::create_dir_all(&weights).unwrap();
        for (from, to) in [
            (
                "weights/tabicl-regressor.safetensors",
                "tabicl-regressor.safetensors",
            ),
            (
                "weights/tabicl-regressor.config.json",
                "tabicl-regressor.config.json",
            ),
            ("fixtures/DIGESTS", "DIGESTS"),
        ] {
            std::os::unix::fs::symlink(sibling.join(from), weights.join(to)).unwrap();
        }
        let rt = KernelRuntime::new(dir.path());

        let factors = [1.0, 0.90, 1.05, 1.10, 1.20, 1.30];
        let mut train_x = Vec::new();
        let mut train_y = Vec::new();
        for f in factors {
            train_x.extend([f, 11.0]);
            train_y.push(1000.0 * f);
        }
        let alphas = [0.05, 0.50, 0.95];
        let q = rt
            .band_grid(
                &train_x,
                factors.len(),
                2,
                &train_y,
                &[1.15, 11.0],
                1,
                &alphas,
            )
            .unwrap();
        assert_eq!(q.len(), alphas.len());
        assert!(q[0] <= q[1] && q[1] <= q[2], "monotone quantiles: {q:?}");
    }
}
