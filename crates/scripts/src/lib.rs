//! The rhai runtime behind [`FunctionRuntime`] (SPEC.md §6): measurements
//! and detectors as scripts, composing vectorized kernels on zero-copy
//! column handles — scripts orchestrate, they never iterate rows
//! (reports/2026-08-03-poc-substrate.md, the spike).
//!
//! Invocation contract: the script file evaluates with three scope
//! constants — `subject` (the extraction's subject path), `context` (the
//! `ACCEPTS` document, or slots + threshold for a detector), `db` (the SQL
//! door; a detector's door refuses) — and its final expression is the
//! result, converted to JSON and validated against `RETURNS` by the session.

use std::collections::{HashMap, HashSet};
use std::path::{Component, PathBuf};
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, UInt64Array,
};
use datafusion::arrow::compute::kernels::aggregate;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use glossql_glossary::FunctionRow;
use glossql_session::{FunctionRuntime, SqlDoor};
use rhai::{AST, Dynamic, Engine, EvalAltResult, Scope};
use serde_json::Value;

/// One engine, configured once; compiled ASTs cached per script (recompiled
/// when the file's text changes). Both are shareable because the crate is
/// built with rhai's `sync` feature.
pub struct RhaiRuntime {
    root: PathBuf,
    engine: Engine,
    asts: RwLock<HashMap<String, (String, Arc<AST>)>>,
    /// Shared with the `tabicl_bands` closure: one lazily loaded model
    /// serves both the script kernel and the `band_grid` seam.
    band_model: Arc<BandModel>,
}

impl std::fmt::Debug for RhaiRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiRuntime")
            .field("root", &self.root)
            .finish()
    }
}

/// A table of batches, from the door.
#[derive(Debug, Clone)]
pub struct Table(Arc<Vec<RecordBatch>>);

/// A zero-copy column handle: cloning bumps the Arc, never the buffer.
#[derive(Debug, Clone)]
pub struct Col(ArrayRef);

#[derive(Clone)]
struct Door(Arc<dyn SqlDoor>);

type ScriptResult<T> = Result<T, Box<EvalAltResult>>;

fn fail<T>(message: impl Into<String>) -> ScriptResult<T> {
    Err(message.into().into())
}

/// The TabICL regressor behind the band kernel: loaded once per runtime
/// from the workspace's weights directory on first call, shared across
/// scripts and threads (the forward takes `&self`). A failed load is
/// never cached — weights provisioned after the first refused read are
/// picked up by the next call, no restart (found live 2026-08-11, the
/// whatif trial: the OnceLock held the error for the process lifetime).
struct BandModel {
    dir: PathBuf,
    model: RwLock<Option<Arc<tabicl_candle::tabicl::TabIcl>>>,
}

impl BandModel {
    fn get(&self) -> Result<Arc<tabicl_candle::tabicl::TabIcl>, String> {
        if let Some(model) = self.model.read().expect("band model lock").as_ref() {
            return Ok(Arc::clone(model));
        }
        let ckpt =
            tabicl_candle::weights::load_dir(&self.dir, "regressor", &tabicl_candle::Device::Cpu)
                .map_err(|e| format!("tabicl weights at {}: {e}", self.dir.display()))?;
        let loaded = Arc::new(
            tabicl_candle::tabicl::TabIcl::from_checkpoint(ckpt).map_err(|e| e.to_string())?,
        );
        let mut slot = self.model.write().expect("band model lock");
        // Two readers racing both load; the first write wins, the loads
        // are identical (digest-verified), nothing is poisoned.
        Ok(Arc::clone(slot.get_or_insert(loaded)))
    }

    /// One fit + one read: bands at the given levels for one test row,
    /// and the PIT — the quantile at which the observed value lands in
    /// the predicted distribution, read off the monotone quantile grid.
    /// Ordinal by construction; raw densities never leave the kernel.
    fn bands(
        &self,
        train_x: &rhai::Array,
        train_y: &rhai::Array,
        test_x: &rhai::Array,
        alphas: &rhai::Array,
        actual: f64,
    ) -> Result<rhai::Map, String> {
        let number = |d: &Dynamic| -> Result<f64, String> {
            d.as_float()
                .or_else(|_| d.as_int().map(|v| v as f64))
                .map_err(|t| format!("tabicl_bands takes numbers, got {t}"))
        };
        let row = |d: &Dynamic| -> Result<Vec<f64>, String> {
            d.clone()
                .into_array()
                .map_err(|t| format!("tabicl_bands takes rows of numbers, got {t}"))?
                .iter()
                .map(&number)
                .collect()
        };

        let rows = train_x.len();
        if rows != train_y.len() || rows < 2 {
            return Err(format!(
                "tabicl_bands: {rows} training rows against {} targets (need >= 2)",
                train_y.len()
            ));
        }
        let test: Vec<f64> = row(&Dynamic::from(test_x.clone()))?;
        let cols = test.len();
        let mut x = Vec::with_capacity(rows * cols);
        for r in train_x {
            let r = row(r)?;
            if r.len() != cols {
                return Err(format!(
                    "tabicl_bands: ragged training row ({} features against {cols})",
                    r.len()
                ));
            }
            x.extend(r);
        }
        let y: Vec<f64> = train_y.iter().map(&number).collect::<Result<_, _>>()?;
        let levels: Vec<f64> = alphas.iter().map(&number).collect::<Result<_, _>>()?;

        let model = self.get()?;
        let est = tabicl_candle::regressor::TabIclRegressor::fit(&model, &x, rows, cols, &y);
        let pred = est
            .predict(&test, 1, &tabicl_candle::Device::Cpu)
            .map_err(|e| e.to_string())?;
        let q: Vec<f32> = pred
            .quantiles(&levels)
            .and_then(|t| Ok(t.flatten_all()?.to_vec1()?))
            .map_err(|e| e.to_string())?;
        let grid: Vec<f32> = pred
            .raw_quantiles()
            .and_then(|t| Ok(t.flatten_all()?.to_vec1()?))
            .map_err(|e| e.to_string())?;
        let below = grid.iter().filter(|v| f64::from(**v) <= actual).count();

        let mut out = rhai::Map::new();
        out.insert(
            "q".into(),
            Dynamic::from(
                q.iter()
                    .map(|v| Dynamic::from(f64::from(*v)))
                    .collect::<rhai::Array>(),
            ),
        );
        out.insert(
            "pit".into(),
            Dynamic::from(below as f64 / (grid.len() + 1) as f64),
        );
        Ok(out)
    }
}

impl RhaiRuntime {
    /// `root` is the workspace's functions directory; `FROM` paths resolve
    /// under it, fenced like import paths (no absolute, no `..`).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let mut engine = Engine::new_raw();
        engine.register_global_module(rhai::packages::Package::as_shared_module(
            &rhai::packages::StandardPackage::new(),
        ));
        // The default file resolver reads the filesystem and its base path
        // is not a jail (rhai-1.25.1 file.rs:272-290); no imports until a
        // corpus script needs them.
        engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver);
        // Runaway backstop, not a sandbox — scripts are workspace-trusted
        // (M2 ruling); every other limit keeps its default. 50M is right
        // BECAUSE the heavy arithmetic lives in kernels (2026-08-06: the
        // f1 rework briefly raised this to 2B to cover interpreter-bound
        // loops — that was covering for a wrong design, reverted with it).
        engine.set_max_operations(50_000_000);
        // Except expression depth, whose default HALVES in debug builds
        // (rhai-1.25.1 limits.rs:17 vs :32) — a library script would then
        // parse in release and fail under `cargo test`. Pin the release
        // defaults so both builds run the same contract.
        engine.set_max_expr_depths(64, 32);

        engine
            .register_type_with_name::<Table>("Table")
            .register_fn("num_rows", |t: &mut Table| -> i64 {
                t.0.iter().map(|b| b.num_rows() as i64).sum()
            })
            .register_fn("columns", |t: &mut Table| -> rhai::Array {
                match t.0.first() {
                    Some(b) => b
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| Dynamic::from(f.name().clone()))
                        .collect(),
                    None => rhai::Array::new(),
                }
            })
            .register_fn("col", |t: &mut Table, name: &str| -> ScriptResult<Col> {
                let Some(first) = t.0.first() else {
                    return fail(format!("no rows carry a column `{name}`"));
                };
                let Some((index, _)) = first.schema().column_with_name(name) else {
                    return fail(format!("no column `{name}` in the result"));
                };
                if t.0.len() == 1 {
                    return Ok(Col(Arc::clone(first.column(index))));
                }
                let arrays: Vec<&dyn Array> =
                    t.0.iter().map(|b| b.column(index).as_ref()).collect();
                datafusion::arrow::compute::concat(&arrays)
                    .map(Col)
                    .map_err(|e| e.to_string().into())
            })
            // The first row's value in a named column — the one-row
            // aggregate read every script does; () for NULL or no rows.
            .register_fn(
                "cell",
                |t: &mut Table, name: &str| -> ScriptResult<Dynamic> {
                    let Some(first) = t.0.first() else {
                        return fail(format!("no rows carry a column `{name}`"));
                    };
                    let Some((index, _)) = first.schema().column_with_name(name) else {
                        return fail(format!("no column `{name}` in the result"));
                    };
                    let Some(batch) = t.0.iter().find(|b| b.num_rows() > 0) else {
                        return Ok(Dynamic::UNIT);
                    };
                    let column = batch.column(index);
                    if column.is_null(0) {
                        return Ok(Dynamic::UNIT);
                    }
                    Ok(Dynamic::from(
                        array_value_to_string(column, 0).map_err(|e| e.to_string())?,
                    ))
                },
            );

        engine
            .register_type_with_name::<Col>("Col")
            // The column's Arrow type name ("Float64", "Date32", …) — the
            // door's empty results still carry schema, so a LIMIT 0 query
            // types a column without scanning it.
            .register_fn("dtype", |c: &mut Col| -> String {
                c.0.data_type().to_string()
            })
            .register_fn("count", |c: &mut Col| -> i64 { c.0.len() as i64 })
            .register_fn("null_count", |c: &mut Col| -> i64 {
                c.0.null_count() as i64
            })
            .register_fn("distinct", |c: &mut Col| -> ScriptResult<i64> {
                let mut seen = std::collections::HashSet::new();
                for i in 0..c.0.len() {
                    if c.0.is_null(i) {
                        continue;
                    }
                    seen.insert(array_value_to_string(&c.0, i).map_err(|e| e.to_string())?);
                }
                Ok(seen.len() as i64)
            })
            .register_fn("entropy", |c: &mut Col| -> ScriptResult<f64> {
                // Shannon entropy (nats) of the non-null value
                // distribution, exact — one pass over typed cell keys,
                // never display buckets. The profile's top_k stays a
                // display cap; this scalar is what a score may read
                // (the 2026-08-06 f1 lesson: a display cap must not
                // become a statistics cap).
                let mut counts: HashMap<u64, i64> = HashMap::new();
                for key in cell_keys(&c.0)?.into_iter().flatten() {
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
            })
            .register_fn("min", |c: &mut Col| -> ScriptResult<Dynamic> {
                extremum(c, true)
            })
            .register_fn("max", |c: &mut Col| -> ScriptResult<Dynamic> {
                extremum(c, false)
            })
            .register_fn("sum", |c: &mut Col| -> ScriptResult<Dynamic> {
                if !numeric_like(c.0.data_type()) {
                    return Ok(Dynamic::UNIT);
                }
                let floats = as_floats(&c.0)?;
                Ok(aggregate::sum(&floats)
                    .map(Dynamic::from)
                    .unwrap_or(Dynamic::UNIT))
            })
            .register_fn("mean", |c: &mut Col| -> ScriptResult<Dynamic> {
                let v = valid_floats(&c.0)?;
                if v.is_empty() {
                    return Ok(Dynamic::UNIT);
                }
                Ok(Dynamic::from(v.iter().sum::<f64>() / v.len() as f64))
            })
            .register_fn("stddev", |c: &mut Col| -> ScriptResult<Dynamic> {
                // Sample standard deviation, matching SQL STDDEV.
                let v = valid_floats(&c.0)?;
                if v.len() < 2 {
                    return Ok(Dynamic::UNIT);
                }
                let mean = v.iter().sum::<f64>() / v.len() as f64;
                let var =
                    v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (v.len() as f64 - 1.0);
                Ok(Dynamic::from(var.sqrt()))
            })
            .register_fn(
                "percentile",
                |c: &mut Col, p: f64| -> ScriptResult<Dynamic> {
                    // Linear interpolation, matching SQL PERCENTILE_CONT.
                    if !(0.0..=1.0).contains(&p) {
                        return fail("percentile wants p in [0, 1]");
                    }
                    let mut v = valid_floats(&c.0)?;
                    if v.is_empty() {
                        return Ok(Dynamic::UNIT);
                    }
                    v.sort_by(f64::total_cmp);
                    Ok(Dynamic::from(interpolate(&v, p)))
                },
            )
            .register_fn("mad", |c: &mut Col| -> ScriptResult<Dynamic> {
                // Median absolute deviation — the robust spread the modified
                // Z-score fences ride on.
                let mut v = valid_floats(&c.0)?;
                if v.is_empty() {
                    return Ok(Dynamic::UNIT);
                }
                v.sort_by(f64::total_cmp);
                let median = interpolate(&v, 0.5);
                let mut deviations: Vec<f64> = v.iter().map(|x| (x - median).abs()).collect();
                deviations.sort_by(f64::total_cmp);
                Ok(Dynamic::from(interpolate(&deviations, 0.5)))
            })
            .register_fn(
                "top_k",
                |c: &mut Col, k: i64| -> ScriptResult<rhai::Array> {
                    let mut counts: HashMap<String, i64> = HashMap::new();
                    for i in 0..c.0.len() {
                        if c.0.is_null(i) {
                            continue;
                        }
                        let value = array_value_to_string(&c.0, i).map_err(|e| e.to_string())?;
                        *counts.entry(value).or_insert(0) += 1;
                    }
                    let mut pairs: Vec<(String, i64)> = counts.into_iter().collect();
                    pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                    pairs.truncate(k.max(0) as usize);
                    Ok(pairs
                        .into_iter()
                        .map(|(value, count)| {
                            let mut row = rhai::Map::new();
                            row.insert("value".into(), Dynamic::from(value));
                            row.insert("count".into(), Dynamic::from(count));
                            Dynamic::from_map(row)
                        })
                        .collect())
                },
            )
            .register_fn("len_stats", |c: &mut Col| -> ScriptResult<Dynamic> {
                let Some(values) = c.0.as_any().downcast_ref::<StringArray>() else {
                    return Ok(Dynamic::UNIT);
                };
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
                if n == 0 {
                    return Ok(Dynamic::UNIT);
                }
                let mut stats = rhai::Map::new();
                stats.insert("min".into(), Dynamic::from(min));
                stats.insert("max".into(), Dynamic::from(max));
                stats.insert("avg".into(), Dynamic::from(total as f64 / n as f64));
                Ok(Dynamic::from_map(stats))
            })
            .register_fn(
                "match_rate",
                |c: &mut Col, pattern: &str| -> ScriptResult<f64> {
                    let re = regex::Regex::new(pattern).map_err(|e| e.to_string())?;
                    let Some(values) = c.0.as_any().downcast_ref::<StringArray>() else {
                        return fail("match_rate reads a string column");
                    };
                    let mut total = 0u64;
                    let mut matched = 0u64;
                    for i in 0..values.len() {
                        if values.is_null(i) {
                            continue;
                        }
                        total += 1;
                        if re.is_match(values.value(i)) {
                            matched += 1;
                        }
                    }
                    Ok(if total == 0 {
                        0.0
                    } else {
                        matched as f64 / total as f64
                    })
                },
            )
            .register_fn(
                "parse_rate",
                |c: &mut Col, target: &str| -> ScriptResult<f64> {
                    let to = sql_type(target).ok_or_else(|| {
                        format!("`{target}` is not a cast target the substrate accepts")
                    })?;
                    let non_null = (c.0.len() - c.0.null_count()) as f64;
                    if non_null == 0.0 {
                        return Ok(1.0);
                    }
                    let cast = cast_with_options(
                        &c.0,
                        &to,
                        &CastOptions {
                            safe: true,
                            ..Default::default()
                        },
                    )
                    .map_err(|e| e.to_string())?;
                    let parsed = (cast.len() - cast.null_count()) as f64;
                    Ok(parsed / non_null)
                },
            )
            .register_fn("value_at", |c: &mut Col, i: i64| -> ScriptResult<Dynamic> {
                let i = i as usize;
                if i >= c.0.len() || c.0.is_null(i) {
                    return Ok(Dynamic::UNIT);
                }
                Ok(Dynamic::from(
                    array_value_to_string(&c.0, i).map_err(|e| e.to_string())?,
                ))
            })
            // The whole column as floats, one vectorized Arrow cast — no
            // per-cell display strings (2026-08-06: the string round-trip
            // was the seam tax that made hot loops interpreter-bound).
            // NULL and unparseable cells arrive as ().
            .register_fn("floats", |c: &mut Col| -> ScriptResult<rhai::Array> {
                let cast = as_floats(&c.0)?;
                Ok((0..cast.len())
                    .map(|i| {
                        if cast.is_null(i) {
                            Dynamic::UNIT
                        } else {
                            Dynamic::from(cast.value(i))
                        }
                    })
                    .collect())
            })
            // The column's distinct values as sorted typed keys — built
            // once, intersected many times (the SPIDER substrate; see the
            // statistical-kernels section).
            .register_fn("key_vec", |c: &mut Col| -> ScriptResult<KeyVec> {
                Ok(key_vec_from(cell_keys(&c.0)?))
            });

        engine
            .register_type_with_name::<KeyVec>("KeyVec")
            .register_fn("count", |k: &mut KeyVec| -> i64 { k.0.len() as i64 })
            // |A ∩ B| by linear merge of two sorted key vectors — the
            // containment numerator, at hash-probe-free speed.
            .register_fn("matched", |a: &mut KeyVec, b: KeyVec| -> i64 {
                merge_matched(&a.0, &b.0)
            });

        engine
            // Two columns' rows as combined keys (both non-null) — the
            // composite rescue's pair domain, deduplicated and sorted.
            .register_fn(
                "pair_keys",
                |t: &mut Table, c1: &str, c2: &str| -> ScriptResult<KeyVec> {
                    let k1 = cell_keys(&column_of(t, c1)?)?;
                    let k2 = cell_keys(&column_of(t, c2)?)?;
                    let keys = k1
                        .into_iter()
                        .zip(k2)
                        .map(|pair| match pair {
                            (Some(a), Some(b)) => {
                                let mut h = fnv1a(FNV_SEED, &a.to_le_bytes());
                                h = fnv1a(h, &b.to_le_bytes());
                                Some(h)
                            }
                            _ => None,
                        })
                        .collect();
                    Ok(key_vec_from(keys))
                },
            )
            // The stock/flow discriminator over two grouped results —
            // see the statistical-kernels section for the contract.
            .register_fn(
                "reconcile",
                |y: Table, m: Table, terms: rhai::Array| -> ScriptResult<rhai::Map> {
                    let terms: Vec<String> = terms
                        .into_iter()
                        .map(|t| {
                            t.into_string()
                                .map_err(|t| format!("reconcile takes term names, got {t}"))
                        })
                        .collect::<Result<_, _>>()?;
                    reconcile_kernel(&y, &m, terms)
                },
            );

        engine
            .register_type_with_name::<Door>("Door")
            .register_fn("query", |d: &mut Door, sql: &str| -> ScriptResult<Table> {
                d.0.sql(sql).map(|b| Table(Arc::new(b))).map_err(Into::into)
            })
            // Many queries at once, answered in order — the door overlaps
            // them below the seam (2026-08-06). One failed query fails the
            // call, named by its position: a batch is one measurement step.
            .register_fn(
                "query_all",
                |d: &mut Door, queries: rhai::Array| -> ScriptResult<rhai::Array> {
                    let sqls: Vec<String> = queries
                        .into_iter()
                        .map(|q| {
                            q.into_string()
                                .map_err(|t| format!("query_all takes strings, got {t}"))
                        })
                        .collect::<Result<_, _>>()?;
                    let mut out = rhai::Array::with_capacity(sqls.len());
                    for (i, r) in d.0.sql_all(&sqls).into_iter().enumerate() {
                        match r {
                            Ok(b) => out.push(Dynamic::from(Table(Arc::new(b)))),
                            Err(e) => return fail(format!("query_all[{i}]: {e}")),
                        }
                    }
                    Ok(out)
                },
            );

        engine
            // SQL text as an identity: parse and re-render, so spelling
            // differences (whitespace, keyword case, redundant parens the
            // parser folds) collapse and identifiers survive verbatim. A
            // body the parser cannot read normalizes by whitespace alone —
            // weaker, honestly so, and stated here rather than hidden.
            .register_fn("canonical_sql", |sql: &str| -> String {
                use datafusion::sql::sqlparser::dialect::GenericDialect;
                use datafusion::sql::sqlparser::parser::Parser;
                match Parser::parse_sql(&GenericDialect {}, sql) {
                    Ok(statements) if statements.len() == 1 => statements[0].to_string(),
                    _ => sql.split_whitespace().collect::<Vec<_>>().join(" "),
                }
            })
            // A stored body (a gloss, a cached value) back into a map the
            // script can read — the inverse of returning one.
            .register_fn("parse_json", |s: &str| -> ScriptResult<Dynamic> {
                let value: serde_json::Value =
                    serde_json::from_str(s).map_err(|e| format!("parse_json: {e}"))?;
                rhai::serde::to_dynamic(&value).map_err(|e| e.to_string().into())
            });

        // The TabICL band kernel (ruled 2026-08-11): the forward pass is
        // native — the model is never reimplemented in script — while the
        // walk-forward protocol stays authored in metric_bands.rhai.
        // Weights load lazily, digest-verified, from the workspace's
        // weights/ directory (flat layout: tabicl-regressor.safetensors,
        // its config json, DIGESTS); a missing directory fails the
        // calling function with the loader's message.
        let band_model = Arc::new(BandModel {
            dir: root.join("weights"),
            model: RwLock::new(None),
        });
        let kernel_model = Arc::clone(&band_model);
        engine.register_fn(
            "tabicl_bands",
            move |train_x: rhai::Array,
                  train_y: rhai::Array,
                  test_x: rhai::Array,
                  alphas: rhai::Array,
                  actual: f64|
                  -> ScriptResult<rhai::Map> {
                kernel_model
                    .bands(&train_x, &train_y, &test_x, &alphas, actual)
                    .map_err(Into::into)
            },
        );

        RhaiRuntime {
            root,
            engine,
            asts: RwLock::new(HashMap::new()),
            band_model,
        }
    }

    fn ast(&self, script: &str) -> Result<Arc<AST>, String> {
        let relative = PathBuf::from(script);
        if relative.is_absolute()
            || relative
                .components()
                .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(format!(
                "script `{script}` must stay under the functions root — relative, no `..`"
            ));
        }
        let path = self.root.join(relative);
        let text = std::fs::read_to_string(&path).map_err(|e| format!("script `{script}`: {e}"))?;
        if let Some((cached_text, ast)) = self.asts.read().expect("asts").get(script)
            && *cached_text == text
        {
            return Ok(Arc::clone(ast));
        }
        let ast = Arc::new(
            self.engine
                .compile(&text)
                .map_err(|e| format!("script `{script}`: {e}"))?,
        );
        self.asts
            .write()
            .expect("asts")
            .insert(script.to_string(), (text, Arc::clone(&ast)));
        Ok(ast)
    }

    /// The registered kernel signatures — what the engine actually offers,
    /// for the skill-mirror test. `kernel-audit` rides only under
    /// `cargo test` (the self dev-dependency in Cargo.toml), never in the
    /// shipped library.
    #[cfg(feature = "kernel-audit")]
    pub fn kernel_signatures(&self) -> Vec<String> {
        self.engine.gen_fn_signatures(false)
    }
}

impl FunctionRuntime for RhaiRuntime {
    fn invoke(
        &self,
        function: &FunctionRow,
        subject: &str,
        context: &Value,
        door: Arc<dyn SqlDoor>,
    ) -> Result<Value, String> {
        let ast = self.ast(&function.script)?;
        let mut scope = Scope::new();
        scope.push_constant("subject", subject.to_string());
        scope.push_constant(
            "context",
            rhai::serde::to_dynamic(context).map_err(|e| e.to_string())?,
        );
        scope.push_constant("db", Door(door));
        let result: Dynamic = self
            .engine
            .eval_ast_with_scope(&mut scope, &ast)
            .map_err(|e| format!("`{}`: {e}", function.name))?;
        serde_json::to_value(&result).map_err(|e| {
            format!(
                "`{}` returned something JSON cannot carry: {e}",
                function.name
            )
        })
    }

    /// The `whatif.` door's kernel (ruled 2026-08-11): the regressor
    /// ensemble over the replayed worlds — a replay grid is a handful
    /// of worlds, exactly the sparse-support regime the ensemble was
    /// ruled in for (dataraum-tabicl README, stage 3). Members from the
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
        let model = self.band_model.get()?;
        let members = tabicl_candle::ensemble::EnsembleMember::generate(cols, 8, 0);
        let est = tabicl_candle::ensemble::TabIclEnsemble::fit(
            &model, train_x, rows, cols, train_y, members,
        );
        est.predict_quantiles(test_x, test_rows, alphas, &tabicl_candle::Device::Cpu)
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

fn extremum(c: &mut Col, min: bool) -> ScriptResult<Dynamic> {
    if let Some(values) = c.0.as_any().downcast_ref::<StringArray>() {
        let v = if min {
            aggregate::min_string(values)
        } else {
            aggregate::max_string(values)
        };
        return Ok(v
            .map(|s| Dynamic::from(s.to_string()))
            .unwrap_or(Dynamic::UNIT));
    }
    if numeric_like(c.0.data_type()) {
        let floats = as_floats(&c.0)?;
        let v = if min {
            aggregate::min(&floats)
        } else {
            aggregate::max(&floats)
        };
        return Ok(v.map(Dynamic::from).unwrap_or(Dynamic::UNIT));
    }
    // Dates, timestamps, and the rest order by their display form — ISO
    // spellings sort chronologically, so min/max stay truthful.
    let mut best: Option<String> = None;
    for i in 0..c.0.len() {
        if c.0.is_null(i) {
            continue;
        }
        let value = array_value_to_string(&c.0, i).map_err(|e| e.to_string())?;
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
    Ok(best.map(Dynamic::from).unwrap_or(Dynamic::UNIT))
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

// ---- statistical kernels (2026-08-06) --------------------------------
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

/// A named column across the table's batches, concatenated once.
fn column_of(t: &Table, name: &str) -> ScriptResult<ArrayRef> {
    let Some(first) = t.0.first() else {
        return fail(format!("no rows carry a column `{name}`"));
    };
    let Some((index, _)) = first.schema().column_with_name(name) else {
        return fail(format!("no column `{name}` in the result"));
    };
    if t.0.len() == 1 {
        return Ok(Arc::clone(first.column(index)));
    }
    let arrays: Vec<&dyn Array> = t.0.iter().map(|b| b.column(index).as_ref()).collect();
    datafusion::arrow::compute::concat(&arrays).map_err(|e| e.to_string().into())
}

/// A column's distinct values as sorted keys — built once, intersected
/// many times.
#[derive(Debug, Clone)]
pub struct KeyVec(Arc<Vec<u64>>);

fn key_vec_from(keys: Vec<Option<u64>>) -> KeyVec {
    let mut v: Vec<u64> = keys.into_iter().flatten().collect();
    v.sort_unstable();
    v.dedup();
    KeyVec(Arc::new(v))
}

fn merge_matched(a: &[u64], b: &[u64]) -> i64 {
    let (mut i, mut j, mut n) = (0usize, 0usize, 0i64);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                n += 1;
                i += 1;
                j += 1;
            }
        }
    }
    n
}

// ---- the reconcile kernel: v0.3's stock/flow discriminator ----------
//
// Constants ported verbatim with their provenance; the two gates are
// COUPLED (see behavior_evidence.rhai's header for the derivation).
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
/// the script.
fn reconcile_kernel(y: &Table, m: &Table, terms: Vec<String>) -> ScriptResult<rhai::Map> {
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

    let mut summaries = rhai::Array::with_capacity(cc);
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
        let mut s = rhai::Map::new();
        s.insert("convention".into(), Dynamic::from(conv_names[c].clone()));
        s.insert(
            "terms".into(),
            Dynamic::from(if t2.is_some() { 2i64 } else { 1i64 }),
        );
        s.insert("verdict".into(), Dynamic::from(verdict.to_string()));
        s.insert("voted".into(), Dynamic::from(voted as i64));
        s.insert("winners".into(), Dynamic::from(winners as i64));
        s.insert("agreement".into(), Dynamic::from(agreement));
        if verdict != "abstain" {
            // Medians over the winning-label voters only — a dissenting
            // minority's residuals would contaminate the diagnostics.
            let (rf, rs) = if stock_wins {
                (rf_stock.clone(), rs_stock.clone())
            } else {
                (rf_flow.clone(), rs_flow.clone())
            };
            if let Some(v) = median(rf) {
                s.insert("r_flow".into(), Dynamic::from(v));
            }
            if let Some(v) = median(rs) {
                s.insert("r_stock".into(), Dynamic::from(v));
            }
            // The sign partition (ported 2026-08-06): every entity
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
            s.insert("sign_primary".into(), Dynamic::from(primary));
            s.insert("sign_mirror".into(), Dynamic::from(mirror));
            s.insert("sign_both".into(), Dynamic::from(both));
            // BIC over the winning voters' best residuals (v0.3's
            // formula): n·ln(RSS/n) + arity·ln(n), RSS floored so an
            // exact fit stays finite. The ΔBIC>10 arity tiebreak in the
            // script reads this.
            if voters > 0 {
                let n = voters as f64;
                let arity = if t2.is_some() { 2.0 } else { 1.0 };
                let bic = n * (rss.max(1e-12) / n).ln() + arity * n.ln();
                s.insert("bic".into(), Dynamic::from(bic));
            }
        }
        summaries.push(Dynamic::from_map(s));
    }

    let mut out = rhai::Map::new();
    out.insert("n_common".into(), Dynamic::from(n_common as i64));
    out.insert("summaries".into(), Dynamic::from(summaries));
    Ok(out)
}

/// The SQL cast-target spellings the substrate accepts, mapped to arrow for
/// trial casts — one arm per family of datafusion-sql 53.1's planner
/// (planner.rs:662-763, decimal defaults utils.rs:289-317), so a trial and
/// the served `TRY_CAST` agree on what exists. One dialect, one list:
/// spellings DataFusion rejects (`TIMESTAMP_NS`, `DATETIME`, `DEC`,
/// `HUGEINT`) are refused here too, loudly, instead of trialing a type the
/// view could never serve. Format-parsed types trial through SQL
/// (`try_to_date`), not here.
fn sql_type(spelling: &str) -> Option<DataType> {
    let upper = spelling.trim().to_uppercase();
    // `HEAD(params)TAIL` — family words around the parameters, so
    // `DECIMAL(18,2)`, `TIMESTAMP(6)`, and `TIMESTAMP(3) WITH TIME ZONE`
    // all resolve.
    let (head, params, tail) = match upper.split_once('(') {
        Some((head, rest)) => {
            let (inside, tail) = rest.split_once(')')?;
            let params: Vec<u64> = inside
                .split(',')
                .map(|p| p.trim().parse().ok())
                .collect::<Option<_>>()?;
            (head, params, tail)
        }
        None => (upper.as_str(), Vec::new(), ""),
    };
    let family = format!("{head} {tail}");
    let family = family.split_whitespace().collect::<Vec<_>>().join(" ");
    Some(match family.as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" | "INT2" => DataType::Int16,
        "INT" | "INTEGER" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,
        "TINYINT UNSIGNED" => DataType::UInt8,
        "SMALLINT UNSIGNED" | "INT2 UNSIGNED" => DataType::UInt16,
        "INT UNSIGNED" | "INTEGER UNSIGNED" | "INT4 UNSIGNED" => DataType::UInt32,
        "BIGINT UNSIGNED" | "INT8 UNSIGNED" => DataType::UInt64,
        "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
        "CHAR" | "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
        "DATE" => DataType::Date32,
        "TIME" | "TIME WITHOUT TIME ZONE" => DataType::Time64(TimeUnit::Nanosecond),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" => {
            // The session's zone rides the real cast; parseability, which is
            // all a trial measures, does not depend on it.
            DataType::Timestamp(
                match params.first() {
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    None | Some(9) => TimeUnit::Nanosecond,
                    _ => return None,
                },
                None,
            )
        }
        "DECIMAL" | "NUMERIC" => {
            let (precision, scale) = match (params.first(), params.get(1)) {
                (Some(&p), Some(&s)) => (p, s),
                (Some(&p), None) => (p, 0),
                (None, _) => (38, 10),
            };
            if precision == 0 || precision > 76 || scale > precision {
                return None;
            }
            if precision > 38 {
                DataType::Decimal256(precision as u8, scale as i8)
            } else {
                DataType::Decimal128(precision as u8, scale as i8)
            }
        }
        "BYTEA" => DataType::Binary,
        "INTERVAL" => DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        _ => return None,
    })
}
