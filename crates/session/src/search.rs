//! The search doors (stage 5, §7e). A search enumerates candidates over
//! an arbitrary table's columns, which a static SQL body cannot spell:
//! the input schema varies while the output schema is fixed
//! (functions-split §7, the table-function asymmetry). So a search
//! computes in the pre-pass like every compute door — over the
//! statement's own pins, so it can never straddle a landing — and
//! optimizes recall: no thresholds here, the judgment lives in the
//! measurement body that reads the door.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::provider_as_source;
use datafusion::functions::expr_fn::{abs, greatest};
use datafusion::functions_aggregate::expr_fn::{avg, count};
use datafusion::logical_expr::{Expr, ExprFunctionExt, LogicalPlanBuilder, ident, lit};
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::SessionError;

/// `derivation_candidates('table')` — row-grain arithmetic identities
/// among the table's numeric columns (`a = b * c` and `a = b + c`) with
/// their violation counts, one row per counted (target; operands; form).
///
/// Why this exists (tfmeval, 2026-08-10): a scoped unit-mix artifact and
/// a real price change move a metric identically; the only instrument
/// that separates them is the derivation the lineage carries —
/// `line_amount = units * unit_price` held at violation rate 0.0 on
/// every clean corpus and fired at the artifact's exact row coverage.
/// No marginal statistic reaches this.
///
/// The door is generous by design — it counts every triple that passes
/// the structural prune, and the 0.95/20-row bar is the body's. The
/// prune is cost, not judgment: a product or sum whose operand
/// magnitudes cannot land within 30x of the target is skipped with no
/// recall lost at that bar. Numeric columns are capped at 12, reported
/// as `truncated`. One aggregate scan sizes the prune, one aggregate
/// scan counts every surviving triple — the per-triple query wave this
/// door replaces was round trips, not algorithm.
pub(crate) async fn derivation_candidates(
    shared: &Arc<Shared>,
    resolved: &crate::prepass::Resolved,
    table: &str,
) -> Result<RecordBatch, SessionError> {
    let bad =
        |d: String| SessionError::BadSubject(format!("derivation_candidates('{table}'): {d}"));
    let provider = resolved
        .pin(table)
        .ok_or_else(|| bad("no such table in the bound dataset".into()))?;
    let schema = provider.schema();

    // Numeric columns in schema order, capped at 12.
    let mut numeric: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| {
            let t = f.data_type().to_string();
            t.contains("Int") || t.contains("Float") || t.contains("Decimal")
        })
        .map(|f| f.name().clone())
        .collect();
    let truncated = numeric.len() > 12;
    numeric.truncate(12);

    if numeric.len() < 3 {
        return rows_batch(
            vec![json!({
                "applicable": false,
                "reason": "fewer than three numeric columns",
            })],
            derivation_shape(),
        );
    }

    let ctx = shared
        .ctx
        .read()
        .expect("ctx lock")
        .clone()
        .ok_or_else(|| SessionError::Runtime("the session context is not wired".into()))?;
    let scan = || {
        LogicalPlanBuilder::scan(table, provider_as_source(Arc::clone(&provider)), None)
            .map_err(|e| bad(e.to_string()))
    };
    let run = |plan| async {
        ctx.execute_logical_plan(plan)
            .await
            .map_err(|e| bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| bad(e.to_string()))
    };

    // One scan: mean magnitude and filled count per column decide which
    // triples are worth counting.
    let mut aggs = Vec::new();
    for c in &numeric {
        aggs.push(avg(abs(ident(c))).alias(format!("m_{c}")));
        aggs.push(count(ident(c)).alias(format!("f_{c}")));
    }
    let plan = scan()?
        .aggregate(Vec::<Expr>::new(), aggs)
        .and_then(|b| b.build())
        .map_err(|e| bad(e.to_string()))?;
    let sized = run(plan).await?;
    let one = sized
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| bad("the sizing scan returned nothing".into()))?;
    let mut mag = Vec::with_capacity(numeric.len());
    let mut filled = Vec::with_capacity(numeric.len());
    for (i, _) in numeric.iter().enumerate() {
        let m = one.column(i * 2);
        // Through the display form, as the script read it — a Decimal
        // average must parse to the same f64 it printed as.
        mag.push(if m.is_null(0) {
            0.0
        } else {
            array_value_to_string(m, 0)
                .map_err(|e| bad(e.to_string()))?
                .parse::<f64>()
                .map_err(|e| bad(e.to_string()))?
        });
        filled.push(
            one.column(i * 2 + 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| bad("a count did not read as an integer".into()))?
                .value(0),
        );
    }

    // Every (target; operand pair) triple, both forms, scale-pruned. The
    // tolerance is money-shaped: half a cent plus a relative hair, so
    // per-row rounding never counts as a violation.
    struct Spec {
        a: usize,
        b: usize,
        c: usize,
        forms: Vec<&'static str>,
    }
    let mut specs = Vec::new();
    for a in 0..numeric.len() {
        if filled[a] < 20 {
            continue;
        }
        for b in 0..numeric.len() {
            if b == a {
                continue;
            }
            for c in (b + 1)..numeric.len() {
                if c == a || filled[b] < 20 || filled[c] < 20 {
                    continue;
                }
                let mut forms = Vec::new();
                if mag[b] > 0.0 && mag[c] > 0.0 && mag[a] > 0.0 {
                    let prod = mag[b] * mag[c];
                    if prod < mag[a] * 30.0 && mag[a] < prod * 30.0 {
                        forms.push("product");
                    }
                }
                let s = mag[b] + mag[c];
                if s > 0.0 && mag[a] > 0.0 && s < mag[a] * 30.0 && mag[a] < s * 30.0 {
                    forms.push("sum");
                }
                if !forms.is_empty() {
                    specs.push(Spec { a, b, c, forms });
                }
            }
        }
    }

    let rows = filled.iter().copied().max().unwrap_or(0);
    let fact = |mut row: serde_json::Map<String, Value>| {
        row.insert("applicable".into(), json!(true));
        row.insert("rows".into(), json!(rows));
        row.insert("truncated".into(), json!(truncated));
        Value::Object(row)
    };
    if specs.is_empty() {
        return rows_batch(vec![fact(Default::default())], derivation_shape());
    }

    // One scan counts every surviving triple: support where all three
    // are present, violations per form beyond the tolerance.
    let column = |i: usize| ident(&numeric[i]);
    let mut aggs = Vec::new();
    for (i, s) in specs.iter().enumerate() {
        let present = column(s.a)
            .is_not_null()
            .and(column(s.b).is_not_null())
            .and(column(s.c).is_not_null());
        aggs.push(
            count(lit(1))
                .filter(present.clone())
                .build()
                .map_err(|e| bad(e.to_string()))?
                .alias(format!("support_{i}")),
        );
        for f in &s.forms {
            let expr = if *f == "product" {
                column(s.b) * column(s.c)
            } else {
                column(s.b) + column(s.c)
            };
            let tolerance = greatest(vec![lit(0.011), abs(column(s.a)) * lit(0.000001)]);
            let beyond = abs(column(s.a) - expr).gt(tolerance);
            aggs.push(
                count(lit(1))
                    .filter(present.clone().and(beyond))
                    .build()
                    .map_err(|e| bad(e.to_string()))?
                    .alias(format!("viol_{i}_{f}")),
            );
        }
    }
    let plan = scan()?
        .aggregate(Vec::<Expr>::new(), aggs)
        .and_then(|b| b.build())
        .map_err(|e| bad(e.to_string()))?;
    let counted = run(plan).await?;
    let one = counted
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| bad("the counting scan returned nothing".into()))?;
    let int_cell = |name: &str| -> Result<i64, SessionError> {
        let idx = one
            .schema()
            .index_of(name)
            .map_err(|e| bad(e.to_string()))?;
        Ok(one
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bad(format!("`{name}` did not read as an integer")))?
            .value(0))
    };

    let mut out = Vec::new();
    let mut seq = 0i64;
    for (i, s) in specs.iter().enumerate() {
        let support = int_cell(&format!("support_{i}"))?;
        for f in &s.forms {
            let violations = int_cell(&format!("viol_{i}_{f}"))?;
            let match_rate = 1.0 - violations as f64 / support as f64;
            let mut row = serde_json::Map::new();
            row.insert("seq".into(), json!(seq));
            row.insert("target".into(), json!(numeric[s.a]));
            row.insert("form".into(), json!(f));
            row.insert("operand_1".into(), json!(numeric[s.b]));
            row.insert("operand_2".into(), json!(numeric[s.c]));
            row.insert("support".into(), json!(support));
            row.insert("violations".into(), json!(violations));
            row.insert("match_rate".into(), json!(match_rate));
            out.push(fact(row));
            seq += 1;
        }
    }
    rows_batch(out, derivation_shape())
}

/// `hierarchy_candidates('table')` — pairwise functional-dependency
/// screens at high recall over one table's dimension-like columns: the
/// cheap SQL core of v0.3's dimension-identity stack
/// (analysis/hierarchies, transcribed 2026-08-05), one row per screened
/// direction.
///
/// v0.3's decision layer, dispositioned by the recall ruling (the
/// measurement's job is recall; the judge removes false positives):
/// - Null policy PORTED: NULL is a category — a null-coded binary
///   {1, NULL} is a lane, not a silent constant-drop. Grouping keeps
///   the NULL group, and distinct counts here include it.
/// - g3 and Goodman–Kruskal λ are SERVED per direction, never gated
///   here — the ship line (g3 ≤ 0.05), the alias line (both directions
///   ≤ 0.01) and λ's vacuous-skew reading live in the measurement body
///   and the judge.
/// - Permutation nulls + false-discovery control NOT ported: precision
///   apparatus compensating for judge-less operation.
/// - Measures stay out by dtype (Float/Decimal): the additivity lane
///   floods FD discovery. v0.3 excluded by semantic role; dtype is the
///   proxy a measurement can see.
/// - Guards are FULL-scan (the rel-hm fold-key lesson: a row sample
///   makes fold keys look near-key; never sample the guards).
/// - Only EXACT uniqueness excludes (2026-08-06, the f1 circuits
///   lesson): a unique column determines everything trivially, but a
///   near-unique one is legitimate hierarchy material; `rows_per_value`
///   keeps thin evidence visible instead of gated.
///
/// Two scans replace the per-pair query wave: one long-format pass
/// sizes every column's cells (filled, groups, modal), and one
/// self-join pass reduces every pool pair's agreement — the fan-out is
/// a join the engine performs, not a loop of statements. Display
/// strings are injective off the float lane, so grouping by them counts
/// what grouping by the raw column counted. Columns pair by pool index,
/// exactly the order the script enumerated, and `seq` carries it so the
/// body's array reproduces it.
pub(crate) async fn hierarchy_candidates(
    shared: &Arc<Shared>,
    resolved: &crate::prepass::Resolved,
    table: &str,
) -> Result<RecordBatch, SessionError> {
    let bad = |d: String| SessionError::BadSubject(format!("hierarchy_candidates('{table}'): {d}"));
    let provider = resolved
        .pin(table)
        .ok_or_else(|| bad("no such table in the bound dataset".into()))?;
    let ctx = shared
        .ctx
        .read()
        .expect("ctx lock")
        .clone()
        .ok_or_else(|| SessionError::Runtime("the session context is not wired".into()))?;
    let run = |plan| async {
        ctx.execute_logical_plan(plan)
            .await
            .map_err(|e| bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| bad(e.to_string()))
    };
    let abstain = |reason: &str| {
        rows_batch(
            vec![json!({"applicable": false, "reason": reason})],
            hierarchy_shape(),
        )
    };

    // Dimension-like columns, in schema order.
    let names: Vec<String> = provider
        .schema()
        .fields()
        .iter()
        .filter(|f| {
            let t = f.data_type().to_string();
            !(t.starts_with("Float") || t.starts_with("Decimal"))
        })
        .map(|f| f.name().clone())
        .collect();

    if names.is_empty() {
        // No long pass to ride: one count settles which abstention.
        let plan = plan_count(table, &provider).map_err(|e| bad(e.to_string()))?;
        let n = int_column(&run(plan).await?, "n").map_err(bad)?[0];
        return abstain(if n == 0 {
            "empty table"
        } else {
            "fewer than two dimension-like columns"
        });
    }

    // Pass one — every candidate column's cells reduced per column, one
    // scan through the long union.
    let indexed: Vec<(usize, String)> = names.iter().cloned().enumerate().collect();
    let plan = plan_colstat(table, &provider, &indexed).map_err(|e| bad(e.to_string()))?;
    let stats = run(plan).await?;

    #[derive(Clone, Default)]
    struct ColStat {
        groups: i64,
        modal: i64,
        distinct_vals: i64,
        filled: i64,
    }
    let mut by_ci: Vec<Option<ColStat>> = vec![None; names.len()];
    let mut n = 0i64;
    for b in stats.iter().filter(|b| b.num_rows() > 0) {
        let ci = int_column(&[b.clone()], "ci").map_err(&bad)?;
        let groups = int_column(&[b.clone()], "groups").map_err(&bad)?;
        let modal = int_column(&[b.clone()], "modal").map_err(&bad)?;
        let total = int_column(&[b.clone()], "total").map_err(&bad)?;
        let dv = int_column(&[b.clone()], "distinct_vals").map_err(&bad)?;
        let filled = int_column(&[b.clone()], "filled").map_err(&bad)?;
        for r in 0..b.num_rows() {
            n = total[r];
            by_ci[ci[r] as usize] = Some(ColStat {
                groups: groups[r],
                modal: modal[r],
                distinct_vals: dv[r],
                filled: filled[r],
            });
        }
    }
    if n == 0 {
        return abstain("empty table");
    }

    // The pool: at least two groups (NULL counted as one), and not
    // exactly unique.
    let pool: Vec<(usize, ColStat)> = by_ci
        .iter()
        .enumerate()
        .filter_map(|(ci, c)| c.clone().map(|c| (ci, c)))
        .filter(|(_, c)| c.groups >= 2 && !(c.filled > 0 && c.distinct_vals == c.filled))
        .collect();
    if pool.len() < 2 {
        return abstain("fewer than two dimension-like columns");
    }

    // Pass two — the pair fan-out as a self-join on row number,
    // restricted to pool columns, reduced to per-pair agreement.
    let pool_cols: Vec<(usize, String)> = pool
        .iter()
        .map(|(ci, _)| (*ci, names[*ci].clone()))
        .collect();
    let plan = plan_pairs(table, &provider, &pool_cols).map_err(|e| bad(e.to_string()))?;
    let paired = run(plan).await?;

    struct Pair {
        pair_groups: i64,
        agree_ab: i64,
        agree_ba: i64,
    }
    let mut pairs: std::collections::HashMap<(usize, usize), Pair> = Default::default();
    for b in paired.iter().filter(|b| b.num_rows() > 0) {
        let ca = int_column(&[b.clone()], "ca").map_err(&bad)?;
        let cb = int_column(&[b.clone()], "cb").map_err(&bad)?;
        let pg = int_column(&[b.clone()], "pair_groups").map_err(&bad)?;
        let ab = int_column(&[b.clone()], "agree_ab").map_err(&bad)?;
        let ba = int_column(&[b.clone()], "agree_ba").map_err(&bad)?;
        for r in 0..b.num_rows() {
            pairs.insert(
                (ca[r] as usize, cb[r] as usize),
                Pair {
                    pair_groups: pg[r],
                    agree_ab: ab[r],
                    agree_ba: ba[r],
                },
            );
        }
    }

    // One row per direction, in the script's enumeration order: pool
    // pairs by index, forward then reverse. All facts, no thresholds.
    let mut out = Vec::new();
    let mut seq = 0i64;
    for i in 0..pool.len() {
        for j in (i + 1)..pool.len() {
            let (ci_a, a) = &pool[i];
            let (ci_b, b) = &pool[j];
            let Some(p) = pairs.get(&(*ci_a, *ci_b)) else {
                continue;
            };
            let g3_ab = (n - p.agree_ab) as f64 / n as f64;
            let g3_ba = (n - p.agree_ba) as f64 / n as f64;
            for (from, to, g3, g3_rev, agree) in [
                ((*ci_a, a), (*ci_b, b), g3_ab, g3_ba, p.agree_ab),
                ((*ci_b, b), (*ci_a, a), g3_ba, g3_ab, p.agree_ba),
            ] {
                out.push(json!({
                    "applicable": true, "rows": n, "seq": seq,
                    "from_col": names[from.0], "to_col": names[to.0],
                    "distinct_from": from.1.groups, "distinct_to": to.1.groups,
                    "pair_groups": p.pair_groups,
                    "g3": g3, "g3_reverse": g3_rev,
                    "lambda": (agree - to.1.modal) as f64 / (n - to.1.modal) as f64,
                    "rows_per_value": n as f64 / from.1.groups as f64,
                }));
                seq += 1;
            }
        }
    }
    if out.is_empty() {
        out.push(json!({"applicable": true, "rows": n}));
    }
    rows_batch(out, hierarchy_shape())
}

/// `SELECT count(*)` over the pin.
fn plan_count(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
    LogicalPlanBuilder::scan(table, provider_as_source(Arc::clone(provider)), None)?
        .aggregate(
            Vec::<Expr>::new(),
            vec![count(lit(1)).alias("n")],
        )?
        .build()
}

/// The long union: one arm per named column, each row as
/// `(rid?, ci, val)` — the pool index as `ci`, the display form as
/// `val`. `rid` (a row number) rides only when the pair pass needs to
/// align arms row-for-row; identical arms enumerate identically.
fn plan_long(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
    with_rid: bool,
) -> datafusion::common::Result<LogicalPlanBuilder> {
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::{cast, col};

    let mut union: Option<LogicalPlanBuilder> = None;
    for (ci, name) in cols {
        let mut b =
            LogicalPlanBuilder::scan(table, provider_as_source(Arc::clone(provider)), None)?;
        let mut exprs = Vec::new();
        if with_rid {
            b = b.window(vec![row_number().alias("rid")])?;
            exprs.push(col("rid"));
        }
        exprs.push(lit(*ci as i64).alias("ci"));
        exprs.push(cast(ident(name), DataType::Utf8).alias("val"));
        let arm = b.project(exprs)?.build()?;
        union = Some(match union {
            None => LogicalPlanBuilder::from(arm),
            Some(u) => u.union(arm)?,
        });
    }
    Ok(union.expect("at least one column"))
}

/// Pass one: cells per (column, value) reduced to per-column statistics.
fn plan_colstat(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::logical_expr::col;

    plan_long(table, provider, cols, false)?
        .aggregate(
            vec![col("ci"), col("val")],
            vec![count(lit(1)).alias("c")],
        )?
        .aggregate(
            vec![col("ci")],
            vec![
                count(lit(1)).alias("groups"),
                max(col("c")).alias("modal"),
                sum(col("c")).alias("total"),
                count(lit(1))
                    .filter(col("val").is_not_null())
                    .build()?
                    .alias("distinct_vals"),
                sum(col("c"))
                    .filter(col("val").is_not_null())
                    .build()?
                    .alias("filled"),
            ],
        )?
        .build()
}

/// Pass two: the self-join on row number, cells per (pair, value pair),
/// reduced three ways and joined back — pair groups, and the summed
/// per-determinant maxima both directions.
fn plan_pairs(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::common::JoinType;
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::logical_expr::col;

    let a = plan_long(table, provider, cols, true)?.alias("a")?;
    let b = plan_long(table, provider, cols, true)?.alias("b")?.build()?;
    let cells = a
        .join(
            b,
            JoinType::Inner,
            (vec!["a.rid"], vec!["b.rid"]),
            Some(col("a.ci").lt(col("b.ci"))),
        )?
        .aggregate(
            vec![
                col("a.ci").alias("ca"),
                col("b.ci").alias("cb"),
                col("a.val").alias("av"),
                col("b.val").alias("bv"),
            ],
            vec![count(lit(1)).alias("c")],
        )?
        .build()?;

    let pg = LogicalPlanBuilder::from(cells.clone())
        .aggregate(
            vec![col("ca"), col("cb")],
            vec![count(lit(1)).alias("pair_groups")],
        )?
        .build()?;
    let fwd = LogicalPlanBuilder::from(cells.clone())
        .aggregate(
            vec![col("ca"), col("cb"), col("av")],
            vec![max(col("c")).alias("mx")],
        )?
        .aggregate(
            vec![col("ca"), col("cb")],
            vec![sum(col("mx")).alias("agree_ab")],
        )?
        .build()?;
    let rev = LogicalPlanBuilder::from(cells)
        .aggregate(
            vec![col("ca"), col("cb"), col("bv")],
            vec![max(col("c")).alias("mx")],
        )?
        .aggregate(
            vec![col("ca"), col("cb")],
            vec![sum(col("mx")).alias("agree_ba")],
        )?
        .build()?;

    LogicalPlanBuilder::from(pg)
        .alias("pg")?
        .join(
            LogicalPlanBuilder::from(fwd).alias("f")?.build()?,
            JoinType::Inner,
            (vec!["pg.ca", "pg.cb"], vec!["f.ca", "f.cb"]),
            None,
        )?
        .join(
            LogicalPlanBuilder::from(rev).alias("r")?.build()?,
            JoinType::Inner,
            (vec!["pg.ca", "pg.cb"], vec!["r.ca", "r.cb"]),
            None,
        )?
        .project(vec![
            col("pg.ca").alias("ca"),
            col("pg.cb").alias("cb"),
            col("pair_groups"),
            col("agree_ab"),
            col("agree_ba"),
        ])?
        .build()
}

/// A named Int64 column of a one-batch result, materialized.
fn int_column(batches: &[RecordBatch], name: &str) -> Result<Vec<i64>, String> {
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| format!("`{name}`: the plan returned nothing"))?;
    let idx = b.schema().index_of(name).map_err(|e| e.to_string())?;
    let a = b
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("`{name}` did not read as an integer"))?;
    Ok((0..b.num_rows()).map(|r| a.value(r)).collect())
}

fn hierarchy_shape() -> Vec<Field> {
    vec![
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("rows", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("from_col", DataType::Utf8, true),
        Field::new("to_col", DataType::Utf8, true),
        Field::new("distinct_from", DataType::Int64, true),
        Field::new("distinct_to", DataType::Int64, true),
        Field::new("pair_groups", DataType::Int64, true),
        Field::new("g3", DataType::Float64, true),
        Field::new("g3_reverse", DataType::Float64, true),
        Field::new("lambda", DataType::Float64, true),
        Field::new("rows_per_value", DataType::Float64, true),
    ]
}


fn derivation_shape() -> Vec<Field> {
    vec![
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("rows", DataType::Int64, true),
        Field::new("truncated", DataType::Boolean, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("form", DataType::Utf8, true),
        Field::new("operand_1", DataType::Utf8, true),
        Field::new("operand_2", DataType::Utf8, true),
        Field::new("support", DataType::Int64, true),
        Field::new("violations", DataType::Int64, true),
        Field::new("match_rate", DataType::Float64, true),
    ]
}

/// A door's fixed shape, decoded from JSON rows through the format's
/// own decoder — the same trick the profile aggregate uses, so there is
/// no hand-built array assembly to drift.
fn rows_batch(rows: Vec<Value>, fields: Vec<Field>) -> Result<RecordBatch, SessionError> {
    let schema = Arc::new(Schema::new(fields));
    let mut decoder = arrow_json::ReaderBuilder::new(schema)
        .build_decoder()
        .map_err(|e| SessionError::Runtime(e.to_string()))?;
    decoder
        .serialize(&rows)
        .map_err(|e| SessionError::Runtime(e.to_string()))?;
    decoder
        .flush()
        .map_err(|e| SessionError::Runtime(e.to_string()))?
        .ok_or_else(|| SessionError::Runtime("a search door emitted no rows".into()))
}
