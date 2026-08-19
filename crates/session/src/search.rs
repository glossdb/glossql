//! The search doors (stage 5, §7e). A search enumerates candidates over
//! an arbitrary table's columns, which a static SQL body cannot spell:
//! the input schema varies while the output schema is fixed
//! (functions-split §7, the table-function asymmetry). So a search
//! computes in the pre-pass like every compute door — over the
//! statement's own pins, so it can never straddle a landing — and
//! optimizes recall: no thresholds here, the judgment lives in the
//! measurement body that reads the door.

use std::collections::HashSet;
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
/// Why this exists: a scoped unit-mix artifact and
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

    let ctx = shared.session_ctx();
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
/// (analysis/hierarchies), one row per screened
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
/// - Only EXACT uniqueness excludes: a unique column determines
///   everything trivially, but a
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
    let ctx = shared.session_ctx();
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
pub(crate) fn int_column(batches: &[RecordBatch], name: &str) -> Result<Vec<i64>, String> {
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
pub(crate) fn rows_batch(rows: Vec<Value>, fields: Vec<Field>) -> Result<RecordBatch, SessionError> {
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

/// `relationship_candidates('dataset')` — the high-recall half of the
/// candidate → verified → declared arc (fixture 12): every plausible
/// join pair across the landed tables, generous by design —
/// the statistical pass optimizes recall and the judge
/// removes false positives against the data; this door never does.
///
/// The algorithm (a per-pair SQL join would be quadratic in engine
/// round-trips): inclusion-dependency discovery on
/// the SPIDER/SINDY shape — every candidate column's distinct values
/// land once through one union-of-distincts plan, and containment
/// between two columns is a set intersection in memory. Values compare
/// by display form, which is equality-faithful inside one dtype (pairs
/// are dtype-gated, and even floats print shortest-roundtrip), so the
/// counts are the typed-key counts. The statistic is containment —
/// matched over the from side's distinct count: the question is
/// directional, and a small key set fully inside a large one scores
/// perfectly whatever the size skew. The only pruning is
/// algebra: a to side with fewer than half the from side's distinct
/// values cannot reach the 0.5 bar. Exact while Σ distinct fits
/// memory; the named ladder past that is BINDER-style hash-range
/// partitioning and bottom-k sketches — not built until a dataset
/// needs them.
///
/// The composite rescue: a to
/// side that is no key alone can be one inside a scope — the
/// multi-tenant shape, (businessID, name). For each overlapping pair
/// whose to side is not key-like, the co-present pairs between the
/// same two tables are tried as the scoping leg in overlap order; the
/// first whose combination makes the to side near-unique and whose
/// two-leg intersection resolves rescues the anchor. Width 2 only —
/// wider composites stay future work. Data decides, not names.
pub(crate) async fn relationship_candidates(
    shared: &Arc<Shared>,
    resolved: &crate::prepass::Resolved,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    use std::collections::HashMap;

    use datafusion::functions_aggregate::expr_fn::count_distinct;
    use datafusion::logical_expr::cast;

    let bad =
        |d: String| SessionError::BadSubject(format!("relationship_candidates('{dataset}'): {d}"));
    let ctx = shared.session_ctx();
    let run = |plan| async {
        ctx.execute_logical_plan(plan)
            .await
            .map_err(|e| bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| bad(e.to_string()))
    };

    let mut tables: Vec<String> = resolved.tables();
    tables.sort();

    // Per-column shape: filled and distinct counts decide which columns
    // look like keys (the join's to side) — near-unique, not strictly
    // unique, so dirty keys stay in the running.
    #[derive(Clone)]
    struct ColShape {
        table: String,
        column: String,
        dtype: String,
        filled: i64,
        distinct: i64,
        unique: bool,
        key_like: bool,
    }
    let mut cols: Vec<ColShape> = Vec::new();
    for t in &tables {
        let provider = resolved.pin(t).ok_or_else(|| bad(format!("no pin for `{t}`")))?;
        let fields = provider.schema();
        if fields.fields().is_empty() {
            continue;
        }
        let mut aggs = Vec::new();
        for f in fields.fields() {
            let c = f.name();
            aggs.push(count(ident(c)).alias(format!("f_{c}")));
            aggs.push(count_distinct(ident(c)).alias(format!("d_{c}")));
        }
        let plan = LogicalPlanBuilder::scan(t.as_str(), provider_as_source(provider), None)
            .and_then(|b| b.aggregate(Vec::<Expr>::new(), aggs))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        let batches = run(plan).await?;
        let one = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| bad(format!("the shape scan of `{t}` returned nothing")))?;
        for (i, f) in fields.fields().iter().enumerate() {
            let int = |col_idx: usize| -> Result<i64, SessionError> {
                one.column(col_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| bad("a count did not read as an integer".into()))
                    .map(|a| a.value(0))
            };
            let filled = int(i * 2)?;
            let distinct = int(i * 2 + 1)?;
            cols.push(ColShape {
                table: t.clone(),
                column: f.name().clone(),
                dtype: f.data_type().to_string(),
                filled,
                distinct,
                unique: filled > 0 && distinct == filled,
                key_like: filled > 0
                    && distinct >= 2
                    && distinct as f64 / filled as f64 >= 0.9,
            });
        }
    }

    // Every type-compatible pair, keys or not — the non-key pairs are
    // the raw material for composite rescue. Same-table pairs stay in.
    let mut specs: Vec<(usize, usize)> = Vec::new(); // (from, to) into cols
    for (ki, k) in cols.iter().enumerate() {
        if k.distinct < 2 {
            continue;
        }
        for (fi, f) in cols.iter().enumerate() {
            if f.table == k.table && f.column == k.column {
                continue;
            }
            if f.dtype != k.dtype || f.distinct == 0 {
                continue;
            }
            // Implied by the acceptance bar, never a heuristic:
            // matched ≤ to_distinct, so a to side under half the from
            // side's distinct count cannot reach 0.5.
            if (k.distinct as f64) < 0.5 * f.distinct as f64 {
                continue;
            }
            specs.push((fi, ki));
        }
    }

    // One distinct pass per involved column, all in one union plan.
    let mut involved: Vec<usize> = Vec::new();
    for (fi, ki) in &specs {
        for side in [*fi, *ki] {
            if !involved.contains(&side) {
                involved.push(side);
            }
        }
    }
    let mut sets: HashMap<usize, HashSet<String>> = HashMap::new();
    if !involved.is_empty() {
        let mut union: Option<LogicalPlanBuilder> = None;
        for side in &involved {
            let shape = &cols[*side];
            let provider = resolved
                .pin(&shape.table)
                .ok_or_else(|| bad(format!("no pin for `{}`", shape.table)))?;
            let arm =
                LogicalPlanBuilder::scan(shape.table.as_str(), provider_as_source(provider), None)
                    .and_then(|b| b.filter(ident(&shape.column).is_not_null()))
                    .and_then(|b| {
                        b.project(vec![
                            lit(*side as i64).alias("ci"),
                            cast(ident(&shape.column), DataType::Utf8).alias("val"),
                        ])
                    })
                    .and_then(|b| b.distinct())
                    .and_then(|b| b.build())
                    .map_err(|e| bad(e.to_string()))?;
            union = Some(match union {
                None => LogicalPlanBuilder::from(arm),
                Some(u) => u.union(arm).map_err(|e| bad(e.to_string()))?,
            });
        }
        let plan = union
            .expect("nonempty")
            .build()
            .map_err(|e| bad(e.to_string()))?;
        for b in run(plan).await?.iter().filter(|b| b.num_rows() > 0) {
            let ci = int_column(std::slice::from_ref(b), "ci").map_err(&bad)?;
            let val_idx = b.schema().index_of("val").map_err(|e| bad(e.to_string()))?;
            let vals = b.column(val_idx);
            for r in 0..b.num_rows() {
                if vals.is_null(r) {
                    continue;
                }
                sets.entry(ci[r] as usize).or_default().insert(
                    array_value_to_string(vals, r).map_err(|e| bad(e.to_string()))?,
                );
            }
        }
    }

    // Pair mathematics, entirely in memory.
    struct Pair {
        f: usize,
        k: usize,
        overlap: f64,
        matched: i64,
    }
    let empty = HashSet::new();
    let set_of = |i: usize| sets.get(&i).unwrap_or(&empty);
    let mut pairs: Vec<Pair> = Vec::new();
    for (fi, ki) in &specs {
        let m = set_of(*fi).intersection(set_of(*ki)).count() as i64;
        let overlap = m as f64 / cols[*fi].distinct as f64;
        if overlap < 0.5 {
            continue;
        }
        pairs.push(Pair {
            f: *fi,
            k: *ki,
            overlap,
            matched: m,
        });
    }

    // Single-column candidates: the pairs whose to side stands as a key
    // on its own.
    #[derive(Clone)]
    struct Candidate {
        from: String,
        to: String,
        cardinality: &'static str,
        overlap: f64,
        matched: i64,
        orphans: i64,
        from_distinct: i64,
        to_distinct: i64,
        key_columns: Option<(String, String)>,
    }
    let path = |i: usize| format!("{}.{}", cols[i].table, cols[i].column);
    let mut candidates: Vec<Candidate> = Vec::new();
    for p in &pairs {
        if !cols[p.k].key_like {
            continue;
        }
        candidates.push(Candidate {
            from: path(p.f),
            to: path(p.k),
            cardinality: if cols[p.f].unique {
                "one-to-one"
            } else {
                "many-to-one"
            },
            overlap: p.overlap,
            matched: p.matched,
            orphans: cols[p.f].distinct - p.matched,
            from_distinct: cols[p.f].distinct,
            to_distinct: cols[p.k].distinct,
            key_columns: None,
        });
    }

    // Composite rescue, three batched phases.
    struct Attempt {
        p: usize, // into pairs — the anchor leg
        s: usize,
        order: i64,
    }
    let mut attempts: Vec<Attempt> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (pi, p) in pairs.iter().enumerate() {
        if cols[p.k].key_like {
            continue;
        }
        let mut scopes: Vec<usize> = (0..pairs.len())
            .filter(|si| {
                let s = &pairs[*si];
                cols[s.f].table == cols[p.f].table
                    && cols[s.k].table == cols[p.k].table
                    && cols[s.f].column != cols[p.f].column
                    && cols[s.k].column != cols[p.k].column
            })
            .collect();
        scopes.sort_by(|a, b| pairs[*b].overlap.partial_cmp(&pairs[*a].overlap).unwrap());
        let mut order = 0i64;
        for si in scopes {
            let s = &pairs[si];
            // Implied by the near-unique bar, never a heuristic: the
            // combined to side's distinct pairs cannot exceed the
            // product of the legs' distinct counts — a product under
            // 0.9 of the co-filled floor can never key the table.
            let floor = cols[p.k].filled.min(cols[s.k].filled);
            if (cols[p.k].distinct as f64) * (cols[s.k].distinct as f64)
                < 0.9 * floor as f64
            {
                continue;
            }
            let key = format!(
                "{}|{}|{}+{}|{}+{}",
                cols[p.f].table,
                cols[p.k].table,
                cols[p.f].column,
                cols[s.f].column,
                cols[p.k].column,
                cols[s.k].column
            );
            let mirror = format!(
                "{}|{}|{}+{}|{}+{}",
                cols[p.f].table,
                cols[p.k].table,
                cols[s.f].column,
                cols[p.f].column,
                cols[s.k].column,
                cols[p.k].column
            );
            if seen.contains(&key) || seen.contains(&mirror) {
                continue;
            }
            seen.insert(key);
            attempts.push(Attempt { p: pi, s: si, order });
            order += 1;
        }
    }

    // Phase 1, one union plan and one count plan per combination: is
    // the combined to side a key inside the scope? The same combination
    // backs many attempts — probed once.
    let mut to_combos: Vec<(String, String, String)> = Vec::new();
    let mut to_of: HashMap<(String, String, String), usize> = HashMap::new();
    for a in &attempts {
        let (p, s) = (&pairs[a.p], &pairs[a.s]);
        let key = (
            cols[p.k].table.clone(),
            cols[p.k].column.clone(),
            cols[s.k].column.clone(),
        );
        if !to_of.contains_key(&key) {
            to_of.insert(key.clone(), to_combos.len());
            to_combos.push(key);
        }
    }
    let to_stats = combo_stats(&ctx, resolved, &format!("relationship_candidates('{dataset}')"), &to_combos).await?;
    let to_ok = |i: usize| {
        let t = &to_stats[i];
        t.filled > 0 && t.distinct >= 2 && t.distinct as f64 / t.filled as f64 >= 0.9
    };

    // Phase 2, survivors only: the from side's co-present pairs.
    let mut from_combos: Vec<(String, String, String)> = Vec::new();
    let mut from_of: HashMap<(String, String, String), usize> = HashMap::new();
    for a in &attempts {
        let (p, s) = (&pairs[a.p], &pairs[a.s]);
        let tk = (
            cols[p.k].table.clone(),
            cols[p.k].column.clone(),
            cols[s.k].column.clone(),
        );
        if !to_ok(to_of[&tk]) {
            continue;
        }
        let key = (
            cols[p.f].table.clone(),
            cols[p.f].column.clone(),
            cols[s.f].column.clone(),
        );
        if !from_of.contains_key(&key) {
            from_of.insert(key.clone(), from_combos.len());
            from_combos.push(key);
        }
    }
    let from_stats = combo_stats(&ctx, resolved, &format!("relationship_candidates('{dataset}')"), &from_combos).await?;

    // Phase 3, no scans: the two-leg resolution is a set intersection.
    // First passing scope per anchor, in overlap order.
    struct Rescue {
        order: i64,
        p: usize,
        s: usize,
        matched: i64,
        overlap: f64,
        to_distinct: i64,
        ffilled: i64,
        fpairs: i64,
    }
    let mut rescued: HashMap<usize, Rescue> = HashMap::new();
    for a in &attempts {
        let (p, s) = (&pairs[a.p], &pairs[a.s]);
        let tk = (
            cols[p.k].table.clone(),
            cols[p.k].column.clone(),
            cols[s.k].column.clone(),
        );
        let ti = to_of[&tk];
        if !to_ok(ti) {
            continue;
        }
        let fk = (
            cols[p.f].table.clone(),
            cols[p.f].column.clone(),
            cols[s.f].column.clone(),
        );
        let fs = &from_stats[from_of[&fk]];
        if fs.set.is_empty() {
            continue;
        }
        let m = fs.set.intersection(&to_stats[ti].set).count() as i64;
        let overlap = m as f64 / fs.set.len() as f64;
        if overlap < 0.5 {
            continue;
        }
        if rescued.get(&a.p).is_some_and(|r| r.order <= a.order) {
            continue;
        }
        rescued.insert(
            a.p,
            Rescue {
                order: a.order,
                p: a.p,
                s: a.s,
                matched: m,
                overlap,
                to_distinct: to_stats[ti].distinct,
                ffilled: fs.filled,
                fpairs: fs.set.len() as i64,
            },
        );
    }
    let mut anchor_keys: Vec<usize> = rescued.keys().copied().collect();
    anchor_keys.sort_unstable();
    for akey in anchor_keys {
        let r = &rescued[&akey];
        // The anchor is the identifying leg — the higher-cardinality to
        // side; the scope is the tenant leg. Which pair triggered the
        // rescue is iteration order, not evidence.
        let (mut a, mut sc) = (&pairs[r.p], &pairs[r.s]);
        if cols[pairs[r.s].k].distinct > cols[pairs[r.p].k].distinct {
            (a, sc) = (&pairs[r.s], &pairs[r.p]);
        }
        candidates.push(Candidate {
            from: path(a.f),
            to: path(a.k),
            cardinality: if r.fpairs == r.ffilled {
                "one-to-one"
            } else {
                "many-to-one"
            },
            overlap: r.overlap,
            matched: r.matched,
            orphans: r.fpairs - r.matched,
            from_distinct: r.fpairs,
            to_distinct: r.to_distinct,
            key_columns: Some((path(sc.f), path(sc.k))),
        });
    }

    // One row per candidate, seq in push order — the body's tie-breaker
    // under overlap DESC, which reproduces the script's stable sort.
    let mut out = Vec::new();
    for (seq, c) in candidates.iter().enumerate() {
        let mut row = serde_json::Map::new();
        row.insert("seq".into(), json!(seq as i64));
        row.insert("from_col".into(), json!(c.from));
        row.insert("to_col".into(), json!(c.to));
        row.insert("cardinality".into(), json!(c.cardinality));
        row.insert("overlap".into(), json!(c.overlap));
        row.insert("matched".into(), json!(c.matched));
        row.insert("orphans".into(), json!(c.orphans));
        row.insert("from_distinct".into(), json!(c.from_distinct));
        row.insert("to_distinct".into(), json!(c.to_distinct));
        if let Some((kf, kt)) = &c.key_columns {
            row.insert("kc_from".into(), json!(kf));
            row.insert("kc_to".into(), json!(kt));
        }
        out.push(Value::Object(row));
    }
    if out.is_empty() {
        out.push(json!({}));
    }
    rows_batch(out, relationship_shape())
}


/// One combination's co-presence: how many rows carry both legs, and
/// the distinct (a, b) pairs — probed once per combination through one
/// union-of-distincts plan and one union of counts.
struct ComboStat {
    filled: i64,
    distinct: i64,
    set: std::collections::HashSet<(String, String)>,
}

async fn combo_stats(
    ctx: &datafusion::prelude::SessionContext,
    resolved: &crate::prepass::Resolved,
    door: &str,
    combos: &[(String, String, String)],
) -> Result<Vec<ComboStat>, SessionError> {
    use datafusion::logical_expr::{cast, col};

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let run = |plan| async {
        ctx.execute_logical_plan(plan)
            .await
            .map_err(|e| bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| bad(e.to_string()))
    };
    let mut out: Vec<ComboStat> = Vec::new();
    if combos.is_empty() {
        return Ok(out);
    }

    let mut union: Option<LogicalPlanBuilder> = None;
    let mut count_plans = Vec::new();
    for (id, (table, a, b)) in combos.iter().enumerate() {
        let provider = resolved
            .pin(table)
            .ok_or_else(|| bad(format!("no pin for `{table}`")))?;
        let both = ident(a).is_not_null().and(ident(b).is_not_null());
        let arm = LogicalPlanBuilder::scan(
            table.as_str(),
            provider_as_source(Arc::clone(&provider)),
            None,
        )
        .and_then(|p| p.filter(both.clone()))
        .and_then(|p| {
            p.project(vec![
                lit(id as i64).alias("ci"),
                cast(ident(a), DataType::Utf8).alias("va"),
                cast(ident(b), DataType::Utf8).alias("vb"),
            ])
        })
        .and_then(|p| p.distinct())
        .and_then(|p| p.build())
        .map_err(|e| bad(e.to_string()))?;
        union = Some(match union {
            None => LogicalPlanBuilder::from(arm),
            Some(u) => u.union(arm).map_err(|e| bad(e.to_string()))?,
        });
        count_plans.push(
            LogicalPlanBuilder::scan(
                table.as_str(),
                provider_as_source(provider),
                None,
            )
            .and_then(|p| p.filter(both))
            .and_then(|p| {
                p.aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("c")])
            })
            .and_then(|p| {
                p.project(vec![lit(id as i64).alias("ci"), col("c")])
            })
            .and_then(|p| p.build())
            .map_err(|e| bad(e.to_string()))?,
        );
    }
    for _ in 0..combos.len() {
        out.push(ComboStat {
            filled: 0,
            distinct: 0,
            set: HashSet::new(),
        });
    }
    let mut counts_union: Option<LogicalPlanBuilder> = None;
    for p in count_plans {
        counts_union = Some(match counts_union {
            None => LogicalPlanBuilder::from(p),
            Some(u) => u.union(p).map_err(|e| bad(e.to_string()))?,
        });
    }
    let counts = counts_union
        .expect("nonempty")
        .build()
        .map_err(|e| bad(e.to_string()))?;
    for b in run(counts).await?.iter().filter(|b| b.num_rows() > 0) {
        let ci = int_column(std::slice::from_ref(b), "ci").map_err(bad)?;
        let c = int_column(std::slice::from_ref(b), "c").map_err(bad)?;
        for r in 0..b.num_rows() {
            out[ci[r] as usize].filled = c[r];
        }
    }
    let plan = union
        .expect("nonempty")
        .build()
        .map_err(|e| bad(e.to_string()))?;
    for b in run(plan).await?.iter().filter(|b| b.num_rows() > 0) {
        let ci = int_column(std::slice::from_ref(b), "ci").map_err(bad)?;
        let va_idx = b.schema().index_of("va").map_err(|e| bad(e.to_string()))?;
        let vb_idx = b.schema().index_of("vb").map_err(|e| bad(e.to_string()))?;
        let (va, vb) = (b.column(va_idx), b.column(vb_idx));
        for r in 0..b.num_rows() {
            let pair = (
                array_value_to_string(va, r).map_err(|e| bad(e.to_string()))?,
                array_value_to_string(vb, r).map_err(|e| bad(e.to_string()))?,
            );
            out[ci[r] as usize].set.insert(pair);
        }
    }
    for s in &mut out {
        s.distinct = s.set.len() as i64;
    }
    Ok(out)
}

fn relationship_shape() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("from_col", DataType::Utf8, true),
        Field::new("to_col", DataType::Utf8, true),
        Field::new("cardinality", DataType::Utf8, true),
        Field::new("overlap", DataType::Float64, true),
        Field::new("matched", DataType::Int64, true),
        Field::new("orphans", DataType::Int64, true),
        Field::new("from_distinct", DataType::Int64, true),
        Field::new("to_distinct", DataType::Int64, true),
        Field::new("kc_from", DataType::Utf8, true),
        Field::new("kc_to", DataType::Utf8, true),
    ]
}

/// `grounding_collisions('dataset')` — two concepts grounding to the
/// same extract make every ratio between them compute 1.0, silently:
/// the cheapest wrong number there is. Every current QUERY grounding
/// buckets by its canonical SQL (parse-and-re-render, so spelling
/// differences collapse and identifiers survive); a bucket holding two
/// or more concepts is a collision — reported, never resolved:
/// deliberate synonyms exist, and telling them from errors is the
/// judge's call against the definitions, never this door's.
///
/// Two bucketings:
/// canonical SQL catches respelled extracts, and the SERVED monthly
/// series catches what canonicalization cannot — revenue and
/// ar_open_items carried different SQL yet served identical totals in
/// every month, the exact failure this read exists to catch. A series
/// collision is reported only when the canonical SQL differs (else the
/// SQL bucket already carries it). A grounding whose SQL does not plan
/// or run cannot serve a number and cannot collide — skipped, not
/// failed.
pub(crate) async fn grounding_collisions(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    use std::collections::BTreeMap;

    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let slots = current_query_slots(shared, &rctx, dataset).await?;

    // Bucket by canonical SQL.
    let mut buckets: BTreeMap<String, Vec<(&str, &str)>> = BTreeMap::new();
    let mut groundings = 0i64;
    let mut grounded: Vec<(&QuerySlot, Value, String)> = Vec::new();
    for slot in &slots {
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };
        groundings += 1;
        let canon = canonical_sql(sql);
        buckets
            .entry(canon.clone())
            .or_default()
            .push((slot.aspect.as_str(), slot.subject.as_str()));
        grounded.push((slot, body.clone(), canon));
    }

    struct Collision {
        kind: &'static str,
        sql: String,
        months: Option<i64>,
        aspects: Vec<String>,
        subjects: Vec<String>,
    }
    let dedup_sorted = |items: Vec<&str>| {
        let mut seen = Vec::new();
        for i in items {
            if !seen.contains(&i.to_string()) {
                seen.push(i.to_string());
            }
        }
        seen.sort();
        seen
    };
    let mut collisions: Vec<Collision> = Vec::new();
    for (canon, members) in &buckets {
        // Two or more distinct concepts on one extract; one concept
        // glossed on two subjects is scope, not collision.
        let aspects = dedup_sorted(members.iter().map(|(a, _)| *a).collect());
        if aspects.len() < 2 {
            continue;
        }
        collisions.push(Collision {
            kind: "sql",
            sql: canon.clone(),
            months: None,
            aspects,
            subjects: dedup_sorted(members.iter().map(|(_, s)| *s).collect()),
        });
    }

    // The served-series pass: fingerprint each grounding's monthly
    // totals at its own verb (flows sum; a marked stock sums the latest
    // observed date — metric_cube's verb).
    let mut series_buckets: BTreeMap<String, Vec<(&str, &str, &str)>> = BTreeMap::new();
    for (slot, body, canon) in &grounded {
        let sql = body["sql"].as_str().expect("filtered above");
        let Some(fp) = series_fingerprint(shared, &ctx, sql, body).await else {
            continue;
        };
        series_buckets
            .entry(fp)
            .or_default()
            .push((slot.aspect.as_str(), slot.subject.as_str(), canon.as_str()));
    }
    let mut series_collisions: Vec<Collision> = Vec::new();
    for (fp, members) in &series_buckets {
        let aspects = dedup_sorted(members.iter().map(|(a, _, _)| *a).collect());
        let canons: HashSet<&str> = members.iter().map(|(_, _, c)| *c).collect();
        if aspects.len() < 2 || canons.len() < 2 {
            continue;
        }
        series_collisions.push(Collision {
            kind: "served_series",
            sql: String::new(),
            months: Some(fp.split(';').count() as i64 - 1),
            aspects,
            subjects: dedup_sorted(members.iter().map(|(_, s, _)| *s).collect()),
        });
    }
    series_collisions.sort_by(|a, b| a.aspects[0].cmp(&b.aspects[0]));
    collisions.extend(series_collisions);

    let mut out = Vec::new();
    for (seq, c) in collisions.iter().enumerate() {
        out.push(json!({
            "groundings": groundings, "seq": seq as i64,
            "kind": c.kind, "sql": c.sql, "months": c.months,
            "aspects": c.aspects, "subjects": c.subjects,
        }));
    }
    if out.is_empty() {
        out.push(json!({ "groundings": groundings }));
    }
    rows_batch(out, collision_shape())
}

/// One current QUERY grounding — a collapsed slot.
pub(crate) struct QuerySlot {
    pub subject: String,
    pub aspect: String,
    pub body: String,
}

/// The current QUERY groundings the metric doors run — the store's own
/// collapsed read (supersession, human over agent, contested withheld,
/// SPEC.md §5.3), narrowed to QUERY aspects: one slot per
/// (subject, aspect) that serves a value. A contested grounding never
/// enters a cube, a walk, or a collision bucket. In slot-key order.
pub(crate) async fn current_query_slots(
    shared: &Arc<Shared>,
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
) -> Result<Vec<QuerySlot>, SessionError> {
    let query_aspects: HashSet<&str> = rctx
        .aspects
        .iter()
        .filter(|a| a.kind == "query")
        .map(|a| a.name.as_str())
        .collect();
    let scope = glossql_glossary::Scope::Dataset;
    let verdicts = crate::reads::verdicts(shared, rctx, dataset, &scope, None).await?;
    let mut slots: Vec<QuerySlot> =
        glossql_glossary::Store::collapsed_read(dataset, &scope, None, rctx, &verdicts)
            .into_iter()
            .filter(|r| query_aspects.contains(r.aspect.as_str()))
            .filter_map(|r| {
                r.value.map(|body| QuerySlot {
                    subject: r.subject,
                    aspect: r.aspect,
                    body,
                })
            })
            .collect();
    slots.sort_by(|a, b| (&a.subject, &a.aspect).cmp(&(&b.subject, &b.aspect)));
    Ok(slots)
}

/// One grounding's monthly fingerprint, `None` when it cannot serve a
/// number: no `value` column, no time column, or any planning or
/// execution failure — the script's try/catch, spelled out.
async fn series_fingerprint(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    body: &Value,
) -> Option<String> {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await.ok()?;
    let fields = probe.schema();
    let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
    // The same verb rule every monthly reader applies: a ratio serves
    // `num` and `den`, a marked stock sums the latest observed date,
    // everything else sums the month.
    let is_ratio = has("num") && has("den");
    if !is_ratio && !has("value") {
        return None;
    }
    let tcol = crate::whatif::date_column(fields.fields())?;
    let verb = if is_ratio {
        "ratio"
    } else if body.get("behavior").and_then(Value::as_str) == Some("stock") {
        "stock"
    } else {
        "flow"
    };
    let q = monthly_sql(sql, &tcol, verb);
    let plan = Box::pin(crate::whatif::build_plan(shared, ctx, &q)).await.ok()?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .ok()?
        .collect()
        .await
        .ok()?;
    let mut fp = String::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let period = b.column(b.schema().index_of("period").ok()?);
        let value = b.column(b.schema().index_of("value").ok()?);
        let floats = cast_with_options(
            value,
            &DataType::Float64,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )
        .ok()?;
        let floats = floats.as_any().downcast_ref::<Float64Array>()?;
        for i in 0..b.num_rows() {
            if floats.is_null(i) {
                // The script's arithmetic threw on a null value and the
                // catch dropped the grounding whole.
                return None;
            }
            let r = (floats.value(i) * 100.0).round() / 100.0;
            let p = array_value_to_string(period, i).ok()?;
            fp.push_str(&format!("{p}={r};"));
        }
    }
    (!fp.is_empty()).then_some(fp)
}

/// SQL text as an identity: parse and re-render, so spelling differences
/// collapse and identifiers survive verbatim. A body the parser cannot
/// read normalizes by whitespace alone — weaker, honestly so.
fn canonical_sql(sql: &str) -> String {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    match Parser::parse_sql(&GenericDialect {}, sql) {
        Ok(statements) if statements.len() == 1 => statements[0].to_string(),
        _ => sql.split_whitespace().collect::<Vec<_>>().join(" "),
    }
}

fn collision_shape() -> Vec<Field> {
    vec![
        Field::new("groundings", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("sql", DataType::Utf8, true),
        Field::new("months", DataType::Int64, true),
        Field::new(
            "aspects",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "subjects",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
    ]
}

/// `relationship_checks('dataset')` — what the declared joins assert,
/// checked against the rows: DECLARED relationships only — the
/// candidate plane proposes, the judge declares, this door measures
/// what the declaration now claims. Two facts per relationship, both
/// invisible to every column-shaped check (measured: tier0,
/// PSI and the classifier two-sample test all scored exactly 0.0 on
/// every referential defect on a high-cardinality key — the join is the
/// only instrument):
///
///   orphans   from-side values that resolve to no to-side row. Exact,
///             catches the invented-key shapes including the repeated
///             orphan that defeats rare-category counting.
///   temporal  for each date column pair across the join (capped at
///             three per side), how often the child's date precedes its
///             parent's. Evidence, not verdict: a payment before its
///             invoice's DUE date is ordinary; before the invoice
///             EXISTS is the trace a wrong pairing leaves. The judge
///             reads the pair names.
///
/// The composite fix rides this door (the foundation report §7a): a
/// tuple endpoint — `t.(a, b)`, the key since fixture 14 — joins on
/// every leg, where the script it replaces dropped any endpoint
/// containing `(` as a crash guard and was blind to booksql's entire
/// declared graph. Endpoints whose sides disagree in width cannot join
/// and are skipped, like the malformed paths the script skipped.
pub(crate) async fn relationship_checks(
    shared: &Arc<Shared>,
    resolved: &crate::prepass::Resolved,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    use datafusion::common::JoinType;

    let bad =
        |d: String| SessionError::BadSubject(format!("relationship_checks('{dataset}'): {d}"));
    let ctx = shared.session_ctx();
    let run = |plan| async {
        ctx.execute_logical_plan(plan)
            .await
            .map_err(|e| bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| bad(e.to_string()))
    };

    use crate::behavior::endpoint;

    struct Entry {
        relationship: String,
        ct: String,
        cc: Vec<String>,
        pt: String,
        pc: Vec<String>,
    }
    let mut entries = Vec::new();
    for row in shared.store.relation_rows("relationships").await? {
        let (Some(left), Some(op), Some(right)) = (&row[1], &row[2], &row[3]) else {
            continue;
        };
        let (Some((ct, cc)), Some((pt, pc))) = (endpoint(left), endpoint(right)) else {
            continue;
        };
        if cc.len() != pc.len() {
            continue;
        }
        entries.push(Entry {
            relationship: format!("{left} {op} {right}"),
            ct,
            cc,
            pt,
            pc,
        });
    }
    if entries.is_empty() {
        return rows_batch(
            vec![json!({"applicable": false, "reason": "no declared relationships"})],
            coherence_shape(),
        );
    }

    // Date columns per involved table, once, from the pins' own schema.
    let mut date_cols: std::collections::HashMap<&str, Vec<String>> = Default::default();
    for e in &entries {
        for t in [e.ct.as_str(), e.pt.as_str()] {
            if date_cols.contains_key(t) {
                continue;
            }
            let provider = resolved
                .pin(t)
                .ok_or_else(|| bad(format!("no table `{t}` in the bound dataset")))?;
            let dates: Vec<String> = provider
                .schema()
                .fields()
                .iter()
                .filter(|f| crate::whatif::is_temporal(f.data_type()))
                .take(3)
                .map(|f| f.name().clone())
                .collect();
            date_cols.insert(t, dates);
        }
    }

    let mut out = Vec::new();
    for (seq, e) in entries.iter().enumerate() {
        let child = resolved
            .pin(&e.ct)
            .ok_or_else(|| bad(format!("no table `{}` in the bound dataset", e.ct)))?;
        let parent = resolved
            .pin(&e.pt)
            .ok_or_else(|| bad(format!("no table `{}` in the bound dataset", e.pt)))?;
        let scan = |name: &str, p: &Arc<dyn datafusion::catalog::TableProvider>| {
            LogicalPlanBuilder::scan(name, provider_as_source(Arc::clone(p)), None)
        };
        let all_present = |cols: &[String]| {
            cols.iter()
                .map(|c| ident(c).is_not_null())
                .reduce(|a, b| a.and(b))
                .expect("at least one column")
        };

        // Filled: rows carrying a complete from-side key.
        let filled_expr = if let [c] = e.cc.as_slice() {
            count(ident(c))
        } else {
            count(lit(1)).filter(all_present(&e.cc)).build().map_err(|e| bad(e.to_string()))?
        };
        let plan = scan(&e.ct, &child)
            .and_then(|b| b.aggregate(Vec::<Expr>::new(), vec![filled_expr.alias("filled")]))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        let filled = int_column(&run(plan).await?, "filled").map_err(&bad)?[0];

        // Orphans: complete from-side keys that resolve to no to-side
        // row — the NOT IN with its null guards, as an anti join.
        let plan = scan(&e.ct, &child)
            .and_then(|b| b.filter(all_present(&e.cc)))
            .and_then(|b| {
                let p = scan(&e.pt, &parent)?
                    .filter(all_present(&e.pc))?
                    .build()?;
                b.join(
                    p,
                    JoinType::LeftAnti,
                    (
                        e.cc.iter().map(|c| datafusion::common::Column::from_name(c)).collect::<Vec<_>>(),
                        e.pc.iter().map(|c| datafusion::common::Column::from_name(c)).collect::<Vec<_>>(),
                    ),
                    None,
                )
            })
            .and_then(|b| b.aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("orphans")]))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        let orphans = int_column(&run(plan).await?, "orphans").map_err(&bad)?[0];
        let orphan_rate = if filled > 0 {
            orphans as f64 / filled as f64
        } else {
            0.0
        };

        // Temporal pairs across the join.
        let pairs: Vec<(String, String)> = date_cols[e.ct.as_str()]
            .iter()
            .flat_map(|cd| {
                date_cols[e.pt.as_str()]
                    .iter()
                    .map(move |pd| (cd.clone(), pd.clone()))
            })
            .collect();
        let mut temporal: Vec<(usize, &(String, String), i64, i64)> = Vec::new();
        if !pairs.is_empty() {
            // Exact-name qualified columns — `col()` would normalize a
            // mixed-case name to lowercase and miss it.
            let cq = |name: &str| {
                Expr::Column(datafusion::common::Column::new(Some("c"), name))
            };
            let pq = |name: &str| {
                Expr::Column(datafusion::common::Column::new(Some("p"), name))
            };
            let mut aggs = Vec::new();
            for (k, (cd, pd)) in pairs.iter().enumerate() {
                aggs.push(
                    count(lit(1))
                        .filter(cq(cd).is_not_null().and(pq(pd).is_not_null()))
                        .build()
                        .map_err(|e| bad(e.to_string()))?
                        .alias(format!("n_{k}")),
                );
                aggs.push(
                    count(lit(1))
                        .filter(cq(cd).lt(pq(pd)))
                        .build()
                        .map_err(|e| bad(e.to_string()))?
                        .alias(format!("pre_{k}")),
                );
            }
            let plan = scan(&e.ct, &child)
                .and_then(|b| b.alias("c"))
                .and_then(|b| {
                    let p = scan(&e.pt, &parent)?.alias("p")?.build()?;
                    b.join(
                        p,
                        JoinType::Inner,
                        (
                            e.cc
                                .iter()
                                .map(|c| datafusion::common::Column::new(Some("c"), c))
                                .collect::<Vec<_>>(),
                            e.pc
                                .iter()
                                .map(|c| datafusion::common::Column::new(Some("p"), c))
                                .collect::<Vec<_>>(),
                        ),
                        None,
                    )
                })
                .and_then(|b| b.aggregate(Vec::<Expr>::new(), aggs))
                .and_then(|b| b.build())
                .map_err(|e| bad(e.to_string()))?;
            let joined = run(plan).await?;
            for (k, pair) in pairs.iter().enumerate() {
                let n = int_column(&joined, &format!("n_{k}")).map_err(&bad)?[0];
                if n == 0 {
                    continue;
                }
                let pre = int_column(&joined, &format!("pre_{k}")).map_err(&bad)?[0];
                temporal.push((k, pair, n, pre));
            }
        }

        let fact = json!({
            "applicable": true, "seq": seq as i64,
            "relationship": e.relationship, "filled": filled,
            "orphans": orphans, "orphan_rate": orphan_rate,
        });
        if temporal.is_empty() {
            out.push(fact);
        } else {
            for (pair_seq, (cd, pd), n, pre) in &temporal {
                let mut row = fact.as_object().expect("object").clone();
                row.insert("pair_seq".into(), json!(*pair_seq as i64));
                row.insert("child_column".into(), json!(cd));
                row.insert("parent_column".into(), json!(pd));
                row.insert("joined".into(), json!(n));
                row.insert("precedes".into(), json!(pre));
                row.insert("precedes_rate".into(), json!(*pre as f64 / *n as f64));
                out.push(Value::Object(row));
            }
        }
    }
    rows_batch(out, coherence_shape())
}

fn coherence_shape() -> Vec<Field> {
    vec![
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("relationship", DataType::Utf8, true),
        Field::new("filled", DataType::Int64, true),
        Field::new("orphans", DataType::Int64, true),
        Field::new("orphan_rate", DataType::Float64, true),
        Field::new("pair_seq", DataType::Int64, true),
        Field::new("child_column", DataType::Utf8, true),
        Field::new("parent_column", DataType::Utf8, true),
        Field::new("joined", DataType::Int64, true),
        Field::new("precedes", DataType::Int64, true),
        Field::new("precedes_rate", DataType::Float64, true),
    ]
}

/// The three monthly verbs, shared by the metric doors: flows sum per
/// month; a marked stock sums the rows standing at the month's LATEST
/// observed date — one arbitrary last row read a 480-row inventory as
/// 94k against a true 12.4M; a ratio serves `num`
/// and `den` and the month reads as sum(num)/sum(den), because a walk
/// or a slice over a summed ratio bands an artefact (DSO at
/// 928.3 days against a true 75.6).
pub(crate) fn monthly_sql(sql: &str, tcol: &str, verb: &str) -> String {
    match verb {
        "ratio" => format!(
            "SELECT date_trunc('month', \"{tcol}\") AS period, \
                    sum(num) / nullif(sum(den), 0) AS value \
             FROM ({sql}) GROUP BY 1 ORDER BY 1"
        ),
        "stock" => format!(
            "SELECT period, sum(value) AS value FROM (\
                SELECT date_trunc('month', \"{tcol}\") AS period, value, \
                       rank() OVER (\
                           PARTITION BY date_trunc('month', \"{tcol}\") \
                           ORDER BY \"{tcol}\" DESC) AS rk \
                FROM ({sql})\
             ) WHERE rk = 1 GROUP BY period ORDER BY period"
        ),
        _ => format!(
            "SELECT date_trunc('month', \"{tcol}\") AS period, sum(value) AS value \
             FROM ({sql}) GROUP BY 1 ORDER BY 1"
        ),
    }
}

/// A grounding's monthly series, NULL months kept — the walk and the
/// cube index by position. Periods come back in the column's display
/// form; callers cut the YYYY-MM head as the scripts did.
async fn run_monthly(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    tcol: &str,
    verb: &str,
) -> Result<Vec<(String, Option<f64>)>, SessionError> {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    let q = monthly_sql(sql, tcol, verb);
    let plan = Box::pin(crate::whatif::build_plan(shared, ctx, &q)).await?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
        .collect()
        .await
        .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?;
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let period = b.column(
            b.schema()
                .index_of("period")
                .map_err(|e| SessionError::Runtime(e.to_string()))?,
        );
        let value = b.column(
            b.schema()
                .index_of("value")
                .map_err(|e| SessionError::Runtime(e.to_string()))?,
        );
        let floats = cast_with_options(
            value,
            &DataType::Float64,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )
        .map_err(|e| SessionError::Runtime(e.to_string()))?;
        let floats = floats
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| SessionError::Runtime("value did not read as a number".into()))?;
        for i in 0..b.num_rows() {
            out.push((
                array_value_to_string(period, i).map_err(|e| SessionError::Runtime(e.to_string()))?,
                (!floats.is_null(i)).then(|| floats.value(i)),
            ));
        }
    }
    Ok(out)
}

/// `metric_band_walk('dataset')` — for every grounded metric, walk the
/// recent months and ask the TabICL forward what range each month
/// should have landed in, given everything before it. The protocol —
/// monthly read at the grounding's verb, the feature recipe, the
/// point-in-time fills, the walk — is authored policy; the model call
/// is one kernel behind the runtime seam, never reimplemented.
/// Evaluated against generated ground truth before it shipped.
///
/// The feature recipe and the fill are graded protocol, exact: the
/// trailing mean excludes the current month, every fill is
/// point-in-time, and no training row exists before the first
/// observed month. Change any of it only against a graded re-run —
/// each of these three has silently drifted once.
///
/// Each walked point records its bands and its PIT — the quantile at
/// which the actual landed, 0..1 and ordinal by construction (raw
/// densities never leave the kernel). The band_breach detector
/// adjudicates PITs; this door only reports.
pub(crate) async fn metric_band_walk(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    const ALPHAS: [f64; 5] = [0.05, 0.10, 0.50, 0.90, 0.95];
    const MIN_TRAIN: usize = 5;
    const MAX_WALK: usize = 6;

    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let runtime = shared.runtime();

    // Median over the present values of one feature column. Even counts
    // average the two middles, as the graded protocol's pandas median
    // does.
    fn col_median(mut values: Vec<f64>) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let k = values.len();
        Some(if k % 2 == 1 {
            values[k / 2]
        } else {
            (values[k / 2 - 1] + values[k / 2]) / 2.0
        })
    }

    let mut out = Vec::new();
    let mut seq = 0i64;
    for slot in current_query_slots(shared, &rctx, dataset).await? {
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };

        // The grounding is a grain-free extract with a time axis and a
        // `value` column; the time column by dtype, as any reader finds
        // it. A grounding that does not plan fails the walk whole, as
        // the script it replaces did.
        let probe = Box::pin(crate::whatif::build_plan(shared, &ctx, sql)).await?;
        let fields = probe.schema();
        let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
        if !has("value") {
            continue;
        }
        let Some(tcol) = crate::whatif::date_column(fields.fields()) else {
            continue;
        };
        let is_ratio = has("num") && has("den");
        let is_stock = !is_ratio
            && body.get("behavior").and_then(Value::as_str) == Some("stock");
        let verb = if is_ratio {
            "ratio"
        } else if is_stock {
            "stock"
        } else {
            "flow"
        };
        let series = run_monthly(shared, &ctx, sql, &tcol, verb).await?;
        let v: Vec<Option<f64>> = series.iter().map(|(_, v)| *v).collect();
        let n = v.len();
        // Feature rows start at month 1 — month 0 has no lag of its own
        // and the graded protocol builds no row for it — so a walk point
        // at t trains on t-1 rows and the earliest walkable t is
        // MIN_TRAIN + 1.
        if n < MIN_TRAIN + 2 {
            out.push(json!({
                "seq": seq, "metric": slot.aspect, "applicable": false,
                "reason": format!("{n} months, need {}", MIN_TRAIN + 2),
            }));
            seq += 1;
            continue;
        }

        // Features for month i (row i-1): index, month-of-year, the 1-
        // and 12-month lags, and the trailing 3-month MEAN — `lag3m` in
        // the graded protocol, not the raw
        // t-3 value. Absent features stay absent; they fill per walk
        // step from training rows only.
        let mut feats: Vec<[Option<f64>; 5]> = Vec::new();
        let mut labels: Vec<Option<f64>> = Vec::new();
        for i in 1..n {
            let moy: f64 = series[i].0[5..7]
                .parse::<i64>()
                .map_err(|e| SessionError::Runtime(format!("period parse: {e}")))?
                as f64;
            let lag1 = v[i - 1];
            let lo = i.saturating_sub(3);
            let present: Vec<f64> = (lo..i).filter_map(|j| v[j]).collect();
            let lag3m = (!present.is_empty())
                .then(|| present.iter().sum::<f64>() / present.len() as f64);
            let lag12 = if i >= 12 { v[i - 12] } else { None };
            feats.push([Some(i as f64), Some(moy), lag1, lag3m, lag12]);
            labels.push(v[i]);
        }

        let mut points: Vec<Value> = Vec::new();
        let floor = MIN_TRAIN + 1;
        let start = if n.saturating_sub(MAX_WALK) > floor {
            n - MAX_WALK
        } else {
            floor
        };
        for t in start..n {
            let Some(actual) = v[t] else { continue };
            // Absent features fill from the training rows only, per
            // feature, recomputed at this step — so the fit stays
            // point-in-time. A feature absent throughout fills 0.0.
            let mut fills = [0.0f64; 5];
            for (c, fill) in fills.iter_mut().enumerate() {
                let col: Vec<f64> = (0..t.saturating_sub(1))
                    .filter_map(|r| feats[r][c])
                    .collect();
                if let Some(m) = col_median(col) {
                    *fill = m;
                }
            }
            let filled =
                |row: &[Option<f64>; 5]| -> Vec<f64> {
                    row.iter()
                        .enumerate()
                        .map(|(c, v)| v.unwrap_or(fills[c]))
                        .collect()
                };
            let mut train_x = Vec::new();
            let mut train_y = Vec::new();
            for r in 0..t.saturating_sub(1) {
                let Some(label) = labels[r] else { continue };
                train_x.extend(filled(&feats[r]));
                train_y.push(label);
            }
            if train_y.len() < MIN_TRAIN {
                continue;
            }
            let (q, pit) = runtime
                .band_point(
                    &train_x,
                    train_y.len(),
                    5,
                    &train_y,
                    &filled(&feats[t - 1]),
                    &ALPHAS,
                    actual,
                )
                .map_err(SessionError::Runtime)?;
            points.push(json!({
                "period": &series[t].0[..7], "actual": actual,
                "p05": q[0], "p10": q[1], "p50": q[2], "p90": q[3], "p95": q[4],
                "pit": pit,
            }));
        }

        for (point_seq, p) in points.iter().enumerate() {
            let mut row = serde_json::Map::new();
            row.insert("seq".into(), json!(seq));
            row.insert("metric".into(), json!(slot.aspect));
            row.insert("applicable".into(), json!(true));
            row.insert("grain".into(), json!("month"));
            row.insert(
                "aggregation".into(),
                json!(if is_stock { "latest-sum" } else { "sum" }),
            );
            row.insert("trained_on".into(), json!(n as i64));
            row.insert("point_seq".into(), json!(point_seq as i64));
            for (k, val) in p.as_object().expect("point object") {
                row.insert(k.clone(), val.clone());
            }
            out.push(Value::Object(row));
        }
        if points.is_empty() {
            out.push(json!({
                "seq": seq, "metric": slot.aspect, "applicable": true,
                "grain": "month",
                "aggregation": if is_stock { "latest-sum" } else { "sum" },
                "trained_on": n as i64,
            }));
        }
        seq += 1;
    }
    if out.is_empty() {
        out.push(json!({}));
    }
    rows_batch(out, band_shape())
}

fn band_shape() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("metric", DataType::Utf8, true),
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("grain", DataType::Utf8, true),
        Field::new("aggregation", DataType::Utf8, true),
        Field::new("trained_on", DataType::Int64, true),
        Field::new("point_seq", DataType::Int64, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("actual", DataType::Float64, true),
        Field::new("p05", DataType::Float64, true),
        Field::new("p10", DataType::Float64, true),
        Field::new("p50", DataType::Float64, true),
        Field::new("p90", DataType::Float64, true),
        Field::new("p95", DataType::Float64, true),
        Field::new("pit", DataType::Float64, true),
    ]
}

/// A member series at a verb: the monthly shapes of [`monthly_sql`]
/// sliced along one dimension column, NULL members excluded. `member`
/// is the member expression — the cast column plainly, or the bucketing
/// CASE. The stock rank still partitions by the *raw* column, so a
/// bucket's value is the sum of its raw members' own latest
/// observations, never one arbitrary latest row of the whole bucket.
fn monthly_member_sql(sql: &str, tcol: &str, dcol: &str, member: &str, verb: &str) -> String {
    match verb {
        "ratio" => format!(
            "SELECT date_trunc('month', \"{tcol}\") AS period, \
                    {member} AS member, \
                    sum(num) / nullif(sum(den), 0) AS value \
             FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL \
             GROUP BY 1, 2 ORDER BY 1, 2"
        ),
        "stock" => format!(
            "SELECT period, member, sum(value) AS value FROM (\
                SELECT date_trunc('month', \"{tcol}\") AS period, \
                       {member} AS member, value, \
                       rank() OVER (\
                           PARTITION BY date_trunc('month', \"{tcol}\"), \"{dcol}\" \
                           ORDER BY \"{tcol}\" DESC) AS rk \
                FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL\
             ) WHERE rk = 1 GROUP BY period, member ORDER BY period, member"
        ),
        _ => format!(
            "SELECT date_trunc('month', \"{tcol}\") AS period, \
                    {member} AS member, sum(value) AS value \
             FROM ({sql}) WHERE \"{dcol}\" IS NOT NULL \
             GROUP BY 1, 2 ORDER BY 1, 2"
        ),
    }
}

/// `metric_cube_slices('dataset')` — every grounded metric's monthly
/// series: the total, the slices along its served dimension columns,
/// and the named rival where a grounding discloses one. What it exists
/// for: a static frame cannot name a metric in FROM, but this door
/// turns metric names into rows once and the `metric_series()` read
/// serves them to any frame with plain value filters.
///
/// Caps, recorded here: at most 4 dimension columns per metric, the
/// last 48 months. A served column is admitted at 2..512 distinct
/// members, fewest members first; up to 24 members every member is
/// named, above that the dimension is *bucketed* — the top 23 by
/// weight (summed value; a ratio weighs by its denominator) keep their
/// names and the rest fold into 'other', so a wide axis enters instead
/// of falling off a cliff (34
/// sales orgs could never enter at a hard 24). Beyond 512 a column
/// reads as an identifier, not a dimension. The fact row names its
/// bucketed dimensions so 'other' is never read as a business member.
/// A grounding assumption carrying `alternative_sql` (the metrics
/// skill's named-rival convention) contributes one more series under
/// dimension 'alternative'. The rival SQL is authored but never
/// admission-validated — it runs behind a guard and a failure is
/// reported, not thrown. The monthly verbs are [`monthly_sql`]'s three,
/// the ratio verb because summing a ratio was wrong: DSO for one month
/// came back 928.3 days against a true 75.6 when twelve member ratios
/// were added together. This is the division node of the
/// formula evaluator that would eventually read the `formulas` gloss
/// directly; the grounding already computes both halves and used to
/// discard them.
///
/// Two row kinds come back: one fact row per metric (`fact` true,
/// carrying dims and the rival outcome), and one row per cube cell.
pub(crate) async fn metric_cube_slices(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    const DIMS_CAP: usize = 4;
    const MEMBERS_CAP: i64 = 24;
    const MEMBERS_ADMIT_CAP: i64 = 512;
    const MONTHS_CAP: usize = 48;

    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;

    let mut out = Vec::new();
    let mut seq = 0i64;
    for slot in current_query_slots(shared, &rctx, dataset).await? {
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };
        let name = &slot.aspect;
        let fact = |seq: i64, extra: serde_json::Map<String, Value>| {
            let mut row = serde_json::Map::new();
            row.insert("seq".into(), json!(seq));
            row.insert("fact".into(), json!(true));
            row.insert("metric".into(), json!(name));
            row.extend(extra);
            Value::Object(row)
        };
        let abstain = |seq: i64, reason: &str| {
            json!({
                "seq": seq, "fact": true, "metric": name,
                "applicable": false, "reason": reason,
            })
        };

        let probe = Box::pin(crate::whatif::build_plan(shared, &ctx, sql)).await?;
        let fields = probe.schema();
        let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
        if !has("value") {
            out.push(abstain(seq, "no value column"));
            seq += 1;
            continue;
        }
        let Some(tcol) = crate::whatif::date_column(fields.fields()) else {
            out.push(abstain(seq, "no time column"));
            seq += 1;
            continue;
        };
        // A ratio declares itself by serving both halves of its
        // division — checked before the stock marker so a ratio over
        // stock components cannot be mistaken for a stock.
        let is_ratio = has("num") && has("den");
        let verb = if is_ratio {
            "ratio"
        } else if body.get("behavior").and_then(Value::as_str) == Some("stock") {
            "stock"
        } else {
            "flow"
        };

        // Served dimension candidates: every column that is neither the
        // value nor time-typed (nor a ratio's own halves). Admission is
        // by member count, one aggregate pass.
        let cand: Vec<String> = fields
            .fields()
            .iter()
            .filter(|f| {
                let n = f.name().as_str();
                if n == "value" || (is_ratio && (n == "num" || n == "den")) {
                    return false;
                }
                !crate::whatif::is_temporal(f.data_type())
            })
            .map(|f| f.name().clone())
            .collect();
        let mut counts: Vec<(String, i64)> = Vec::new();
        if !cand.is_empty() {
            let parts: Vec<String> = cand
                .iter()
                .enumerate()
                .map(|(i, c)| format!("count(DISTINCT \"{c}\") AS \"n_{i}\""))
                .collect();
            let q = format!("SELECT {} FROM ({sql})", parts.join(", "));
            let plan = Box::pin(crate::whatif::build_plan(shared, &ctx, &q)).await?;
            let batches = ctx
                .execute_logical_plan(plan)
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
                .collect()
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?;
            for (i, c) in cand.iter().enumerate() {
                let n = int_column(&batches, &format!("n_{i}"))
                    .map_err(SessionError::Runtime)?[0];
                counts.push((c.clone(), n));
            }
        }
        let mut dims: Vec<String> = Vec::new();
        let mut bucketed: Vec<String> = Vec::new();
        while dims.len() < DIMS_CAP {
            let mut best: Option<(&str, i64)> = None;
            for (c, n) in &counts {
                if *n < 2 || *n > MEMBERS_ADMIT_CAP || dims.iter().any(|d| d == c) {
                    continue;
                }
                if best.is_none_or(|(_, bn)| *n < bn) {
                    best = Some((c, *n));
                }
            }
            let Some((c, n)) = best else { break };
            dims.push(c.to_string());
            if n > MEMBERS_CAP {
                bucketed.push(c.to_string());
            }
        }

        // The total series bounds the window: the last MONTHS_CAP
        // periods, held as a set every other series filters against.
        let total = run_monthly(shared, &ctx, sql, &tcol, verb).await?;
        let start = total.len().saturating_sub(MONTHS_CAP);
        let keep: HashSet<String> = total[start..]
            .iter()
            .map(|(p, _)| p[..7].to_string())
            .collect();
        let mut cells: Vec<(String, String, String, f64)> = Vec::new();
        for (p, v) in &total {
            let Some(v) = v else { continue };
            if keep.contains(&p[..7]) {
                cells.push((String::new(), String::new(), p[..7].to_string(), *v));
            }
        }

        // Member series per admitted dimension, same verb, same window.
        for dcol in &dims {
            // A bucketed dimension names its top members by weight and
            // folds the rest into 'other'. The set is resolved here and
            // spliced as literals — deterministic (weight, then name)
            // so two computations at one pin agree.
            let member = if bucketed.iter().any(|b| b == dcol) {
                let weight = if verb == "ratio" { "sum(den)" } else { "sum(value)" };
                let q = format!(
                    "SELECT CAST(\"{dcol}\" AS VARCHAR) AS mc_member FROM ({sql}) \
                     WHERE \"{dcol}\" IS NOT NULL GROUP BY 1 \
                     ORDER BY {weight} DESC NULLS LAST, mc_member LIMIT {}",
                    MEMBERS_CAP - 1
                );
                let plan = Box::pin(crate::whatif::build_plan(shared, &ctx, &q)).await?;
                let batches = ctx
                    .execute_logical_plan(plan)
                    .await
                    .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
                    .collect()
                    .await
                    .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?;
                let mut named = Vec::new();
                for b in batches.iter().filter(|b| b.num_rows() > 0) {
                    let col = b
                        .schema()
                        .index_of("mc_member")
                        .map(|i| b.column(i).clone())
                        .map_err(|e| SessionError::Runtime(e.to_string()))?;
                    for i in 0..b.num_rows() {
                        let m = array_value_to_string(&col, i)
                            .map_err(|e| SessionError::Runtime(e.to_string()))?;
                        named.push(format!("'{}'", m.replace('\'', "''")));
                    }
                }
                format!(
                    "CASE WHEN CAST(\"{dcol}\" AS VARCHAR) IN ({}) \
                     THEN CAST(\"{dcol}\" AS VARCHAR) ELSE 'other' END",
                    named.join(", ")
                )
            } else {
                format!("CAST(\"{dcol}\" AS VARCHAR)")
            };
            let q = monthly_member_sql(sql, &tcol, dcol, &member, verb);
            let plan = Box::pin(crate::whatif::build_plan(shared, &ctx, &q)).await?;
            let batches = ctx
                .execute_logical_plan(plan)
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
                .collect()
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?;
            for b in batches.iter().filter(|b| b.num_rows() > 0) {
                let get = |n: &str| {
                    b.schema()
                        .index_of(n)
                        .map(|i| b.column(i).clone())
                        .map_err(|e| SessionError::Runtime(e.to_string()))
                };
                let (period, member, value) = (get("period")?, get("member")?, get("value")?);
                let floats = datafusion::arrow::compute::cast_with_options(
                    &value,
                    &DataType::Float64,
                    &datafusion::arrow::compute::CastOptions {
                        safe: true,
                        ..Default::default()
                    },
                )
                .map_err(|e| SessionError::Runtime(e.to_string()))?;
                let floats = floats
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                    .ok_or_else(|| SessionError::Runtime("value is not numeric".into()))?;
                for i in 0..b.num_rows() {
                    if floats.is_null(i) {
                        continue;
                    }
                    let p = array_value_to_string(&period, i)
                        .map_err(|e| SessionError::Runtime(e.to_string()))?;
                    let p = p[..7].to_string();
                    if !keep.contains(&p) {
                        continue;
                    }
                    let m = array_value_to_string(&member, i)
                        .map_err(|e| SessionError::Runtime(e.to_string()))?;
                    cells.push((dcol.clone(), m, p, floats.value(i)));
                }
            }
        }

        // The named rival, when a grounding assumption discloses one.
        let mut extra = serde_json::Map::new();
        extra.insert("applicable".into(), json!(true));
        extra.insert("behavior".into(), json!(verb));
        extra.insert("dims".into(), json!(dims));
        extra.insert("bucketed".into(), json!(bucketed));
        if let Some(assumptions) = body.get("assumptions").and_then(Value::as_array) {
            for a in assumptions {
                let Some(alt_sql) = a.get("alternative_sql").and_then(Value::as_str) else {
                    continue;
                };
                let rival = a
                    .get("alternative")
                    .and_then(Value::as_str)
                    .unwrap_or("(rival)");
                match rival_series(shared, &ctx, alt_sql, verb).await {
                    Ok(Some(series)) => {
                        for (p, v) in series {
                            let Some(v) = v else { continue };
                            let p = p[..7].to_string();
                            if keep.contains(&p) {
                                cells.push(("alternative".into(), rival.to_string(), p, v));
                            }
                        }
                        extra.insert("alternative".into(), json!(rival));
                    }
                    Ok(None) => {
                        extra.insert(
                            "alternative_error".into(),
                            json!("rival SQL serves no value/time column"),
                        );
                    }
                    Err(e) => {
                        extra.insert(
                            "alternative_error".into(),
                            json!(format!("rival SQL failed: {e}")),
                        );
                    }
                }
                break;
            }
        }
        out.push(fact(seq, extra));
        for (cell_seq, (dimension, member, period, value)) in cells.iter().enumerate() {
            out.push(json!({
                "seq": seq, "cell_seq": cell_seq as i64,
                "dimension": dimension, "member": member,
                "period": period, "value": value,
            }));
        }
        seq += 1;
    }
    if out.is_empty() {
        out.push(json!({}));
    }
    rows_batch(out, cube_shape())
}

/// The rival's monthly series at its own verb: a rival that serves
/// num/den totals as a ratio even where the chosen reading does not,
/// and the reverse. `None` = no value or time column.
async fn rival_series(
    shared: &Arc<Shared>,
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
    chosen_verb: &str,
) -> Result<Option<Vec<(String, Option<f64>)>>, SessionError> {
    let probe = Box::pin(crate::whatif::build_plan(shared, ctx, sql)).await?;
    let fields = probe.schema();
    let has = |n: &str| fields.fields().iter().any(|f| f.name() == n);
    let Some(tcol) = crate::whatif::date_column(fields.fields()) else {
        return Ok(None);
    };
    if !has("value") {
        return Ok(None);
    }
    let verb = if has("num") && has("den") {
        "ratio"
    } else if chosen_verb == "ratio" {
        "flow"
    } else {
        chosen_verb
    };
    Ok(Some(run_monthly(shared, ctx, sql, &tcol, verb).await?))
}

fn cube_shape() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("fact", DataType::Boolean, true),
        Field::new("metric", DataType::Utf8, true),
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("behavior", DataType::Utf8, true),
        Field::new(
            "dims",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "bucketed",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("alternative", DataType::Utf8, true),
        Field::new("alternative_error", DataType::Utf8, true),
        Field::new("cell_seq", DataType::Int64, true),
        Field::new("dimension", DataType::Utf8, true),
        Field::new("member", DataType::Utf8, true),
        Field::new("period", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]
}
