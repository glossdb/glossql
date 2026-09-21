//! `hierarchy_candidates('table')`: functional dependencies between a
//! table's columns, counted in the engine.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, ExprFunctionExt, LogicalPlanBuilder, ident, lit};
use serde_json::json;

use crate::reads::Shared;
use crate::session::SessionError;

use super::{detector_state, int_column, rows_batch, run_plan};

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
/// One scan per pass replaces the per-pair query wave: pass one
/// unpivots every candidate column's cells through one `unnest` and
/// sizes each column (filled, groups, modal); pass two unpivots every
/// pool pair's cells the same way, [`PAIRS_PER_PASS`] pairs to a scan,
/// and reduces them to per-pair agreement — the fan-out is a list the
/// engine unnests, not a join and not a loop of statements. Both passes
/// run through the detector's state, so their aggregates spill under
/// its share. Display strings are injective off the float lane, so
/// grouping by them counts what grouping by the raw column counted.
/// Columns pair by pool index, exactly the order the script
/// enumerated, and `seq` carries it so the body's array reproduces it.
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
    let state = detector_state(&ctx);
    let run = |plan| async {
        run_plan(&state, plan)
            .await
            .map_err(|e| SessionError::door(&format!("hierarchy_candidates('{table}')"), e))
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
        let n = int_column(&run(plan).await?, "n").map_err(|e| bad(e.to_string()))?[0];
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
        let ci = int_column(std::slice::from_ref(b), "ci").map_err(|e| bad(e.to_string()))?;
        let groups =
            int_column(std::slice::from_ref(b), "groups").map_err(|e| bad(e.to_string()))?;
        let modal = int_column(std::slice::from_ref(b), "modal").map_err(|e| bad(e.to_string()))?;
        let total = int_column(std::slice::from_ref(b), "total").map_err(|e| bad(e.to_string()))?;
        let dv =
            int_column(std::slice::from_ref(b), "distinct_vals").map_err(|e| bad(e.to_string()))?;
        let filled =
            int_column(std::slice::from_ref(b), "filled").map_err(|e| bad(e.to_string()))?;
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

    // Pass two — the pair fan-out, restricted to pool columns and
    // reduced to per-pair agreement, a slice of the pairs to a scan.
    let pool_cols: Vec<(usize, String)> = pool
        .iter()
        .map(|(ci, _)| (*ci, names[*ci].clone()))
        .collect();
    let mut all_pairs: Vec<(usize, usize)> = Vec::new();
    for i in 0..pool_cols.len() {
        for j in (i + 1)..pool_cols.len() {
            all_pairs.push((i, j));
        }
    }

    struct Pair {
        pair_groups: i64,
        agree_ab: i64,
        agree_ba: i64,
    }
    let mut pairs: std::collections::HashMap<(usize, usize), Pair> = Default::default();
    for slice in all_pairs.chunks(PAIRS_PER_PASS) {
        let plan =
            plan_pairs(table, &provider, &pool_cols, slice).map_err(|e| bad(e.to_string()))?;
        let paired = run(plan).await?;
        for b in paired.iter().filter(|b| b.num_rows() > 0) {
            let ca = int_column(std::slice::from_ref(b), "ca").map_err(|e| bad(e.to_string()))?;
            let cb = int_column(std::slice::from_ref(b), "cb").map_err(|e| bad(e.to_string()))?;
            let pg = int_column(std::slice::from_ref(b), "pair_groups")
                .map_err(|e| bad(e.to_string()))?;
            let ab =
                int_column(std::slice::from_ref(b), "agree_ab").map_err(|e| bad(e.to_string()))?;
            let ba =
                int_column(std::slice::from_ref(b), "agree_ba").map_err(|e| bad(e.to_string()))?;
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
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("n")])?
        .build()
}

/// The long form of some columns: one scan, every row unpivoted to
/// `(ci, val)` — the pool index as `ci`, the display form as `val` —
/// through `make_array` and one `unnest`, so the plan carries one scan
/// and one set of consumers however many columns take part. A null
/// cell stays a null `val`, so NULL counts as one group downstream.
fn plan_long(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
) -> datafusion::common::Result<LogicalPlanBuilder> {
    use datafusion::common::{Column, UnnestOptions};
    use datafusion::functions_nested::expr_fn::make_array;
    use datafusion::logical_expr::cast;

    let ci = make_array(cols.iter().map(|(ci, _)| lit(*ci as i64)).collect()).alias("ci");
    let val = make_array(
        cols.iter()
            .map(|(_, name)| cast(ident(name), DataType::Utf8))
            .collect(),
    )
    .alias("val");
    LogicalPlanBuilder::scan(table, provider_as_source(Arc::clone(provider)), None)?
        .project(vec![ci, val])?
        .unnest_columns_with_options(
            vec![Column::from_name("ci"), Column::from_name("val")],
            UnnestOptions::default(),
        )
}

/// Pass one: cells per (column, value) reduced to per-column statistics.
fn plan_colstat(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::logical_expr::col;

    plan_long(table, provider, cols)?
        .aggregate(vec![col("ci"), col("val")], vec![count(lit(1)).alias("c")])?
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

/// Pairs to a scan in the pair pass. Unnest emits one output batch per
/// input batch, rows × list length, and a grouped aggregate's first
/// reservation is one input batch of state; at the detector's batch of
/// 1024 rows, 512 pairs put 524,288 cells in a batch. The slicing
/// sizes the work's batches, never the result: every pair runs.
const PAIRS_PER_PASS: usize = 512;

/// Pass two, one slice of the pool's pairs: every row unpivoted to a
/// cell per pair — `(ca, cb, av, bv)` — through four lists unnested in
/// step, no join and no row number, since the row's own values are the
/// pair. Cells per (pair, value pair) reduce to pair groups and the
/// summed per-determinant maxima both directions.
fn plan_pairs(
    table: &str,
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
    cols: &[(usize, String)],
    pairs: &[(usize, usize)],
) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::common::{Column, UnnestOptions};
    use datafusion::functions_aggregate::expr_fn::{max, sum};
    use datafusion::functions_nested::expr_fn::make_array;
    use datafusion::logical_expr::{cast, col};

    // Each display form once, in a projection of its own: the lists
    // name a column as many times as it has partners, and the optimizer
    // merges a projection into the next only where every expression is
    // referenced once (optimize_projections
    // `merge_consecutive_projections`), so the casts stay hoisted.
    let mut used: Vec<usize> = pairs.iter().flat_map(|(a, b)| [*a, *b]).collect();
    used.sort_unstable();
    used.dedup();
    let casts: Vec<Expr> = used
        .iter()
        .map(|&k| cast(ident(&cols[k].1), DataType::Utf8).alias(format!("s{k}")))
        .collect();
    let lists = vec![
        make_array(pairs.iter().map(|(a, _)| lit(cols[*a].0 as i64)).collect()).alias("ca"),
        make_array(pairs.iter().map(|(_, b)| lit(cols[*b].0 as i64)).collect()).alias("cb"),
        make_array(pairs.iter().map(|(a, _)| col(format!("s{a}"))).collect()).alias("av"),
        make_array(pairs.iter().map(|(_, b)| col(format!("s{b}"))).collect()).alias("bv"),
    ];
    let cells = LogicalPlanBuilder::scan(table, provider_as_source(Arc::clone(provider)), None)?
        .project(casts)?
        .project(lists)?
        .unnest_columns_with_options(
            ["ca", "cb", "av", "bv"]
                .into_iter()
                .map(Column::from_name)
                .collect(),
            UnnestOptions::default(),
        )?
        .aggregate(
            vec![col("ca"), col("cb"), col("av"), col("bv")],
            vec![count(lit(1)).alias("c")],
        )?
        .build()?;

    // The three reductions in one aggregate over the cells: grouping
    // sets (pair, av) and (pair, bv) carry each determinant's maximum
    // and its group count, told apart by `grouping`, and the pair's
    // row sums them — so the cells, and the scan under them, plan once.
    use datafusion::functions_aggregate::expr_fn::grouping;
    use datafusion::logical_expr::grouping_set;
    LogicalPlanBuilder::from(cells)
        .aggregate(
            vec![grouping_set(vec![
                vec![col("ca"), col("cb"), col("av")],
                vec![col("ca"), col("cb"), col("bv")],
            ])],
            vec![
                max(col("c")).alias("mx"),
                count(lit(1)).alias("ng"),
                grouping(col("bv")).alias("by_av"),
            ],
        )?
        .aggregate(
            vec![col("ca"), col("cb")],
            vec![
                sum(col("ng"))
                    .filter(col("by_av").eq(lit(1)))
                    .build()?
                    .alias("pair_groups"),
                sum(col("mx"))
                    .filter(col("by_av").eq(lit(1)))
                    .build()?
                    .alias("agree_ab"),
                sum(col("mx"))
                    .filter(col("by_av").eq(lit(0)))
                    .build()?
                    .alias("agree_ba"),
            ],
        )?
        .project(vec![
            col("ca"),
            col("cb"),
            col("pair_groups"),
            col("agree_ab"),
            col("agree_ba"),
        ])?
        .build()
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    use super::*;

    /// The hierarchy pass is one scan however many columns take part:
    /// pass one unpivots through one unnest, and pass two unpivots a
    /// cell per pair the same way, with no join and no row number
    /// above the scan — so the consumers the pass registers do not
    /// scale with the table's width.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_hierarchy_pass_is_one_scan_and_one_unnest() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 10, 20])),
                Arc::new(StringArray::from(vec![Some("x"), None, Some("y")])),
            ],
        )
        .unwrap();
        let provider: Arc<dyn datafusion::catalog::TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let cols = vec![
            (0, "a".to_string()),
            (1, "b".to_string()),
            (2, "c".to_string()),
        ];
        let state = detector_state(&ctx);

        let scans = |rendered: &str| {
            rendered
                .lines()
                .filter(|l| l.contains("DataSourceExec") || l.contains("MemoryExec"))
                .count()
        };
        let colstat = plan_colstat("t", &provider, &cols).unwrap();
        let physical = state.create_physical_plan(&colstat).await.unwrap();
        let rendered = displayable(physical.as_ref()).indent(false).to_string();
        assert_eq!(scans(&rendered), 1, "pass one scans once:\n{rendered}");

        let pairs = plan_pairs("t", &provider, &cols, &[(0, 1), (0, 2), (1, 2)]).unwrap();
        let physical = state.create_physical_plan(&pairs).await.unwrap();
        let rendered = displayable(physical.as_ref()).indent(false).to_string();
        assert_eq!(scans(&rendered), 1, "pass two scans once:\n{rendered}");
        assert_eq!(
            rendered.matches("UnnestExec").count(),
            1,
            "one unnest:\n{rendered}"
        );
        assert!(!rendered.contains("Join"), "no join:\n{rendered}");
    }
}
