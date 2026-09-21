//! `relationship_checks('dataset')`: what the declared joins assert,
//! checked against the rows.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, ExprFunctionExt, LogicalPlanBuilder, ident, lit};
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::SessionError;

use super::{dataset_pins, int_column, rows_batch};

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
/// A tuple endpoint — `t.(a, b)`, fixture 14's key — joins on every
/// leg. Endpoints whose sides disagree in width cannot join and are
/// skipped, like malformed paths.
///
/// A relationship within one table is one of two things, and the
/// to-side tells them apart. When the to-side is unique over the table
/// — a key — the edge is a self-reference (`parent_id -> account_id`)
/// and checks as a join: the table against itself, orphans the rows
/// whose reference resolves to no row. Otherwise it is a nest
/// (recorded finer → coarser, `city -> country`): what it asserts is
/// that every finer value determines one coarser value, and its
/// orphans are the rows whose finer value maps to more than one.
/// Neither carries temporal evidence — a date pair across a table and
/// itself says nothing.
pub(crate) async fn relationship_checks(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    use datafusion::common::JoinType;

    let bad =
        |d: String| SessionError::BadSubject(format!("relationship_checks('{dataset}'): {d}"));
    let pinned = dataset_pins(shared, dataset).await?;
    let resolved = &pinned;
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
        // `relationships` is workspace-wide and its first column says
        // whose edge this is. Every edge below is resolved against the
        // statement's pins and a miss is fatal, so another dataset's
        // edge does not widen this read — it kills it.
        if row[0].as_deref() != Some(dataset) {
            continue;
        }
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
            count(lit(1))
                .filter(all_present(&e.cc))
                .build()
                .map_err(|e| bad(e.to_string()))?
        };
        let plan = scan(&e.ct, &child)
            .and_then(|b| b.aggregate(Vec::<Expr>::new(), vec![filled_expr.alias("filled")]))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        let filled = int_column(&run(plan).await?, "filled").map_err(|e| bad(e.to_string()))?[0];

        let same_table = e.ct == e.pt;
        // Within one table, a unique to-side is a key and the edge a
        // self-reference; a repeating to-side is the coarser level of
        // a nest. One pass over the to-side decides.
        let nest = same_table && {
            use datafusion::functions_aggregate::expr_fn::sum;
            let coarser: Vec<Expr> = e.pc.iter().map(ident).collect();
            let plan = scan(&e.pt, &parent)
                .and_then(|b| b.filter(all_present(&e.pc)))
                .and_then(|b| b.aggregate(coarser, vec![count(lit(1)).alias("n")]))
                .and_then(|b| {
                    b.aggregate(
                        Vec::<Expr>::new(),
                        vec![count(lit(1)).alias("groups"), sum(ident("n")).alias("rows")],
                    )
                })
                .and_then(|b| b.build())
                .map_err(|e| bad(e.to_string()))?;
            let counted = run(plan).await?;
            let groups = int_column(&counted, "groups").map_err(|e| bad(e.to_string()))?[0];
            let rows = int_column(&counted, "rows").map_err(|e| bad(e.to_string()))?[0];
            groups != rows
        };
        let orphans = if nest {
            // The dependency, in one pass: rows per (finer, coarser),
            // then coarser values per finer; a finer value with more
            // than one fails, and its rows are the orphans.
            use datafusion::functions::expr_fn::coalesce;
            use datafusion::functions_aggregate::expr_fn::sum;
            let keys: Vec<Expr> = e.cc.iter().chain(e.pc.iter()).map(ident).collect();
            let finer: Vec<Expr> = e.cc.iter().map(ident).collect();
            let plan = scan(&e.ct, &child)
                .and_then(|b| b.filter(all_present(&e.cc).and(all_present(&e.pc))))
                .and_then(|b| b.aggregate(keys, vec![count(lit(1)).alias("n")]))
                .and_then(|b| {
                    b.aggregate(
                        finer,
                        vec![count(lit(1)).alias("k"), sum(ident("n")).alias("n")],
                    )
                })
                .and_then(|b| b.filter(ident("k").gt(lit(1))))
                .and_then(|b| {
                    b.aggregate(
                        Vec::<Expr>::new(),
                        vec![coalesce(vec![sum(ident("n")), lit(0i64)]).alias("orphans")],
                    )
                })
                .and_then(|b| b.build())
                .map_err(|e| bad(e.to_string()))?;
            int_column(&run(plan).await?, "orphans").map_err(|e| bad(e.to_string()))?[0]
        } else {
            // Orphans: complete from-side keys that resolve to no to-side
            // row — the NOT IN with its null guards, as an anti join.
            // A self-reference joins the table to itself, so the to-side
            // scan takes an alias and each key normalizes on its own
            // side.
            let plan = scan(&e.ct, &child)
                .and_then(|b| b.filter(all_present(&e.cc)))
                .and_then(|b| {
                    let p = scan(&e.pt, &parent)?.filter(all_present(&e.pc))?;
                    let p = if same_table { p.alias("to_side")? } else { p };
                    let p = p.build()?;
                    b.join(
                        p,
                        JoinType::LeftAnti,
                        (
                            e.cc.iter()
                                .map(datafusion::common::Column::from_name)
                                .collect::<Vec<_>>(),
                            e.pc.iter()
                                .map(datafusion::common::Column::from_name)
                                .collect::<Vec<_>>(),
                        ),
                        None,
                    )
                })
                .and_then(|b| b.aggregate(Vec::<Expr>::new(), vec![count(lit(1)).alias("orphans")]))
                .and_then(|b| b.build())
                .map_err(|e| bad(e.to_string()))?;
            int_column(&run(plan).await?, "orphans").map_err(|e| bad(e.to_string()))?[0]
        };
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
        if !same_table && !pairs.is_empty() {
            // Exact-name qualified columns — `col()` would normalize a
            // mixed-case name to lowercase and miss it.
            let cq = |name: &str| Expr::Column(datafusion::common::Column::new(Some("c"), name));
            let pq = |name: &str| Expr::Column(datafusion::common::Column::new(Some("p"), name));
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
                            e.cc.iter()
                                .map(|c| datafusion::common::Column::new(Some("c"), c))
                                .collect::<Vec<_>>(),
                            e.pc.iter()
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
                let n = int_column(&joined, &format!("n_{k}")).map_err(|e| bad(e.to_string()))?[0];
                if n == 0 {
                    continue;
                }
                let pre =
                    int_column(&joined, &format!("pre_{k}")).map_err(|e| bad(e.to_string()))?[0];
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
