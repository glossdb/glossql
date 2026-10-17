//! `relationship_checks('dataset')`: what the declared joins assert,
//! checked against the rows.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::TableProvider;
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{
    Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, ident, lit,
};
use serde_json::{Value, json};

use crate::shared::{SessionError, Shared};

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
    let pinned = dataset_pins(shared, dataset).await?;
    let check = Check {
        dataset,
        ctx: shared.session_ctx(),
        resolved: &pinned,
    };
    let entries = declared_entries(shared, dataset).await?;
    if entries.is_empty() {
        return rows_batch(
            vec![json!({"applicable": false, "reason": "no declared relationships"})],
            coherence_shape(),
        );
    }
    let date_cols = date_columns(&check, &entries)?;

    let mut out = Vec::new();
    for (seq, e) in entries.iter().enumerate() {
        let child = check.pin(&e.ct)?;
        let parent = check.pin(&e.pt)?;
        let filled = filled_keys(&check, e, &child).await?;
        let same_table = e.ct == e.pt;
        let nest = same_table && is_nest(&check, e, &parent).await?;
        let orphans = if nest {
            nest_orphans(&check, e, &child).await?
        } else {
            join_orphans(&check, e, &child, &parent, same_table).await?
        };
        let orphan_rate = if filled > 0 {
            orphans as f64 / filled as f64
        } else {
            0.0
        };
        let temporal = if same_table {
            Vec::new()
        } else {
            temporal_pairs(&check, e, &child, &parent, &date_cols).await?
        };

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

/// One declared relationship as the checks read it: the from (child)
/// side's table and columns, the to (parent) side's, and the edge as
/// written.
struct Entry {
    relationship: String,
    ct: String,
    cc: Vec<String>,
    pt: String,
    pc: Vec<String>,
}

/// The dataset's declared relationships, endpoints parsed.
/// `relationships` is workspace-wide and its first column says whose
/// edge this is. Every edge is resolved against the statement's pins
/// and a miss is fatal, so another dataset's edge does not widen this
/// read — it kills it.
async fn declared_entries(shared: &Arc<Shared>, dataset: &str) -> Result<Vec<Entry>, SessionError> {
    use crate::behavior::endpoint;
    let mut entries = Vec::new();
    for row in shared.store.relation_rows("relationships").await? {
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
    Ok(entries)
}

/// One read's context: the dataset for its refusals, the session's
/// context its plans run on, and the statement's pins.
struct Check<'a> {
    dataset: &'a str,
    ctx: datafusion::prelude::SessionContext,
    resolved: &'a crate::shared::Resolved,
}

impl Check<'_> {
    fn bad(&self, d: String) -> SessionError {
        SessionError::shape(
            self.dataset,
            format!("relationship_checks('{}'): {d}", self.dataset),
        )
    }

    fn pin(&self, t: &str) -> Result<Arc<dyn TableProvider>, SessionError> {
        self.resolved
            .pin(t)
            .ok_or_else(|| self.bad(format!("no table `{t}` in the bound dataset")))
    }

    async fn run(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>, SessionError> {
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| self.bad(e.to_string()))?
            .collect()
            .await
            .map_err(|e| self.bad(e.to_string()))
    }

    /// One named Int64 cell of a plan's one-row answer.
    async fn count(&self, plan: LogicalPlan, name: &str) -> Result<i64, SessionError> {
        Ok(int_column(&self.run(plan).await?, name).map_err(|e| self.bad(e.to_string()))?[0])
    }
}

fn scan(name: &str, p: &Arc<dyn TableProvider>) -> datafusion::common::Result<LogicalPlanBuilder> {
    LogicalPlanBuilder::scan(name, provider_as_source(Arc::clone(p)), None)
}

fn all_present(cols: &[String]) -> Expr {
    cols.iter()
        .map(|c| ident(c).is_not_null())
        .reduce(|a, b| a.and(b))
        .expect("at least one column")
}

/// Date columns per involved table, once, from the pins' own schema,
/// capped at three per side.
fn date_columns(
    check: &Check<'_>,
    entries: &[Entry],
) -> Result<HashMap<String, Vec<String>>, SessionError> {
    let mut date_cols: HashMap<String, Vec<String>> = Default::default();
    for e in entries {
        for t in [e.ct.as_str(), e.pt.as_str()] {
            if date_cols.contains_key(t) {
                continue;
            }
            let provider = check.pin(t)?;
            let dates: Vec<String> = provider
                .schema()
                .fields()
                .iter()
                .filter(|f| crate::plan::is_temporal(f.data_type()))
                .take(3)
                .map(|f| f.name().clone())
                .collect();
            date_cols.insert(t.to_string(), dates);
        }
    }
    Ok(date_cols)
}

/// Filled: rows carrying a complete from-side key.
async fn filled_keys(
    check: &Check<'_>,
    e: &Entry,
    child: &Arc<dyn TableProvider>,
) -> Result<i64, SessionError> {
    let filled_expr = if let [c] = e.cc.as_slice() {
        count(ident(c))
    } else {
        count(lit(1))
            .filter(all_present(&e.cc))
            .build()
            .map_err(|e| check.bad(e.to_string()))?
    };
    let plan = scan(&e.ct, child)
        .and_then(|b| b.aggregate(Vec::<Expr>::new(), vec![filled_expr.alias("filled")]))
        .and_then(|b| b.build())
        .map_err(|e| check.bad(e.to_string()))?;
    check.count(plan, "filled").await
}

/// Within one table, a unique to-side is a key and the edge a
/// self-reference; a repeating to-side is the coarser level of a
/// nest. One pass over the to-side decides.
async fn is_nest(
    check: &Check<'_>,
    e: &Entry,
    parent: &Arc<dyn TableProvider>,
) -> Result<bool, SessionError> {
    use datafusion::functions_aggregate::expr_fn::sum;
    let coarser: Vec<Expr> = e.pc.iter().map(ident).collect();
    let plan = scan(&e.pt, parent)
        .and_then(|b| b.filter(all_present(&e.pc)))
        .and_then(|b| b.aggregate(coarser, vec![count(lit(1)).alias("n")]))
        .and_then(|b| {
            b.aggregate(
                Vec::<Expr>::new(),
                vec![count(lit(1)).alias("groups"), sum(ident("n")).alias("rows")],
            )
        })
        .and_then(|b| b.build())
        .map_err(|e| check.bad(e.to_string()))?;
    let counted = check.run(plan).await?;
    let groups = int_column(&counted, "groups").map_err(|e| check.bad(e.to_string()))?[0];
    let rows = int_column(&counted, "rows").map_err(|e| check.bad(e.to_string()))?[0];
    Ok(groups != rows)
}

/// A nest's orphans — the dependency, in one pass: rows per (finer,
/// coarser), then coarser values per finer; a finer value with more
/// than one fails, and its rows are the orphans.
async fn nest_orphans(
    check: &Check<'_>,
    e: &Entry,
    child: &Arc<dyn TableProvider>,
) -> Result<i64, SessionError> {
    use datafusion::functions::expr_fn::coalesce;
    use datafusion::functions_aggregate::expr_fn::sum;
    let keys: Vec<Expr> = e.cc.iter().chain(e.pc.iter()).map(ident).collect();
    let finer: Vec<Expr> = e.cc.iter().map(ident).collect();
    let plan = scan(&e.ct, child)
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
        .map_err(|e| check.bad(e.to_string()))?;
    check.count(plan, "orphans").await
}

/// Orphans: complete from-side keys that resolve to no to-side row —
/// the NOT IN with its null guards, as an anti join. A self-reference
/// joins the table to itself, so the to-side scan takes an alias and
/// each key normalizes on its own side.
async fn join_orphans(
    check: &Check<'_>,
    e: &Entry,
    child: &Arc<dyn TableProvider>,
    parent: &Arc<dyn TableProvider>,
    same_table: bool,
) -> Result<i64, SessionError> {
    use datafusion::common::JoinType;
    let plan = scan(&e.ct, child)
        .and_then(|b| b.filter(all_present(&e.cc)))
        .and_then(|b| {
            let p = scan(&e.pt, parent)?.filter(all_present(&e.pc))?;
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
        .map_err(|e| check.bad(e.to_string()))?;
    check.count(plan, "orphans").await
}

/// Temporal pairs across the join: for each date column pair, how
/// many joined rows carry both dates and how often the child's
/// precedes its parent's. Exact-name qualified columns — `col()` would
/// normalize a mixed-case name to lowercase and miss it. A pair no
/// joined row carries is not served.
async fn temporal_pairs(
    check: &Check<'_>,
    e: &Entry,
    child: &Arc<dyn TableProvider>,
    parent: &Arc<dyn TableProvider>,
    date_cols: &HashMap<String, Vec<String>>,
) -> Result<Vec<(usize, (String, String), i64, i64)>, SessionError> {
    use datafusion::common::JoinType;
    let pairs: Vec<(String, String)> = date_cols[e.ct.as_str()]
        .iter()
        .flat_map(|cd| {
            date_cols[e.pt.as_str()]
                .iter()
                .map(move |pd| (cd.clone(), pd.clone()))
        })
        .collect();
    let mut temporal = Vec::new();
    if pairs.is_empty() {
        return Ok(temporal);
    }
    let cq = |name: &str| Expr::Column(datafusion::common::Column::new(Some("c"), name));
    let pq = |name: &str| Expr::Column(datafusion::common::Column::new(Some("p"), name));
    let mut aggs = Vec::new();
    for (k, (cd, pd)) in pairs.iter().enumerate() {
        aggs.push(
            count(lit(1))
                .filter(cq(cd).is_not_null().and(pq(pd).is_not_null()))
                .build()
                .map_err(|e| check.bad(e.to_string()))?
                .alias(format!("n_{k}")),
        );
        aggs.push(
            count(lit(1))
                .filter(cq(cd).lt(pq(pd)))
                .build()
                .map_err(|e| check.bad(e.to_string()))?
                .alias(format!("pre_{k}")),
        );
    }
    let plan = scan(&e.ct, child)
        .and_then(|b| b.alias("c"))
        .and_then(|b| {
            let p = scan(&e.pt, parent)?.alias("p")?.build()?;
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
        .map_err(|e| check.bad(e.to_string()))?;
    let joined = check.run(plan).await?;
    for (k, pair) in pairs.into_iter().enumerate() {
        let n = int_column(&joined, &format!("n_{k}")).map_err(|e| check.bad(e.to_string()))?[0];
        if n == 0 {
            continue;
        }
        let pre =
            int_column(&joined, &format!("pre_{k}")).map_err(|e| check.bad(e.to_string()))?[0];
        temporal.push((k, pair, n, pre));
    }
    Ok(temporal)
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
