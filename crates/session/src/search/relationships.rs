//! `relationship_candidates('dataset')`: every plausible join pair
//! across the landed tables, counted in the engine.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::datasource::provider_as_source;
use datafusion::execution::session_state::SessionState;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ident, lit};
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::SessionError;

use super::{dataset_pins, detector_state, int_column, rows_batch, run_plan};

/// `relationship_candidates('dataset')` — the high-recall half of the
/// candidate → verified → declared arc (fixture 12): every plausible
/// join pair across the landed tables, generous by design —
/// the statistical pass optimizes recall and the judge
/// removes false positives against the data; this door never does.
///
/// The algorithm is inclusion-dependency discovery on the SPIDER/SINDY
/// shape, and every set operation in it is a plan: no column's values
/// are ever held outside the memory pool. One pass per dtype
/// ([`pair_counts`]) unpivots each table's same-typed columns into
/// `(arm, value)` rows, takes the distinct of those, and joins the
/// result to itself on the value — so a matched row is one value two
/// arms share, the input being distinct. Grouped by the arm pair, the
/// count is the containment numerator; the pair `(i, i)` is the arm's
/// own distinct count. What leaves the engine is that matrix, one row
/// per arm pair: sized by the schema and never by the data, which is
/// what separates it from a set of values. The statistic is
/// containment — matched over the from side's distinct count: the
/// question is directional, and a small key set fully inside a large
/// one scores perfectly whatever the size skew.
///
/// Values compare in their own type. A pass is scoped to one dtype, so
/// nothing is cast and the join compares values as they are stored;
/// arms of different types never meet in a pass, which is where the
/// dtype gate lives.
///
/// Nothing prunes what is looked at. The 0.5 acceptance bar implies
/// the prune this door used to carry — matched ≤ to_distinct, so a to
/// side under half the from side's distinct count cannot reach it —
/// and a pair costs nothing once the matrix exists.
///
/// The composite rescue: a to side that is no key alone can be one
/// inside a scope — the multi-tenant shape, (businessID, name). For
/// each overlapping pair whose to side is not key-like, the co-present
/// pairs between the same two tables are tried as the scoping leg in
/// overlap order; the first whose combination makes the to side
/// near-unique and whose two-leg intersection resolves rescues the
/// anchor. A scoping leg that is unique on its own is never tried: the
/// pair could identify no more than the leg does, and the leg's own
/// candidate already stands. Two tables a key-like pair already joins
/// are never tried either: a scope could only re-key a pair that has
/// a key. Here only the named from–to pairs are
/// wanted, not every pair, so the composite pass has a different shape
/// from the width-1 matrix: first every combination's own distinct
/// count, an aggregate with no join, which settles the key test and
/// the prunes it implies in Rust over schema-sized numbers; then the
/// surviving from combinations joined to the surviving to combinations
/// on both legs, never a side to itself. Width 2 only — wider
/// composites stay future work. Data decides, not names.
pub(crate) async fn relationship_candidates(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<RecordBatch, SessionError> {
    let door = format!("relationship_candidates('{dataset}')");
    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let ctx = shared.session_ctx();
    // The shape scans are fixed-memory aggregates and run through the
    // session's own state; the pair passes run through the detector's.
    let scans = ctx.state();
    let state = detector_state(&ctx);

    let pinned = dataset_pins(shared, dataset).await?;
    let resolved = &pinned;
    let mut tables: Vec<String> = resolved.tables();
    tables.sort();

    // Per-column shape: the filled count. The distinct count is the
    // diagonal of the pair pass and is never asked for separately.
    struct ColShape {
        table: String,
        column: String,
        filled: i64,
        /// Date or Timestamp: a reference is never a point in time.
        temporal: bool,
    }
    let mut cols: Vec<ColShape> = Vec::new();
    let mut rows: HashMap<String, i64> = HashMap::new();
    for t in &tables {
        let provider = resolved
            .pin(t)
            .ok_or_else(|| bad(format!("no pin for `{t}`")))?;
        // Scalar columns only: a key is a scalar, so a nested column is
        // never a candidate.
        let fields = provider.schema();
        let scalar: Vec<_> = fields
            .fields()
            .iter()
            .filter(|f| !f.data_type().is_nested())
            .collect();
        if scalar.is_empty() {
            continue;
        }
        let mut aggs: Vec<Expr> = scalar
            .iter()
            .map(|f| count(ident(f.name())).alias(format!("f_{}", f.name())))
            .collect();
        aggs.push(count(lit(1)).alias("n"));
        let plan = LogicalPlanBuilder::scan(t.as_str(), provider_as_source(provider), None)
            .and_then(|b| b.aggregate(Vec::<Expr>::new(), aggs))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        let batches = run_plan(&scans, plan)
            .await
            .map_err(|e| SessionError::door(&door, e))?;
        let one = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| bad(format!("the shape scan of `{t}` returned nothing")))?;
        let counted = |i: usize| {
            one.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| bad("a count did not read as an integer".into()))
                .map(|a| a.value(0))
        };
        for (i, f) in scalar.iter().enumerate() {
            cols.push(ColShape {
                table: t.clone(),
                column: f.name().clone(),
                filled: counted(i)?,
                temporal: matches!(
                    f.data_type(),
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
                ),
            });
        }
        rows.insert(t.clone(), counted(scalar.len())?);
    }

    // One arm per scalar column, and the whole matrix in one pass per
    // dtype.
    let arms: Vec<Arm> = cols
        .iter()
        .map(|c| Arm {
            table: c.table.clone(),
            columns: vec![c.column.clone()],
        })
        .collect();
    let counts = pair_counts(&state, resolved, &door, &arms).await?;
    let distinct = |i: usize| counts.get(i, i);
    // Near-unique, not strictly unique, so dirty keys stay in the
    // running.
    let key_like = |i: usize| {
        let d = distinct(i);
        cols[i].filled > 0 && d >= 2 && d as f64 / cols[i].filled as f64 >= 0.9
    };
    let unique = |i: usize| cols[i].filled > 0 && distinct(i) == cols[i].filled;

    // Every pair the bar admits, keys or not — the non-key pairs are
    // the raw material for composite rescue. Same-table pairs stay in.
    // A cross-dtype pair carries a zero off the matrix and falls out
    // here rather than being excluded by name.
    struct Pair {
        f: usize,
        k: usize,
        overlap: f64,
        matched: i64,
    }
    let mut pairs: Vec<Pair> = Vec::new();
    for ki in 0..cols.len() {
        if distinct(ki) < 2 {
            continue;
        }
        for fi in 0..cols.len() {
            if cols[fi].table == cols[ki].table && cols[fi].column == cols[ki].column {
                continue;
            }
            let from_distinct = distinct(fi);
            if from_distinct == 0 {
                continue;
            }
            let matched = counts.get(fi, ki);
            let overlap = matched as f64 / from_distinct as f64;
            if overlap < 0.5 {
                continue;
            }
            pairs.push(Pair {
                f: fi,
                k: ki,
                overlap,
                matched,
            });
        }
    }

    // Single-column candidates: the pairs whose to side stands as a key
    // on its own. The row says what the to side is — exactly unique in
    // its table, a point in time — beside how far the from side
    // reaches it; the body ranks on both, and demotes only.
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
        to_unique: bool,
        to_temporal: bool,
        key_columns: Option<(String, String)>,
    }
    let path = |i: usize| format!("{}.{}", cols[i].table, cols[i].column);
    let mut candidates: Vec<Candidate> = Vec::new();
    for p in &pairs {
        if !key_like(p.k) {
            continue;
        }
        candidates.push(Candidate {
            from: path(p.f),
            to: path(p.k),
            cardinality: if unique(p.f) {
                "one-to-one"
            } else {
                "many-to-one"
            },
            overlap: p.overlap,
            matched: p.matched,
            orphans: distinct(p.f) - p.matched,
            from_distinct: distinct(p.f),
            to_distinct: distinct(p.k),
            to_unique: unique(p.k),
            to_temporal: cols[p.k].temporal,
            key_columns: None,
        });
    }

    // Composite rescue: the attempts, then the combinations either side
    // needs, counted in two phases with the exact prunes between them.
    struct Attempt {
        p: usize, // into pairs — the anchor leg
        s: usize,
        order: i64,
        from: usize, // into combos
        to: usize,
    }
    // The combinations both sides need, interned once each: the same
    // (table, a, b) can be a to side for one attempt and a from side
    // for another, and an arm counted twice is a scan wasted.
    let mut combos: Vec<(String, String, String)> = Vec::new();
    let mut combo_of: HashMap<(String, String, String), usize> = HashMap::new();
    let mut intern = |leg: &ColShape, scope: &ColShape| {
        let key = (leg.table.clone(), leg.column.clone(), scope.column.clone());
        let next = combos.len();
        let at = *combo_of.entry(key.clone()).or_insert(next);
        if at == next {
            combos.push(key);
        }
        at
    };
    // The scopes an anchor can take are the pairs between its two
    // tables, in overlap order — one order per table pair, shared by
    // every anchor on it.
    let mut by_tables: HashMap<(&str, &str), Vec<usize>> = HashMap::new();
    for (si, s) in pairs.iter().enumerate() {
        by_tables
            .entry((cols[s.f].table.as_str(), cols[s.k].table.as_str()))
            .or_default()
            .push(si);
    }
    for list in by_tables.values_mut() {
        // Total order over floats: a NaN sorts last instead of panicking
        // the read.
        list.sort_by(|a, b| pairs[*b].overlap.total_cmp(&pairs[*a].overlap));
    }
    // Two tables one key-like pair already joins are never tried for
    // a composite: a scope beside a second column could only re-key a
    // pair that has a key, and what such a pass serves is two date or
    // code columns that happen to coincide. The multi-tenant shape is
    // untouched — no single column keys it.
    let linked: HashSet<(&str, &str)> = pairs
        .iter()
        .filter(|p| key_like(p.k))
        .map(|p| {
            let (a, b) = (cols[p.f].table.as_str(), cols[p.k].table.as_str());
            (a.min(b), a.max(b))
        })
        .collect();
    let mut attempts: Vec<Attempt> = Vec::new();
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    for (pi, p) in pairs.iter().enumerate() {
        if key_like(p.k) {
            continue;
        }
        let (ta, tb) = (cols[p.f].table.as_str(), cols[p.k].table.as_str());
        if linked.contains(&(ta.min(tb), ta.max(tb))) {
            continue;
        }
        let Some(scopes) = by_tables.get(&(cols[p.f].table.as_str(), cols[p.k].table.as_str()))
        else {
            continue;
        };
        let mut order = 0i64;
        for &si in scopes {
            let s = &pairs[si];
            if cols[s.f].column == cols[p.f].column || cols[s.k].column == cols[p.k].column {
                continue;
            }
            // A scoping leg that is unique on its own did all the
            // identifying: the pair's distinct count cannot exceed the
            // leg's, so the composite would add nothing to the leg's
            // own candidate.
            if unique(s.k) {
                continue;
            }
            // The near-unique bar, bounded from the shape scan alone:
            // the pair's distinct count is at most the product of the
            // legs', and the rows carrying both legs are at least the
            // legs' filled counts less the table's rows. Implied by the
            // bar, never a heuristic — what it keeps, the co-filled
            // count settles below.
            let floor = (cols[p.k].filled + cols[s.k].filled - rows[&cols[p.k].table]).max(0);
            if (distinct(p.k) as f64) * (distinct(s.k) as f64) < 0.9 * floor as f64 {
                continue;
            }
            // An attempt and its mirror — the scope as the anchor — are
            // one composite.
            if !seen.insert((pi.min(si), pi.max(si))) {
                continue;
            }
            attempts.push(Attempt {
                p: pi,
                s: si,
                order,
                from: intern(&cols[p.f], &cols[s.f]),
                to: intern(&cols[p.k], &cols[s.k]),
            });
            order += 1;
        }
    }
    // What the composite pass is asked, for the run log: the attempts,
    // the combinations they need, and the table pairs they span.
    let table_pairs: HashSet<(&str, &str)> = attempts
        .iter()
        .map(|a| {
            (
                cols[pairs[a.p].f].table.as_str(),
                cols[pairs[a.p].k].table.as_str(),
            )
        })
        .collect();
    tracing::debug!(
        attempts = attempts.len(),
        combos = combos.len(),
        table_pairs = table_pairs.len(),
        "composite rescue asked"
    );
    let combo_arms: Vec<Arm> = combos
        .iter()
        .map(|(t, a, b)| Arm {
            table: t.clone(),
            columns: vec![a.clone(), b.clone()],
        })
        .collect();

    // The co-filled count of every combination, then the first exact
    // prune: implied by the near-unique bar, never a heuristic. The
    // combined to side's distinct pairs cannot exceed the product of
    // the legs' distinct counts, so a product under 0.9 of the
    // co-filled count can never key the table.
    let filled = combo_filled(&scans, resolved, &door, &combos).await?;
    attempts.retain(|a| {
        let (p, s) = (&pairs[a.p], &pairs[a.s]);
        filled[a.to] > 0
            && (distinct(p.k) as f64) * (distinct(s.k) as f64) >= 0.9 * filled[a.to] as f64
    });

    // Phase A: every surviving combination's own distinct count, an
    // aggregate and no join. Then the prunes it makes exact, in Rust
    // over schema-sized numbers.
    let needed: HashSet<usize> = attempts.iter().flat_map(|a| [a.from, a.to]).collect();
    let combo_distinct = arm_distinct(&state, resolved, &door, &combo_arms, &needed).await?;
    // The combined to side keys its table inside the scope.
    let combo_ok = |i: usize| {
        let d = combo_distinct[i];
        filled[i] > 0 && d >= 2 && d as f64 / filled[i] as f64 >= 0.9
    };
    attempts.retain(|a| {
        let s = &pairs[a.s];
        let (df, dt) = (combo_distinct[a.from], combo_distinct[a.to]);
        combo_ok(a.to)
            && df > 0
            // matched ≤ the to side's distinct count, and the rescue
            // needs matched ≥ half the from side's.
            && dt as f64 >= 0.5 * df as f64
            // The scoping leg must identify more with the anchor than
            // it does alone; otherwise the leg's own candidate already
            // carries everything the pair would. Inside one scope the
            // anchor may well be unique by itself — that is the shape
            // the rescue exists for — so the anchor leg is not held to
            // this.
            && dt > distinct(s.k)
    });

    // Phase B: the from sides joined to the to sides on both legs,
    // never one side to itself, grouped by the pair.
    let from_arms: HashSet<usize> = attempts.iter().map(|a| a.from).collect();
    let to_arms: HashSet<usize> = attempts.iter().map(|a| a.to).collect();
    tracing::debug!(
        attempts = attempts.len(),
        from_arms = from_arms.len(),
        to_arms = to_arms.len(),
        "composite rescue joined"
    );
    let matched = cross_counts(&state, resolved, &door, &combo_arms, &from_arms, &to_arms).await?;

    // The two-leg resolution reads off the pass. First passing scope
    // per anchor, in overlap order.
    struct Rescue {
        order: i64,
        p: usize,
        s: usize,
        matched: i64,
        overlap: f64,
        to_distinct: i64,
        tfilled: i64,
        ffilled: i64,
        fpairs: i64,
    }
    let mut rescued: HashMap<usize, Rescue> = HashMap::new();
    for a in &attempts {
        let fpairs = combo_distinct[a.from];
        let matched = matched.get(&(a.from, a.to)).copied().unwrap_or(0);
        let overlap = matched as f64 / fpairs as f64;
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
                matched,
                overlap,
                to_distinct: combo_distinct[a.to],
                tfilled: filled[a.to],
                ffilled: filled[a.from],
                fpairs,
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
        if distinct(pairs[r.s].k) > distinct(pairs[r.p].k) {
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
            // The tuple is the key: unique when its distinct pairs are
            // its co-filled rows; temporal when the identifying leg is.
            to_unique: r.to_distinct == r.tfilled,
            to_temporal: cols[a.k].temporal,
            key_columns: Some((path(sc.f), path(sc.k))),
        });
    }

    // One row per candidate, seq in push order — the body's last
    // tie-breaker, which reproduces the script's stable sort.
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
        row.insert("to_unique".into(), json!(c.to_unique));
        row.insert("to_temporal".into(), json!(c.to_temporal));
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

/// One arm of a pair pass: a table's column, or the two columns of a
/// composite. Every arm of one pass carries the same column types.
struct Arm {
    table: String,
    columns: Vec<String>,
}

/// A pair pass's answer: `get(i, j)` is how many values arms `i` and
/// `j` both carry, and `get(i, i)` is arm `i`'s own distinct count.
/// Square in the arm count — sized by the schema and never by the
/// data, which is what separates it from a set of values.
struct Counts {
    n: usize,
    m: Vec<i64>,
}

impl Counts {
    fn new(n: usize) -> Self {
        Self {
            n,
            m: vec![0; n * n],
        }
    }

    fn at(&self, a: usize, b: usize) -> usize {
        let (i, j) = if a <= b { (a, b) } else { (b, a) };
        i * self.n + j
    }

    fn get(&self, a: usize, b: usize) -> i64 {
        self.m[self.at(a, b)]
    }

    fn set(&mut self, a: usize, b: usize, v: i64) {
        let at = self.at(a, b);
        self.m[at] = v;
    }
}

/// A key the merge join can compare. Its comparator has no arm for a
/// zoned timestamp or a time of day (datafusion-physical-plan
/// `joins/utils.rs`, `compare_join_arrays`), which is how Iceberg lands
/// `timestamptz` and `time`; those join as the integer they are stored
/// as. A pass is scoped to one stored type, so the reinterpretation
/// meets only its own kind, and it is one-to-one, so every count is
/// the count of the stored values. Everything else joins as stored.
fn joinable(value: Expr, stored: &DataType) -> Expr {
    use datafusion::logical_expr::cast;
    match stored {
        DataType::Timestamp(_, Some(_)) | DataType::Time64(_) => cast(value, DataType::Int64),
        DataType::Time32(_) => cast(value, DataType::Int32),
        _ => value,
    }
}

/// One pass: the stored types its arms share, and the arms.
type Pass = (Vec<DataType>, Vec<usize>);

/// The passes an arm list needs: the arms grouped by their columns'
/// stored types, in first-seen order. Arms of different types can share
/// nothing, and a pass scoped to one type never casts — the join
/// compares values as they are stored, which is the typed identity the
/// counts claim.
fn passes(
    resolved: &crate::prepass::Resolved,
    door: &str,
    arms: &[Arm],
) -> Result<Vec<Pass>, SessionError> {
    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let mut groups: Vec<Pass> = Vec::new();
    for (i, arm) in arms.iter().enumerate() {
        let provider = resolved
            .pin(&arm.table)
            .ok_or_else(|| bad(format!("no pin for `{}`", arm.table)))?;
        let schema = provider.schema();
        let mut shape = Vec::with_capacity(arm.columns.len());
        for c in &arm.columns {
            shape.push(
                schema
                    .field_with_name(c)
                    .map_err(|e| bad(e.to_string()))?
                    .data_type()
                    .clone(),
            );
        }
        match groups.iter_mut().find(|(s, _)| *s == shape) {
            Some((_, members)) => members.push(i),
            None => groups.push((shape, vec![i])),
        }
    }
    Ok(groups)
}

/// The distinct `(ci, value…)` rows of some arms of one pass — `ci`
/// the arm's index, `v0…` its columns' values.
///
/// Each table is unpivoted — `make_array` over its arms' columns and
/// one `unnest`, so the plan carries a union arm per table rather than
/// per column, which is what bounds its memory consumers: the engine
/// repartitions every union arm and runs a partial aggregate per
/// partition, and the fair pool divides its share among every consumer
/// registered. The union of the tables is then made distinct. A row
/// with a null leg is dropped, so no join key downstream is ever null
/// and null equality never enters it.
fn distinct_values(
    resolved: &crate::prepass::Resolved,
    door: &str,
    arms: &[Arm],
    shape: &[DataType],
    members: &[usize],
) -> Result<LogicalPlan, SessionError> {
    use datafusion::common::{Column, UnnestOptions};
    use datafusion::functions_nested::expr_fn::make_array;
    use datafusion::logical_expr::col;

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let width = shape.len();
    let mut tables: Vec<&str> = Vec::new();
    for &i in members {
        if !tables.contains(&arms[i].table.as_str()) {
            tables.push(arms[i].table.as_str());
        }
    }
    let mut union: Option<LogicalPlanBuilder> = None;
    for table in tables {
        let provider = resolved
            .pin(table)
            .ok_or_else(|| bad(format!("no pin for `{table}`")))?;
        let mine: Vec<usize> = members
            .iter()
            .copied()
            .filter(|&i| arms[i].table == table)
            .collect();
        let mut lists = vec![make_array(mine.iter().map(|&i| lit(i as i64)).collect()).alias("ci")];
        for (k, stored) in shape.iter().enumerate() {
            lists.push(
                make_array(
                    mine.iter()
                        .map(|&i| joinable(ident(&arms[i].columns[k]), stored))
                        .collect(),
                )
                .alias(format!("v{k}")),
            );
        }
        let mut zipped = vec![Column::from_name("ci")];
        zipped.extend((0..width).map(|k| Column::from_name(format!("v{k}"))));
        let mut present = col("v0").is_not_null();
        for k in 1..width {
            present = present.and(col(format!("v{k}")).is_not_null());
        }
        let arm_plan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .and_then(|b| b.project(lists))
            .and_then(|b| b.unnest_columns_with_options(zipped, UnnestOptions::default()))
            .and_then(|b| b.filter(present))
            .and_then(|b| b.build())
            .map_err(|e| bad(e.to_string()))?;
        union = Some(match union {
            None => LogicalPlanBuilder::from(arm_plan),
            Some(u) => u.union(arm_plan).map_err(|e| bad(e.to_string()))?,
        });
    }
    union
        .ok_or_else(|| bad("a pass with no arms".into()))?
        .distinct()
        .and_then(|b| b.build())
        .map_err(|e| bad(e.to_string()))
}

/// The plan of `left` joined to `right` on every value leg, grouped by
/// the arm pair, `m` the shared-value count. Both inputs are distinct
/// `(ci, value…)` rows, so a joined row is one value both arms carry.
fn shared_counts(
    door: &str,
    width: usize,
    left: LogicalPlan,
    right: LogicalPlan,
    filter: Option<Expr>,
) -> Result<LogicalPlan, SessionError> {
    use datafusion::common::{Column, NullEquality};
    use datafusion::logical_expr::{JoinType, col};

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let keys: (Vec<Column>, Vec<Column>) = (
        (0..width)
            .map(|k| Column::from_qualified_name(format!("a.v{k}")))
            .collect(),
        (0..width)
            .map(|k| Column::from_qualified_name(format!("b.v{k}")))
            .collect(),
    );
    let right = LogicalPlanBuilder::from(right)
        .alias("b")
        .and_then(|b| b.build())
        .map_err(|e| bad(e.to_string()))?;
    LogicalPlanBuilder::from(left)
        .alias("a")
        .and_then(|b| {
            b.join_detailed(
                right,
                JoinType::Inner,
                keys,
                filter,
                NullEquality::NullEqualsNothing,
            )
        })
        .and_then(|b| b.project(vec![col("a.ci").alias("i"), col("b.ci").alias("j")]))
        .and_then(|b| b.aggregate(vec![col("i"), col("j")], vec![count(lit(1)).alias("m")]))
        .and_then(|b| b.build())
        .map_err(|e| bad(e.to_string()))
}

/// Every arm pair's shared-value count, as plans: one pass per type
/// tuple, the distinct `(arm, value…)` rows joined to themselves on the
/// value. Grouped by the arm pair it is the containment numerator. The
/// `a <= b` filter halves the output and keeps the diagonal, so one
/// plan answers both the matched counts and the distinct counts. Every
/// pair is wanted here, which is what makes the self-join the right
/// shape: a value carried by `k` arms costs `k(k+1)/2` join rows, one
/// per pair asked. A type carried by one arm alone is skipped: nothing
/// can pair with it.
async fn pair_counts(
    state: &SessionState,
    resolved: &crate::prepass::Resolved,
    door: &str,
    arms: &[Arm],
) -> Result<Counts, SessionError> {
    use datafusion::logical_expr::col;

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let mut counts = Counts::new(arms.len());
    for (shape, members) in passes(resolved, door, arms)?
        .iter()
        .filter(|(_, m)| m.len() > 1)
    {
        let values = distinct_values(resolved, door, arms, shape, members)?;
        let plan = shared_counts(
            door,
            shape.len(),
            values.clone(),
            values,
            Some(col("a.ci").lt_eq(col("b.ci"))),
        )?;
        for b in run_plan(state, plan)
            .await
            .map_err(|e| SessionError::door(door, e))?
            .iter()
            .filter(|b| b.num_rows() > 0)
        {
            let one = std::slice::from_ref(b);
            let i = int_column(one, "i").map_err(|e| bad(e.to_string()))?;
            let j = int_column(one, "j").map_err(|e| bad(e.to_string()))?;
            let m = int_column(one, "m").map_err(|e| bad(e.to_string()))?;
            for r in 0..b.num_rows() {
                counts.set(i[r] as usize, j[r] as usize, m[r]);
            }
        }
    }
    Ok(counts)
}

/// The wanted arms' own distinct counts, as plans: one pass per type
/// tuple, the distinct `(arm, value…)` rows grouped by the arm. No
/// join — an aggregate that spills, one traversal per table — so a
/// combination's key test costs nothing of the pairing.
async fn arm_distinct(
    state: &SessionState,
    resolved: &crate::prepass::Resolved,
    door: &str,
    arms: &[Arm],
    wanted: &HashSet<usize>,
) -> Result<Vec<i64>, SessionError> {
    use datafusion::logical_expr::col;

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let mut out = vec![0i64; arms.len()];
    for (shape, members) in passes(resolved, door, arms)? {
        let members: Vec<usize> = members.into_iter().filter(|i| wanted.contains(i)).collect();
        if members.is_empty() {
            continue;
        }
        let plan =
            LogicalPlanBuilder::from(distinct_values(resolved, door, arms, &shape, &members)?)
                .aggregate(vec![col("ci")], vec![count(lit(1)).alias("d")])
                .and_then(|b| b.build())
                .map_err(|e| bad(e.to_string()))?;
        for b in run_plan(state, plan)
            .await
            .map_err(|e| SessionError::door(door, e))?
            .iter()
            .filter(|b| b.num_rows() > 0)
        {
            let one = std::slice::from_ref(b);
            let ci = int_column(one, "ci").map_err(|e| bad(e.to_string()))?;
            let d = int_column(one, "d").map_err(|e| bad(e.to_string()))?;
            for r in 0..b.num_rows() {
                out[ci[r] as usize] = d[r];
            }
        }
    }
    Ok(out)
}

/// How many values each from arm shares with each to arm, as plans:
/// one pass per type tuple, the from arms' distinct rows joined to the
/// to arms' on the value, grouped by the pair. Two sides, never one
/// side joined to itself: a value carried by `k` from arms and `l` to
/// arms costs `k × l` join rows, and no from–from or to–to pair, which
/// nothing asks for, is ever computed. An arm on both sides is read on
/// both.
async fn cross_counts(
    state: &SessionState,
    resolved: &crate::prepass::Resolved,
    door: &str,
    arms: &[Arm],
    from: &HashSet<usize>,
    to: &HashSet<usize>,
) -> Result<HashMap<(usize, usize), i64>, SessionError> {
    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let mut out = HashMap::new();
    for (shape, members) in passes(resolved, door, arms)? {
        let f: Vec<usize> = members
            .iter()
            .copied()
            .filter(|i| from.contains(i))
            .collect();
        let t: Vec<usize> = members.iter().copied().filter(|i| to.contains(i)).collect();
        if f.is_empty() || t.is_empty() {
            continue;
        }
        let plan = shared_counts(
            door,
            shape.len(),
            distinct_values(resolved, door, arms, &shape, &f)?,
            distinct_values(resolved, door, arms, &shape, &t)?,
            None,
        )?;
        for b in run_plan(state, plan)
            .await
            .map_err(|e| SessionError::door(door, e))?
            .iter()
            .filter(|b| b.num_rows() > 0)
        {
            let one = std::slice::from_ref(b);
            let i = int_column(one, "i").map_err(|e| bad(e.to_string()))?;
            let j = int_column(one, "j").map_err(|e| bad(e.to_string()))?;
            let m = int_column(one, "m").map_err(|e| bad(e.to_string()))?;
            for r in 0..b.num_rows() {
                out.insert((i[r] as usize, j[r] as usize), m[r]);
            }
        }
    }
    Ok(out)
}

/// How many rows carry both legs of each combination — one aggregate
/// scan per table with a count per combination, the shape scan's form,
/// fixed memory, in the combinations' order.
async fn combo_filled(
    state: &SessionState,
    resolved: &crate::prepass::Resolved,
    door: &str,
    combos: &[(String, String, String)],
) -> Result<Vec<i64>, SessionError> {
    use datafusion::logical_expr::when;

    let bad = |d: String| SessionError::BadSubject(format!("{door}: {d}"));
    let mut out = vec![0i64; combos.len()];
    let mut tables: Vec<&str> = Vec::new();
    for (table, _, _) in combos {
        if !tables.contains(&table.as_str()) {
            tables.push(table);
        }
    }
    for table in tables {
        let provider = resolved
            .pin(table)
            .ok_or_else(|| bad(format!("no pin for `{table}`")))?;
        let mine: Vec<usize> = (0..combos.len())
            .filter(|&id| combos[id].0 == table)
            .collect();
        let mut aggs = Vec::with_capacity(mine.len());
        for &id in &mine {
            let (_, a, b) = &combos[id];
            let both = ident(a).is_not_null().and(ident(b).is_not_null());
            let one = when(both, lit(1)).end().map_err(|e| bad(e.to_string()))?;
            aggs.push(count(one).alias(format!("c_{id}")));
        }
        let plan = LogicalPlanBuilder::scan(table, provider_as_source(provider), None)
            .and_then(|p| p.aggregate(Vec::<Expr>::new(), aggs))
            .and_then(|p| p.build())
            .map_err(|e| bad(e.to_string()))?;
        let batches = run_plan(state, plan)
            .await
            .map_err(|e| SessionError::door(door, e))?;
        let one = batches.iter().find(|b| b.num_rows() > 0).ok_or_else(|| {
            bad(format!(
                "the combination scan of `{table}` returned nothing"
            ))
        })?;
        for (i, &id) in mine.iter().enumerate() {
            out[id] = one
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| bad("a count did not read as an integer".into()))?
                .value(0);
        }
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
        Field::new("to_unique", DataType::Boolean, true),
        Field::new("to_temporal", DataType::Boolean, true),
        Field::new("kc_from", DataType::Utf8, true),
        Field::new("kc_to", DataType::Utf8, true),
    ]
}
