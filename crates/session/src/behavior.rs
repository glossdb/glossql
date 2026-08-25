//! `behavior_anchors('table.column')` — the stock/flow discriminator
//! from v0.3's lineage reconcile (analysis/lineage/reconcile.py),
//! reshaped as an evidence door — the judge
//! reads it before glossing `behavior`; it is never a voice in the
//! behavior slots.
//!
//! The falsification that shaped it (v0.3's own record): a column's OWN
//! trajectory cannot decide stock vs flow — a trending flow and a
//! mean-reverting stock look alike. The evidence must be cross-table:
//! an INDEPENDENT per-period movement m aggregated from a related event
//! table, scored against two hypotheses with scale-free residuals
//!   FLOW : y[t]  ≈ m[t]        R_flow  = Σ|y − m| / Σ|m|
//!   STOCK: Δy[t] ≈ m[t] (t≥1)  R_stock = Σ|Δy − m'| / Σ|m'|
//!
//! Anchors come from DECLARED relationships only:
//! the plane order is relationships first, and the wrong-anchor gate
//! then guards misdeclared edges rather than a combinatorial sweep.
//! Since the port, a composite (tuple) endpoint takes part like any
//! other — every leg is an identifier, the entity key is the tuple —
//! where the script it replaces skipped them and was blind to the
//! composite corpus's whole declared graph (the foundation report §7a).
//!
//! The division of labor (interpreter-bound loops are the
//! alternative): this door discovers anchors and holds POLICY — axes,
//! alignments, grain, which terms are movements, Wilson support, the
//! winner and its alternatives. The `reconcile` kernel, behind the
//! runtime seam, holds the ARITHMETIC. Pairing is on the intersection
//! of (entity, period) cells present on both sides; a calendar gap
//! between kept cells makes Δy span it and read as noise — honest
//! abstention absorbs the miss.
//!
//! Anchor shape (the f1 lessons, kept from the rework):
//! - Declared-edge legs leave the movement pool: a join leg is an
//!   identifier, not a movement. Served on each anchor as
//!   `identifier_columns` — visible, never silent.
//! - A table's own time axes are preferred; it borrows through an edge
//!   only when it has none.
//! - The anchor grain is the COARSER of the two sides' native grains; a
//!   day-native alignment also serves a month variant, and a table
//!   whose only edges are document-keyed borrows its entity one hop
//!   through the document — measure side and event side alike; without
//!   the hop, every anchor on such a table starves.
//! - A cumulative that RESETS is a stock only inside its scope: the raw
//!   alignment AND a year-scoped variant both serve, and the gates kill
//!   the wrong one.
//! - An alignment where fewer than two entities carry 4+ periods
//!   abstains on one HAVING probe, before either side is scanned wide.
//! - A measure that never decreases within an entity — 4+ periods at
//!   the axis's native grain, inside the scope — is its own anchor,
//!   convention `monotone`, voting stock. It is what a cumulative with
//!   no event column to reconcile against (season wins beside a table
//!   of finishing positions) has to say for itself, and a reset shows
//!   as the year scope deciding where the raw one abstains. On equal
//!   support a reconciliation outranks it: the movement explains the
//!   level, monotonicity only describes it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field};
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::search::rows_batch;
use crate::session::SessionError;

/// Wilson score lower bound (Wilson 1927): the parameter-free way to
/// rank a vote rate under small n. n MUST be the pairing's COMMON
/// entity denominator, never a convention's own aligned subset —
/// v0.3's recorded support-gameability trap.
fn wilson_lcb(successes: f64, n: i64) -> f64 {
    if n <= 0 {
        return 0.0;
    }
    let z = 1.96f64;
    let nf = n as f64;
    let p = successes / nf;
    let denom = 1.0 + z * z / nf;
    let centre = p + z * z / (2.0 * nf);
    let margin = z * (p * (1.0 - p) / nf + z * z / (4.0 * nf * nf)).sqrt();
    let v = (centre - margin) / denom;
    if v < 0.0 { 0.0 } else { v }
}

fn grain_rank(g: &str) -> i32 {
    match g {
        "year" => 3,
        "quarter" => 2,
        "month" => 1,
        _ => 0,
    }
}

fn coarser<'a>(a: &'a str, b: &'a str) -> &'a str {
    if grain_rank(a) >= grain_rank(b) { a } else { b }
}

/// A quoted identifier; embedded quotes double, as SQL wants them.
fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// One endpoint of a declared relationship: `t.c` or the tuple
/// `t.(a, b)` — the one parser every reader of `relationships` shares.
pub(crate) fn endpoint(path: &str) -> Option<(String, Vec<String>)> {
    if let Some((t, rest)) = path.split_once(".(") {
        let inner = rest.strip_suffix(')')?;
        let cols: Vec<String> = inner.split(", ").map(str::to_string).collect();
        return (cols.len() >= 2).then(|| (t.to_string(), cols));
    }
    let parts: Vec<&str> = path.split('.').collect();
    let [t, c] = parts.as_slice() else {
        return None;
    };
    Some((t.to_string(), vec![c.to_string()]))
}

#[derive(Clone)]
struct Pointer {
    src_t: String,
    src_cols: Vec<String>,
    dst_t: String,
    dst_cols: Vec<String>,
}

#[derive(Clone)]
struct Axis {
    label: String,
    time_col: String,
    /// A borrowed axis joins the time table on the edge's legs.
    borrowed: Option<(String, Vec<String>, Vec<String>)>, // (table, fk, key)
}

#[derive(Clone)]
struct Align {
    m_cols: Vec<String>,
    e_cols: Vec<String>,
    /// The borrowed-entity hop: the measure side joins the document
    /// table `dd` and keys on ITS dimension column.
    via: Option<(String, Vec<String>, Vec<String>)>, // (table, fk, key)
    /// The same hop on the EVENT side: a document-keyed event table
    /// joins its document master `ee`, and `e_cols` name columns on
    /// that master. Discovered only when the event table aligns no
    /// other way — borrow when starving, like the time-axis borrow.
    e_via: Option<(String, Vec<String>, Vec<String>)>, // (table, fk, key)
}

fn numeric_dtype(d: &str) -> bool {
    d.starts_with("Int")
        || d.starts_with("UInt")
        || d.starts_with("Float")
        || d.starts_with("Decimal")
}

fn temporal_dtype(d: &str) -> bool {
    d.starts_with("Date") || d.starts_with("Timestamp")
}

/// The tuple as one grouping expression: a single column stays its raw
/// typed self (the kernel keys it exactly); legs concatenate by display
/// form, which is injective per dtype.
fn entity_expr(base: &str, cols: &[String]) -> String {
    if let [c] = cols {
        return format!("{base}.{}", qi(c));
    }
    cols.iter()
        .map(|c| format!("CAST({base}.{} AS VARCHAR)", qi(c)))
        .collect::<Vec<_>>()
        .join(" || '|' || ")
}

fn not_null_guard(base: &str, cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("{base}.{} IS NOT NULL", qi(c)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn join_on(left: &str, lcols: &[String], right: &str, rcols: &[String]) -> String {
    lcols
        .iter()
        .zip(rcols)
        .map(|(l, r)| format!("{left}.{} = {right}.{}", qi(l), qi(r)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn cols_label(cols: &[String]) -> String {
    if let [c] = cols {
        c.clone()
    } else {
        format!("({})", cols.join(", "))
    }
}

pub(crate) async fn behavior_anchors(
    shared: &Arc<Shared>,
    resolved: &crate::prepass::Resolved,
    subject: &str,
) -> Result<RecordBatch, SessionError> {
    let ctx = shared.session_ctx();
    let runtime = shared.runtime();
    let run = |q: String| {
        let shared = Arc::clone(shared);
        let ctx = ctx.clone();
        async move {
            let plan = Box::pin(crate::whatif::build_plan(&shared, &ctx, &q)).await?;
            ctx.execute_logical_plan(plan)
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))?
                .collect()
                .await
                .map_err(|e| SessionError::BadSubject(format!("not served: {e}")))
        }
    };
    let bare = |value: Value| rows_batch(vec![value], behavior_shape());

    let Some((table, column)) = subject.split_once('.') else {
        return bare(json!({"applicable": false}));
    };

    // Landed tables and their shapes, from the statement's own pins.
    let mut tables = resolved.tables();
    tables.sort();
    let mut schemas: HashMap<&str, Vec<(String, String)>> = HashMap::new();
    for t in &tables {
        let provider = resolved.pin(t).expect("pinned");
        schemas.insert(
            t,
            provider
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().to_string()))
                .collect(),
        );
    }
    let numeric = schemas
        .get(table)
        .into_iter()
        .flatten()
        .any(|(n, d)| n == column && numeric_dtype(d));
    if !numeric {
        // Not a numeric measure — genuinely doesn't fit, like outliers
        // on text.
        return bare(json!({"applicable": false}));
    }

    // Declared edges as directed pointers; "<->" points both ways.
    // `relationships` is workspace-wide and its first column says whose
    // edge this is. The endpoint check below drops an edge naming a
    // table this dataset lacks, but a foreign edge whose table name
    // also exists here would pass it and supply the wrong evidence.
    let bound = shared.dataset.read().expect("state lock").clone();
    let mut pointers: Vec<Pointer> = Vec::new();
    for row in shared.store.relation_rows("relationships").await? {
        if row[0] != bound {
            continue;
        }
        let (Some(left), Some(op), Some(right)) = (&row[1], &row[2], &row[3]) else {
            continue;
        };
        let (Some((lt, lc)), Some((rt, rc))) = (endpoint(left), endpoint(right)) else {
            continue;
        };
        if lc.len() != rc.len()
            || !schemas.contains_key(lt.as_str())
            || !schemas.contains_key(rt.as_str())
        {
            continue;
        }
        pointers.push(Pointer {
            src_t: lt.clone(),
            src_cols: lc.clone(),
            dst_t: rt.clone(),
            dst_cols: rc.clone(),
        });
        if op == "<->" {
            pointers.push(Pointer {
                src_t: rt,
                src_cols: rc,
                dst_t: lt,
                dst_cols: lc,
            });
        }
    }

    // A declared edge's legs are identifiers — every leg of a tuple too.
    let mut legs: std::collections::HashSet<(String, String)> = Default::default();
    for p in &pointers {
        for c in &p.src_cols {
            legs.insert((p.src_t.clone(), c.clone()));
        }
        for c in &p.dst_cols {
            legs.insert((p.dst_t.clone(), c.clone()));
        }
    }

    // Time axes of a table: its OWN date columns; only a table with
    // none borrows one declared hop away.
    let mut all_axes: HashMap<&str, Vec<Axis>> = HashMap::new();
    for t in &tables {
        let mut axes: Vec<Axis> = Vec::new();
        for (name, dtype) in &schemas[t.as_str()] {
            if temporal_dtype(dtype) {
                axes.push(Axis {
                    label: format!("{t}.{name}"),
                    time_col: name.clone(),
                    borrowed: None,
                });
            }
        }
        if axes.is_empty() {
            for p in &pointers {
                if p.src_t != *t || p.dst_t == *t {
                    continue;
                }
                for (name, dtype) in &schemas[p.dst_t.as_str()] {
                    if temporal_dtype(dtype) {
                        let label = format!("{}.{name} via {}", p.dst_t, cols_label(&p.src_cols));
                        if !axes.iter().any(|a| a.label == label) {
                            axes.push(Axis {
                                label,
                                time_col: name.clone(),
                                borrowed: Some((
                                    p.dst_t.clone(),
                                    p.src_cols.clone(),
                                    p.dst_cols.clone(),
                                )),
                            });
                        }
                    }
                }
            }
        }
        all_axes.insert(t, axes);
    }

    let m_axes = all_axes[table].clone();
    if m_axes.is_empty() {
        return bare(json!({
            "applicable": false,
            "reason": format!(
                "no period axis: no date column on {table} or one declared hop away"
            ),
        }));
    }

    // The axis's NATIVE grain: the coarsest named grain it is already
    // truncated to; event-grain axes are daily.
    let mut grain_cache: HashMap<String, String> = HashMap::new();
    let mut viable_cache: HashMap<String, i64> = HashMap::new();
    let mut y_cache: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    // The monotone read per (axis, entity, scope): entities with 4+
    // periods, how many never decrease, the steps walked.
    let mut mono_cache: HashMap<String, (i64, i64, i64)> = HashMap::new();
    let mut mono_pushed: HashSet<String> = HashSet::new();

    let from_of = |t: &str, axis: &Axis| -> (String, String) {
        let mut from = format!("{} s", qi(t));
        let texpr = match &axis.borrowed {
            None => format!("s.{}", qi(&axis.time_col)),
            Some((tt, fk, key)) => {
                from.push_str(&format!(
                    " JOIN {} tt ON {}",
                    qi(tt),
                    join_on("s", fk, "tt", key)
                ));
                format!("tt.{}", qi(&axis.time_col))
            }
        };
        (from, texpr)
    };

    let mut anchors: Vec<Value> = Vec::new();
    for maxis in &m_axes {
        let (m_from, m_texpr) = from_of(table, maxis);
        let m_ts = format!("CAST({m_texpr} AS TIMESTAMP)");

        if !grain_cache.contains_key(&maxis.label) {
            let g = native_grain(&run, &m_from, &m_texpr, &m_ts).await?;
            grain_cache.insert(maxis.label.clone(), g);
        }
        let m_native = grain_cache[&maxis.label].clone();
        if m_native == "empty" {
            continue;
        }

        for ev in &tables {
            if ev == table {
                continue;
            }

            // Entity alignments: a declared edge between the two tables
            // directly, or two edges meeting at the same dimension key.
            let mut aligns: Vec<Align> = Vec::new();
            for p in &pointers {
                if p.src_t == table && p.dst_t == *ev {
                    aligns.push(Align {
                        m_cols: p.src_cols.clone(),
                        e_cols: p.dst_cols.clone(),
                        via: None,
                        e_via: None,
                    });
                }
                if p.src_t == *ev && p.dst_t == table {
                    aligns.push(Align {
                        m_cols: p.dst_cols.clone(),
                        e_cols: p.src_cols.clone(),
                        via: None,
                        e_via: None,
                    });
                }
            }
            for p in &pointers {
                if p.src_t != table {
                    continue;
                }
                for q in &pointers {
                    if q.src_t == *ev
                        && q.dst_t == p.dst_t
                        && q.dst_cols == p.dst_cols
                        && p.dst_t != table
                        && p.dst_t != *ev
                    {
                        aligns.push(Align {
                            m_cols: p.src_cols.clone(),
                            e_cols: q.src_cols.clone(),
                            via: None,
                            e_via: None,
                        });
                    }
                }
            }
            // Borrowed entity: the measure side joins the
            // document table and keys on ITS dimension column; the
            // event side must carry the key on a direct edge of its
            // own. The via table may BE the event table.
            for p1 in &pointers {
                if p1.src_t != table || p1.dst_t == table {
                    continue;
                }
                for p2 in &pointers {
                    if p2.src_t != p1.dst_t || p2.dst_t == table || p2.dst_t == *ev {
                        continue;
                    }
                    for q in &pointers {
                        if q.src_t == *ev && q.dst_t == p2.dst_t && q.dst_cols == p2.dst_cols {
                            aligns.push(Align {
                                m_cols: p2.src_cols.clone(),
                                e_cols: q.src_cols.clone(),
                                via: Some((
                                    p1.dst_t.clone(),
                                    p1.src_cols.clone(),
                                    p1.dst_cols.clone(),
                                )),
                                e_via: None,
                            });
                        }
                    }
                }
            }
            // Borrowed entity, event side — only when the event table
            // aligned no other way (borrow when starving, like the
            // time-axis borrow): a document-keyed event table's one
            // path to the measure runs through its document master.
            // event → master (q1), and the master either carries the
            // measure's dimension key (q2 meeting p) or points at the
            // measure directly (q2). The event side joins the master
            // `ee` and keys on ITS columns; cost is that one m:1 join
            // ahead of the aggregate, and the HAVING probe still gates
            // the alignment before either side is scanned wide.
            if aligns.is_empty() {
                for q1 in &pointers {
                    if q1.src_t != *ev || q1.dst_t == *ev || q1.dst_t == table {
                        continue;
                    }
                    for q2 in &pointers {
                        if q2.src_t != q1.dst_t || q2.dst_t == *ev {
                            continue;
                        }
                        let e_via =
                            Some((q1.dst_t.clone(), q1.src_cols.clone(), q1.dst_cols.clone()));
                        if q2.dst_t == table {
                            aligns.push(Align {
                                m_cols: q2.dst_cols.clone(),
                                e_cols: q2.src_cols.clone(),
                                via: None,
                                e_via,
                            });
                            continue;
                        }
                        for p in &pointers {
                            if p.src_t == table && p.dst_t == q2.dst_t && p.dst_cols == q2.dst_cols
                            {
                                aligns.push(Align {
                                    m_cols: p.src_cols.clone(),
                                    e_cols: q2.src_cols.clone(),
                                    via: None,
                                    e_via: e_via.clone(),
                                });
                            }
                        }
                    }
                }
            }
            let mut seen: Vec<String> = Vec::new();
            let mut deduped: Vec<Align> = Vec::new();
            for a in aligns {
                let via = a
                    .via
                    .as_ref()
                    .map(|(t, _, _)| format!("{t}>"))
                    .unwrap_or_default();
                let e_via = a
                    .e_via
                    .as_ref()
                    .map(|(t, _, _)| format!(">{t}"))
                    .unwrap_or_default();
                let k = format!("{via}{e_via}{}={}", a.m_cols.join(","), a.e_cols.join(","));
                if !seen.contains(&k) {
                    seen.push(k);
                    deduped.push(a);
                }
            }
            if deduped.is_empty() {
                continue;
            }

            let e_axes = all_axes[ev.as_str()].clone();
            if e_axes.is_empty() {
                continue;
            }

            // Movement candidates: the event table's numeric columns,
            // minus declared-edge legs and the alignment column itself.
            let mut identifier_cols: Vec<String> = Vec::new();
            let mut base_pool: Vec<String> = Vec::new();
            for (name, dtype) in &schemas[ev.as_str()] {
                if !numeric_dtype(dtype) {
                    continue;
                }
                if legs.contains(&(ev.clone(), name.clone())) {
                    identifier_cols.push(name.clone());
                } else {
                    base_pool.push(name.clone());
                }
            }

            for al in &deduped {
                let terms_pool: Vec<String> = base_pool
                    .iter()
                    .filter(|c| !al.e_cols.contains(c))
                    .cloned()
                    .collect();
                if terms_pool.is_empty() {
                    continue;
                }

                for eaxis in &e_axes {
                    let (e_from, e_texpr) = from_of(ev, eaxis);
                    let e_ts = format!("CAST({e_texpr} AS TIMESTAMP)");

                    if !grain_cache.contains_key(&eaxis.label) {
                        let g = native_grain(&run, &e_from, &e_texpr, &e_ts).await?;
                        grain_cache.insert(eaxis.label.clone(), g);
                    }
                    let e_native = grain_cache[&eaxis.label].clone();
                    if e_native == "empty" {
                        continue;
                    }
                    // A day-native alignment also serves a month
                    // variant; native first, so the first anchor per
                    // event keeps its old meaning.
                    let native = coarser(&m_native, &e_native).to_string();
                    let mut grains = vec![native.clone()];
                    if native == "day" {
                        grains.push("month".into());
                    }
                    for grain in &grains {
                        let scopes: &[&str] = if grain == "year" {
                            &["none"]
                        } else {
                            &["none", "year"]
                        };
                        for scope in scopes {
                            let mut am_from = m_from.clone();
                            let (am_base, alabel, align_label) = match &al.via {
                                None => (
                                    "s".to_string(),
                                    cols_label(&al.m_cols),
                                    format!(
                                        "{table}.{} = {ev}.{}",
                                        cols_label(&al.m_cols),
                                        cols_label(&al.e_cols)
                                    ),
                                ),
                                Some((via_t, via_fk, via_key)) => {
                                    am_from.push_str(&format!(
                                        " JOIN {} dd ON {}",
                                        qi(via_t),
                                        join_on("s", via_fk, "dd", via_key)
                                    ));
                                    (
                                        "dd".to_string(),
                                        format!("{via_t}>{}", cols_label(&al.m_cols)),
                                        format!(
                                            "{table} via {via_t}.{} = {ev}.{}",
                                            cols_label(&al.m_cols),
                                            cols_label(&al.e_cols)
                                        ),
                                    )
                                }
                            };
                            // The event side mirrors the hop: a
                            // document-keyed event table joins its
                            // master `ee` and keys on the master's
                            // columns; its movements stay its own.
                            let mut ae_from = e_from.clone();
                            let (ae_base, align_label) = match &al.e_via {
                                None => ("s".to_string(), align_label),
                                Some((via_t, via_fk, via_key)) => {
                                    ae_from.push_str(&format!(
                                        " JOIN {} ee ON {}",
                                        qi(via_t),
                                        join_on("s", via_fk, "ee", via_key)
                                    ));
                                    (
                                        "ee".to_string(),
                                        format!(
                                            "{table}.{} = {ev} via {via_t}.{}",
                                            cols_label(&al.m_cols),
                                            cols_label(&al.e_cols)
                                        ),
                                    )
                                }
                            };
                            let mut m_e = entity_expr(&am_base, &al.m_cols);
                            let mut e_e = entity_expr(&ae_base, &al.e_cols);
                            if *scope == "year" {
                                m_e = format!(
                                    "CAST({m_e} AS VARCHAR) || '@' || CAST(extract(year FROM {m_ts}) AS VARCHAR)"
                                );
                                e_e = format!(
                                    "CAST({e_e} AS VARCHAR) || '@' || CAST(extract(year FROM {e_ts}) AS VARCHAR)"
                                );
                            }
                            let m_guard = not_null_guard(&am_base, &al.m_cols);
                            let e_guard = not_null_guard(&ae_base, &al.e_cols);

                            // Viability, on one probe: fewer than two
                            // entities with 4+ periods cannot clear the
                            // voting floor.
                            let vkey = format!("viable|{}|{alabel}|{grain}|{scope}", maxis.label);
                            if !viable_cache.contains_key(&vkey) {
                                let vq = run(format!(
                                    "SELECT count(*) AS v FROM \
                                     (SELECT {m_e} AS e FROM {am_from} \
                                      WHERE {m_guard} AND {m_texpr} IS NOT NULL \
                                        AND s.{qcol} IS NOT NULL \
                                      GROUP BY 1 \
                                      HAVING count(DISTINCT date_trunc('{grain}', {m_ts})) >= 4) AS g",
                                    qcol = qi(column)
                                ))
                                .await?;
                                let v = crate::search::int_column(&vq, "v")
                                    .map(|v| v[0])
                                    .unwrap_or(0);
                                viable_cache.insert(vkey.clone(), v);
                            }
                            let base = json!({
                                "event": ev, "align": align_label,
                                "measure_time": maxis.label,
                                "event_time": eaxis.label,
                                "grain": grain, "scope": scope,
                                "identifier_columns": identifier_cols,
                            });
                            let mono_base = base.clone();
                            'reconcile: {
                                if viable_cache[&vkey] < 2 {
                                    let mut a = base.as_object().expect("object").clone();
                                    a.insert("viable_entities".into(), json!(viable_cache[&vkey]));
                                    a.insert("verdict".into(), json!("abstain"));
                                    a.insert(
                                    "reason".into(),
                                    json!(
                                        "fewer than two entities carry 4+ periods on this alignment"
                                    ),
                                );
                                    anchors.push(Value::Object(a));
                                    break 'reconcile;
                                }

                                // Measure-side series, cached per (axis,
                                // entity, grain, scope); the kernel consumes
                                // the grouped result directly.
                                let ykey = format!("{}|{alabel}|{grain}|{scope}", maxis.label);
                                if !y_cache.contains_key(&ykey) {
                                    let yq = run(format!(
                                        "SELECT {m_e} AS e, \
                                     date_trunc('{grain}', {m_ts}) AS b, \
                                     SUM(CAST(s.{qcol} AS DOUBLE)) AS yv \
                                     FROM {am_from} \
                                     WHERE {m_guard} AND {m_texpr} IS NOT NULL \
                                       AND s.{qcol} IS NOT NULL \
                                     GROUP BY 1, 2 ORDER BY 1, 2",
                                        qcol = qi(column)
                                    ))
                                    .await?;
                                    y_cache.insert(ykey.clone(), yq);
                                }
                                let yq = y_cache[&ykey].clone();

                                // Event-side sums for every term at once.
                                let sel: String = terms_pool
                                    .iter()
                                    .map(|c| {
                                        format!(", SUM(CAST(s.{} AS DOUBLE)) AS \"s_{c}\"", qi(c))
                                    })
                                    .collect();
                                let mq = run(format!(
                                    "SELECT {e_e} AS e, \
                                 date_trunc('{grain}', {e_ts}) AS b{sel} \
                                 FROM {ae_from} \
                                 WHERE {e_guard} AND {e_texpr} IS NOT NULL \
                                 GROUP BY 1, 2 ORDER BY 1, 2"
                                ))
                                .await?;

                                // The discriminator in one kernel call.
                                let rec = runtime
                                    .reconcile(&yq, &mq, &terms_pool)
                                    .map_err(SessionError::Runtime)?;
                                let n_common = rec["n_common"].as_i64().unwrap_or(0);
                                let mut summaries: Vec<Value> =
                                    rec["summaries"].as_array().cloned().unwrap_or_default();
                                for s in &mut summaries {
                                    let winners = s["winners"].as_f64().unwrap_or(0.0);
                                    s["support"] = json!(wilson_lcb(winners, n_common));
                                }

                                anchors.push(judge_anchor(base, n_common, &summaries));
                            }

                            // The monotone anchor, once per alignment and
                            // scope, at the axis's native grain — after
                            // the reconciliation, which is the anchor a
                            // reader meets first.
                            if !mono_pushed.insert(format!("{ev}|{alabel}|{scope}")) {
                                continue;
                            }
                            let mkey = format!("mono|{}|{alabel}|{scope}", maxis.label);
                            if !mono_cache.contains_key(&mkey) {
                                let mq = run(format!(
                                    "SELECT count(*) AS considered, \
                                     coalesce(sum(CASE WHEN decreases = 0 AND increases > 0 \
                                                        THEN 1 ELSE 0 END), 0) AS monotone, \
                                     coalesce(sum(steps), 0) AS steps \
                                     FROM (SELECT e, count(*) AS steps, \
                                             sum(CASE WHEN yv < prev THEN 1 ELSE 0 END) AS decreases, \
                                             sum(CASE WHEN yv > prev THEN 1 ELSE 0 END) AS increases \
                                           FROM (SELECT e, b, yv, \
                                                   lag(yv) OVER (PARTITION BY e ORDER BY b) AS prev \
                                                 FROM (SELECT {m_e} AS e, \
                                                         date_trunc('{m_native}', {m_ts}) AS b, \
                                                         SUM(CAST(s.{qcol} AS DOUBLE)) AS yv \
                                                       FROM {am_from} \
                                                       WHERE {m_guard} AND {m_texpr} IS NOT NULL \
                                                         AND s.{qcol} IS NOT NULL \
                                                       GROUP BY 1, 2)) \
                                           WHERE prev IS NOT NULL \
                                           GROUP BY e HAVING count(*) >= 3)",
                                    qcol = qi(column)
                                ))
                                .await?;
                                let read = |name: &str| {
                                    crate::search::int_column(&mq, name)
                                        .map(|v| v[0])
                                        .unwrap_or(0)
                                };
                                mono_cache.insert(
                                    mkey.clone(),
                                    (read("considered"), read("monotone"), read("steps")),
                                );
                            }
                            let (considered, monotone, steps) = mono_cache[&mkey];
                            let mut a = mono_base.as_object().expect("object").clone();
                            a.insert("grain".into(), json!(m_native));
                            a.insert("convention".into(), json!("monotone"));
                            a.insert("entities".into(), json!(considered));
                            a.insert("viable_entities".into(), json!(considered));
                            a.insert("alternatives".into(), json!([]));
                            if considered < 2 {
                                a.insert("verdict".into(), json!("abstain"));
                                a.insert(
                                    "reason".into(),
                                    json!("fewer than two entities carry 4+ periods at the native grain"),
                                );
                            } else {
                                a.insert("voted".into(), json!(considered));
                                a.insert(
                                    "agreement".into(),
                                    json!(monotone as f64 / considered as f64),
                                );
                                a.insert(
                                    "support".into(),
                                    json!(wilson_lcb(monotone as f64, considered)),
                                );
                                if monotone * 2 >= considered && monotone > 0 {
                                    a.insert("verdict".into(), json!("stock"));
                                } else {
                                    a.insert("verdict".into(), json!("abstain"));
                                    a.insert(
                                        "reason".into(),
                                        json!(format!(
                                            "{} of {considered} entities decrease within the scope over {steps} steps — not a cumulative here",
                                            considered - monotone
                                        )),
                                    );
                                }
                            }
                            anchors.push(Value::Object(a));
                        }
                    }
                }
            }
        }
    }

    if anchors.is_empty() {
        return bare(json!({
            "applicable": false,
            "reason": format!(
                "no declared relationships connect {table} to an event table with a time axis"
            ),
        }));
    }

    // The verdict, in the shape a reader actually needs. Extraction
    // serves the summary alone (run 4 spent 60KB of an agent's context
    // — 102 anchors — to learn the word "flow"); every anchor still
    // reads back via GLOSSARY when the judge wants the losers.
    let sup = |v: &Value| v["support"].as_f64().unwrap_or(0.0);
    let monotone = |v: &Value| v["convention"] == json!("monotone");
    let mut best: Option<&Value> = None;
    let mut decided = 0i64;
    for a in &anchors {
        if a["verdict"] != json!("abstain") {
            decided += 1;
            // Support first; on equal support a reconciliation outranks
            // the monotone shape — the movement explains the level.
            if best.is_none_or(|b| {
                sup(a) > sup(b) || (sup(a) == sup(b) && monotone(b) && !monotone(a))
            }) {
                best = Some(a);
            }
        }
    }
    let mut fact = serde_json::Map::new();
    fact.insert("applicable".into(), json!(true));
    fact.insert("anchors_n".into(), json!(anchors.len() as i64));
    fact.insert("decided".into(), json!(decided));
    if let Some(b) = best {
        for (from, to) in [
            ("verdict", "s_verdict"),
            ("support", "s_support"),
            ("voted", "s_voted"),
            ("convention", "s_convention"),
            ("align", "s_align"),
            ("scope", "s_scope"),
            ("r_flow", "s_r_flow"),
            ("r_stock", "s_r_stock"),
        ] {
            if let Some(v) = b.get(from).filter(|v| !v.is_null()) {
                fact.insert(to.into(), v.clone());
            }
        }
        if let Some(sign) = b.get("sign").filter(|v| !v.is_null()) {
            fact.insert("s_sign_primary".into(), sign["primary"].clone());
            fact.insert("s_sign_mirror".into(), sign["mirror"].clone());
            fact.insert("s_sign_both".into(), sign["both"].clone());
        }
    } else {
        // Every anchor abstained, and WHY is the whole finding.
        fact.insert("s_verdict".into(), json!("abstain"));
        if let Some(r) = anchors[0].get("reason").filter(|v| !v.is_null()) {
            fact.insert("s_reason".into(), r.clone());
        }
    }

    let mut out: Vec<Value> = Vec::new();
    for (seq, a) in anchors.iter().enumerate() {
        let mut row = a.as_object().expect("object").clone();
        row.insert("seq".into(), json!(seq as i64));
        if let Some(sign) = row.remove("sign") {
            row.insert("sign_primary".into(), sign["primary"].clone());
            row.insert("sign_mirror".into(), sign["mirror"].clone());
            row.insert("sign_both".into(), sign["both"].clone());
        }
        out.push(Value::Object(row));
    }
    out.push(Value::Object(fact));
    rows_batch(out, behavior_shape())
}

/// The winner, the runner-up field, and the anchor's own record — the
/// policy half the script held, verbatim: support-first; on a support
/// tie the fewer-term convention wins unless the higher-arity fit is
/// decisive, ΔBIC > 10 (Kass–Raftery), v0.3's tiebreak.
fn judge_anchor(base: Value, n_common: i64, summaries: &[Value]) -> Value {
    let sup = |s: &Value| s["support"].as_f64().unwrap_or(0.0);
    let mut winner: Option<&Value> = None;
    for s in summaries {
        if s["verdict"] == json!("abstain") {
            continue;
        }
        let Some(w) = winner else {
            winner = Some(s);
            continue;
        };
        if sup(s) > sup(w) + 1.0e-9 {
            winner = Some(s);
            continue;
        }
        if (sup(s) - sup(w)).abs() <= 1.0e-9 && s["terms"] != w["terms"] {
            let (simple, complex) = if s["terms"].as_i64() < w["terms"].as_i64() {
                (s, w)
            } else {
                (w, s)
            };
            winner = if simple.get("bic").is_some_and(|v| !v.is_null())
                && complex.get("bic").is_some_and(|v| !v.is_null())
                && simple["bic"].as_f64().unwrap_or(0.0) - complex["bic"].as_f64().unwrap_or(0.0)
                    > 10.0
            {
                Some(complex)
            } else {
                Some(simple)
            };
        }
    }

    // The runner-up field the judge sees beside the verdict:
    // most-voted first, capped at four.
    let mut alts: Vec<Value> = Vec::new();
    let mut used: Vec<Value> = winner.iter().map(|w| w["convention"].clone()).collect();
    while alts.len() < 4 {
        let mut best: Option<&Value> = None;
        for s in summaries {
            if used.contains(&s["convention"]) {
                continue;
            }
            let Some(b) = best else {
                best = Some(s);
                continue;
            };
            let (sv, bv) = (
                s["voted"].as_i64().unwrap_or(0),
                b["voted"].as_i64().unwrap_or(0),
            );
            if sv > bv || (sv == bv && sup(s) > sup(b)) {
                best = Some(s);
            }
        }
        let Some(b) = best else { break };
        used.push(b["convention"].clone());
        alts.push(json!({
            "convention": b["convention"], "verdict": b["verdict"],
            "voted": b["voted"], "support": b["support"],
        }));
    }

    let mut a = base.as_object().expect("object").clone();
    a.insert("entities".into(), json!(n_common));
    a.insert("alternatives".into(), Value::Array(alts));
    match winner {
        Some(w) => {
            a.insert("verdict".into(), w["verdict"].clone());
            a.insert("convention".into(), w["convention"].clone());
            a.insert("voted".into(), w["voted"].clone());
            a.insert("agreement".into(), w["agreement"].clone());
            a.insert("support".into(), w["support"].clone());
            for k in ["r_flow", "r_stock"] {
                if let Some(v) = w.get(k).filter(|v| !v.is_null()) {
                    a.insert(k.into(), v.clone());
                }
            }
            // The sign partition: a mirror-heavy count says the store
            // carries the negated convention (ledger-signed).
            if let Some(p) = w.get("sign_primary").filter(|v| !v.is_null()) {
                a.insert(
                    "sign".into(),
                    json!({
                        "primary": p, "mirror": w["sign_mirror"],
                        "both": w["sign_both"],
                    }),
                );
            }
        }
        None => {
            a.insert("verdict".into(), json!("abstain"));
            let any_votes = summaries
                .iter()
                .any(|s| s["voted"].as_i64().unwrap_or(0) > 0);
            a.insert(
                "reason".into(),
                json!(if any_votes {
                    "votes below the candidacy floor (2 voters, 0.8 agreement)"
                } else {
                    "no entity series reconciled: wrong anchor, short series, or dead values"
                }),
            );
        }
    }
    Value::Object(a)
}

/// The coarsest named grain an axis is already truncated to.
async fn native_grain<F, Fut>(
    run: &F,
    from: &str,
    texpr: &str,
    ts: &str,
) -> Result<String, SessionError>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<RecordBatch>, SessionError>>,
{
    let g = run(format!(
        "SELECT \
         SUM(CASE WHEN date_trunc('year', {ts}) = {ts} THEN 0 ELSE 1 END) AS ny, \
         SUM(CASE WHEN date_trunc('quarter', {ts}) = {ts} THEN 0 ELSE 1 END) AS nq, \
         SUM(CASE WHEN date_trunc('month', {ts}) = {ts} THEN 0 ELSE 1 END) AS nm \
         FROM {from} WHERE {texpr} IS NOT NULL"
    ))
    .await?;
    let cell =
        |name: &str| -> Option<i64> { crate::search::int_column(&g, name).ok().map(|v| v[0]) };
    Ok(match (cell("ny"), cell("nq"), cell("nm")) {
        (None, _, _) => "empty".into(),
        (Some(0), _, _) => "year".into(),
        (_, Some(0), _) => "quarter".into(),
        (_, _, Some(0)) => "month".into(),
        _ => "day".into(),
    })
}

fn behavior_shape() -> Vec<Field> {
    let list_utf8 = |name: &str| {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        )
    };
    vec![
        Field::new("applicable", DataType::Boolean, true),
        Field::new("reason", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("event", DataType::Utf8, true),
        Field::new("align", DataType::Utf8, true),
        Field::new("measure_time", DataType::Utf8, true),
        Field::new("event_time", DataType::Utf8, true),
        Field::new("grain", DataType::Utf8, true),
        Field::new("scope", DataType::Utf8, true),
        Field::new("entities", DataType::Int64, true),
        Field::new("viable_entities", DataType::Int64, true),
        list_utf8("identifier_columns"),
        Field::new("verdict", DataType::Utf8, true),
        Field::new("convention", DataType::Utf8, true),
        Field::new("voted", DataType::Int64, true),
        Field::new("agreement", DataType::Float64, true),
        Field::new("support", DataType::Float64, true),
        Field::new("r_flow", DataType::Float64, true),
        Field::new("r_stock", DataType::Float64, true),
        Field::new("sign_primary", DataType::Int64, true),
        Field::new("sign_mirror", DataType::Int64, true),
        Field::new("sign_both", DataType::Int64, true),
        Field::new(
            "alternatives",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(
                    vec![
                        Field::new("convention", DataType::Utf8, true),
                        Field::new("verdict", DataType::Utf8, true),
                        Field::new("voted", DataType::Int64, true),
                        Field::new("support", DataType::Float64, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        ),
        Field::new("anchors_n", DataType::Int64, true),
        Field::new("decided", DataType::Int64, true),
        Field::new("s_verdict", DataType::Utf8, true),
        Field::new("s_support", DataType::Float64, true),
        Field::new("s_voted", DataType::Int64, true),
        Field::new("s_convention", DataType::Utf8, true),
        Field::new("s_align", DataType::Utf8, true),
        Field::new("s_scope", DataType::Utf8, true),
        Field::new("s_r_flow", DataType::Float64, true),
        Field::new("s_r_stock", DataType::Float64, true),
        Field::new("s_sign_primary", DataType::Int64, true),
        Field::new("s_sign_mirror", DataType::Int64, true),
        Field::new("s_sign_both", DataType::Int64, true),
        Field::new("s_reason", DataType::Utf8, true),
    ]
}
