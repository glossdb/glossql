//! `grounding_collisions('dataset')`, and the reads of the current
//! groundings the other doors share.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::util::display::array_value_to_string;
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::SessionError;

use super::{monthly_sql, rows_batch};

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
    let anchors = crate::cube::Anchors::at(&rctx, dataset).await?;
    let slots = current_query_slots(&rctx, dataset).await?;

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
    // observed date — the cube's verb).
    let mut series_buckets: BTreeMap<String, Vec<(&str, &str, &str)>> = BTreeMap::new();
    for (slot, body, canon) in &grounded {
        let sql = body["sql"].as_str().expect("filtered above");
        let Some(fp) = series_fingerprint(shared, &ctx, dataset, &anchors, sql, body).await else {
            continue;
        };
        series_buckets.entry(fp).or_default().push((
            slot.aspect.as_str(),
            slot.subject.as_str(),
            canon.as_str(),
        ));
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
#[derive(Clone)]
pub(crate) struct QuerySlot {
    pub subject: String,
    pub aspect: String,
    pub body: String,
    /// The serving voice's rank — 0 human, 1 agent — whose word the
    /// body's `axes` are.
    pub rank: u8,
}

/// The current QUERY groundings the metric doors run — the store's own
/// collapsed read (supersession, human over agent, contested withheld,
/// SPEC.md §5.3), narrowed to QUERY aspects: one slot per
/// (subject, aspect) that serves a value. A contested grounding never
/// enters a cube, a walk, or a collision bucket. In slot-key order.
/// One FACT aspect's collapsed values across the dataset, keyed by
/// subject, each with the serving voice's rank (0 human, 1 agent) — the
/// read policy's view of what was said, human over agent, contested
/// withheld.
pub(crate) async fn current_fact_values(
    rctx: &glossql_glossary::ReadContext,
    dataset: &str,
    aspect: &str,
) -> Result<HashMap<String, (Value, u8)>, SessionError> {
    let scope = glossql_glossary::Scope::Dataset;
    let verdicts = crate::reads::verdicts(rctx, dataset, &scope, Some(aspect)).await?;
    Ok(
        glossql_glossary::Store::collapsed_read(dataset, &scope, Some(aspect), rctx, &verdicts)
            .into_iter()
            .filter_map(|r| {
                let body = serde_json::from_str::<Value>(r.value.as_deref()?).ok()?;
                Some((r.subject, (body, r.rank?)))
            })
            .collect(),
    )
}

pub(crate) async fn current_query_slots(
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
    let verdicts = crate::reads::verdicts(rctx, dataset, &scope, None).await?;
    let mut slots: Vec<QuerySlot> =
        glossql_glossary::Store::collapsed_read(dataset, &scope, None, rctx, &verdicts)
            .into_iter()
            .filter(|r| query_aspects.contains(r.aspect.as_str()))
            .filter_map(|r| {
                r.value.map(|body| QuerySlot {
                    subject: r.subject,
                    aspect: r.aspect,
                    body,
                    rank: r.rank.unwrap_or(1),
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
    dataset: &str,
    anchors: &crate::cube::Anchors,
    sql: &str,
    body: &Value,
) -> Option<String> {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    let probe = crate::whatif::build_plan(shared, ctx, sql).await.ok()?;
    let fields = probe.schema();
    let has = |name: &str| fields.fields().iter().any(|f| f.name() == name);
    // The same verb rule every monthly reader applies: a ratio serves
    // `num` and `den`, a marked stock sums the latest observed date,
    // everything else sums the month.
    let is_ratio = has("num") && has("den");
    if !is_ratio && !has("value") {
        return None;
    }
    // The judged axis where one stands, the first date column where
    // none does — a fallback, never a refusal. Two groundings that
    // compute the same thing must fingerprint alike, and anchoring on
    // the judged column is what makes that hold across differently
    // spelled frames; but refusing an unprofiled grounding would drop
    // it out of the collision pass entirely, shrinking the check
    // instead of failing it.
    let sources = crate::provenance::served_sources(&probe, dataset);
    let tcol = crate::cube::judged_time_column(fields, &sources, &anchors.temporal)
        .map(|(column, ..)| column)
        .or_else(|| crate::whatif::date_column(fields.fields()))?;
    let verb = crate::cube::verb_of(
        body,
        is_ratio,
        &probe,
        dataset,
        &anchors.behavior,
        &anchors.behavior_gloss,
    )
    .verb;
    let q = monthly_sql(sql, &tcol, verb);
    let plan = crate::whatif::build_plan(shared, ctx, &q).await.ok()?;
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
