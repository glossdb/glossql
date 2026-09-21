//! `fact_values()` and `metric_sources()`: the declared facts and what
//! each metric reads.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field};
use serde_json::{Value, json};

use crate::reads::Shared;
use crate::session::SessionError;

use super::{current_query_slots, rows_batch};

/// `fact_values()` — the declared facts, served whole: one row per
/// current QUERY grounding whose aspect declares `x-kind: fact`, with
/// `value` where the frame is one row with a `value` column and
/// `reason` where it is not: a derived relation (no `value`), a frame
/// of many rows, an empty one, or SQL the engine refused. The
/// declaration is what makes a fact — a grounding that serves no date
/// and declares no kind is charted by nothing, and `metric_axes()`
/// carries the cube's reason. The data at read, like the cube's cells:
/// nothing is recorded.
pub(crate) async fn fact_values(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let kinds: HashMap<&str, String> = rctx
        .aspects
        .iter()
        .filter_map(|a| {
            let schema: Value = serde_json::from_str(&a.schema).ok()?;
            let kind = schema["x-kind"].as_str().unwrap_or("").to_string();
            Some((a.name.as_str(), kind))
        })
        .collect();
    let mut out = Vec::new();
    for slot in current_query_slots(&rctx, &dataset).await? {
        if kinds.get(slot.aspect.as_str()).map(String::as_str) != Some("fact") {
            continue;
        }
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };
        let mut row = serde_json::Map::new();
        row.insert("metric".into(), json!(slot.aspect));
        let serve = |row: &mut serde_json::Map<String, Value>, reason: String| {
            row.insert("reason".into(), json!(reason));
        };
        let plan = match crate::whatif::build_plan(shared, &ctx, sql).await {
            Ok(plan) => plan,
            Err(e) => {
                serve(&mut row, format!("not served: {e}"));
                out.push(Value::Object(row));
                continue;
            }
        };
        let fields = plan.schema();
        if !fields.fields().iter().any(|f| f.name() == "value") {
            serve(
                &mut row,
                "no value column — a derived relation, served whole as read.<name>()".into(),
            );
            out.push(Value::Object(row));
            continue;
        }
        let q = format!("SELECT value FROM ({sql}) LIMIT 2");
        let served = async {
            let plan = crate::whatif::build_plan(shared, &ctx, &q).await?;
            ctx.execute_logical_plan(plan)
                .await
                .map_err(SessionError::not_served)?
                .collect()
                .await
                .map_err(SessionError::not_served)
        }
        .await;
        let batches = match served {
            Ok(batches) => batches,
            Err(e) => {
                serve(&mut row, e.to_string());
                out.push(Value::Object(row));
                continue;
            }
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        match rows {
            0 => serve(&mut row, "no rows: the frame is empty".into()),
            1 => {
                let b = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
                let value = cast_with_options(
                    b.column(0),
                    &DataType::Float64,
                    &CastOptions {
                        safe: true,
                        ..Default::default()
                    },
                )
                .ok()
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned())
                .filter(|c| !c.is_null(0))
                .map(|c| c.value(0));
                match value {
                    Some(v) => {
                        row.insert("value".into(), json!(v));
                    }
                    None => serve(&mut row, "value did not read as a number".into()),
                }
            }
            _ => serve(
                &mut row,
                "more than one row: a relation, not a fact — served whole as read.<name>()".into(),
            ),
        }
        out.push(Value::Object(row));
    }
    rows_batch(
        out,
        vec![
            Field::new("metric", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new("reason", DataType::Utf8, true),
        ],
    )
}

/// `metric_sources()` — which dataset table columns each served field
/// of every current grounding descends from: `metric`, `field`,
/// `source` (`table.column`) and its `table_name`, one row per field
/// and source — a union descends from every arm's column. A computed
/// field (an aggregate, an expression) descends from no column and
/// has no row. Beside those, one row per table the grounding scans,
/// `table_name` alone: the tables a metric reads, whether or not a
/// served field traces to a column — a fact's whole frame is computed
/// and still reads its tables. A grounding its author stopped, or one
/// the engine cannot plan, serves one row with `reason` and no field.
/// The walk is the cube's own (`provenance::served_sources`,
/// `scanned_tables`); this read serves it, so a page can draw what
/// feeds a metric without building the cube. Requires a bound
/// dataset.
pub(crate) async fn metric_sources(shared: &Arc<Shared>) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let ctx = shared.session_ctx();
    let rctx = shared.read_context().await?;
    let mut out = Vec::new();
    for slot in current_query_slots(&rctx, &dataset).await? {
        let Ok(body) = serde_json::from_str::<Value>(&slot.body) else {
            continue;
        };
        let reason = |why: String| json!({ "metric": slot.aspect, "reason": why });
        if let Some(stopped) = body.get("stopped").and_then(Value::as_str) {
            out.push(reason(format!("stopped: {stopped}")));
            continue;
        }
        let Some(sql) = body.get("sql").and_then(Value::as_str) else {
            continue;
        };
        let plan = match crate::whatif::build_plan(shared, &ctx, sql).await {
            Ok(plan) => plan,
            Err(e) => {
                out.push(reason(format!("not served: {e}")));
                continue;
            }
        };
        let sources = crate::provenance::served_sources(&plan, &dataset);
        let mut any = false;
        for field in plan.schema().fields() {
            let Some(columns) = sources.get(field.name()) else {
                continue;
            };
            for source in columns {
                any = true;
                let table = source.split('.').next().unwrap_or(source);
                out.push(json!({
                    "metric": slot.aspect,
                    "field": field.name(),
                    "source": source,
                    "table_name": table,
                }));
            }
        }
        if !any {
            out.push(reason(
                "no served field descends from a table column".into(),
            ));
        }
        let mut scanned: Vec<String> = crate::provenance::scanned_tables(&plan, &dataset)
            .into_iter()
            .collect();
        scanned.sort();
        for table in scanned {
            out.push(json!({ "metric": slot.aspect, "table_name": table }));
        }
    }
    rows_batch(
        out,
        vec![
            Field::new("metric", DataType::Utf8, true),
            Field::new("field", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, true),
            Field::new("reason", DataType::Utf8, true),
        ],
    )
}
