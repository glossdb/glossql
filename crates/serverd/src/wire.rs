//! Outcomes on the wire. The MCP door and the summary path of `/query`
//! share one JSON shape. A tool result lands verbatim in the agent's
//! context window, so rows are capped — and the cap bounds the engine's
//! work when the source is a stream, not just the rendering.
//! `row_count` counts the rows shipped; a declared `truncated` says the
//! result held more, so the agent refines (aggregate, LIMIT, WHERE)
//! instead of reading a capped result as complete.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use glossql_session::Outcome;
use serde_json::{Value, json};

pub const DEFAULT_ROW_CAP: usize = 200;

pub fn outcomes_json(outcomes: &[Outcome], cap: usize) -> Result<Value, String> {
    let mut rendered = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        rendered.push(match outcome {
            Outcome::Done(done) => json!({ "done": done }),
            Outcome::Affected(n) => json!({ "affected": n }),
            Outcome::Rows(batches) => rows_json(batches, cap)?,
        });
    }
    Ok(Value::Array(rendered))
}

fn rows_json(batches: &[RecordBatch], cap: usize) -> Result<Value, String> {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut kept = Vec::new();
    let mut remaining = cap;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = batch.num_rows().min(remaining);
        remaining -= take;
        if take > 0 {
            kept.push(batch.slice(0, take));
        }
    }
    let schema = batches.first().map(|b| b.schema());
    render(schema, &kept, total.min(cap), total > cap)
}

/// Pull batches until the cap is met, then drop the stream — what the
/// agent won't see, the engine stops computing.
pub async fn stream_json(
    mut stream: SendableRecordBatchStream,
    cap: usize,
) -> Result<Value, String> {
    let schema = stream.schema();
    let mut kept: Vec<RecordBatch> = Vec::new();
    let mut shipped = 0usize;
    let mut truncated = false;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| e.to_string())?;
        if shipped + batch.num_rows() > cap {
            let take = cap - shipped;
            if take > 0 {
                kept.push(batch.slice(0, take));
            }
            shipped = cap;
            truncated = true;
            break;
        }
        shipped += batch.num_rows();
        kept.push(batch);
    }
    render(Some(schema), &kept, shipped, truncated)
}

fn render(
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    kept: &[RecordBatch],
    shipped: usize,
    truncated: bool,
) -> Result<Value, String> {
    let rows = if kept.is_empty() {
        json!([])
    } else {
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        let refs: Vec<&RecordBatch> = kept.iter().collect();
        writer.write_batches(&refs).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
        serde_json::from_slice(&writer.into_inner()).map_err(|e| e.to_string())?
    };
    // The shape survives an empty result — (name, type) is what a
    // LIMIT 0 rehearsal exists to learn, and types matter to agents on
    // every read.
    let columns: Value = match schema {
        Some(s) => s
            .fields()
            .iter()
            .map(|f| json!({ "name": f.name(), "type": f.data_type().to_string() }))
            .collect(),
        None => json!([]),
    };
    Ok(json!({
        "columns": columns,
        "rows": rows,
        "row_count": shipped,
        "truncated": truncated
    }))
}
