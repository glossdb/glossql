//! The `misfit.` door (fixture 20): a declared sample
//! frame — an ordinary QUERY aspect, `x-kind: "sample"` by convention —
//! served back row by row with a `misfit` score: how badly each row fits
//! the rest of the same frame. One frame ranked against itself (the
//! model is protocol-robust — self-fit read the same as a clean
//! reference in the evaluation), the kernel behind the runtime seam, the
//! judge's conclusion landing as an ordinary gloss. Nothing is cached:
//! an investigation frame is small by construction and the ranking is
//! ephemeral by design.
//!
//! Judgment rides the refusal messages, never a silent guess: a frame
//! past the stated caps is refused with the number, a frame with too
//! little numeric surface abstains with the reason (the evaluation's
//! `sales_orders` lesson — some tables cannot carry a density read),
//! and every excluded column is named in `basis`. Unlike `whatif.`,
//! a refusal refuses the read — the relation's shape *is* the frame,
//! so there is no fixed schema to carry refusal rows.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use glossql_glossary::Scope;
use serde_json::Value;

use crate::reads::{Shared, verdicts};
use crate::session::SessionError;

/// Stated caps (fixture 20 §6): refused by name, never silently cut.
/// The row cap bounds the kernel's context — a bigger population is
/// sampled down in the frame SQL, never streamed through the model.
/// 2000 is the measured bound (3.7s at 2048 on Metal with
/// the parallel chain rule).
/// The `whatif.` door mirrors this cap on its replay frame.
pub(crate) const ROW_CAP: usize = 2000;
const COL_CAP: usize = 16;

pub(crate) async fn misfit_batch(
    shared: &Arc<Shared>,
    frame: &str,
) -> Result<RecordBatch, SessionError> {
    let dataset = shared
        .dataset
        .read()
        .expect("state lock")
        .clone()
        .ok_or(SessionError::NoDataset)?;
    let bad = |detail: String| SessionError::BadSubject(format!("misfit.{frame}(): {detail}"));

    let Some((_, kind, _)) = shared.store.aspect(frame).await? else {
        return Err(bad(format!("no aspect `{frame}` is declared")));
    };
    if kind != "query" {
        return Err(bad(format!(
            "`{frame}` is a {kind} aspect — a frame is a QUERY aspect glossed with SQL \
             (fixture 20)"
        )));
    }

    // The frame's collapsed current grounding, witness-gated and judged
    // like any read.
    let scope = Scope::Subject(dataset.clone());
    let ctx = shared.read_context().await?;
    let verdicts = verdicts(shared, &ctx, &dataset, &scope, Some(frame)).await?;
    let collapsed = glossql_glossary::Store::collapsed_read(&dataset, &scope, Some(frame), &ctx, &verdicts);
    let current = collapsed
        .into_iter()
        .find(|r| r.subject == dataset && r.aspect == frame)
        .ok_or_else(|| bad(format!("no frame is glossed on `{dataset}`")))?;
    if current.state != "current" {
        return Err(bad(format!(
            "the frame on `{dataset}` is {}",
            current.state
        )));
    }
    let body: Value = current
        .value
        .as_deref()
        .and_then(|v| serde_json::from_str(v).ok())
        .ok_or_else(|| bad("the frame body is not JSON".into()))?;
    let Some(sql) = body["sql"].as_str() else {
        return Err(bad("the frame grounding carries no `sql`".into()));
    };

    let ctx = shared.session_ctx();

    // The frame's rows, capped one past the stated limit so the refusal
    // can say "more than" without materializing the excess.
    let capped = format!("SELECT * FROM ({sql}) LIMIT {}", ROW_CAP + 1);
    let plan = crate::whatif::build_plan(shared, &ctx, &capped).await?;
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| bad(format!("not served: {e}")))?
        .collect()
        .await
        .map_err(|e| bad(format!("not served: {e}")))?;
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| bad("the frame serves no rows — nothing to rank".into()))?;
    let frame_batch =
        concat_batches(&schema, &batches).map_err(|e| SessionError::Runtime(e.to_string()))?;
    let rows = frame_batch.num_rows();
    if rows == 0 {
        return Err(bad("the frame serves no rows — nothing to rank".into()));
    }
    if rows > ROW_CAP {
        return Err(bad(format!(
            "the frame serves more than {ROW_CAP} rows — past the stated cap; sample in \
             the frame SQL (ORDER BY random() LIMIT {ROW_CAP}) or narrow with WHERE"
        )));
    }
    for reserved in ["misfit", "basis"] {
        if schema.fields().iter().any(|f| f.name() == reserved) {
            return Err(bad(format!(
                "the frame already carries a `{reserved}` column — the read adds its own; \
                 alias yours"
            )));
        }
    }

    // The numeric surface, discovered; every exclusion named. Id-named
    // columns are out by the evaluation's structural lesson: key-like
    // columns separate any two row sets trivially.
    let mut included: Vec<(String, Vec<f64>)> = Vec::new();
    let mut excluded: Vec<String> = Vec::new();
    for (field, col) in schema.fields().iter().zip(frame_batch.columns()) {
        let name = field.name();
        if name == "id" || name.ends_with("_id") {
            excluded.push(format!("{name} (id)"));
            continue;
        }
        let Some(values) = column_floats(col) else {
            excluded.push(format!("{name} ({})", field.data_type()));
            continue;
        };
        let mut seen: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
        seen.sort_by(f64::total_cmp);
        seen.dedup();
        if seen.len() < 2 {
            excluded.push(format!("{name} (constant)"));
            continue;
        }
        included.push((name.clone(), values));
    }
    if included.len() < 2 {
        let named = if excluded.is_empty() {
            "nothing to exclude".to_string()
        } else {
            format!("excluded {}", excluded.join(", "))
        };
        return Err(bad(format!(
            "too little numeric surface: {} usable column(s) after {named} — a density \
             read needs at least two, so the frame abstains",
            included.len()
        )));
    }
    if included.len() > COL_CAP {
        return Err(bad(format!(
            "the frame carries {} usable columns — past the stated cap ({COL_CAP}); \
             select the columns that carry the question",
            included.len()
        )));
    }

    // Row-major (rows × cols), nulls as NaN — the kernel's contract.
    let cols = included.len();
    let mut x = vec![0f64; rows * cols];
    for (c, (_, values)) in included.iter().enumerate() {
        for (r, v) in values.iter().enumerate() {
            x[r * cols + c] = *v;
        }
    }
    let scores = shared
        .runtime()
        .misfit_scores(&x, rows, cols)
        .map_err(|e| bad(format!("not served: the misfit kernel refused — {e}")))?;
    // Complementary null patterns can make a conditioning column's
    // impute mean NaN inside the kernel, and NaN survives its unique
    // filter — a non-finite score carries no ranking, so the read
    // refuses (the refuse-or-abstain contract) instead of serving NaN.
    if scores.iter().any(|s| !s.is_finite()) {
        return Err(bad(format!(
            "the density read came back non-finite over [{}] — the columns' null \
             patterns leave no common support; narrow the frame to columns that are \
             filled together",
            included
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let ranked: Vec<String> = included.iter().map(|(n, _)| n.clone()).collect();
    let basis = format!(
        "ranked {rows} rows on [{}]; {}; higher misfit = fits the frame worse",
        ranked.join(", "),
        if excluded.is_empty() {
            "excluded none".to_string()
        } else {
            format!("excluded [{}]", excluded.join(", "))
        }
    );

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("misfit", DataType::Float64, false));
    fields.push(Field::new("basis", DataType::Utf8, false));
    let mut columns: Vec<ArrayRef> = frame_batch.columns().to_vec();
    columns.push(Arc::new(Float64Array::from_iter_values(
        scores.iter().map(|s| -s),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| basis.as_str()),
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| SessionError::Runtime(e.to_string()))
}

/// A column read as f64s (nulls as NaN), or None where the type carries
/// no numeric reading. Text is never parsed; temporal types ride their
/// underlying representation (days, milliseconds) — order and spacing
/// are what the density read uses.
fn column_floats(col: &ArrayRef) -> Option<Vec<f64>> {
    let attempt = match col.data_type() {
        dt if dt.is_numeric() => cast(col, &DataType::Float64).ok(),
        DataType::Boolean | DataType::Date32 => cast(col, &DataType::Int32)
            .ok()
            .and_then(|c| cast(&c, &DataType::Float64).ok()),
        DataType::Date64 | DataType::Timestamp(_, _) | DataType::Duration(_) => {
            cast(col, &DataType::Int64)
                .ok()
                .and_then(|c| cast(&c, &DataType::Float64).ok())
        }
        _ => None,
    }?;
    let floats = attempt.as_any().downcast_ref::<Float64Array>()?;
    Some(
        (0..floats.len())
            .map(|i| {
                if floats.is_null(i) {
                    f64::NAN
                } else {
                    floats.value(i)
                }
            })
            .collect(),
    )
}
