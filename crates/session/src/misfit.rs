//! The `misfit.` door (fixture 20): a declared sample
//! frame — an ordinary QUERY aspect, `x-kind: "sample"` by convention —
//! served back row by row with a `misfit` score: how badly each row fits
//! the rest of the same frame. One frame ranked against itself (the
//! model is protocol-robust — self-fit read the same as a clean
//! reference in the evaluation), the service behind the runtime seam,
//! the judge's conclusion landing as an ordinary gloss. Nothing is cached:
//! an investigation frame is small by construction and the ranking is
//! ephemeral by design.
//!
//! Judgment rides the refusal messages, never a silent guess: a frame
//! past the stated caps is refused with the number, a frame with too
//! little numeric surface abstains with the reason (the evaluation's
//! `sales_orders` lesson — some tables cannot carry a density read),
//! and every excluded column is named in `basis`. A refusal refuses
//! the read — the relation's shape *is* the frame, so there is no fixed
//! schema to carry refusal rows.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use glossql_glossary::Scope;
use serde_json::{Value, json};

use crate::reads::verdicts;
use crate::shared::{MISFIT, SessionError, Shared};

/// Stated caps (fixture 20 §6): refused by name, never silently cut.
/// The row cap bounds the model's context — a bigger population is
/// sampled down in the frame SQL, never streamed through the model.
/// 2000 is the measured bound (3.7s at 2048 on Metal with
/// the parallel chain rule).
const ROW_CAP: usize = 2000;
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
    if !shared.runtime().serves(MISFIT) {
        return Err(crate::shared::no_model(
            &format!("misfit.{frame}()"),
            MISFIT,
        ));
    }

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
    let verdicts = verdicts(&ctx, &dataset, &scope, Some(frame)).await?;
    let collapsed =
        glossql_glossary::Store::collapsed_read(&dataset, &scope, Some(frame), &ctx, &verdicts);
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
    let plan = crate::plan::build_plan(shared, &ctx, &capped).await?;
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
    let frame_batch = concat_batches(&schema, &batches).map_err(SessionError::from)?;
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

    // The surface, discovered; every exclusion named. Id-named columns
    // are out by the evaluation's structural lesson: key-like columns
    // separate any two row sets trivially. A constant column says
    // nothing about any row. Every other column travels as it is: a
    // number as a number, text and booleans as categories the service
    // reads as such, a date as the count it is stored as.
    let mut included: Vec<(String, ArrayRef)> = Vec::new();
    let mut excluded: Vec<String> = Vec::new();
    for (field, col) in schema.fields().iter().zip(frame_batch.columns()) {
        let name = field.name();
        if name == "id" || name.ends_with("_id") {
            excluded.push(format!("{name} (id)"));
            continue;
        }
        let Some(sent) = service_column(col) else {
            excluded.push(format!("{name} ({})", field.data_type()));
            continue;
        };
        if distinct_values(&sent)? < 2 {
            excluded.push(format!("{name} (constant)"));
            continue;
        }
        included.push((name.clone(), sent));
    }
    if included.len() < 2 {
        let named = if excluded.is_empty() {
            "nothing to exclude".to_string()
        } else {
            format!("excluded {}", excluded.join(", "))
        };
        return Err(bad(format!(
            "too little surface: {} usable column(s) after {named} — a density read \
             needs at least two, so the frame abstains",
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

    // The frame as the one table the service takes: every usable
    // column under its own name. `cells` would name the column to look
    // at per row; the relation's two added columns are fixture 20's
    // shape, so the door asks for none.
    // TODO: serve the column to look at once fixture 20 gives it a
    // place — `cells: true` answers one column per frame column.
    let sent = RecordBatch::try_new(
        Arc::new(Schema::new(
            included
                .iter()
                .map(|(name, col)| Field::new(name, col.data_type().clone(), true))
                .collect::<Vec<_>>(),
        )),
        included.iter().map(|(_, col)| Arc::clone(col)).collect(),
    )
    .map_err(SessionError::from)?;
    let answered = shared
        .runtime()
        .table(MISFIT, sent, json!({ "cells": false }))
        .await
        .map_err(|e| bad(format!("not served: the misfit service refused — {e}")))?;
    // One row per frame row, placed by `row`: the score is how far the
    // row's strangest cell lies below what is typical for its column,
    // lower stranger; the door serves it negated, so a higher misfit
    // fits the frame worse (fixture 20).
    let scores = scores_by_row(&answered.table, rows)
        .map_err(|e| bad(format!("not served: the misfit service answered {e}")))?;
    // A score the service could not read — a column whose null pattern
    // leaves no common support — comes back missing or non-finite, and
    // a non-finite score carries no ranking, so the read refuses (the
    // refuse-or-abstain contract) instead of serving NaN.
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
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(SessionError::from)
}

/// A column as the service takes it, or None where the type carries no
/// reading. Numbers travel as they are, a decimal as a float; text and
/// booleans are categories; temporal types ride their underlying count
/// (days, milliseconds) — order and spacing are what the density read
/// uses. Text is never parsed.
fn service_column(col: &ArrayRef) -> Option<ArrayRef> {
    match col.data_type() {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            cast(col, &DataType::Float64).ok()
        }
        dt if dt.is_numeric() => Some(Arc::clone(col)),
        DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some(Arc::clone(col))
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Duration(_) => {
            cast(col, &DataType::Int64).ok()
        }
        _ => None,
    }
}

/// How many distinct values a column holds, nulls aside. The frame is
/// under the row cap when this runs, so the set is bounded by it.
fn distinct_values(col: &ArrayRef) -> Result<usize, SessionError> {
    let mut seen = std::collections::HashSet::new();
    for i in 0..col.len() {
        if !col.is_null(i) {
            seen.insert(array_value_to_string(col, i).map_err(SessionError::from)?);
        }
    }
    Ok(seen.len())
}

/// The answer's `score` per frame row, placed by its `row`: every row
/// once, in the frame's order. A missing score is NaN, which the caller
/// refuses on.
fn scores_by_row(answer: &RecordBatch, rows: usize) -> Result<Vec<f64>, String> {
    let column = |name: &str| {
        answer
            .column_by_name(name)
            .ok_or_else(|| format!("no `{name}` column"))
    };
    let row = cast_with_options(
        column("row")?,
        &DataType::Int64,
        &CastOptions {
            safe: true,
            ..Default::default()
        },
    )
    .map_err(|e| format!("`row` does not read as an integer: {e}"))?;
    let row = row
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("`row` does not read as an integer")?;
    let score = cast_with_options(
        column("score")?,
        &DataType::Float64,
        &CastOptions {
            safe: true,
            ..Default::default()
        },
    )
    .map_err(|e| format!("`score` does not read as a number: {e}"))?;
    let score = score
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("`score` does not read as a number")?;
    if answer.num_rows() != rows {
        return Err(format!("{} rows for a frame of {rows}", answer.num_rows()));
    }
    let mut out = vec![f64::NAN; rows];
    let mut placed = vec![false; rows];
    for i in 0..answer.num_rows() {
        if row.is_null(i) {
            return Err("a row without its position".into());
        }
        let at = usize::try_from(row.value(i)).map_err(|_| "a row position below zero")?;
        if at >= rows || placed[at] {
            return Err(format!("row {at} outside the frame or answered twice"));
        }
        placed[at] = true;
        if !score.is_null(i) {
            out[at] = score.value(i);
        }
    }
    Ok(out)
}
