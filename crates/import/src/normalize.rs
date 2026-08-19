//! Batch normalization before a recipe result lands as an Iceberg table.
//!
//! One map: `compat` folds Arrow types Iceberg 0.10.1 rejects or would
//! promote to format-v3 types onto their v2 equivalents (ns timestamps →
//! µs, `UInt64` → `Int64`, …). Nothing else touches the schema — the
//! recipe's authored casts are the landed types (a `force_utf8`
//! refold is retired raw-twin machinery — it once landed eight
//! string-typed tables in a single run).

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::{Error, Result};

fn compat_type(t: &DataType) -> DataType {
    match t {
        DataType::Timestamp(TimeUnit::Microsecond, _) => t.clone(),
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        DataType::Time32(_) | DataType::Time64(_) => DataType::Time64(TimeUnit::Microsecond),
        DataType::Date64 => DataType::Date32,
        DataType::UInt64 => DataType::Int64,
        DataType::Float16 => DataType::Float32,
        // Iceberg reads these back as Utf8 / LargeBinary; land them that way
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::Binary | DataType::BinaryView => DataType::LargeBinary,
        other => other.clone(),
    }
}

/// Fold types Iceberg v2 cannot hold onto their nearest v2 shape.
pub fn compat(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), compat_type(f.data_type()), f.is_nullable()))
        .collect();
    let out_schema = Arc::new(Schema::new(fields));
    if out_schema.fields() == schema.fields() {
        return Ok((out_schema, batches));
    }
    let out_batches = batches
        .into_iter()
        .map(|batch| {
            let columns = batch
                .columns()
                .iter()
                .zip(out_schema.fields())
                .map(|(col, field)| cast(col, field.data_type()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::Batches(e.to_string()))?;
            RecordBatch::try_new(Arc::clone(&out_schema), columns)
                .map_err(|e| Error::Batches(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((out_schema, out_batches))
}
