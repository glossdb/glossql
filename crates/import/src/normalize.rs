//! Batch normalization before a recipe result lands as an Iceberg table.
//!
//! One map: `compat` folds Arrow types iceberg-rust rejects or would
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
        // A zoned timestamp is an instant; Iceberg holds it as
        // `timestamptz` and reads it back zoned `+00:00`, and the
        // parquet writer refuses a batch whose field names the zone
        // any other way — `UTC` included, which is what pyarrow and
        // pandas write. The cast keeps the instant.
        DataType::Timestamp(_, Some(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => t.clone(),
        DataType::Timestamp(_, None) => DataType::Timestamp(TimeUnit::Microsecond, None),
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

/// The schema a landing holds: types Iceberg v2 cannot hold folded onto
/// their nearest v2 shape. Decided by the schema alone, so it is known
/// before the first row.
pub fn compat_schema(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), compat_type(f.data_type()), f.is_nullable()))
        .collect();
    Arc::new(Schema::new(fields))
}

/// One batch folded onto [`compat_schema`]'s shape — a batch already in
/// it passes as it is.
pub fn compat_batch(batch: RecordBatch, out_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().fields() == out_schema.fields() {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(out_schema.fields())
        .map(|(col, field)| cast(col, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Batches(e.to_string()))?;
    RecordBatch::try_new(Arc::clone(out_schema), columns).map_err(|e| Error::Batches(e.to_string()))
}
