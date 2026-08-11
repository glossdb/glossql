//! `try_to_date` / `try_to_timestamp`: format parsing that yields NULL on
//! failure — recipe vocabulary (SPEC.md §3: the recipe carries the casts,
//! and one dirty value must cost a NULL cell, never the import). DataFusion
//! 53's own `to_date`/`to_timestamp` abort the whole scan on one dirty
//! value and ship no `try_` variants (verified in source, 2026-08-04), so
//! these register in the recipe/probe context and in the session — usable
//! in recipes, probes, scripts, and user SQL alike.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result as DFResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

pub fn register_try_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TryParse::date()));
    ctx.register_udf(ScalarUDF::from(TryParse::timestamp()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TryParse {
    name: &'static str,
    returns: DataType,
    signature: Signature,
}

impl TryParse {
    fn date() -> Self {
        TryParse {
            name: "try_to_date",
            returns: DataType::Date32,
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }

    fn timestamp() -> Self {
        TryParse {
            name: "try_to_timestamp",
            returns: DataType::Timestamp(TimeUnit::Microsecond, None),
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TryParse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(self.returns.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(values) = arrays[0].as_any().downcast_ref::<StringArray>() else {
            return exec_err!("{} expects a string column", self.name);
        };
        let Some(formats) = arrays[1].as_any().downcast_ref::<StringArray>() else {
            return exec_err!("{} expects a string format", self.name);
        };
        let format_at = |i: usize| {
            if formats.is_null(i) {
                None
            } else {
                Some(formats.value(i))
            }
        };
        let out: ArrayRef = match self.returns {
            DataType::Date32 => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
                Arc::new(Date32Array::from_iter((0..values.len()).map(|i| {
                    let f = format_at(i)?;
                    if values.is_null(i) {
                        return None;
                    }
                    chrono::NaiveDate::parse_from_str(values.value(i), f)
                        .ok()
                        .map(|d| (d - epoch).num_days() as i32)
                })))
            }
            _ => Arc::new(TimestampMicrosecondArray::from_iter((0..values.len()).map(
                |i| {
                    let f = format_at(i)?;
                    if values.is_null(i) {
                        return None;
                    }
                    chrono::NaiveDateTime::parse_from_str(values.value(i), f)
                        .ok()
                        .map(|t| t.and_utc().timestamp_micros())
                },
            ))),
        };
        Ok(ColumnarValue::Array(out))
    }
}
