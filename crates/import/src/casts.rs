//! `try_to_date` / `try_to_timestamp`: format parsing that yields NULL on
//! failure — recipe vocabulary (SPEC.md §3: the recipe carries the casts,
//! and one dirty value must cost a NULL cell, never the import). DataFusion
//! 54's own `to_date`/`to_timestamp` abort the whole scan on one dirty
//! value and ship no `try_` variants (verified in source), so
//! these register in the recipe/probe context and in the session — usable
//! in recipes, probes, scripts, and user SQL alike.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, StringArray, StringArrayType, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
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
    /// Any argument types: the value column arrives at whichever string
    /// width the engine carries it (Utf8View off a parquet scan), and a
    /// signature naming `Utf8` alone made the planner coerce the column
    /// first — a copy of every row. `invoke_with_args` checks the types
    /// itself and dispatches on the width.
    fn date() -> Self {
        TryParse {
            name: "try_to_date",
            returns: DataType::Date32,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn timestamp() -> Self {
        TryParse {
            name: "try_to_timestamp",
            returns: DataType::Timestamp(TimeUnit::Microsecond, None),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TryParse {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() < 2 {
            return exec_err!("{} takes a value and at least one format", self.name);
        }
        Ok(self.returns.clone())
    }

    /// Formats are tried in order and the first that parses wins, so one
    /// call handles a column carrying several conventions:
    /// `try_to_date(d, '%Y-%m-%d', '%d/%m/%Y', '%d-%b-%y')`. Run 4 met a
    /// payments column with three interleaved formats and no ISO at all,
    /// and had to write a coalesce ladder of three separate calls to
    /// read it — the same work, spelled three times, with the value
    /// expression repeated in each.
    ///
    /// Order is the author's and it is load-bearing where two formats
    /// can both parse one value: `'02/03/2025'` is March 2nd under
    /// `%d/%m/%Y` and February 3rd under `%m/%d/%Y`, and whichever is
    /// named first decides. Put the unambiguous formats first and the
    /// ambiguous pair last, in the order the source system writes them.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // The formats are the author's literals; whatever width the
        // planner spelled them at, Utf8 is one cast of a handful of
        // strings (and none at all when they already are).
        let mut formats = Vec::with_capacity(arrays.len() - 1);
        for array in &arrays[1..] {
            if !matches!(
                array.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return exec_err!("{} expects string formats", self.name);
            }
            formats.push(cast(array, &DataType::Utf8)?);
        }
        let formats: Vec<&StringArray> = formats.iter().map(|f| f.as_string::<i32>()).collect();
        // The column at whichever width the engine carries it. A parquet
        // scan under DataFusion 54 yields Utf8View, and a downcast to
        // `StringArray` alone made the planner coerce the whole column
        // first — a copy per row — before a single value was read. Three
        // arms, as datafusion-functions' own datetime dispatch has them.
        let values = &arrays[0];
        let out = match values.data_type() {
            DataType::Utf8 => self.parse(values.as_string::<i32>(), &formats),
            DataType::LargeUtf8 => self.parse(values.as_string::<i64>(), &formats),
            DataType::Utf8View => self.parse(values.as_string_view(), &formats),
            other => return exec_err!("{} expects a string column, not {other}", self.name),
        };
        Ok(ColumnarValue::Array(out))
    }
}

impl TryParse {
    /// Every row through the format ladder, into the declared type.
    fn parse<'a, V: StringArrayType<'a>>(&self, values: V, formats: &[&StringArray]) -> ArrayRef {
        // The first format that parses this row's value, in the order
        // the author named them.
        let parsed = |i: usize, take: &dyn Fn(&str, &str) -> Option<i64>| -> Option<i64> {
            if values.is_null(i) {
                return None;
            }
            let value = values.value(i);
            formats.iter().find_map(|f| {
                if f.is_null(i) {
                    return None;
                }
                take(value, f.value(i))
            })
        };
        let out: ArrayRef = match self.returns {
            DataType::Date32 => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
                let day = move |value: &str, format: &str| {
                    chrono::NaiveDate::parse_from_str(value, format)
                        .ok()
                        .map(|d| (d - epoch).num_days())
                };
                Arc::new(Date32Array::from_iter(
                    (0..values.len()).map(|i| parsed(i, &day).map(|d| d as i32)),
                ))
            }
            _ => {
                let micros = |value: &str, format: &str| {
                    chrono::NaiveDateTime::parse_from_str(value, format)
                        .ok()
                        .map(|t| t.and_utc().timestamp_micros())
                };
                Arc::new(TimestampMicrosecondArray::from_iter(
                    (0..values.len()).map(|i| parsed(i, &micros)),
                ))
            }
        };
        out
    }
}
