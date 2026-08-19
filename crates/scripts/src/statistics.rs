//! The shipped statistics as engine aggregates (stage 5, the foundation
//! report §7e): `profile`, `mad` and `entropy` over the same kernels the
//! scripts compose — one implementation behind both doors. `mad` and
//! `entropy` are the two statistics DataFusion lacks; `profile` is the
//! column profile as one value, so a measurement body is
//! `SELECT profile(v) FROM subject_column($subject)` and an agent's own
//! SQL can ask the same question of anything.
//!
//! Each accumulator collects its column and evaluates once at the end —
//! the same cost the door paid when a script pulled the column through
//! `db.query`, now bounded by the engine's own memory accounting.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use serde_json::{Value, json};

use crate::{
    Extremum, display_counts, entropy_of, extremum_of, interpolate, len_stats_of, mad_of, mean_of,
    numeric_like, stddev_of, top_k, valid_floats,
};

/// The shipped aggregate, for registration when the runtime attaches
/// to a session. `mad` and `entropy` ride inside its output —
/// `profile(v)['numeric']['mad']`, `profile(v)['entropy']` — so a body
/// or an agent's own SQL reads them off the one struct.
pub fn udafs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::from(Profile::new())]
}

fn kernel(e: String) -> DataFusionError {
    DataFusionError::Execution(e)
}

/// The collected column: every accumulator here holds what it has seen
/// and computes once, exactly like `median` in DataFusion's own library.
/// The partial state is one `List` of the input type, nulls kept — the
/// profile counts them.
#[derive(Debug)]
struct Collected {
    dt: DataType,
    arrays: Vec<ArrayRef>,
}

impl Collected {
    fn new(dt: DataType) -> Self {
        Collected {
            dt,
            arrays: Vec::new(),
        }
    }

    fn all(&self) -> Result<ArrayRef> {
        if self.arrays.is_empty() {
            return Ok(new_empty_array(&self.dt));
        }
        let slices: Vec<&dyn Array> = self.arrays.iter().map(AsRef::as_ref).collect();
        Ok(concat(&slices)?)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let child = self.all()?;
        let list = ListArray::new(
            Arc::new(Field::new_list_field(self.dt.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, child.len() as i32])),
            child,
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn update(&mut self, values: &[ArrayRef]) {
        self.arrays.push(Arc::clone(&values[0]));
    }

    fn merge(&mut self, states: &[ArrayRef]) {
        for v in states[0].as_list::<i32>().iter().flatten() {
            self.arrays.push(v);
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .arrays
                .iter()
                .map(|a| a.get_array_memory_size())
                .sum::<usize>()
    }

    fn state_field(name: &str, dt: &DataType) -> FieldRef {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new_list_field(dt.clone(), true))),
            true,
        )
        .into()
    }
}

// ---- profile ---------------------------------------------------------

#[derive(PartialEq, Eq, Hash, Debug)]
struct Profile {
    signature: Signature,
}

impl Profile {
    fn new() -> Self {
        Profile {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Profile {
    fn name(&self) -> &str {
        "profile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(profile_fields(&arg_types[0])))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Collected::state_field(
            &format_state_name(args.name, "profile"),
            args.input_fields[0].data_type(),
        )])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ProfileAccumulator(Collected::new(
            acc_args.expr_fields[0].data_type().clone(),
        ))))
    }
}

#[derive(Debug)]
struct ProfileAccumulator(Collected);

impl Accumulator for ProfileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.update(values);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states);
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let a = self.0.all()?;
        let value = profile_value(&a).map_err(kernel)?;
        struct_scalar(&profile_fields(a.data_type()), &value)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

/// The profile's shape as an Arrow struct. `min`/`max` mirror
/// [`extremum_of`]: plain text stays text, a numeric-readable column
/// reads as floats, everything else by display form. The conditional
/// blocks (`lengths`, `numeric`) are nullable fields — absent in the
/// JSON, null in the struct, omitted again on the way out.
fn profile_fields(dt: &DataType) -> Fields {
    let minmax = if !matches!(dt, DataType::Utf8) && numeric_like(dt) {
        DataType::Float64
    } else {
        DataType::Utf8
    };
    let float = |name: &str| Field::new(name, DataType::Float64, true);
    let int = |name: &str| Field::new(name, DataType::Int64, true);
    let percentiles = Fields::from(vec![
        float("p01"),
        float("p25"),
        float("p50"),
        float("p75"),
        float("p99"),
    ]);
    let numeric = Fields::from(vec![
        float("mean"),
        float("stddev"),
        float("mad"),
        Field::new("percentiles", DataType::Struct(percentiles), true),
    ]);
    let lengths = Fields::from(vec![int("min"), int("max"), float("avg")]);
    let bucket = Fields::from(vec![
        Field::new("value", DataType::Utf8, true),
        int("count"),
    ]);
    let summary = Fields::from(vec![
        int("total"),
        float("null_ratio"),
        int("distinct"),
        Field::new("note", DataType::Utf8, true),
    ]);
    Fields::from(vec![
        int("total"),
        float("null_ratio"),
        int("distinct"),
        float("cardinality_ratio"),
        Field::new("min", minmax.clone(), true),
        Field::new("max", minmax, true),
        float("entropy"),
        Field::new(
            "top_values",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(bucket),
                true,
            ))),
            true,
        ),
        Field::new("lengths", DataType::Struct(lengths), true),
        Field::new("numeric", DataType::Struct(numeric), true),
        Field::new("summary", DataType::Struct(summary), true),
    ])
}

/// The profile itself — the exact mirror of what the profile script computed
/// until stage 5, kernel for kernel, so the crossing gates byte-identical
/// against the goldens. `summary` is what extraction serves (full
/// bodies through the door are the fan-out tax); the
/// full body reads back via `GLOSSARY`.
fn profile_value(a: &ArrayRef) -> Result<Value, String> {
    let n = a.len() as i64;
    let nulls = a.null_count() as i64;
    let non_null = n - nulls;
    let counts = display_counts(a)?;
    let distinct = counts.len() as i64;
    let null_ratio = if n == 0 { 0.0 } else { nulls as f64 / n as f64 };
    let extremum = |min: bool| -> Result<Value, String> {
        Ok(match extremum_of(a, min)? {
            Some(Extremum::Num(v)) => json!(v),
            Some(Extremum::Text(s)) => json!(s),
            None => Value::Null,
        })
    };

    let mut profile = serde_json::Map::new();
    profile.insert("total".into(), json!(n));
    profile.insert("null_ratio".into(), json!(null_ratio));
    profile.insert("distinct".into(), json!(distinct));
    profile.insert(
        "cardinality_ratio".into(),
        json!(if non_null == 0 {
            0.0
        } else {
            distinct as f64 / non_null as f64
        }),
    );
    profile.insert("min".into(), extremum(true)?);
    profile.insert("max".into(), extremum(false)?);
    profile.insert("entropy".into(), json!(entropy_of(a)?));
    profile.insert(
        "top_values".into(),
        Value::Array(
            top_k(&counts, 20)
                .into_iter()
                .map(|(value, count)| json!({ "value": value, "count": count }))
                .collect(),
        ),
    );
    if let Some((min, max, avg)) = len_stats_of(a) {
        profile.insert(
            "lengths".into(),
            json!({ "min": min, "max": max, "avg": avg }),
        );
    }
    let floats = valid_floats(a)?;
    if let Some(mean) = mean_of(&floats) {
        let mut sorted = floats.clone();
        sorted.sort_by(f64::total_cmp);
        let p = |q: f64| interpolate(&sorted, q);
        profile.insert(
            "numeric".into(),
            json!({
                "mean": mean,
                "stddev": stddev_of(&floats),
                "mad": mad_of(&floats),
                "percentiles": {
                    "p01": p(0.01), "p25": p(0.25), "p50": p(0.5),
                    "p75": p(0.75), "p99": p(0.99),
                },
            }),
        );
    }
    profile.insert(
        "summary".into(),
        json!({
            "total": n,
            "null_ratio": null_ratio,
            "distinct": distinct,
            "note": "cached — the full profile reads back via GLOSSARY(table.column::profile)",
        }),
    );
    Ok(Value::Object(profile))
}

/// One JSON object into a one-row struct scalar, through the format's
/// own decoder — no hand-built nested builders to drift.
fn struct_scalar(fields: &Fields, value: &Value) -> Result<ScalarValue> {
    let schema = Arc::new(Schema::new(fields.clone()));
    let mut decoder = arrow_json::ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(std::slice::from_ref(value))?;
    let batch = decoder
        .flush()?
        .ok_or_else(|| DataFusionError::Execution("the profile decoded to nothing".into()))?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::from(batch))))
}

