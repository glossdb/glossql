//! The period calendar, defined once. A period is the start of the
//! bucket a time falls in at a resolution, and the engine's
//! `date_trunc` is the only thing that says where a bucket starts:
//! the plan buckets a frame's rows with it ([`bucket`]), and where
//! Rust needs a bucket, the period after one or the periods ahead of
//! one, it asks the engine the same question over a batch of periods
//! ([`over`], [`ahead`], [`ahead_until`]). A week starts on Monday
//! because `date_trunc` says so, nowhere else; a month, a quarter and
//! a year step by calendar months, the engine's interval arithmetic.
//!
//! A period travels as microseconds since the epoch — the unit the
//! lake lands a timestamp in (`glossql_catalog`, `timestamp`) — so no
//! cast sits between a landed column and a cell. `TIMESTAMP(6)` names
//! that unit in a plan; [`TYPE`] spells it.

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::functions::expr_fn::date_trunc;
use datafusion::logical_expr::{Expr, cast, lit};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use crate::cube::Resolution;
use crate::session::SessionError;

/// The period unit.
pub(crate) const UNIT: TimeUnit = TimeUnit::Microsecond;
/// The period type as a plan spells it.
pub(crate) const TYPE: &str = "TIMESTAMP(6)";

pub(crate) fn data_type() -> DataType {
    DataType::Timestamp(UNIT, None)
}

pub(crate) fn field(name: &str, nullable: bool) -> Field {
    Field::new(name, data_type(), nullable)
}

/// The bucket a time expression falls in at a resolution, as a plan
/// names a period: `date_trunc` at the resolution, in the period type
/// whatever the column's own.
pub(crate) fn bucket(time: &str, resolution: Resolution) -> String {
    format!(
        "CAST(date_trunc('{}', {time}) AS {TYPE})",
        resolution.as_str()
    )
}

/// [`bucket`] as an expression, for a plan built rather than parsed.
pub(crate) fn bucket_expr(time: Expr, resolution: Resolution) -> Expr {
    cast(date_trunc(lit(resolution.as_str()), time), data_type())
}

/// A period as an expression.
pub(crate) fn scalar(period: i64) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(Some(period), None))
}

/// A window's rung as the engine's interval expression — `48 months`,
/// `3 years`, `13 weeks` as the cube aspect writes them.
pub(crate) fn rung(text: &str) -> Result<Expr, DataFusionError> {
    ScalarValue::try_from_string(
        text.to_string(),
        &DataType::Interval(IntervalUnit::MonthDayNano),
    )
    .map(lit)
}

/// `k` periods at a resolution as the engine's interval: calendar
/// months for a month, a quarter and a year; days for a week and a
/// day; fixed lengths below.
pub(crate) fn interval(resolution: Resolution, k: usize) -> String {
    let (n, unit) = match resolution {
        Resolution::Minute => (k, "minutes"),
        Resolution::Hour => (k, "hours"),
        Resolution::Day => (k, "days"),
        Resolution::Week => (7 * k, "days"),
        Resolution::Month => (k, "months"),
        Resolution::Quarter => (3 * k, "months"),
        Resolution::Year => (12 * k, "months"),
    };
    format!("INTERVAL '{n} {unit}'")
}

/// A period as a plan literal.
pub(crate) fn literal(period: i64) -> String {
    format!("CAST('{}' AS {TYPE})", text(period))
}

/// A period as civil time, UTC.
pub(crate) fn civil(period: i64) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(period).map(|t| t.naive_utc())
}

/// A period as text: `YYYY-MM-DDTHH:MM:SS`, the fraction where there
/// is one.
pub(crate) fn text(period: i64) -> String {
    match civil(period) {
        Some(t) if period % 1_000_000 == 0 => t.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Some(t) => t.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
        None => period.to_string(),
    }
}

/// A period as a date, `YYYY-MM-DD`.
pub(crate) fn date(period: i64) -> String {
    civil(period).map_or_else(|| period.to_string(), |t| t.format("%Y-%m-%d").to_string())
}

/// A period as text for a person: the date where the bucket starts at
/// midnight, the full text where it does not.
pub(crate) fn short(period: i64) -> String {
    if period.rem_euclid(86_400_000_000) == 0 {
        date(period)
    } else {
        text(period)
    }
}

/// `YYYY-MM-DD` as the period at that day's start.
pub(crate) fn parse_date(s: &str) -> Option<i64> {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .map(|d| {
            d.and_time(chrono::NaiveTime::MIN)
                .and_utc()
                .timestamp_micros()
        })
}

/// A column read as periods; None where it is not in the period type.
pub(crate) fn column(array: &ArrayRef) -> Option<&TimestampMicrosecondArray> {
    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
}

/// A period column as values, a NULL as None.
pub(crate) fn values(array: &TimestampMicrosecondArray) -> Vec<Option<i64>> {
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array.value(i)))
        .collect()
}

/// One pass of the engine over a batch, registered as `p`: the
/// query's columns in its row order. The calendar's one door to the
/// engine; a plan over a metric's frame has its own.
pub(crate) async fn eval(batch: RecordBatch, sql: &str) -> Result<Vec<ArrayRef>, DataFusionError> {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("p", Arc::new(table))?;
    let batches = ctx.sql(sql).await?.collect().await?;
    let Some(width) = batches.first().map(RecordBatch::num_columns) else {
        return Ok(Vec::new());
    };
    let batches: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    (0..width)
        .map(|i| {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(i).as_ref()).collect();
            if parts.is_empty() {
                return Ok(datafusion::arrow::array::new_empty_array(&data_type()));
            }
            datafusion::arrow::compute::concat(&parts).map_err(DataFusionError::from)
        })
        .collect()
}

/// What the engine refuses in the calendar, as a session error.
pub(crate) fn refused(source: DataFusionError) -> SessionError {
    SessionError::Engine {
        context: "the period calendar: ".into(),
        source,
    }
}

fn no_period() -> DataFusionError {
    DataFusionError::Execution("the calendar answered no period".into())
}

/// The bucket `at` falls in at a resolution.
pub(crate) async fn start(at: i64, resolution: Resolution) -> Result<i64, DataFusionError> {
    over(&[at], &[bucket("p", resolution)])
        .await?
        .first()
        .and_then(|c| c.first().copied().flatten())
        .ok_or_else(no_period)
}

/// Every expression over `p`, a column of periods, read back in the
/// period type: one column per expression, aligned with the input.
pub(crate) async fn over(
    periods: &[i64],
    exprs: &[String],
) -> Result<Vec<Vec<Option<i64>>>, DataFusionError> {
    if periods.is_empty() {
        return Ok(vec![Vec::new(); exprs.len()]);
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field("p", false)])),
        vec![Arc::new(TimestampMicrosecondArray::from_iter_values(
            periods.iter().copied(),
        ))],
    )?;
    let cast: Vec<String> = exprs
        .iter()
        .enumerate()
        .map(|(i, e)| format!("CAST({e} AS {TYPE}) AS c{i}"))
        .collect();
    let columns = eval(batch, &format!("SELECT {} FROM p", cast.join(", "))).await?;
    if columns.len() != exprs.len() {
        return Err(no_period());
    }
    columns
        .iter()
        .map(|c| column(c).map(values).ok_or_else(no_period))
        .collect()
}

/// The period after `period` at a resolution.
pub(crate) async fn next(period: i64, resolution: Resolution) -> Result<i64, DataFusionError> {
    ahead(period, resolution, 1)
        .await?
        .first()
        .copied()
        .ok_or_else(no_period)
}

/// The periods ahead of `origin` at a resolution: the `k`th is
/// `origin` plus `k` of the resolution, `k` from 1 to `n`.
pub(crate) async fn ahead(
    origin: i64,
    resolution: Resolution,
    n: usize,
) -> Result<Vec<i64>, DataFusionError> {
    if n == 0 {
        return Ok(Vec::new());
    }
    series(
        origin,
        resolution,
        &format!("p + {}", interval(resolution, n)),
    )
    .await
}

/// The periods ahead of `origin` at a resolution, up to and including
/// `until` where it lies on the grid.
pub(crate) async fn ahead_until(
    origin: i64,
    resolution: Resolution,
    until: i64,
) -> Result<Vec<i64>, DataFusionError> {
    series(origin, resolution, &literal(until)).await
}

/// `origin` plus one period, then every period to `stop` (an
/// expression over `p`, the origin), in order.
async fn series(
    origin: i64,
    resolution: Resolution,
    stop: &str,
) -> Result<Vec<i64>, DataFusionError> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field("p", false)])),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![origin]))],
    )?;
    let step = interval(resolution, 1);
    let sql = format!(
        "SELECT CAST(unnest(generate_series(p + {step}, {stop}, {step})) AS {TYPE}) AS c0 FROM p"
    );
    let columns = eval(batch, &sql).await?;
    let Some(periods) = columns.first().and_then(column) else {
        return Err(no_period());
    };
    Ok(values(periods).into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> i64 {
        parse_date(s).expect("a date")
    }

    #[tokio::test]
    async fn a_week_starts_on_monday_and_a_month_steps_by_the_calendar() {
        let periods = [at("2026-01-04"), at("2026-01-05"), at("2024-02-29")];
        let got = over(
            &periods,
            &[
                bucket("p", Resolution::Week),
                bucket("p", Resolution::Quarter),
                format!("p + {}", interval(Resolution::Month, 1)),
            ],
        )
        .await
        .expect("the calendar answers");
        let dates = |c: &Vec<Option<i64>>| -> Vec<String> {
            c.iter().map(|p| date(p.expect("a period"))).collect()
        };
        assert_eq!(dates(&got[0]), ["2025-12-29", "2026-01-05", "2024-02-26"]);
        assert_eq!(dates(&got[1]), ["2026-01-01", "2026-01-01", "2024-01-01"]);
        assert_eq!(dates(&got[2]), ["2026-02-04", "2026-02-05", "2024-03-29"]);
    }

    #[tokio::test]
    async fn the_periods_ahead_step_from_the_origin() {
        let months = ahead(at("2025-11-01"), Resolution::Month, 3)
            .await
            .expect("the calendar answers");
        assert_eq!(
            months.iter().map(|p| date(*p)).collect::<Vec<_>>(),
            ["2025-12-01", "2026-01-01", "2026-02-01"]
        );
        let weeks = ahead_until(at("2025-12-22"), Resolution::Week, at("2026-01-12"))
            .await
            .expect("the calendar answers");
        assert_eq!(
            weeks.iter().map(|p| date(*p)).collect::<Vec<_>>(),
            ["2025-12-29", "2026-01-05", "2026-01-12"]
        );
        assert_eq!(
            next(at("2024-02-29"), Resolution::Day)
                .await
                .expect("a day"),
            at("2024-03-01")
        );
        assert!(
            ahead(at("2025-11-01"), Resolution::Month, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn the_expression_twins_agree_with_the_text() {
        use datafusion::arrow::array::RecordBatch;
        use datafusion::datasource::{MemTable, provider_as_source};
        use datafusion::logical_expr::{LogicalPlanBuilder, col};
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field("p", false)])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![at(
                "2026-01-04",
            )]))],
        )
        .unwrap();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let plan = LogicalPlanBuilder::scan("p", provider_as_source(Arc::new(table)), None)
            .unwrap()
            .project(vec![
                bucket_expr(col("p"), Resolution::Week).alias("w"),
                (bucket_expr(col("p"), Resolution::Month) - rung("2 months").unwrap()).alias("m"),
                scalar(at("2024-02-29")).alias("s"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let got: Vec<String> = (0..3)
            .map(|i| date(column(batches[0].column(i)).unwrap().value(0)))
            .collect();
        assert_eq!(got, ["2025-12-29", "2025-11-01", "2024-02-29"]);
    }

    #[test]
    fn a_period_prints_as_its_civil_time() {
        assert_eq!(text(at("2026-01-05")), "2026-01-05T00:00:00");
        assert_eq!(short(at("2026-01-05")), "2026-01-05");
        assert_eq!(
            short(at("2026-01-05") + 3_600_000_000),
            "2026-01-05T01:00:00"
        );
        assert_eq!(text(at("2026-01-05") + 1), "2026-01-05T00:00:00.000001");
        assert_eq!(
            literal(at("2026-01-05")),
            "CAST('2026-01-05T00:00:00' AS TIMESTAMP(6))"
        );
    }
}
