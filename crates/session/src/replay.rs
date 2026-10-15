//! The plan rewrite: a column's values changed in every scan of its
//! table, by a factor per period, and the plan replayed over the change.
//! Storage is never touched — the override is a projection the rewrite
//! adds over the scan, re-aliased under the table's own name so every
//! outer reference resolves unchanged. A grounding replayed under it
//! recomputes exactly as its SQL says, which is the arithmetic half of
//! a scenario: what a declared change does through the definitions,
//! before any model estimates what else moves.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DFResult};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, lit, when};

use crate::plan::date_column;

/// One factor over one period: the rows whose date is `from` or later
/// and before `until` are multiplied by it; no `until` runs to the end.
/// Dates as the date column reads them (`YYYY-MM-DD`).
#[derive(Clone, Debug, PartialEq)]
pub struct Factor {
    pub from: String,
    pub until: Option<String>,
    pub factor: f64,
}

/// One override: a real column of a landed table and its factors per
/// period. A period no factor names is unchanged; where two periods
/// overlap, the later listed wins.
#[derive(Clone, Debug, PartialEq)]
pub struct Override {
    pub table: String,
    pub column: String,
    pub factors: Vec<Factor>,
}

fn moves(f: &Factor) -> bool {
    (f.factor - 1.0).abs() > 1e-9
}

/// Every `TableScan` of an overridden table gains a projection scaling
/// the overridden columns over the periods named, under the condition
/// of the table's first date column. A table without a date column is
/// left as it is: there is nothing to anchor a period to. A factor of
/// one changes nothing and adds no arm; an override of none adds no
/// projection.
pub fn apply_overrides(plan: LogicalPlan, overrides: &[Override]) -> DFResult<LogicalPlan> {
    plan.transform_up(|node| {
        let (table_name, fields) = match &node {
            LogicalPlan::TableScan(scan) => (
                scan.table_name.clone(),
                scan.projected_schema.fields().clone(),
            ),
            _ => return Ok(Transformed::no(node)),
        };
        let table = table_name.table().to_string();
        let active: Vec<&Override> = overrides
            .iter()
            .filter(|o| o.table == table && o.factors.iter().any(moves))
            .collect();
        if active.is_empty() {
            return Ok(Transformed::no(node));
        }
        let Some(tcol) = date_column(&fields) else {
            return Ok(Transformed::no(node));
        };
        let tcol_type: DataType = fields
            .iter()
            .find(|f| f.name() == &tcol)
            .expect("date column found above")
            .data_type()
            .clone();
        let qualified =
            |name: &str| Expr::Column(Column::new(Some(table_name.clone()), name.to_string()));
        let exprs: DFResult<Vec<Expr>> = fields
            .iter()
            .map(|field| {
                let raw = qualified(field.name());
                let mut expr = raw.clone();
                for o in &active {
                    if o.column != *field.name() {
                        continue;
                    }
                    for f in o.factors.iter().filter(|f| moves(f)) {
                        let start = cast(lit(f.from.clone()), tcol_type.clone());
                        let mut inside = qualified(&tcol).gt_eq(start);
                        if let Some(until) = &f.until {
                            let end = cast(lit(until.clone()), tcol_type.clone());
                            inside = inside.and(qualified(&tcol).lt(end));
                        }
                        let scaled = cast(raw.clone() * lit(f.factor), field.data_type().clone());
                        expr = when(inside, scaled).otherwise(expr)?;
                    }
                }
                Ok(expr.alias(field.name()))
            })
            .collect();
        let rebuilt = LogicalPlanBuilder::from(node)
            .project(exprs?)?
            .alias(table_name)?
            .build()?;
        Ok(Transformed::yes(rebuilt))
    })
    .map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::{Factor, Override, apply_overrides};

    /// Monthly revenue as units times price over the 15th of every
    /// month of 2026, replayed under `overrides`: the twelve monthly
    /// values and the twelve monthly unit sums.
    async fn replayed(overrides: &[Override]) -> (Vec<f64>, Vec<f64>) {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_date", DataType::Date32, false),
            Field::new("units", DataType::Float64, false),
            Field::new("unit_price", DataType::Float64, false),
        ]));
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
        let dates: Vec<i32> = (1..=12)
            .map(|m| {
                let day = chrono::NaiveDate::from_ymd_opt(2026, m, 15).expect("a date");
                (day - epoch).num_days() as i32
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Date32Array::from(dates)),
                Arc::new(Float64Array::from(vec![10.0; 12])),
                Arc::new(Float64Array::from(vec![100.0; 12])),
            ],
        )
        .expect("a batch");
        ctx.register_table(
            "sales",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .expect("registers");
        let sql = "SELECT date_trunc('month', order_date) AS period, \
                   sum(units * unit_price) AS value, sum(units) AS units \
                   FROM sales GROUP BY 1 ORDER BY 1";
        let plan = ctx.state().create_logical_plan(sql).await.expect("plans");
        let replayed = apply_overrides(plan, overrides).expect("rewrites");
        let batches = ctx
            .execute_logical_plan(replayed)
            .await
            .expect("executes")
            .collect()
            .await
            .expect("collects");
        let batch = datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .expect("one");
        assert_eq!(batch.num_rows(), 12);
        let floats = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("floats")
                .values()
                .to_vec()
        };
        (floats(1), floats(2))
    }

    fn price(factors: Vec<Factor>) -> Override {
        Override {
            table: "sales".into(),
            column: "unit_price".into(),
            factors,
        }
    }

    /// One grounding replayed with the price scaled by 1.15 from July
    /// on: the months before hold their value, the months from July on
    /// carry exactly the arithmetic, and a column the override never
    /// names is untouched.
    #[tokio::test]
    async fn one_column_scaled_from_a_start_month_equals_the_arithmetic() {
        let (value, units) = replayed(&[price(vec![Factor {
            from: "2026-07-01".into(),
            until: None,
            factor: 1.15,
        }])])
        .await;
        for month in 0..12 {
            let expected = if month >= 6 { 1150.0 } else { 1000.0 };
            assert!(
                (value[month] - expected).abs() < 1e-9,
                "month {}: {} against {expected}",
                month + 1,
                value[month]
            );
            assert_eq!(units[month], 10.0, "a column not named stays");
        }
    }

    /// A factor per period: July at 1.15 and September at 0.5, each
    /// one month, leave August and the rest as they were; a factor of
    /// one is no arm.
    #[tokio::test]
    async fn a_factor_per_period_scales_its_period_alone() {
        let month = |m: u32, factor: f64| Factor {
            from: format!("2026-{m:02}-01"),
            until: Some(format!("2026-{:02}-01", m + 1)),
            factor,
        };
        let (value, _) =
            replayed(&[price(vec![month(7, 1.15), month(8, 1.0), month(9, 0.5)])]).await;
        let expected = [
            1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1150.0, 1000.0, 500.0, 1000.0, 1000.0,
            1000.0,
        ];
        for (month, (got, want)) in value.iter().zip(expected).enumerate() {
            assert!(
                (got - want).abs() < 1e-9,
                "month {}: {got} against {want}",
                month + 1
            );
        }
    }
}
