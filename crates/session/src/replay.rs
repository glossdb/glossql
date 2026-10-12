//! The plan rewrite: a column's values changed in every scan of its
//! table, from a start date on, and the plan replayed over the change.
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

/// One override: a real column of a landed table, the factor its values
/// are multiplied by, and the first day the factor applies, as the
/// date column reads it (`YYYY-MM-DD`).
#[derive(Clone, Debug, PartialEq)]
pub struct Override {
    pub table: String,
    pub column: String,
    pub factor: f64,
    pub from: String,
}

/// Every `TableScan` of an overridden table gains a projection scaling
/// the overridden columns from the start date on, under the condition
/// of the table's first date column. A table without a date column is
/// left as it is: there is nothing to anchor the start to. A factor of
/// one changes nothing and adds no projection.
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
            .filter(|o| o.table == table && (o.factor - 1.0).abs() > 1e-9)
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
                let mut expr = qualified(field.name());
                for o in &active {
                    if o.column != *field.name() {
                        continue;
                    }
                    let start = cast(lit(o.from.clone()), tcol_type.clone());
                    let scaled = cast(expr.clone() * lit(o.factor), field.data_type().clone());
                    expr = when(qualified(&tcol).gt_eq(start), scaled).otherwise(expr)?;
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

    use super::{Override, apply_overrides};

    /// One grounding — monthly revenue as units times price — replayed
    /// with the price scaled by 1.15 from July: the months before hold
    /// their value, the months from July on carry exactly the
    /// arithmetic, and a column the override never names is untouched.
    #[tokio::test]
    async fn one_column_scaled_from_a_start_month_equals_the_arithmetic() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_date", DataType::Date32, false),
            Field::new("units", DataType::Float64, false),
            Field::new("unit_price", DataType::Float64, false),
        ]));
        // The 15th of every month of 2026, as days since the epoch.
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
        let replayed = apply_overrides(
            plan,
            &[Override {
                table: "sales".into(),
                column: "unit_price".into(),
                factor: 1.15,
                from: "2026-07-01".into(),
            }],
        )
        .expect("rewrites");
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
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("floats");
        let units = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("floats");
        for month in 0..12 {
            let expected = if month >= 6 { 1150.0 } else { 1000.0 };
            assert!(
                (value.value(month) - expected).abs() < 1e-9,
                "month {}: {} against {expected}",
                month + 1,
                value.value(month)
            );
            assert_eq!(units.value(month), 10.0, "a column not named stays");
        }
    }
}
