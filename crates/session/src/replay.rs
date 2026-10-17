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
use datafusion::common::{Column, DataFusionError, Result as DFResult, ScalarValue};
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

/// A table dated through a key: the column of the table that keys into
/// a dated table, and per factor — at the factor's index — the keys
/// whose dates fall in its period. A factor with no key applies to no
/// row.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyAnchor {
    pub column: String,
    pub keys: Vec<Vec<ScalarValue>>,
}

/// One override: a real column of a landed table and its factors per
/// period. A period no factor names is unchanged; where two periods
/// overlap, the later listed wins. The rows a period holds are placed
/// by the table's first date column, or by `anchor` where the table is
/// dated through a key.
#[derive(Clone, Debug, PartialEq)]
pub struct Override {
    pub table: String,
    pub column: String,
    pub factors: Vec<Factor>,
    pub anchor: Option<KeyAnchor>,
}

fn moves(f: &Factor) -> bool {
    (f.factor - 1.0).abs() > 1e-9
}

/// Every `TableScan` of an overridden table gains a projection scaling
/// the overridden columns over the periods named, under the condition
/// of the table's first date column, or of its key column against the
/// period's keys where the override carries an anchor. An override of
/// a table that has no date column and no anchor is refused: there is
/// nothing to place a period by, and a replay that changed nothing
/// must not pass for one. A factor of one changes nothing and adds no
/// arm; an override of none adds no projection. A scaled column is
/// read as a real number — a double under the table's name, inside and
/// outside the periods named — never cast back to its own type: an
/// integer quantity at ×1.02 is not floored on every row, a decimal
/// amount not rounded per line.
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
        let dated = date_column(&fields);
        if dated.is_none() && active.iter().any(|o| o.anchor.is_none()) {
            let columns: Vec<&str> = active
                .iter()
                .filter(|o| o.anchor.is_none())
                .map(|o| o.column.as_str())
                .collect();
            return Err(DataFusionError::Plan(format!(
                "`{table}` carries no date column to place the factors in time, and the \
                 override of `{}` names no key to place them by",
                columns.join("`, `")
            )));
        }
        let tcol_type: Option<DataType> = dated.as_ref().map(|tcol| {
            fields
                .iter()
                .find(|f| f.name() == tcol)
                .expect("date column found above")
                .data_type()
                .clone()
        });
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
                    // The column reads as a double once a factor names
                    // it, in every period: the arithmetic is exact and
                    // the type is one along the whole series.
                    let real = cast(raw.clone(), DataType::Float64);
                    expr = real.clone();
                    for (i, f) in o.factors.iter().enumerate().filter(|(_, f)| moves(f)) {
                        let inside = match (&o.anchor, &dated, &tcol_type) {
                            (Some(anchor), _, _) => {
                                let keys = anchor.keys.get(i).cloned().unwrap_or_default();
                                if keys.is_empty() {
                                    continue;
                                }
                                qualified(&anchor.column)
                                    .in_list(keys.into_iter().map(lit).collect(), false)
                            }
                            (None, Some(tcol), Some(tcol_type)) => {
                                let start = cast(lit(f.from.clone()), tcol_type.clone());
                                let mut inside = qualified(tcol).gt_eq(start);
                                if let Some(until) = &f.until {
                                    let end = cast(lit(until.clone()), tcol_type.clone());
                                    inside = inside.and(qualified(tcol).lt(end));
                                }
                                inside
                            }
                            (None, ..) => unreachable!("refused above"),
                        };
                        let scaled = real.clone() * lit(f.factor);
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
                i32::try_from((day - epoch).num_days()).expect("a day")
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
            anchor: None,
        }
    }

    /// An integer quantity scaled by a factor that is no integer reads
    /// as the real product, not the sum of per-row floors: ten units a
    /// month at 1.15 is 11.5 a month, 115 over ten rows, never 110.
    #[tokio::test]
    async fn an_integer_quantity_scaled_is_a_real_number_not_a_floor_per_row() {
        use datafusion::arrow::array::Int64Array;

        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_date", DataType::Date32, false),
            Field::new("units", DataType::Int64, false),
        ]));
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch");
        let july = chrono::NaiveDate::from_ymd_opt(2026, 7, 15).expect("a date");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Date32Array::from(vec![
                    i32::try_from(
                        (july - epoch).num_days()
                    )
                    .expect("a day");
                    10
                ])),
                Arc::new(Int64Array::from(vec![10i64; 10])),
            ],
        )
        .expect("a batch");
        ctx.register_table(
            "lines",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .expect("registers");
        let plan = ctx
            .state()
            .create_logical_plan("SELECT sum(units) AS units FROM lines")
            .await
            .expect("plans");
        let replayed = apply_overrides(
            plan,
            &[Override {
                table: "lines".into(),
                column: "units".into(),
                factors: vec![Factor {
                    from: "2026-07-01".into(),
                    until: None,
                    factor: 1.15,
                }],
                anchor: None,
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
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a double, not the column's integer")
            .value(0);
        assert!((total - 115.0).abs() < 1e-9, "{total}");
    }

    /// A table dated through a key: twelve rows keyed 1 to 12, no date
    /// column. An override anchored on the key with July's key scales
    /// July alone; the same override without an anchor is refused, not
    /// passed through unchanged.
    #[tokio::test]
    async fn a_table_dated_through_a_key_is_placed_by_its_keys_or_refused() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::common::ScalarValue;

        use super::KeyAnchor;

        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("date_sk", DataType::Int64, false),
            Field::new("units", DataType::Float64, false),
            Field::new("unit_price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from((1..=12).collect::<Vec<i64>>())),
                Arc::new(Float64Array::from(vec![10.0; 12])),
                Arc::new(Float64Array::from(vec![100.0; 12])),
            ],
        )
        .expect("a batch");
        ctx.register_table(
            "facts",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .expect("registers");
        let sql =
            "SELECT date_sk, sum(units * unit_price) AS value FROM facts GROUP BY 1 ORDER BY 1";
        let july = Factor {
            from: "2026-07-01".into(),
            until: Some("2026-08-01".into()),
            factor: 1.15,
        };
        let plan = ctx.state().create_logical_plan(sql).await.expect("plans");
        let anchored = apply_overrides(
            plan.clone(),
            &[Override {
                table: "facts".into(),
                column: "unit_price".into(),
                factors: vec![july.clone()],
                anchor: Some(KeyAnchor {
                    column: "date_sk".into(),
                    keys: vec![vec![ScalarValue::Int64(Some(7))]],
                }),
            }],
        )
        .expect("rewrites");
        let batches = ctx
            .execute_logical_plan(anchored)
            .await
            .expect("executes")
            .collect()
            .await
            .expect("collects");
        let batch = datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .expect("one");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("floats");
        for row in 0..12 {
            let expected = if row == 6 { 1150.0 } else { 1000.0 };
            assert!(
                (values.value(row) - expected).abs() < 1e-9,
                "key {}: {} against {expected}",
                row + 1,
                values.value(row)
            );
        }
        let refused = apply_overrides(
            plan,
            &[Override {
                table: "facts".into(),
                column: "unit_price".into(),
                factors: vec![july],
                anchor: None,
            }],
        )
        .expect_err("no date column, no anchor");
        assert!(
            refused
                .to_string()
                .contains("`facts` carries no date column"),
            "{refused}"
        );
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
