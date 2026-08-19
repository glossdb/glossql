//! Served-column provenance: which dataset table column a metric's
//! served field descends from. The cube's judged reads key verdicts by
//! subject (`table.column`), but a served frame names aliases — so the
//! walk follows a plain column reference down the logical plan to the
//! table scan that serves it. A computed field (an aggregate, an
//! expression, a union arm) descends from no single column and stays
//! unmapped; under judged admission an unmapped field is a gap, never
//! a candidate.

use std::collections::HashMap;

use datafusion::common::Column;
use datafusion::logical_expr::{Expr, LogicalPlan};

/// The source subject (`table.column`) of each served field, keyed by
/// served name. `dataset` guards the terminal: a scan whose reference
/// carries a qualifier is only a dataset table when that qualifier is
/// the bound dataset — the `read.<aspect>` door scans under a
/// `read`-qualified name and must not mint a subject.
pub(crate) fn served_subjects(plan: &LogicalPlan, dataset: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (qualifier, field) in plan.schema().iter() {
        let col = Column::new(qualifier.cloned(), field.name());
        if let Some(subject) = source_of(plan, &col, dataset) {
            out.insert(field.name().clone(), subject);
        }
    }
    out
}

fn source_of(plan: &LogicalPlan, col: &Column, dataset: &str) -> Option<String> {
    let index = |p: &LogicalPlan| p.schema().index_of_column(col).ok();
    match plan {
        LogicalPlan::Projection(p) => follow(&p.input, &p.expr[index(plan)?], dataset),
        LogicalPlan::SubqueryAlias(a) => {
            let (qualifier, field) = a.input.schema().qualified_field(index(plan)?);
            source_of(
                &a.input,
                &Column::new(qualifier.cloned(), field.name()),
                dataset,
            )
        }
        LogicalPlan::Filter(f) => source_of(&f.input, col, dataset),
        LogicalPlan::Sort(s) => source_of(&s.input, col, dataset),
        LogicalPlan::Limit(l) => source_of(&l.input, col, dataset),
        LogicalPlan::Distinct(d) => source_of(d.input(), col, dataset),
        // Window outputs pass the input columns through ahead of the
        // window expressions; only the pass-through half descends.
        LogicalPlan::Window(w) => {
            w.input.schema().index_of_column(col).ok()?;
            source_of(&w.input, col, dataset)
        }
        // Group keys lead the output schema in group-expression order;
        // an index past them names an aggregate, which descends from
        // no single column.
        LogicalPlan::Aggregate(a) => follow(&a.input, a.group_expr.get(index(plan)?)?, dataset),
        LogicalPlan::Join(j) => {
            if j.left.schema().index_of_column(col).is_ok() {
                source_of(&j.left, col, dataset)
            } else {
                source_of(&j.right, col, dataset)
            }
        }
        LogicalPlan::TableScan(t) => {
            let table = t.table_name.table();
            match t.table_name.schema() {
                Some(q) if q != dataset => None,
                _ => Some(format!("{table}.{}", col.name)),
            }
        }
        _ => None,
    }
}

fn follow(input: &LogicalPlan, expr: &Expr, dataset: &str) -> Option<String> {
    match expr {
        Expr::Alias(a) => follow(input, &a.expr, dataset),
        Expr::Column(c) => source_of(input, c, dataset),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Date32Array, Float64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let lines = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("date", DataType::Date32, false),
                Field::new("amount", DataType::Float64, false),
                Field::new("region", DataType::Utf8, false),
                Field::new("customer", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Date32Array::from(vec![19723])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["r1"])),
                Arc::new(StringArray::from(vec!["c1"])),
            ],
        )
        .unwrap();
        let customers = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("segment", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["c1"])),
                Arc::new(StringArray::from(vec!["A"])),
            ],
        )
        .unwrap();
        for (name, batch) in [("lines", lines), ("customers", customers)] {
            let schema = batch.schema();
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        }
        ctx
    }

    async fn subjects(sql: &str) -> std::collections::HashMap<String, String> {
        let plan = ctx().await.state().create_logical_plan(sql).await.unwrap();
        super::served_subjects(&plan, "fin")
    }

    #[tokio::test]
    async fn a_plain_projection_maps_each_field_to_its_column() {
        let map = subjects("SELECT date, amount AS value, region FROM lines").await;
        assert_eq!(map.get("date").unwrap(), "lines.date");
        assert_eq!(map.get("value").unwrap(), "lines.amount");
        assert_eq!(map.get("region").unwrap(), "lines.region");
    }

    #[tokio::test]
    async fn aliases_joins_and_subqueries_walk_to_the_scanned_table() {
        let map = subjects(
            "SELECT l.date, l.amount AS value, c.segment \
             FROM lines l JOIN (SELECT id, segment FROM customers) c \
             ON l.customer = c.id",
        )
        .await;
        assert_eq!(map.get("date").unwrap(), "lines.date");
        assert_eq!(map.get("value").unwrap(), "lines.amount");
        assert_eq!(map.get("segment").unwrap(), "customers.segment");
    }

    #[tokio::test]
    async fn a_computed_field_descends_from_nothing() {
        let map = subjects(
            "SELECT date, amount * 2 AS value, upper(region) AS region FROM lines",
        )
        .await;
        assert_eq!(map.get("date").unwrap(), "lines.date");
        assert!(!map.contains_key("value"));
        assert!(!map.contains_key("region"));
    }

    #[tokio::test]
    async fn group_keys_descend_and_aggregates_do_not() {
        let map = subjects(
            "SELECT region, sum(amount) AS value FROM lines GROUP BY region",
        )
        .await;
        assert_eq!(map.get("region").unwrap(), "lines.region");
        assert!(!map.contains_key("value"));
    }
}
