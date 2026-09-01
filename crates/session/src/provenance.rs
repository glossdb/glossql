//! Served-column provenance: which dataset table column a metric's
//! served field descends from. The cube's judged reads key verdicts by
//! subject (`table.column`), but a served frame names aliases — so the
//! walk follows a plain column reference down the logical plan to the
//! table scan that serves it. A computed field (an aggregate, an
//! expression, a union arm) descends from no single column and stays
//! unmapped; under judged admission an unmapped field is a gap, never
//! a candidate. The one exception is the verb's: `summed_source` steps
//! through a single `sum` for the served value alone, so a metric's
//! stock/flow can be read off the column it sums without an aggregate
//! ever becoming a dimension candidate.

use std::collections::{HashMap, HashSet};

use datafusion::common::Column;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan};

/// The dataset tables a plan scans, by name — every scan whose
/// reference is unqualified or qualified by the bound dataset.
pub(crate) fn scanned_tables(plan: &LogicalPlan, dataset: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    // The visitor never fails; the Result is the trait's shape.
    let _ = plan.apply(|p| {
        if let LogicalPlan::TableScan(t) = p
            && t.table_name.schema().is_none_or(|q| q == dataset)
        {
            out.insert(t.table_name.table().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// The source subject (`table.column`) of each served field, keyed by
/// served name. `dataset` guards the terminal: a scan whose reference
/// carries a qualifier is only a dataset table when that qualifier is
/// the bound dataset — the `read.<aspect>` door scans under a
/// `read`-qualified name and must not mint a subject.
pub(crate) fn served_subjects(plan: &LogicalPlan, dataset: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (qualifier, field) in plan.schema().iter() {
        let col = Column::new(qualifier.cloned(), field.name());
        if let Some(subject) = source_of(plan, &col, dataset, false) {
            out.insert(field.name().clone(), subject);
        }
    }
    out
}

/// The column a served field is, or is one `sum` of — the verb's
/// descent, and only the verb's. `sum` alone, because the cube folds
/// by summing (a flow per period, a stock at its last standing), so
/// `sum(x)` is the one aggregate that behaves as `x` does; a plain
/// `sum` — no `DISTINCT`, no `FILTER` — over one column-bearing
/// argument, once on the path. Anything else descends from nothing
/// and the verb falls to its default.
pub(crate) fn summed_source(plan: &LogicalPlan, field: &str, dataset: &str) -> Option<String> {
    let (qualifier, f) = plan.schema().iter().find(|(_, f)| f.name() == field)?;
    source_of(
        plan,
        &Column::new(qualifier.cloned(), f.name()),
        dataset,
        true,
    )
}

/// `summed` is the verb's descent: past an aggregate's group keys the
/// walk may step through one `sum`; admission never sets it.
fn source_of(plan: &LogicalPlan, col: &Column, dataset: &str, summed: bool) -> Option<String> {
    let index = |p: &LogicalPlan| p.schema().index_of_column(col).ok();
    match plan {
        LogicalPlan::Projection(p) => follow(&p.input, &p.expr[index(plan)?], dataset, summed),
        LogicalPlan::SubqueryAlias(a) => {
            let (qualifier, field) = a.input.schema().qualified_field(index(plan)?);
            source_of(
                &a.input,
                &Column::new(qualifier.cloned(), field.name()),
                dataset,
                summed,
            )
        }
        LogicalPlan::Filter(f) => source_of(&f.input, col, dataset, summed),
        LogicalPlan::Sort(s) => source_of(&s.input, col, dataset, summed),
        LogicalPlan::Limit(l) => source_of(&l.input, col, dataset, summed),
        LogicalPlan::Distinct(d) => source_of(d.input(), col, dataset, summed),
        // Window outputs pass the input columns through ahead of the
        // window expressions; only the pass-through half descends.
        LogicalPlan::Window(w) => {
            w.input.schema().index_of_column(col).ok()?;
            source_of(&w.input, col, dataset, summed)
        }
        // Group keys lead the output schema in group-expression order;
        // an index past them names an aggregate, which descends from
        // no single column — except one `sum`, on the verb's descent.
        LogicalPlan::Aggregate(a) => {
            let i = index(plan)?;
            match a.group_expr.get(i) {
                Some(key) => follow(&a.input, key, dataset, summed),
                None if summed => {
                    let arg = summed_arg(a.aggr_expr.get(i - a.group_expr.len())?)?;
                    follow(&a.input, arg, dataset, false)
                }
                None => None,
            }
        }
        LogicalPlan::Join(j) => {
            if j.left.schema().index_of_column(col).is_ok() {
                source_of(&j.left, col, dataset, summed)
            } else {
                source_of(&j.right, col, dataset, summed)
            }
        }
        LogicalPlan::TableScan(t) => {
            let table = t.table_name.table();
            match t.table_name.schema() {
                Some(q) if q != dataset => None,
                _ => Some(format!("{table}.{}", col.name)),
            }
        }
        // A union serves each column by position: the walk descends it
        // through every input and answers only where all of them land
        // on the same subject — the composed shape `read.a() UNION ALL
        // read.b()` expands to scans under the union, so the shared
        // axis survives it. Arms landing on different columns stay
        // None, and the grounding abstains as before.
        LogicalPlan::Union(u) => {
            let i = index(plan)?;
            let mut sources = u.inputs.iter().map(|input| {
                let (qualifier, field) = input.schema().qualified_field(i);
                source_of(
                    input,
                    &Column::new(qualifier.cloned(), field.name()),
                    dataset,
                    summed,
                )
            });
            let first = sources.next()??;
            sources
                .all(|s| s.as_deref() == Some(first.as_str()))
                .then_some(first)
        }
        _ => None,
    }
}

/// A bare column is its own source, and so is a *call* over exactly one
/// column-bearing argument: `date_trunc('month', posted_at)` and
/// `CAST(posted_at AS TIMESTAMP)` are the posting date's axis, bucketed
/// or retyped. Literal arguments are parameters, never sources.
///
/// Arithmetic is deliberately not a call: `amount * 2` and `a + b`
/// descend from nothing, because a computed number is not the column it
/// was computed from. Aggregates are not reached here — the `Aggregate`
/// arm follows group expressions, and steps through one `sum` only on
/// the verb's descent (`summed_source`).
fn follow(input: &LogicalPlan, expr: &Expr, dataset: &str, summed: bool) -> Option<String> {
    match expr {
        Expr::Alias(a) => follow(input, &a.expr, dataset, summed),
        Expr::Column(c) => source_of(input, c, dataset, summed),
        Expr::Cast(c) => follow(input, &c.expr, dataset, summed),
        Expr::TryCast(c) => follow(input, &c.expr, dataset, summed),
        Expr::ScalarFunction(f) => {
            let mut bearing = f.args.iter().filter(|a| !a.column_refs().is_empty());
            match (bearing.next(), bearing.next()) {
                (Some(arg), None) => follow(input, arg, dataset, summed),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The one column-bearing argument of a plain `sum`, or nothing.
fn summed_arg(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Alias(a) => summed_arg(&a.expr),
        Expr::AggregateFunction(f)
            if f.func.name() == "sum" && !f.params.distinct && f.params.filter.is_none() =>
        {
            let mut bearing = f.params.args.iter().filter(|a| !a.column_refs().is_empty());
            match (bearing.next(), bearing.next()) {
                (Some(arg), None) => Some(arg),
                _ => None,
            }
        }
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
        let map = subjects("SELECT date, amount * 2 AS value FROM lines").await;
        assert_eq!(map.get("date").unwrap(), "lines.date");
        assert!(!map.contains_key("value"));
    }

    #[tokio::test]
    async fn a_call_over_one_column_is_that_column() {
        let map = subjects(
            "SELECT date_trunc('month', date) AS period, upper(region) AS region,                     concat(region, customer) AS pair \
             FROM lines",
        )
        .await;
        // The bucketed axis is still the date column — without this a
        // grounding that buckets its own time has no judged verdict.
        assert_eq!(map.get("period").unwrap(), "lines.date");
        assert_eq!(map.get("region").unwrap(), "lines.region");
        // Two column-bearing arguments name no single source.
        assert!(!map.contains_key("pair"));
    }

    #[tokio::test]
    async fn group_keys_descend_and_aggregates_do_not() {
        let map = subjects("SELECT region, sum(amount) AS value FROM lines GROUP BY region").await;
        assert_eq!(map.get("region").unwrap(), "lines.region");
        assert!(!map.contains_key("value"));
    }

    async fn summed(sql: &str) -> Option<String> {
        let plan = ctx().await.state().create_logical_plan(sql).await.unwrap();
        super::summed_source(&plan, "value", "fin")
    }

    #[tokio::test]
    async fn the_verbs_descent_steps_through_one_plain_sum_and_nothing_else() {
        // A column, and one sum of it, name the column.
        assert_eq!(
            summed("SELECT date, amount AS value FROM lines")
                .await
                .as_deref(),
            Some("lines.amount")
        );
        assert_eq!(
            summed("SELECT date, sum(amount) AS value FROM lines GROUP BY date")
                .await
                .as_deref(),
            Some("lines.amount")
        );
        assert_eq!(
            summed("SELECT date, sum(CAST(amount AS DOUBLE)) AS value FROM lines GROUP BY date")
                .await
                .as_deref(),
            Some("lines.amount")
        );
        // Any other shape descends from nothing: a different aggregate, a
        // distinct sum, arithmetic under the sum, two sums on the path.
        for sql in [
            "SELECT date, count(*) AS value FROM lines GROUP BY date",
            "SELECT date, max(amount) AS value FROM lines GROUP BY date",
            "SELECT date, sum(DISTINCT amount) AS value FROM lines GROUP BY date",
            "SELECT date, sum(amount * 2) AS value FROM lines GROUP BY date",
            "SELECT sum(v) AS value FROM (SELECT date, sum(amount) AS v FROM lines GROUP BY date)",
        ] {
            assert_eq!(summed(sql).await, None, "{sql}");
        }
    }
}
