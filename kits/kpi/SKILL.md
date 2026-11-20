---
name: glossql-kpi
description: The kpi kit's properties for a glossql workspace — what each one means, the order of the work, how a metric is defined as a view. Use after glossql_kit('kits/kpi/kit.sql') when annotating columns, tables, sources and metrics.
---

# The kpi kit

For an agent working in a DuckDB that has the glossql extension loaded
and this kit. The kit declares the semantic model's properties, two
measurements that profile a column, a detector, and a test per
property. Read the tables, the declarations and the glossary from the
workspace; this page says what the words mean.

## Load and check

```sql
FROM glossql_kit('kits/kpi/kit.sql');     -- also in the opener, so every session has the kit
SELECT * FROM kits;
SELECT name, kind, grains FROM properties;
```

## The properties

| Property | On | Body | Says |
|---|---|---|---|
| `meaning` | table, column, relationship | `value`: a sentence; `term`: the business term | what it is |
| `role` | column | `value`: `key`, `measure`, `dimension`, `timestamp`, `attribute` | what kind of column it is. Annotate it first: the properties below are required by role |
| `additivity` | measure columns | `value`: `additive` (a flow: sums over time and members), `semi-additive` (a stock: sums over members, not over time), `ratio`, `non-additive`; `numerator` and `denominator` for a ratio; `basis` | how the number combines over periods and members |
| `unit` | measure columns | `value`: the unit; `source_column`: the column that holds each row's unit | the unit of measure |
| `dimension` | dimension columns | `value`: `primary`, `supporting`, `none`; `basis` | whether reports slice by it |
| `entity` | table | `value`: what one row is; `role`: `fact` or `dimension`; `grain`; `time_axis`; `identity_columns` | the table's grain and key |
| `conventions` | source | any object | fiscal calendar, sign conventions, what nulls mean |

The measurements: `column_profile` (`total`, `null_ratio`, `distinct`,
`min`, `max`) and `outlier_profile`, both on a column.

The tests: every property takes annotations from an agent and a
person. `role`, `additivity` and `unit` are scored by `value_entropy`
with threshold 0.7: where the agent's and the person's values differ,
the asset reads `conflict` and its value is withheld. `meaning`,
`entity` and `dimension` have no detector.

## The order of the work

1. Read what is missing:
   `SELECT * FROM GLOSSARY(orders) WHERE state <> 'current';`
2. Annotate `role` on every column first. `additivity` and `unit` are
   required only on measures, `dimension` only on dimensions.
3. Measure before you annotate. `SELECT column_profile() FROM
   orders.amount;` and `SELECT outlier_profile() FROM orders.amount;`
   store results that `GLOSSARY()` returns beside the annotations. Name
   them in `basis`. With the quality kit loaded and the relationships
   declared, `SELECT additivity_evidence() FROM orders.amount;` says
   whether a column looks like a stock or a flow.
4. Annotate with a basis: `GLOSS additivity ON orders.amount AS
   $${"value": "additive", "basis": "sums over orders, not a
   balance"}$$;`. You write as `agent`. A person's review decision is
   written as `human`, on the person's own connection. Do not set
   `human` yourself.
5. Run the test and read its results: `SELECT role_test() FROM
   orders.amount;`, then `SELECT * FROM TESTS(orders.amount::role);`. A
   `red` state is a disagreement. Resolve it with the person; do not
   overwrite it.

## Metrics

A metric is a `VIEW` property, defined by an annotation on the
dataset. Its SQL is served as a view under the metric's name. The view
is a table of the dataset, so its columns take this kit's properties
like a loaded table's columns. That is where a reader of the numbers
finds what it needs.

```sql
GLOSS revenue ON shop AS $${
  "kind": "metric",
  "sql": "SELECT month AS period, customer, sum(amount) AS value FROM shop.orders GROUP BY ALL",
  "assumptions": [{"dimension": "scope", "assumption": "amounts are net of tax",
                   "basis": "column_profile", "confidence": 0.9}]
}$$;
GLOSS role ON revenue.period AS $${"value": "timestamp"}$$;
GLOSS role ON revenue.value AS $${"value": "measure"}$$;
GLOSS additivity ON revenue.value AS $${"value": "additive"}$$;
GLOSS role ON revenue.customer AS $${"value": "dimension"}$$;
FROM shop.revenue;
```

The body:

- `sql`, or `stopped`. `stopped` says that you serve no number, and
  why: what is missing and what would fix it.
- `assumptions`: each with its `assumption`, and `dimension`, `basis`
  and `confidence` (0 to 1) when you have them.

On the view's columns:

- `role` says which column is the period and which the measure.
- `additivity` on the measure says how it combines: a flow sums, a
  stock takes the period's last level, a ratio divides the summed
  columns that `numerator` and `denominator` name.
- `dimension` says whether the metric is sliced by a column.
- `entity` on the view says what one row is.

Annotating again replaces your own value. A person's value is kept
beside it. Put what the company revises (a metric's meaning, unit)
into annotations on the view and its columns: the metric's
declaration cannot change once it is annotated.

## Limits

The kit stores the annotations. It has no function that reads a
metric's series from the view.
