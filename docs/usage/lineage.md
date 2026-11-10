# Lineage

glossql records what computes every column: read off the query's bound
plan, down to the source columns and their types. It uses that to mark
values `stale` when the derivation behind a column changes, and you can
read it for any query.

## Lineage of a query

```sql
FROM glossql_lineage($$SELECT id, amount * 2 AS twice, id::VARCHAR AS id_text FROM fin.orders WHERE id > 1$$);
```

| column | derivation | complete |
|---|---|---|
| `id` | `fin.orders.id:BIGINT` | true |
| `twice` | `*(fin.orders.amount:DECIMAL(21,1), cast(2 as DECIMAL(31,0)))` | true |
| `id_text` | `cast(fin.orders.id:BIGINT as VARCHAR)` | true |

A leaf is `scan.column:type`. The text names what a column is derived
from, not how the plan is shaped: two plans that derive a column the
same way read the same. `complete` is false where a part of the plan
could not be resolved.

A fourth column, `shape`, says how a column was computed where that
decides how it adds up: `ratio` for a division by an aggregate
(`sum(clicks) / sum(impressions)`), `average` for an average or a
quantile, `distinct` for a distinct count. A sum over one of them keeps
the shape, since ratios, averages and distinct counts do not add up. A
plain sum, or a division by a constant, has none.

`glossql_views_lineage(dataset)` returns the same for every column of
every view in a dataset, with the dataset and the view's name first.
`glossql_views_lineage(NULL)` reads the views of every dataset.

Lineage names the columns that compute a view's outputs. A table a view
only joins or filters on computes no output column, so it does not
appear there. `glossql_views_scans(dataset)` lists every table each view
scans, one row per view and scan, with the columns the view reads of it.
`glossql_views_joins(dataset)` lists the column pairs each view's joins
equate, and per side whether the view joins it with one row per value
(`left_unique`, `right_unique`): a join to a subquery grouped by the
join columns cannot multiply rows, whatever the table underneath holds.

## Lineage of every load

Each load stores its load query's lineage per column on its `imports`
row. Comparing two rows shows what changed in what the source sends,
for example a column that now arrives under another type.

## When a value goes stale

A value on a column reads `stale` once:

- the column was retyped or dropped, or its table lost a column; or
- the lineage behind it differs between the load current when the value
  was written and the newest load.

A column added, or a filter changed, derives nothing differently and
leaves values current.

Normative text: [SPEC.md §5.2](../../SPEC.md#52-glosses); the DuckDB
mechanism: [internals](../internals/duckdb.md).
