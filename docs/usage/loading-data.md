# Loading data

You load data in three steps: name where it comes from (a source),
preview the query that reads it (`PROBE`), then declare the load query
(a recipe), which creates the table. `IMPORT` runs it again later.

## Declare a source

```sql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
```

| Setting | Meaning |
|---|---|
| `type` | `relational_db`, `parquet`, `csv` or `json`; anything else is refused |
| `location` | a URL; for a file source, the directory the load query's paths resolve under |
| `via` | the name of a DuckDB secret (`CREATE SECRET`); credentials never appear in a statement |

A relational source is a database DuckDB has attached (`postgres`,
`mysql`, `sqlite`, `duckdb`). The load query runs over it in DuckDB SQL;
pushdown is the scanner's.

## Preview the load query

`PROBE` runs a query at the source and loads nothing. The result
always carries its schema, so `LIMIT 0` shows exactly the columns and
types the table will get.

```sql
PROBE erp_export AS $$SELECT id, amount FROM read_parquet('orders/*.parquet') LIMIT 0$$;
```

## Declare the load query

```sql
DECLARE RECIPE orders ON fin FROM erp_export SET (key: id)
  AS $$SELECT id::INTEGER AS id, amount::DOUBLE AS amount FROM read_parquet('orders/*.parquet')$$;
```

The declaration runs the query and loads the table `fin.orders`.

- **The query carries the casts.** Typing is yours; a failed `try_cast`
  stores NULL.
- **Name the columns.** A `SELECT *` passes a changed source schema
  through and fails later, far from the cause.
- **Keyed or not.** With `SET (key: id)` (a column, or a comma-separated
  list), every load merges into the table by key: rows whose key the
  result carries are replaced, the rest stay. A result that carries a
  key twice is refused. Without a key, every load replaces the table.
- **Re-declaring** with the same SQL does nothing. Changed SQL replaces
  the table and its recipe. Values recorded on its columns stay; one on
  a column the new query derives differently reads `stale`
  ([lineage](lineage.md)).

## Load again

```sql
IMPORT orders;
```

`IMPORT` runs the table's load query again over the source as it is
now. Nothing watches a source: a schedule or an agent sends `IMPORT`.

## What every load records

Every load is one DuckLake snapshot, so the table's history is the
lake's:

```sql
SELECT * FROM fin.orders AT (VERSION => 3);
```

Each load also adds a row to the `imports` table: dataset, table, mode
(`replace` or `merge`), row count, `dropped_rows_count`, the column
lineage, the snapshot and the time. `dropped_rows_count` is the CSV
reader's rejected rows when the query stores them (`store_rejects`),
NULL when it ignores errors without storing them, and 0 otherwise.

Normative text: [SPEC.md §3](../../SPEC.md#3-sources-and-datasets).
