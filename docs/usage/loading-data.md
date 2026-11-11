# Loading data

You load data in four steps: declare a dataset, name where the data
comes from (a source), preview the query that reads it (`PREVIEW`),
then declare the load query (`DECLARE RECIPE`), which creates the
table. `IMPORT` runs the load query again later.

## Declare a dataset

A dataset is a schema of the lake: the tables for one topic.

```sql
DECLARE DATASET logistics SET (purpose: 'delivery times per depot');
USE logistics;
```

`USE` is DuckDB's own. Under it, unprefixed paths resolve against the
dataset: `deliveries` is `logistics.deliveries`, `deliveries.weight_kg`
is `logistics.deliveries.weight_kg`. The full `dataset.table.column`
path works from anywhere. In a two-part path, a table of the dataset
in use comes before a dataset of the same name.

## Declare a source

```sql
DECLARE SOURCE depot_files SET (type: parquet, location: 'lake/depots');
DECLARE SOURCE crm SET (type: relational_db, location: 'crm_db', via: crm_secret);
```

| Setting | Meaning |
|---|---|
| `type` | required. `relational_db` is a database DuckDB has attached. Any other word (`parquet`, `csv`, `json`, …) is a file source |
| `location` | required. For a file source, the directory or URL prefix that relative paths in the load query resolve under. For `relational_db`, the attached database's name |
| `via` | the name of a DuckDB secret (`CREATE SECRET`). It is stored with the source; credentials do not appear in a statement |

For a `relational_db` source, `ATTACH` the database first (postgres,
mysql, sqlite, duckdb). The load query runs over it in DuckDB SQL.

## Preview the load query

`PREVIEW` runs a query at the source and loads nothing. The result
carries its schema, so `LIMIT 0` shows the columns and types the table
would get.

```sql
PREVIEW depot_files AS $$SELECT id, weight_kg FROM read_parquet('deliveries/*.parquet') LIMIT 0$$;
```

## Declare the load query

```sql
DECLARE RECIPE deliveries ON logistics FROM depot_files SET (key: id)
  AS $$SELECT id::INTEGER AS id, depot, delivered_at::TIMESTAMP AS delivered_at, weight_kg::DOUBLE AS weight_kg
       FROM read_parquet('deliveries/*.parquet')$$;
```

The statement runs the query and creates the table
`logistics.deliveries`. It answers
`loaded logistics.deliveries: 1200 rows, 0 dropped (new)`.

- **The query carries the casts.** The table gets the types the query
  returns.
- **Name the columns.** A `SELECT *` passes a changed source schema
  through to the table.
- **Keyed or not.** With `SET (key: id)` (one column, or a list such as
  `'depot, id'`), a later load merges into the table by key: rows whose
  key the result carries are replaced, the rest stay. A result that
  carries a key twice is refused. Without a key, every load replaces
  the table.
- **Declaring it again** with the same SQL does nothing. Changed SQL
  loads the table again from the new query. Annotations on its columns
  stay; one on a column the new query derives differently reads `stale`
  ([reading](reading.md#when-a-value-is-stale)).
- A table and a view of one dataset cannot share a name.

## Load again

```sql
IMPORT deliveries;
```

`IMPORT` runs the table's load query over the source as it is now. It
answers `imported logistics.deliveries: 1250 rows, 0 dropped (merged by id)`, or
`(replaced)` without a key. The extension does not watch a source: a
schedule or an agent sends `IMPORT`.

## What a load stores

Every load is one DuckLake snapshot, so the table's history is the
lake's:

```sql
SELECT * FROM logistics.deliveries AT (VERSION => 3);
```

Each load also adds a row to the [imports](../specification/tables/imports.md)
table: dataset, table, mode (`replace` or `merge`), row count,
`dropped_rows_count`, the column lineage, the snapshot and the time.
`dropped_rows_count` is the number of lines the CSV reader rejected
when the query stores them (`store_rejects`), NULL when the query
ignores errors without storing them (`ignore_errors`), and 0
otherwise.

Normative text: [SPEC.md §3](../../SPEC.md#3-sources-and-datasets).
