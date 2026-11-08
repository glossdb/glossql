# imports

A row is one load of a table. `DECLARE RECIPE` and `IMPORT` write it,
right after the lake's own commit of the table. No key: the table is the
history of every load.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `dataset` | VARCHAR | The dataset of the loaded table. |
| `table_name` | VARCHAR | The loaded table. |
| `mode` | VARCHAR | `replace` (`CREATE OR REPLACE TABLE … AS`) or `merge` (`MERGE INTO` by the load query's key). |
| `rows` | BIGINT | The table's row count after the load. |
| `dropped_rows_count` | BIGINT | The CSV reader's stored rejects when the load stored them (`store_rejects`); NULL when the load query ignores errors without storing them (`ignore_errors`); 0 otherwise. |
| `lineage` | VARCHAR | Each output column of the load query resolved to the source columns it reads, as JSON from `glossql_lineage`. `{}` when the plan cannot be walked. Comparing two loads' lineage shows what the source changed. The view serves it as JSON. |
| `snapshot_id` | BIGINT | The lake snapshot the load created. |
| `imported_at` | TIMESTAMP | When the row was written. |
