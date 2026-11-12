# imports

A row is one load of a table. `DECLARE RECIPE` and `IMPORT` write it,
right after the lake's own commit of the table. Key: `dataset`,
`table_name`: the view serves a table's latest load, and
`imports_history` serves every load.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `dataset` | VARCHAR | The dataset of the loaded table. |
| `table_name` | VARCHAR | The loaded table. |
| `mode` | VARCHAR | `replace` (`CREATE OR REPLACE TABLE … AS`) or `merge` (`MERGE INTO` by the load query's key). |
| `rows` | BIGINT | The table's row count after the load. |
| `dropped_rows_count` | BIGINT | The CSV reader's stored rejects when the load stored them (`store_rejects`); NULL when the load query ignores errors without storing them (`ignore_errors`); 0 otherwise. |
| `lineage` | VARCHAR | Each output column of the load query resolved to the source columns it reads, as JSON from `glossql_lineage`. `{}` when the plan cannot be walked. Comparing two loads' lineage shows what the source changed. The view serves it as JSON. |
| `snapshot_id` | BIGINT | The lake snapshot the load created. |
| `imported_at` | TIMESTAMP | When the row was written. |
