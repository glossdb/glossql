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
| `lineage` | VARCHAR | Per column of the load query, the source columns it is computed from and their types as the source delivered them, read from the load query's plan: `{"amount": {"inputs": ["read_csv.amount"], "types": ["BIGINT"]}}`. A source column is `scan.column`, a table function's scan named by the function. `{}` when the plan cannot be walked. Under the same load query, a column whose inputs or types differ from the load before's makes its annotations stale. The view serves it as JSON. |
| `snapshot_id` | BIGINT | The lake snapshot the load created. |
| `imported_at` | TIMESTAMP | When the row was written. |
| `source_rows` | BIGINT | The rows the load query's result carried. A keyed load's result carries each key once, so these are the keys the load carried. |
| `changed_rows` | BIGINT | A merge's inserted and updated rows; a row the result carries unchanged is not written. NULL for a replace. |
| `fingerprint` | VARCHAR | Of a replace of 100,000 rows or more: the sum of the hashes of the table's rows. NULL otherwise. |
| `identical` | BOOLEAN | Of a replace with a fingerprint: whether the table holds as many rows as after the load before and the same fingerprint. NULL where either load has none. Two tables that differ and sum alike read as identical. |
