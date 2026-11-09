# recipes

A row is a declared load query: the SQL that builds one table of a
dataset from a source. `DECLARE RECIPE` writes it after loading the
table once; `IMPORT` runs it again. Key: `dataset`, `table_name`.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `dataset` | VARCHAR | The dataset the table belongs to. |
| `table_name` | VARCHAR | The table the query builds. |
| `source` | VARCHAR | The source the query reads. |
| `settings` | VARCHAR | The `SET` pairs as one JSON object, such as `key`: with a key a load merges by it, without one it replaces the table. The view serves it as JSON. |
| `sql` | VARCHAR | The load query. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
