# glossary

A row is one annotation: one actor's value for an aspect on an asset.
`GLOSS` writes it after validating the value against the aspect's
schema. `DELETE FROM glossary` removes rows. No key: the table is the
history; `GLOSSARY()` reads the newest row per asset, aspect and kind of
actor, a person's over an agent's.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `dataset` | VARCHAR | The dataset the asset belongs to. |
| `subject` | VARCHAR | The asset: `dataset`, `dataset.table`, `dataset.table.column`, a relationship path, or a source's name. |
| `aspect` | VARCHAR | The aspect the value is for. |
| `actor_kind` | VARCHAR | `agent` or `human`, from the connection's `glossql_actor` variable. |
| `actor_id` | VARCHAR | Who wrote it, from `glossql_actor_id`; NULL when the connection names nobody. |
| `body` | VARCHAR | The value, a JSON document. For a `QUERY` aspect, the metric definition: `sql` or `stopped`, and its `assumptions`. The view serves it as JSON. |
| `snapshot_id` | BIGINT | The lake snapshot at the write, for a table or column asset; NULL for a dataset, a relationship or a source. A value reads `stale` once the data under it changed after this snapshot. |
| `written_at` | TIMESTAMP | When the row was written. |
