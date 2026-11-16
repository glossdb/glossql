# glossary

A row is one annotation: one actor's value for an aspect on an asset.
`GLOSS` writes it after validating the value against the aspect's
schema. `DELETE FROM glossary` ends rows. Key: `subject`, `aspect`,
`actor_kind`; a new annotation ends the one it replaces. `GLOSSARY()`
reads the current rows, a person's over an agent's.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. A value reads `stale` once the data under it changed after this snapshot. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `dataset` | VARCHAR | The dataset the asset belongs to. |
| `subject` | VARCHAR | The asset: `dataset`, `dataset.table`, `dataset.table.column`, a relationship path, or a source's name. |
| `aspect` | VARCHAR | The aspect the value is for. |
| `actor_kind` | VARCHAR | `agent` or `human`, from the connection's `glossql_actor` variable. |
| `actor_id` | VARCHAR | Who wrote it, from `glossql_actor_id`; NULL when the connection names nobody. |
| `body` | VARCHAR | The value, a JSON document. For a `VIEW` aspect, the view's definition: `sql` or `stopped`, and its `assumptions`. The view serves it as JSON. |
| `written_at` | TIMESTAMP | When the row was written. |
