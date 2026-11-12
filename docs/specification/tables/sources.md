# sources

A row is a declared source: a directory or URL prefix of files, or a
database DuckDB has attached. `DECLARE SOURCE` writes it. Key: `name`.
A source is also an asset: properties declared `ON SOURCE` are
annotated on its name.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `name` | VARCHAR | The source's name. |
| `settings` | VARCHAR | The `SET` pairs as one JSON object: `type`, `location`, `via`. Checked at parse against the statement's schema. The view serves it as JSON. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
