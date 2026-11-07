# aspects

A row is a declared aspect: one property of the harness schema.
`DECLARE ASPECT` writes it. Key: `name`. An aspect cannot be declared
again once anything is annotated on it.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `name` | VARCHAR | The aspect's name. Annotations and functions name it. |
| `kind` | VARCHAR | `FACT` (stated by an actor), `MEASUREMENT` (computed by a function, never annotated) or `VIEW` (SQL, served as a view under the aspect's name). |
| `grains` | VARCHAR | The kinds of asset it applies to, as a JSON list of `DATASET`, `TABLE`, `COLUMN`, `RELATIONSHIP`, `SOURCE`; empty for all. The view serves it as a list. |
| `condition` | VARCHAR | When the aspect is required, as JSON: `{"aspect": …, "value": …}` for a sibling aspect's value (`WHEN role = 'measure'`), or `{"type": [...]}` for column types (`WHEN TYPE IN (…)`). NULL when it is required wherever its grains apply. The view serves it as JSON. |
| `schema` | VARCHAR | The JSON Schema every value is validated against. For a `VIEW` aspect it carries the definition's metadata (`title`, `x-kind`) and the annotation is held to the metric-definition contract instead. The view serves it as JSON. |
| `declared_at` | TIMESTAMP | When the row was written. |
