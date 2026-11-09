# datasets

A row is a declared dataset. `DECLARE DATASET` writes it and creates the
dataset's schema in the lake. Key: `name`; a new row with the same name
replaces the settings.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `name` | VARCHAR | The dataset's name, which is also its schema in the lake. |
| `settings` | VARCHAR | The `SET` pairs as one JSON object, such as `purpose`. The view serves it as JSON. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
