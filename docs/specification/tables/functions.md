# functions

A row is a declared function: one SQL query. `DECLARE FUNCTION` writes
it. Key: `name`, `scope`. A function that returns an aspect is a
measurement, also created as a table macro in its scope's schema; one
without `RETURNS` is a detector, run by the tests that name it.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `name` | VARCHAR | The function's name. |
| `scope` | VARCHAR | A dataset's name, or `GLOBAL` for every dataset (its macro lives in `main`). |
| `script` | VARCHAR | The SQL body. A measurement reads its asset as `$subject`; a detector reads its test's `annotations` and `$threshold`. |
| `returns` | VARCHAR | The aspect a measurement's result is validated against and stored under; NULL for a detector. |
| `reads` | VARCHAR | What the body reads, taken from its text at the declaration, macros followed, as a JSON list (the view serves it as JSON), one object per read, `kind` (`read` a read function, `data` a table or the subject's data, `catalog` the catalog reached directly, `other`), `read`, `what`, `where`, `constant` (false where a read function's what or where is not a constant), `via` (the macros it was reached through). |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
