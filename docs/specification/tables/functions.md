# functions

A row is a declared function: one SQL query. `DECLARE FUNCTION` writes
it. Key: `name`, `scope`. A function that returns an aspect is a
measurement, also created as a table macro in its scope's schema; one
without `RETURNS` is a detector, run by the tests that name it.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `name` | VARCHAR | The function's name. |
| `scope` | VARCHAR | A dataset's name, or `GLOBAL` for every dataset (its macro lives in `main`). |
| `script` | VARCHAR | The SQL body. A measurement reads its asset as `$subject`; a detector reads its test's `slots` and `$threshold`. |
| `returns` | VARCHAR | The aspect a measurement's result is validated against and stored under; NULL for a detector. |
| `declared_at` | TIMESTAMP | When the row was written. |
