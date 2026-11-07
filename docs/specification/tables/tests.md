# tests

A row is a declared test on an aspect (`TEST` in the grammar).
`DECLARE TEST` writes it. Key: `aspect`; one test per aspect.
`TESTS()` runs the test's detector when it is read.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `name` | VARCHAR | The test's name. |
| `aspect` | VARCHAR | The aspect it tests. |
| `actors` | VARCHAR | The kinds of actor that may annotate the aspect (`AGENT`, `HUMAN`), as a JSON list; empty when the statement has no `BY`, as on a measurement aspect, where `BY` is refused. The view serves it as a list. |
| `detector` | VARCHAR | The function that scores agreement among the aspect's values; NULL when the test only gates who may annotate. |
| `threshold` | DOUBLE | The score, from 0 to 1, above which differing values read `contested`. |
| `declared_at` | TIMESTAMP | When the row was written. |
