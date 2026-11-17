# tests

A row is a declared test on an aspect.
`DECLARE TEST` writes it. Key: `aspect`; one test per aspect.
`SELECT <test>() FROM <subject>` stores its results in [measurements](measurements.md), one per subject, and `TESTS()` reads them.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `name` | VARCHAR | The test's name. |
| `aspect` | VARCHAR | The aspect it tests. |
| `actors` | VARCHAR | The kinds of actor that may annotate the aspect (`AGENT`, `HUMAN`), as a JSON list; empty when the statement has no `BY`, as on a measurement aspect, where `BY` is refused. The view serves it as a list. |
| `detector` | VARCHAR | The function that scores agreement among the aspect's values; NULL when the test only says who may annotate. |
| `threshold` | DOUBLE | The score, from 0 to 1, above which differing values read `conflict`. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
