# tests

A row is a declared test on a property.
`DECLARE TEST` writes it. Key: `property`; one test per property.
`SELECT <test>() FROM <subject>` stores its results in [results](results.md), one per subject, and `TESTS()` reads them.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `name` | VARCHAR | The test's name. |
| `property` | VARCHAR | The property it tests. |
| `actors` | VARCHAR | The kinds of actor that may annotate the property (`AGENT`, `HUMAN`), as a JSON list; empty when the statement has no `BY`, as on a measurement property, where `BY` is refused. The view serves it as a list. |
| `detector` | VARCHAR | The function that scores agreement among the property's values; NULL when the test only says who may annotate. |
| `threshold` | DOUBLE | The score, from 0 to 1, above which differing values read `conflict`. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
