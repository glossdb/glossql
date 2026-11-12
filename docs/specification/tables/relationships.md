# relationships

A row is a declared key between columns. `DECLARE RELATIONSHIP` writes
it. No key; declaring the same relationship again adds nothing. A
relationship is also an asset: properties declared `ON RELATIONSHIP`
are annotated on its path.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `dataset` | VARCHAR | The dataset of the left side. |
| `left_side` | VARCHAR | The referencing column, `dataset.table.column`, or a tuple of columns for a composite key. |
| `op` | VARCHAR | `->` (many to one) or `<->` (one to one). |
| `right_side` | VARCHAR | The referenced column or tuple. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
