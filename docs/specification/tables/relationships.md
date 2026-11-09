# relationships

A row is a declared key between columns. `DECLARE RELATIONSHIP` writes
it. No key; declaring the same relationship again adds nothing. A
relationship is also an asset: properties declared `ON RELATIONSHIP`
are annotated on its path.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `dataset` | VARCHAR | The dataset of the left side. |
| `left_side` | VARCHAR | The referencing column, `dataset.table.column`, or a tuple of columns for a composite key. |
| `op` | VARCHAR | `->` (many to one) or `<->` (one to one). |
| `right_side` | VARCHAR | The referenced column or tuple. |
| `declared_at` | TIMESTAMP | When the row was written. |
| `declared_by` | VARCHAR | Who wrote the row: `kit:<name>` for a declaration a kit made, else the actor as `<kind>:<id>` (`agent:builder`, `human:ana`), or the kind alone where the connection named no one. |
