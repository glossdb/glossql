# kits

A row is one load of a kit. `glossql_kit(path)` writes it after running
the kit's statements. Key: `name`. When a kit's row carries the
same `hash` as the kit's text, the kit is current: loading it again runs
only its `LOAD` lines and writes no row.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `name` | VARCHAR | The kit's name. |
| `location` | VARCHAR | The path or URL it was loaded from. |
| `statements` | INTEGER | How many statements it ran. |
| `loaded_at` | TIMESTAMP | When the row was written. |
| `hash` | VARCHAR | The hash of the kit's text. |
