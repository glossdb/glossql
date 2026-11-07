# kits

A row is one load of a kit. `glossql_kit(path)` writes it after running
the kit's statements. No key. When the newest row for a kit carries the
same `hash` as the kit's text, the kit is current: loading it again runs
only its `LOAD` lines and writes no row.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `name` | VARCHAR | The kit's name. |
| `location` | VARCHAR | The path or URL it was loaded from. |
| `statements` | INTEGER | How many statements it ran. |
| `loaded_at` | TIMESTAMP | When the row was written. |
| `hash` | VARCHAR | The hash of the kit's text. |
