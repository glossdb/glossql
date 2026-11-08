# measurements

A row is one result of a measurement on an asset. `SELECT f() FROM
asset` writes it when the function's inputs changed since its last
result; otherwise the stored row is served. No key: `GLOSSARY()` reads
the newest row per asset, aspect and function.

| Column | Type | Description |
|---|---|---|
| `seq` | BIGINT | The row's order in the table, from 1. |
| `dataset` | VARCHAR | The dataset the asset belongs to. |
| `function` | VARCHAR | The measurement that computed it. |
| `subject` | VARCHAR | The asset it measured. |
| `aspect` | VARCHAR | The aspect the function returns. |
| `pin` | VARCHAR | Every input at the version read: a lake table at the last snapshot that wrote it, a catalog table at its highest `seq`. The result reads stale once an input moved. |
| `value` | VARCHAR | The result, validated against the aspect's schema. The view serves it as JSON. |
| `computed_at` | TIMESTAMP | When the row was written. |
| `reads` | VARCHAR | What the function's plan scans: lake tables as `dataset.table`, catalog tables as `glossql.<name>`, `**` for a scan that cannot be named (a file, another database). |
