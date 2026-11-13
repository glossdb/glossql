# measurements

A row is one result of a measurement on an asset. `SELECT f() FROM
asset` writes it when the function's inputs changed since its last
result; otherwise the stored row is served. A computation that fails
is a row too, with its error and no value: the call fails with that
error until an input changes. Key: `dataset`,
`function`, `subject`: the view serves the latest result, and
`measurements_history` serves every result. What a result read is in
[inputs](inputs.md).

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `dataset` | VARCHAR | The dataset the asset belongs to. |
| `function` | VARCHAR | The measurement that computed it. |
| `subject` | VARCHAR | The asset it measured. |
| `aspect` | VARCHAR | The aspect the function returns. |
| `value` | VARCHAR | The result, validated against the aspect's schema. The view serves it as JSON. NULL where the computation failed. |
| `computed_at` | TIMESTAMP | When the row was written. |
| `error` | VARCHAR | Why the computation failed: the function's error, or the schema check's. NULL for a value. |
