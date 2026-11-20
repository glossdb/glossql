# results

A row is one result of a measurement on an asset. `SELECT f() FROM
asset` writes it when no row is stored for the call or the stored row
is out of date; otherwise the stored row is served. A computation that
fails is a row too, with its error and no value; a call computes it
again. A row is current while no node it read changed after the
snapshot it read at. What a result read is in [inputs](inputs.md).
Key: `dataset`, `function`, `subject`: the view serves the latest
result, and `results_history` serves every result.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the write that made the row took. One write may store several results. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced or removed the row; NULL while the row is current. The view leaves this column out. |
| `id` | BIGINT | The result's own id, which its `inputs` and `edges` rows carry. |
| `dataset` | VARCHAR | The dataset the asset belongs to. |
| `function` | VARCHAR | The measurement that computed it; a test's name for a test's result, `lineage` for a view's lineage. |
| `subject` | VARCHAR | The asset it measured. |
| `property` | VARCHAR | The property the function returns; `test` for a test's result, whose value is its `state` and `score` (SPEC §7.2), and `lineage` for a view's lineage. |
| `value` | VARCHAR | The result, validated against the property's schema. The view serves it as JSON. NULL where the computation failed. |
| `computed_at` | TIMESTAMP | When the row was written. |
| `error` | VARCHAR | Why the computation failed: the function's error, or the schema check's. NULL for a value. |
| `read_at` | BIGINT | The lake's newest snapshot when the computation began to read, before its body ran. A node that changed after it makes the row out of date. |
| `changed_at` | BIGINT | The snapshot its value and error last changed at. A computation that returns the value and error of the row it replaces keeps that row's `changed_at`, so a result that read it stays current. |
