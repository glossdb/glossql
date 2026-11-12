# inputs

A row is one input a stored result read, at the version it read.
`SELECT f() FROM asset` writes the rows with the result, in the same
transaction. Key: the result. A row carries its result's
`begin_snapshot` and ends when its result does. A result is current
while each of its inputs still has the version stored here.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced the result; NULL while the result is current. The view leaves this column out. |
| `input` | VARCHAR | What was read: a lake table or view as `dataset.table`; a catalog table as `main.<name>`; one function's results as `main.measurements/<function>`; `<dataset>.*` for the dataset's every table; `<dataset>.**` where the reads could not be named; `main.measurements/~<function>` for every other function's results. |
| `version` | VARCHAR | The input's version at the read: the last lake snapshot that changed it; for a set of tables, a digest of their names and versions. NULL when the input was not there. |
