# inputs

A row is one input a stored result read, at the version it read.
`SELECT f() FROM asset` writes the rows with the result, in the same
transaction. Key: `begin_snapshot` and `input`. A row carries its
result's `begin_snapshot` and ends when its result does. A result is
current while each of its inputs still has the version stored here
and no input that is a stored result is out of date.

`glossql_parts()` serves every input's version now, as `(name,
version, kind)`. Joined on the name, it shows which result is out of
date and which input moved:

```sql
SELECT m.function, m.subject, i.input
FROM measurements m JOIN inputs i USING (begin_snapshot)
LEFT JOIN glossql_parts() p ON p.name = i.input
WHERE p.version IS DISTINCT FROM i.version;
```

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced the result; NULL while the result is current. The view leaves this column out. |
| `input` | VARCHAR | What was read: a lake table or view as `dataset.table`; a catalog table as `main.<name>`; one property's annotations as `main.glossary/<aspect>`; one function's results as `main.measurements/<function>`; the other functions' results of one property in the result's own dataset as `main.measurements@<aspect>~<function>` (a read of another dataset's values names every other function's results); every other function's results as `main.measurements/~<function>`; `<dataset>.*` for the dataset's every table; `<dataset>.**` where the reads could not be named; `lake.shape` for the lake's datasets, columns and views. |
| `version` | VARCHAR | The input's version at the read: the last lake snapshot that changed it; for a set, a digest of its members' names and versions. NULL when the input was not there. |
