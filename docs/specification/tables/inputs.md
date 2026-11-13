# inputs

A row is one input a stored result read, at the version it read.
`SELECT f() FROM asset` writes the rows with the result, in the same
transaction. Key: `begin_snapshot` and `input`. A row carries its
result's `begin_snapshot` and ends when its result does.

A function runs on an asset and reads the glossary, the results and
the declarations within the asset's closure: for a view, the view, its
definition, and the tables and columns it scans and joins on; for a
loaded table or column, the tables that declared relationships connect
it to; for a dataset, everything in it (SPEC §6). An input that names
rows of the closure stores a digest of what those rows held. A result
is current while each input still has the version stored here and no
result it read within its closure is out of date. An annotation
changed and changed back leaves it current, and so does an annotation
outside the closure.

`glossql_parts()` serves every input's version now, as `(name,
version, kind, result)`. An input of the closure has one row per
result that read it, `result` its `begin_snapshot`. Joined on the name
and the result, it shows which result is out of date and which input
moved:

```sql
SELECT m.function, m.subject, i.input
FROM measurements m JOIN inputs i USING (begin_snapshot)
LEFT JOIN glossql_parts() p ON p.name = i.input AND coalesce(p.result, m.begin_snapshot) = m.begin_snapshot
WHERE i.input <> 'closure' AND p.version IS DISTINCT FROM i.version;
```

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced the result; NULL while the result is current. The view leaves this column out. |
| `input` | VARCHAR | What was read: a lake table or view as `dataset.table`; `<dataset>.*` for the dataset's every table; `<dataset>.**` where the reads could not be named; `lake.shape` for the lake's columns and views, `lake.shape/<dataset>` for one dataset's; a catalog table that holds no rows about assets as `main.<name>`. Within the closure: one property's annotations as `main.glossary/<aspect>`, every annotation as `main.glossary`; one function's results as `main.measurements/<function>`, the other functions' results of one property as `main.measurements@<aspect>`, every other function's results as `main.measurements`; `main.aspects`, `main.tests`, `main.relationships`, `main.datasets`, `main.imports` and `main.recipes` for the declarations of the closure. `closure` for the closure itself. |
| `version` | VARCHAR | The input's version at the read: the last lake snapshot that changed it; for a set, a digest of its members' names and versions; for an input of the closure, a digest of what its rows held. For `closure`, its members as a JSON list. NULL when the input was not there. |
