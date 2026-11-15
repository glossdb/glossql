# inputs

A row is one input of a stored result: one read of the function's
body and what it returned. `SELECT f() FROM asset` writes the rows
with the result, in the same transaction. Key: `begin_snapshot` and
`input`. A row carries its result's `begin_snapshot` and ends when its
result does.

A body reads the catalog through `annotations(property, where)`,
`results(function, where)` and `definition(where)`; the where is a
place walked from the function's asset over the lineage (SPEC §6).
The reads are taken from the body when the function is declared
(`functions.reads`), and again when it computes. A result is current while each read returns what
this row stores and no result it read is out of date. An annotation
changed and changed back leaves it current, and so does an annotation
the body does not read.

`glossql_outdated()` serves each stored result that is out of date
with the input that differs:

```sql
SELECT function, subject, input, reason FROM glossql_outdated();
```

`reason` is `changed` (the read returns something else now) or
`outdated` (a result it read is out of date).

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The lake snapshot of the write that replaced the result; NULL while the result is current. The view leaves this column out. |
| `input` | VARCHAR | The read, as the body wrote it: `annotations(<property>, <where>)`, `results(<function>, <where>)`, `definition(definition, <where>)`, `data(<what>, <where>)`. `function` and `property` for the function's own declaration and that of the property it fills. A body that reaches the catalog past the read functions is refused at its declaration (`untracked_read`). |
| `version` | VARCHAR | A digest of what the read returned for the asset. For annotations: each asset of the place with every actor's annotation and whether it is stale, and the property's declaration and tests. For results: each asset with the value and the error. For a definition: the view's SQL, columns, reads and joins. For data: DuckLake's version of each table and view read. |
| `read` | VARCHAR | The input's read: `annotations`, `results`, `definition`, `data`, `function` or `property`. The view leaves this column out. |
| `what` | VARCHAR | The property, the function or the data the read names. The view leaves this column out. |
| `where` | VARCHAR | The place the read walks from the asset, or the table it names. The view leaves this column out. |
