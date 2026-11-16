# Reading what is known

`GLOSSARY()` returns what is known about an asset: one row per asset
and property, with its value and its state. `TESTS()` returns the test
results ([tests](tests.md)). The rest is plain SQL over the catalog's
tables.

## The glossary

```sql
SELECT * FROM GLOSSARY(readings.temperature);
```

Columns: `(subject, aspect, value, test_state, score, state)`. The
value is a person's if there is one, else an agent's, else a
function's.

| State | Meaning |
|---|---|
| `current` | the value is served and what it describes is unchanged |
| `stale` | the value is served; the asset changed after it was written, or a table the computed value read has changed |
| `conflict` | the values differ and the test's score is above its threshold; `value` is NULL, `test_state` and `score` say how far |
| `missing` | the property is required here and nobody has recorded it; `value` is NULL |

- With no argument, `GLOSSARY()` covers the dataset in use.
- A table returns itself, its columns, and the relationships it takes
  part in, from either side.
- `subject::aspect` narrows to one property.

```sql
SELECT * FROM GLOSSARY(plant::uptime);
SELECT * FROM GLOSSARY(readings) WHERE state <> 'current';
```

## Every value, side by side

```sql
SELECT * FROM GLOSSARY(readings.temperature, all => true);
```

Columns: `(subject, aspect, kind, test, actor, body, written_at,
current)`, one row per value: the agent's, the person's and each
function's. `kind` is the property's kind, `actor` is `agent`, `human`
or `function`. `current` is false for a stale value.

## When a value is stale

An annotation on a table or a column reads `stale` once:

- the column was retyped or dropped, or its table lost a column; or
- the column's lineage differs between the load that was newest when
  the annotation was written and the newest load now.

A new column, or a changed filter in the load query, leaves
annotations current. An annotation on a view or its column is stale
once the view's SQL changed. A computed value is stale once one of its
reads returns other rows than it did, or a computed value it read is
stale.
A computation that failed shows its `error` in the raw read and no
value.

## The catalog tables

Every declaration, annotation and measurement is a row of a catalog
table. Each table is a view in the lake's `main` schema, and reads by
its bare name from any dataset:

```sql
SELECT name, kind, grains FROM aspects;
SELECT subject, aspect, actor_kind, body FROM glossary WHERE aspect = 'unit';
SELECT function, subject, computed_at FROM measurements ORDER BY computed_at DESC;
```

The views serve the current rows. Every row a write replaced or
removed stays readable in `<name>_history`. One page per table:
[the catalog](../specification/introduction.md).

## Lineage

`glossql_lineage(sql)` returns what computes each output column of a
`SELECT`, read from its plan down to the source columns and their
types.

```sql
FROM glossql_lineage($$SELECT sensor_id, temperature * 1.8 + 32 AS fahrenheit FROM plant.readings WHERE sensor_id > 1$$);
```

Here `sensor_id` derives from `plant.readings.sensor_id:INTEGER`, and
`fahrenheit` from an expression over
`plant.readings.temperature:DOUBLE`. The columns, and the same for the
views of a dataset: [functions](../functions.md#glossql_lineage).

Every load stores its load query's lineage on its
[imports](../specification/tables/imports.md) row. Comparing two rows
shows what changed in what the source sends, for example a column
that now arrives under another type.

## How reads are cached

`GLOSSARY()` and `TESTS()` are computed once per version of the
catalog and the lake and kept in DuckDB's object cache. A write, a
load or a delete changes the version, and the next read computes
again. Which stored results are out of date is asked when it is read:
`glossql_outdated()` checks every result, and the `current` mark of a
result checks that result and the results it read.

A kit keeps a read of its own the same way: `glossql_kept('<macro>')`
serves the rows of the table macro `main.<macro>()`, which takes no
arguments, over the dataset in use, computed once per version. A read
that several reads compose is then planned and run once per write.

Normative text: [SPEC.md §5.3](../../SPEC.md#53-reading).
