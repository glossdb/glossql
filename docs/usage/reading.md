# Reading what is known

`GLOSSARY()` returns what is known about an asset: one row per asset
and property, with its value and its state. `TESTS()` returns the test
results ([tests](tests.md)). The rest is plain SQL over the catalog's
tables.

## The glossary

```sql
SELECT * FROM GLOSSARY(readings.temperature);
```

Columns: `(subject, property, value, test_state, score, state)`. The
value is a person's if there is one, else an agent's, else a
function's.

| State | Meaning |
|---|---|
| `current` | the value is served and what it describes is unchanged |
| `stale` | the value is served; the asset changed after it was written, or something the computed value read has changed |
| `possibly` | the computed value is served; a result it read is out of date and not computed again yet |
| `conflict` | the values differ and the test's score is above its threshold; `value` is NULL, `test_state` and `score` say how far |
| `missing` | the property is required here and nobody has recorded it; `value` is NULL |

- With no argument, `GLOSSARY()` covers the dataset in use.
- A table returns itself, its columns, and the relationships it takes
  part in, from either side.
- `subject::property` narrows to one property.

```sql
SELECT * FROM GLOSSARY(plant::uptime);
SELECT * FROM GLOSSARY(readings) WHERE state <> 'current';
```

## Every value, side by side

```sql
SELECT * FROM GLOSSARY(readings.temperature, all => true);
```

Columns: `(subject, property, kind, test, actor, body, written_at,
current)`, one row per value: the agent's, the person's and each
function's. `kind` is the property's kind, `actor` is `agent`, `human`
or `function`. `current` is false for a stale value.

## When a value is stale

An annotation reads `stale` once its asset's definition changed after
the snapshot it was written at:

- on a column: the column was retyped or dropped, or a load derived
  it otherwise than the load before;
- on a table: one of its columns changed so;
- on a view: its definition was written again, or its SQL changed;
- on a view's column: the view's SQL changed.

A load whose columns differ from the table's alters the table: a
column added is new, and a column with the same name and type keeps
its annotations current, as does the table when a column was only
added. A table of one column retyped is made again. A load with the
same columns, or a changed filter in the load query, leaves them
current. A type changed and changed back still reads stale until
the annotation is written again. A computed value is stale once
something it read changed after it was computed, or a computed value
it read is stale.
A computation that failed shows its `error` in the raw read and no
value.

## The catalog tables

Every declaration, annotation and measurement is a row of a catalog
table. Each table is a view in the lake's `main` schema, and reads by
its bare name from any dataset:

```sql
SELECT name, kind, grains FROM properties;
SELECT subject, property, actor_kind, body FROM glossary WHERE property = 'unit';
SELECT function, subject, computed_at FROM results ORDER BY computed_at DESC;
```

The views serve the current rows. Every row a write replaced or
removed stays readable in `<name>_history`. One page per table:
[the catalog](../specification/introduction.md).

## Lineage

A view's lineage is stored when the view is defined: what computes
each of its columns, read from its plan down to the source columns and
their types. A view column `fahrenheit` computed as
`temperature * 1.8 + 32` derives from an expression over
`plant.readings.temperature:DOUBLE`. The columns, and how the lineage
kit reads it per dataset: [functions](../functions.md#lineage).

Every load stores its load query's lineage on its
[imports](../specification/tables/imports.md) row. Comparing two rows
shows what changed in what the source sends, for example a column
that now arrives under another type.

## What a read computes

`GLOSSARY()` and `TESTS()` read stored rows: the annotations, the
results and each test's stored result. A read marks each row
once per statement. An annotation is stale once the definition of its
asset changed after it was written. A stored result is out of date
once something it read changed after it was computed:
`glossql_outdated()` lists each one with the change. A read computes
no test and stores nothing; a test's statement does
([tests](tests.md)).

Normative text: [SPEC.md §5.3](../../SPEC.md#53-reading).
