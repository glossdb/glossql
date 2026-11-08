# Reading what is known

`GLOSSARY()` returns what is known about an asset: one row per asset
and property, with its value and its state. Everything else is plain SQL
over the catalog's tables.

## The glossary

```sql
SELECT * FROM GLOSSARY(orders.amount);
```

Columns: `(subject, aspect, value, band, score, state)`. The value is
the winning one: a person's over an agent's over a function's. `state`
shows every gap; the read never hides one.

| State | Meaning |
|---|---|
| `current` | the value is served and its basis is unchanged |
| `stale` | served, and marked: the asset changed after the value was written, or an input of the computed value moved |
| `contested` | the values disagree beyond the test's threshold; the value is withheld, `band` and `score` say how far |
| `unassessed` | the property is required here and nobody has recorded it; the row still appears |

- With no argument, `GLOSSARY()` covers the dataset in use.
- A table returns itself, its columns, and every relationship it takes
  part in, from either side. The other table's own values are not
  included.
- `subject::aspect` narrows to one property: a metric's declaration and
  SQL are one read away.

```sql
SELECT * FROM GLOSSARY(fin::dso);
SELECT * FROM GLOSSARY(orders) WHERE state <> 'current';
```

## Every value, side by side

```sql
SELECT * FROM GLOSSARY(orders.amount, all => true);
```

Columns: `(subject, aspect, kind, test, actor, body, written_at,
current)`, one row per source of a value. Which one wins is yours to
decide. `current` is false for a computed value whose inputs moved.

## Test results

`TESTS()` returns one row per test and asset: band, score, whether it
is current ([tests](tests.md)).

## The catalog tables

Every declaration and value is a row of a catalog table, readable as a
view in the lake's `main` schema, bare from any dataset:

```sql
SELECT name, kind, grains FROM aspects;
SELECT subject, aspect, actor_kind, body FROM glossary WHERE aspect = 'unit';
SELECT function, subject, computed_at FROM measurements ORDER BY computed_at DESC;
```

History is rows ordered by `seq`; the reads above take the newest.
One page per table: [specification](../specification/introduction.md).

## How reads are cached

`GLOSSARY()` and `TESTS()` are computed once per catalog version and
kept in DuckDB's object cache. A write, a load or a delete moves the
version, and the next read computes again.

Normative text: [SPEC.md §5.3](../../SPEC.md#53-reading) and
[§7.2](../../SPEC.md#72-test results).
