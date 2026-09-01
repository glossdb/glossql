# Statements

glossql is a declarative context language over a SQL host. It adds a
small set of statements and two table functions to SQL; it does not
re-specify SQL. Recipes, groundings, and every read body are host SQL
and stay opaque to the grammar. [SPEC.md](../../SPEC.md) is the
normative definition; this page is the working map.

## The writes

- `DECLARE SOURCE` — names where data comes from: parquet, csv, json,
  or a relational database reached by its connection URI.
- `DECLARE RECIPE` — materializes a table from a source. The recipe
  SQL runs at the source and carries the casts: typing is authored,
  not detected.
- `PROBE` — the recipe rehearsal: the same SQL surface, executed at
  the source, landing nothing, always returning its schema.
- `DECLARE DATASET` / `USE` — the working unit and the resolution
  context: unprefixed `table.column` paths resolve against the `USE`'d
  dataset.
- `DECLARE RELATIONSHIP` — a declared join: `->` many-to-one, `<->`
  one-to-one, tuple endpoints for composite keys. Only declared
  relationships exist; a rejected candidate is not declared.
- `DECLARE ASPECT` — a vocabulary entry: a name, a JSON Schema, a kind
  ([aspects](aspects.md)).
- `GLOSS` — the one write verb for knowledge: applies an aspect to a
  subject with a JSON body. On a QUERY aspect — a metric's grounding —
  the outcome is the metric's fact row ([reads](../reference/reads.md)).
- `DECLARE FUNCTION` — a measurement, voice, or detector; the body
  rides the declaration ([functions](functions.md)).
- `DECLARE WITNESS` — who may speak an aspect and what adjudicates the
  slots ([validation](validation.md)).

Every JSON or SQL body is dollar-quoted (`$$ … $$`), so it rides
byte-exact — no escaping, ever. There is no BY clause anywhere: the
actor rides the connection and the engine stamps every statement
([actors](actors.md)).

## The reads

- `GLOSSARY(subject)` — the collapsed context read: one row per
  (subject, aspect) with the precedence pick, band, score, and a
  `state` that makes every gap visible — `unassessed`, `contested`,
  `current`, `stale`. Absence is a visible row, never an omission.
- `GLOSSARY(subject, all => true)` — the raw read: every slot side by
  side, marked `current`; precedence is the reader's business.
- `ATTEST(subject)` — the verdict surface: band and score per witness.
- `SELECT f() FROM subject` — extraction: runs a measurement at the
  read's pin ([functions](functions.md)). The outcome is one row per
  call — `function`, `subject`, `body`, `computed_at`, `computed` —
  and `computed` is false when the recorded row was served at an
  unchanged pin, its `computed_at` the earlier run's.
- `read.<aspect>()` — a grounded metric as a relation: the current
  grounding expands at plan time, composable in any FROM position.
- Plain SQL — tables, and the declaration relations (`functions`,
  `aspects`, `witnesses`, `sources`, `relationships`, `glossary`,
  `measurements`, `imports`) read as ordinary tables. Removal is
  spelled as SQL — `DELETE FROM glossary WHERE …` — and currently
  refuses: rows cannot yet be removed from the lake, so a slot is
  superseded, never deleted.

`subject::aspect` narrows a read to one declared aspect:
`GLOSSARY(fin::dso)` is a metric's declaration and grounding in one
row.

## What the host refuses

Substrate SQL runs behind an allowlist: queries pass, `DESCRIBE` and
`EXPLAIN` pass, `DROP TABLE` refuses while the table holds data or
glosses, and everything else that would alter schema or data directly
is refused. Tables come from recipes.

Statement identity is content: an unchanged re-declaration is a no-op;
a changed recipe supersedes and re-lands its table.

## A minimal flow

```glossql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE DATASET fin SET (purpose: 'working-capital analysis over ERP and CRM exports');
USE fin;
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
SELECT * FROM GLOSSARY(orders.amount);
```
