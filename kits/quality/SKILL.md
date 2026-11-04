---
name: glossql-quality
description: The quality kit's screens for a glossql workspace — how the data holds together: which columns key into which, whether declared relationships hold, which columns form hierarchies, which numbers are other numbers combined, which metrics are the same thing twice. Use after glossql_kit('kits/quality/kit.sql'), once roles are glossed, to find structure before declaring it.
---

# The quality kit: what it teaches

For an actor working in a DuckDB that has the glossql extension loaded,
the KPI kit declared and this kit after it. The extension knows the
grammar and the store; the KPI kit is the words; this kit is the screens
that find structure in the data and land what they find as measurements.
Everything live is read through the language: the tables, the
declarations, the glossary.

## Load and check

```sql
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');    -- after the KPI kit; the same two lines in the opener
SELECT name, scope, returns FROM functions WHERE returns IS NOT NULL;
```

## The screens

A screen is a table macro over `dataset_cells(subject)`, every cell of a
dataset's tables, or of one table, as one relation. Run a screen directly
to look; extract its measurement to land what it found.

| Measurement | On | Reads | Lands |
|---|---|---|---|
| `detect_relationships()` | dataset | `relationship_candidates(dataset)` | `candidates`: `from`, `to`, `cardinality`, `overlap`, `matched`, `orphans`, `to_unique`, `to_temporal`, `key_columns`; `summary`: the count, the top 16, a note |
| `relationship_coherence()` | dataset | `relationship_checks(dataset)` | `relationships`: per declared edge `filled`, `orphans`, `orphan_rate`, `temporal` pairs with `joined`, `precedes`, `precedes_rate` |
| `detect_hierarchies()` | table | `hierarchy_candidates(table)` | `candidates`: `from`, `to`, `g3`, `lambda`, `kind` (`edge` or `alias`), `rows` |
| `detect_derivations()` | table | `derivation_candidates(table)` | `candidates`: `target`, `form` (`product` or `sum`), `operands`, `support`, `violations`, `match_rate`; `truncated` |
| `detect_grounding_collisions()` | dataset | `grounding_collisions(dataset)` | `groundings`; `collisions`: `kind` (`sql` or `served_series`), `aspects`, `subjects` |

What each line means:

- A relationship candidate is a column whose values appear in another
  column that is key-like: nearly every filled row distinct. `overlap` is
  the share of the source's distinct values found in the target;
  `orphans` the rest. `key_columns` names a second pair that scopes the
  first, where a reference keys only within its account.
- `g3` is the share of rows that break `from → to`; a candidate ships at
  `g3 <= 0.05`, and reads `alias` when both directions are under 0.01.
  `lambda` is how much better `to` is predicted knowing `from`.
- A derivation ships at `match_rate >= 0.95` over twenty supporting rows
  or more.
- A collision is two groundings of one flavor, the aspect's `x-kind`: a
  `sql` collision two with the same statement once parsed, a
  `served_series` collision two whose monthly series agree though their
  SQL differs. The series is read off the view's claims: the column
  glossed `role` timestamp is the period, the column glossed `role`
  measure the value, else the columns named `period` and `value`; the
  `behavior` glossed on the value column folds the month, a flow summed,
  a stock at its last level, a ratio its summed halves re-divided.
- `applicable: false` with a `reason` is a screen that could not run:
  an empty table, too few columns of the right kind, no declared
  relationships, no groundings. Examined, nothing to say.

## The order

1. Gloss `role` on the columns first. The screens read types, not roles,
   but what they find is read against the roles.
2. `SELECT detect_relationships() FROM fin;` then read back:
   `SELECT value FROM GLOSSARY(fin::relationship_candidates);`. The top
   16 are ranked: clean keys first, then many-to-one, then overlap.
   Declare what holds: `DECLARE RELATIONSHIP orders.customer_id ->
   customers.id;`. A candidate is evidence, never a declaration.
3. `SELECT relationship_coherence() FROM fin;` after declaring, and
   after every landing: orphans and dates out of order are what a
   declared edge owes.
4. Per table with dimension columns, `SELECT detect_hierarchies() FROM
   fin.geo;`; per table with three numeric columns or more, `SELECT
   detect_derivations() FROM fin.lines;`. Cite a found edge or
   derivation in the `grounds` of the gloss it supports.
5. After grounding metrics, `SELECT detect_grounding_collisions() FROM
   fin;`: two names for one number is a definition to settle, not two
   metrics.

A measurement lands with what it read; a repeat serves the landed row
while that is unchanged and computes once a table moved.

## A check of your own

A validation is a FACT aspect whose schema carries `tolerance` and
`breach_rate`. Gloss the expectation with the tolerance you measured at
landing, never an assumed zero. Declare a function that returns the
same aspect and measures the violation share as `breach_rate`, 0.0
passing. A witness on the aspect names the kit's detector:

```glossql
DECLARE WITNESS ids_w ON ids_present BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

`ATTEST()` bands each subject: yellow while no check has spoken, green
at or under the tolerance, red above it. The human's tolerance outranks
the agent's; the witness's threshold stands in while no gloss carries
one. The detector is one-sided: a source whose expected dirt must not
be cleaned away declares its own, red on both sides.

## What this kit does not do

No witness judges these measurements: they are evidence, served beside
the glosses, not claims. Declaring an edge, a hierarchy gloss or a
derived metric is the actor's statement, made from the evidence. The
screens compare values as text within one stored type, read a NULL as
a category, and scan whole tables, never a sample.
