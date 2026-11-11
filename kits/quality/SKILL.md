---
name: glossql-quality
description: The quality kit's checks for a glossql workspace — which columns reference which, whether declared relationships are valid in the data, which columns form hierarchies, which numbers are computed from other numbers, which metrics are defined twice, whether a measure is a stock or a flow. Use after glossql_kit('kits/quality/kit.sql'), once roles are annotated, to find structure before declaring it.
---

# The quality kit

For an agent working in a DuckDB that has the glossql extension, the
kpi kit and then this kit loaded. The kit's checks find structure in
the data and store what they find as measurements. A result is input
for your annotation or declaration. It does not declare anything
itself.

## Load and check

```sql
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');    -- after the kpi kit; both lines in the opener
SELECT name, scope, returns FROM functions WHERE returns IS NOT NULL;
```

## The checks

Each check is a table macro you can call to look, and a measurement
that stores the result.

| Measurement | On | Table macro | Stores |
|---|---|---|---|
| `detect_relationships()` | dataset | `relationship_candidates(dataset)` | `candidates`: `from`, `to`, `cardinality`, `overlap`, `matched`, `orphans`, `to_unique`, `to_temporal`, `key_columns`; `summary`: the count, the top 16, a note |
| `relationship_coherence()` | dataset | `relationship_checks(dataset)` | `relationships`: per declared relationship `filled`, `orphans`, `orphan_rate`, and `temporal` pairs with `joined`, `precedes`, `precedes_rate` |
| `detect_hierarchies()` | table | `hierarchy_candidates(table)` | `candidates`: `from`, `to`, `g3`, `lambda`, `kind` (`edge` or `alias`); `rows`: the rows read |
| `detect_derivations()` | table | `derivation_candidates(table)` | `candidates`: `target`, `form` (`product` or `sum`), `operands`, `support`, `violations`, `match_rate`; `truncated`; `rows`: the rows read |
| `detect_definition_collisions()` | dataset | `definition_collisions(dataset)` | `definitions`; `collisions`: `kind` (`sql` or `served_series`), `aspects`, `subjects` |
| `additivity_evidence()` | column | `additivity_anchors(column)` | `summary`: `result` (`semi-additive`, `additive`, `abstain`), `convention`, `support`, `voted`, `event`, `align`, `grain`, `scope`, `r_flow`, `r_stock`, `sign`, `tiebreak`, `reason`; `anchors`: one row per anchor; `anchors_n`, `decided` |
| `temporal()` | timestamp columns | | `temporal_profile`: `granularity`, `confidence`, `completeness`, `gaps` |

How to read them:

- **Relationship candidates.** A candidate is a column whose values
  appear in another column that is nearly unique. `overlap` is the
  share of the first column's distinct values found in the second;
  `orphans` is the rest. `key_columns` names a second column pair that
  is needed for the key to be unique.
- **Hierarchies.** `g3` is the share of rows that break `from → to`. A
  candidate is reported at `g3 <= 0.05`. It is an `alias` when both
  directions are at or under 0.01. `lambda` says how much better `to`
  is predicted when `from` is known.
- **Derivations.** A candidate is reported at `match_rate >= 0.95`
  over at least 20 rows.
- **Definition collisions.** Two metrics with the same `x-kind` that
  are the same thing. `sql`: the two SQL texts parse to the same
  statement. `served_series`: the SQL differs but the monthly series
  agree. The series is read from the view's annotations: the column
  with `role` `timestamp` is the period, the column with `role`
  `measure` the value, else the columns named `period` and `value`.
  The value's `additivity` says how a month is aggregated.
- **Additivity evidence.** Says whether a numeric column is a stock (a
  level at a point in time) or a flow (a movement that sums over a
  period). The check compares the column with movements in an event
  table that a declared relationship links to the column's table. A
  flow matches the movement; a stock's change matches it. The
  movements are the event table's columns annotated `role` `measure`,
  so annotate roles on the event tables first. An anchor is one
  combination of event table, link, time column, grain and scope.
  `support` is a lower confidence bound on the share of entities that
  agree. `r_flow` and `r_stock` are the median residuals under each
  reading. A result of `abstain` carries its `reason`. Read the
  summary, then annotate `additivity` and name the anchor in `basis`.
- **`applicable: false`** with a `reason` means the check could not
  run: an empty table, too few columns of the right kind, no declared
  relationships, no metrics, a column that is not a number or is an
  identifier, a table with no time column.

## The order of the work

1. Annotate `role` on the columns first.
2. `SELECT detect_relationships() FROM shop;`, then read the result:
   `SELECT value FROM GLOSSARY(shop::relationship_candidates);`. The
   top 16 are ranked: unique targets first, then many-to-one, then
   overlap. Declare the ones that are right: `DECLARE RELATIONSHIP
   orders.customer_id -> customers.id;`.
3. `SELECT relationship_coherence() FROM shop;` after declaring, and
   after every load: it reports orphans and dates out of order.
4. Per table with dimension columns, `SELECT detect_hierarchies() FROM
   shop.regions;`. Per table with three or more numeric columns,
   `SELECT detect_derivations() FROM shop.order_lines;`. Name what was
   found in the `basis` of the annotation it supports.
5. Per measure column, after the relationships are declared and the
   event tables' measures have `role`: `SELECT additivity_evidence()
   FROM shop.stock_levels.quantity;`. Then annotate `additivity`.
6. After defining metrics, `SELECT detect_definition_collisions() FROM
   shop;`. Two names for one number is a definition to settle with a
   person.

A measurement is stored with the versions of the tables it read. A
repeat returns the stored result while those are unchanged.

## A check of your own

A check is a `FACT` property whose schema has `tolerance` and
`breach_rate`. Annotate the tolerance you expect. Declare a function
that returns the same property and measures the share of violations
as `breach_rate`. A test on the property names the kit's detector:

```glossql
DECLARE TEST ids_test ON ids_present BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

`TESTS()` gives each asset a state: `yellow` while no function has
measured a rate, `green` at or under the tolerance, `red` above it.
The person's tolerance is used before the agent's. The test's
threshold is used while no annotation has a tolerance. The detector is
one-sided: it does not flag a rate that is lower than expected.

## Limits

- No test scores these measurements. They are input for a declaration
  or an annotation, which you or a person make.
- The checks compare values as text within one stored type and treat
  NULL as a category.
- `hierarchy_candidates` and `derivation_candidates` read at most two
  million cells of a table. A larger table is read as a sample of its
  rows, the same rows on every run; `rows` says how many, and the rates
  are then estimates.
