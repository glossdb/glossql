# Kits

The extension ships the language and the catalog. It declares no
properties of its own. A kit adds them for one domain: a script of
declarations (properties, functions, tests) with a `SKILL.md` beside it
that teaches an agent what the words mean.

## The kits that ship

| Kit | Adds | Skill |
|---|---|---|
| `lineage` | The lineage of a dataset as one relation, `lineage(dataset)`: what contains what, what a view reads, what a relationship relates, what a plan plans. The read functions a function's body reads the catalog through: `annotations(property, where)`, `results(function, where)`, `definition(where)`. No property. Load it first | [kits/lineage/SKILL.md](../kits/lineage/SKILL.md) |
| `kpi` | The semantic model's words. Properties: `meaning`, `role`, `additivity`, `unit`, `dimension`, `entity`, `conventions` (on a source), `definitions` (on a dataset). Measurements: `column_profile`, `outlier_profile`. Detector: `value_entropy`. A test on each property; `role`, `additivity` and `unit` are scored by `value_entropy` | [kits/kpi/SKILL.md](../kits/kpi/SKILL.md) |
| `quality` | Data quality checks. Table macros: `relationship_candidates`, `relationship_checks`, `hierarchy_candidates`, `derivation_candidates`, `definition_collisions`, `additivity_anchors`. Measurements that store their results: `detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations`, `detect_definition_collisions`, `additivity_evidence`, `temporal`. Detector: `rate_tolerance`, which compares a measured rate with a stated tolerance. Load it after `lineage` and `kpi` | [kits/quality/SKILL.md](../kits/quality/SKILL.md) |
| `spatial` | Geometry columns. Properties: `shape` and `crs`, required on columns of type `GEOMETRY`. Measurement: `geometry_profile`. Its first lines install and load DuckDB's `spatial` extension | [kits/spatial/SKILL.md](../kits/spatial/SKILL.md) |

`examples/spatial/` runs the `spatial` kit on a public shapefile.

## Load a kit

```sql
FROM glossql_kit('kits/lineage/kit.sql');
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');
```

- The path is anything DuckDB's file system opens: a local path, or a
  URL once `httpfs` is loaded.
- The statements run in order. The call returns one row per statement:
  `(statement, outcome)`. Each statement commits on its own. A
  statement that fails stops the load; the ones before it stay.
- The load adds a row to the [kits](specification/tables/kits.md)
  table with the path, the statement count and a hash of the text.
- Loading the same text again runs only its `LOAD` and `INSTALL`
  lines and returns one row. So a kit can be loaded at the start of
  every session.
- A changed text runs whole: an unchanged property does nothing, a
  function is replaced, and a changed property that has annotations is
  refused.
- A declaration a kit makes has `kit:<name>` in its `declared_by`
  column.

## Write a kit

A kit is statements in order: properties first, then functions, then
the tests that name both. See the usage pages for each statement:
[properties](usage/describing-data.md),
[functions](usage/measurements.md), [tests](usage/tests.md).

- A property's `WITH` schema is a JSON Schema. Keywords such as `title`
  or `x-kind` are stored with it.
- A kit that needs its own keys in a view's body declares a schema for
  them: a property whose schema carries `"x-kind": "definition"`. Every
  `VIEW` value is validated against it. It cannot be annotated or
  tested.
- A measurement that reads a file, a table function or another
  database is computed again after any change in the dataset. Read
  tables where you can.
- A kit may hold plain SQL too, such as `CREATE MACRO`, `INSTALL` and
  `LOAD`.

Write the `SKILL.md` for the agent: what each word means, the order of
the work, and what a reader of the data expects.
