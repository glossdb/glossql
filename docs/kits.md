# Kits

The extension ships the language and the catalog, and no vocabulary. A
kit is the vocabulary for a domain: a script of declarations
(properties, functions, tests) with a `SKILL.md` beside it that teaches
an agent what the words mean. The extension validates what a kit
declares; the kit decides what is said.

## The kits that ship

| Kit | Brings |
|---|---|
| `kpi` | the semantic layer's words for columns and tables: `meaning`, `role`, `additivity` (additive, semi-additive, ratio with its numerator and denominator, non-additive), `unit`, `dimension`, `entity`; source `conventions`; the `definitions` handbook; the `slot_entropy` detector; the `column_profile` and `outlier_profile` measurements; the tests that bind them. A metric's view carries the same words on its columns |
| `quality` | how the data holds together, over `dataset_cells()`: `relationship_candidates`, `relationship_checks`, `hierarchy_candidates`, `derivation_candidates`, `grounding_collisions` and `additivity_anchors` as table macros; the measurements that store them (`detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations`, `detect_grounding_collisions`, `additivity_evidence`); the `temporal` profile; the `rate_tolerance` detector, which scores a stated expectation against a computed value. Load it after `kpi` |
| `spatial` | a small domain kit: `shape` on every geometry column (`WHEN TYPE = 'GEOMETRY'`), the `crs` its coordinates are in, and the `geometry_profile` measurement. Its first lines install and load DuckDB's `spatial` extension |

A workspace loads kits of its own after these.

## Load a kit

```sql
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');
```

- The path is anything DuckDB's file system opens: a local path, or a
  URL once `httpfs` is loaded (`INSTALL httpfs; LOAD httpfs;`).
- The kit runs statement by statement on a second connection, under
  the glossql grammar, and returns one row per statement:
  `(statement, outcome)`. Each statement commits on its own; a failing
  statement stops the load, and the ones before it stay.
- The load is recorded in the `kits` table with its path, statement
  count and the hash of its text. Loaded again unchanged, the kit is
  already in place: the call returns one row and runs only its `LOAD`
  lines.
- A changed kit re-declares: unchanged properties stay, functions are
  replaced, and a changed property that has values under it is refused.

## Write a kit

A kit is statements in order: properties first, then functions, then
the tests that name both.

- A property's `WITH` schema is a JSON Schema. Annotations such as
  `title` or `x-kind` sit beside the structure.
- Every `VIEW` value is checked against the metric contract (`sql` or
  `stopped`, `assumptions`). What a reader of a metric's rows needs (the
  period, the measure and how it adds up, the slices) is recorded on
  the view's columns under the kit's properties. A kit whose readers
  need a key in the body declares a schema for it as an aspect with
  `"x-kind": "grounding"`; every `VIEW` value is then checked against
  it too.
- A measurement (`RETURNS aspect`) becomes a table macro in the
  dataset's schema, or in `glossql` for `GLOBAL`. Its body is DuckDB
  SQL; names resolve as in the declaring session. A body may open with
  its own `WITH`.
- A detector (no `RETURNS`) runs at read over its test's `slots` with
  `$threshold` bound, and returns `(subject, band, score)`.
- A measurement is pinned to the tables and catalog tables its plan
  scans. One that reads a file, a catalog function or another database
  is served only while nothing in the dataset moved, so read tables.
- Conditions narrow what is required: `AS FACT ON COLUMN WHEN role =
  'measure'`.

The `SKILL.md` is the half the extension cannot check: what each word
means, the order of the work, what a reader expects.

## Wrap it

The extension has no interface of its own. A wrapper holds one
connection per actor, sets the actor on it
([sessions](usage/sessions.md#say-who-is-speaking)), and exposes the
statements: an MCP server as one tool with the kit's `SKILL.md` as a
resource, an HTTP endpoint per route. The two that ship are in
[interfaces](interfaces.md).
