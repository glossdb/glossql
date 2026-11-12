---
name: glossql
description: Use glossql in a DuckDB workspace through the duckdb CLI or the example MCP server — how to open a workspace, the statements, the reads, the catalog tables, what a failed statement looks like. Use when reading or writing anything in a glossql workspace (datasets, sources, load queries, annotations, functions, tests, views).
---

# Using glossql

glossql adds statements to DuckDB SQL for a workspace's data and its
glossary. Data is stored in tables of a DuckLake. The glossary is JSON
values attached to assets (a dataset, a table, a column, a
relationship, a source) under declared properties (aspects). The
`glossql` extension provides the grammar and the catalog. A kit adds
the properties for a domain; each kit has its own skill. Read the
tables, the properties and the glossary from the workspace before you
write.

## Open a workspace

A workspace is a directory with `glossql.sql` (the opener),
`ws.ducklake` (the lake's catalog) and `ws/` (its data). A call is one
`duckdb` process run in that directory, the opener first, your
statements on stdin in a quoted heredoc:

```sh
duckdb -unsigned -bail -init glossql.sql <<'SQL'
USE staff;
SELECT * FROM GLOSSARY(employees) WHERE state <> 'current';
SQL
```

The opener loads the extension, attaches the lake, turns the grammar
on, creates the catalog where missing and loads the workspace's kits:

```sql
.startup_text none
LOAD '/path/to/glossql.duckdb_extension';
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
.output /dev/null
PRAGMA glossql;
FROM glossql_kit('/path/to/kits/kpi/kit.sql');
FROM glossql_kit('/path/to/kits/quality/kit.sql');        -- after the kpi kit
.output
```

- **Each call is a new process.** The dataset in use, the actor and
  every variable are reset. Start a call that names a dataset's tables
  with `USE <dataset>;`. A full `dataset.table.column` path works from
  anywhere.
- **Use a quoted heredoc.** `$$…$$` bodies, single quotes and
  `"quoted"` names pass through unchanged.
- **`-bail` stops the call at the first statement that fails.** The
  statements before it stay done; the ones after it do not run.
- **A kit loaded again with the same text** runs only its `LOAD` and
  `INSTALL` lines. A DuckDB extension is loaded per process, so the
  opener is what loads a kit's extension in each call.

Through the example MCP server the tool is `glossql(statements)`, on
one connection the server keeps open. The statements are the same.
`USE`, variables and the actor stay set from call to call. A statement
that fails is the tool's error, with the output of the statements
before it.

## What comes back

- A read is a table. In the CLI's agent mode it is a markdown table
  with `name:TYPE` headers and a footer with the row count. A large
  result is cut; do not read a cut result as complete.
- A statement answers with one `outcome` row:
  `declared dataset staff (schema ws.staff)`,
  `loaded staff.employees: 300024 rows, 0 dropped (new)`,
  `imported staff.employees: 300024 rows, 0 dropped (replaced)`,
  `glossed unit on staff.employees.salary as agent`. A declaration
  repeated without change answers `… unchanged: no-op`.
- Running a measurement answers with `(function, subject, aspect,
  value, computed)`.
- A statement that fails is one JSON line on stderr and exit status 1.
  The extension's errors start with `glossql:` and say what was wrong.

## Who annotates

You are `agent`, the default. A person writes through their own call
with `SET VARIABLE glossql_actor = 'human';` ahead of their statements,
and `SET VARIABLE glossql_actor_id = '<who>';` to be named. Do not set
`human` yourself. `GLOSSARY()` serves the person's value before yours.
Where a test scores a disagreement as `red`, annotate again with a
`basis`, or ask the person.

## The statements

| Statement | Does |
|---|---|
| `USE staff;` | sets the dataset for the statements after it; DuckDB's own `USE` |
| `DECLARE DATASET staff SET (purpose: '…');` | creates a dataset: a schema of the lake and a catalog row |
| `DECLARE SOURCE hr SET (type: csv, location: '/exports');` | declares a source. `type`: `relational_db` for an attached database, any other word for files. `location`: a directory or URL prefix, or the attached database's name. `via`: optional, the name of a DuckDB secret |
| `PREVIEW hr AS $$SELECT … LIMIT 5$$;` | runs a query at the source and loads nothing |
| `DECLARE RECIPE employees ON staff FROM hr [SET (key: id)] AS $$sql$$;` | declares a load query and loads the table `staff.employees` now. With a key (`id`, or `'a, b'`) a later load merges by it, and a result with a key twice is refused. Without a key a load replaces the table |
| `IMPORT employees;` | runs the load query again: a new snapshot of the lake |
| `DECLARE RELATIONSHIP employees.department_id -> departments.id;` | declares a key between columns: `->` many-to-one, `<->` one-to-one, `a.(x, y) -> b.(x, y)` composite |
| `DECLARE ASPECT name WITH $$json-schema$$ AS FACT\|MEASUREMENT\|VIEW [ON TABLE, COLUMN, DATASET, SOURCE, RELATIONSHIP [WHEN aspect = 'value' \| WHEN TYPE = 'T' \| WHEN TYPE IN ('T', 'U')]];` | declares a property. Every value is validated against the schema. `ON` lists the kinds of asset it applies to. `WHEN` limits where it is required: where another property has a value, or on columns of a type (`ON COLUMN` only) |
| `GLOSS aspect ON subject AS $$json$$;` | annotates an asset. A `VIEW` property is annotated on the dataset (`GLOSS headcount ON staff AS …`) and is then served as the view `staff.headcount` |
| `DELETE FROM glossary WHERE subject = '…' AND aspect = '…';` | removes annotations from the catalog |
| `SELECT … FROM GLOSSARY(subject);` | the glossary: one row per asset and property |
| `SELECT … FROM GLOSSARY(subject, all => true);` | every value, one row per actor |
| `SELECT … FROM TESTS(subject \| subject::aspect);` | the test results |
| `DECLARE FUNCTION f FOR staff\|GLOBAL AS $$sql$$ [RETURNS aspect];` | with `RETURNS`, a measurement: a query that reads its asset as `$subject`, or its column as `subject_column($subject)`. Without, a detector: a query over a test's `annotations` with `$threshold` |
| `SELECT f() FROM employees.salary;` | runs a measurement and stores the result, or returns the stored result while the tables it read are unchanged (`computed` says which) |
| `DECLARE TEST t ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | says who may annotate a property and which detector scores its values |
| `FROM glossql_kit('path-or-url');` | runs a kit's statements, one row each |
| `FROM glossql_lineage($$sql$$);` | what computes each output column of a `SELECT`: `(column, derivation, complete, shape)` |

A body is written between `$$` and `$$` as it is: plain double quotes
inside, nothing escaped. A view's `sql` is one line inside its JSON
string, with single quotes for its literals. An unquoted name folds to
lowercase; a `"Quoted"` one keeps its case. A property that has
annotations cannot be changed.

## Assets

Write an asset as a bare path, not a string: `employees`,
`employees.salary`, `staff.employees.salary`. Quote a column name with
a dot or a space: `employees."a.b"`.

- One name is a dataset if one exists by that name, else a declared
  source, else a table of the dataset in use.
- Two names are `table.column` when the dataset in use has that table
  and column, else `dataset.table`.
- Three names are `dataset.table.column`.
- `GLOSSARY(staff)` reads a whole dataset; `GLOSSARY(staff::headcount)`
  narrows to one property.
- An annotation on a kind of asset the property is not declared `ON`
  is refused.
- A source's annotations are shared: annotate the bare name (`GLOSS
  conventions ON hr`), and every dataset's `GLOSSARY()` returns them.

## Reading

- `GLOSSARY(subject)` returns `(subject, aspect, value, test_state,
  score, state)`. `state` is `current`, `stale`, `conflict` or
  `missing`. A value in conflict is NULL. A required property nobody
  has annotated is a row with state `missing`.
- `GLOSSARY(subject, all => true)` returns `(subject, aspect, kind,
  test, actor, body, written_at, current)`.
- `TESTS(subject)` returns `(subject, aspect, test, state, score,
  computed_at, current, error)`.
- The catalog tables are views in the lake's `main` schema and read by
  their bare names: `datasets (begin_snapshot, name, settings, declared_at,
  declared_by)` · `sources (begin_snapshot, name, settings, declared_at,
  declared_by)` · `recipes (begin_snapshot, dataset, table_name, source, settings,
  sql, declared_at, declared_by)` · `imports (begin_snapshot, dataset, table_name,
  mode, rows, dropped_rows_count, lineage, snapshot_id, imported_at)` ·
  `relationships (begin_snapshot, dataset, left_side, op, right_side, declared_at,
  declared_by)` · `aspects (begin_snapshot, name, kind, grains, condition, schema,
  declared_at, declared_by)` · `functions (begin_snapshot, name, scope, script,
  returns, declared_at, declared_by)` · `tests (begin_snapshot, name, aspect,
  actors, detector, threshold, declared_at, declared_by)` · `glossary
  (begin_snapshot, dataset, subject, aspect, actor_kind, actor_id, body,
  written_at)` · `measurements (begin_snapshot, dataset, function,
  subject, aspect, value, computed_at)` · `inputs
  (begin_snapshot, input, version)` · `kits
  (begin_snapshot, name, location, statements, loaded_at, hash)`. The views
  serve the current rows; `<name>_history` serves every row a write
  replaced or removed, with its `end_snapshot`.
- The tables and columns of a dataset: `FROM
  glossql_columns('<dataset>')`, or `DESCRIBE <table>`; `SUMMARIZE
  <table>` for a profile of the data.
- The data itself is plain `SELECT`. A table's history is `FROM
  staff.employees AT (VERSION => n)`.

## Measure before you annotate

Run a kit's measurements first (`SELECT column_profile() FROM
employees.salary;`), name them in the annotation's `basis`, and
declare only what the data supports. What a column contains, a date's
grain, which columns join: the measurements answer these, so work them
out yourself. Ask the person about meaning: definitions, conventions,
a choice between two readings. The person's decision is stored through
their own call.

The kits' skills say what each property means and the order of the
work: `kits/kpi/SKILL.md`, `kits/quality/SKILL.md`,
`kits/spatial/SKILL.md`.
