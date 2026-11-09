---
name: glossql
description: Speak glossql in a DuckDB workspace through the duckdb CLI — the opener, the call shape, the statement set, the reads, the store relations, how a refusal looks. Use when reading or writing anything in a glossql workspace (datasets, sources, recipes, glosses, functions, tests, metrics).
---

# Speaking glossql through the duckdb CLI

glossql is one SQL-shaped surface for a workspace's data and its
context. Data is stored in tables of a DuckLake; context is JSON attached
to subjects (`dataset`, `table`, `table.column`; a column named with a
  dot or a space is spelled quoted, `table."a.b"`) under declared aspects.
The `gloss` extension gives DuckDB the grammar and the store; a kit
gives the words (`kits/kpi/SKILL.md` teaches the KPI kit's,
`kits/quality/SKILL.md` the quality kit's screens). Everything
live — the vocabulary, the tables, the catalog — is read through the
language, never assumed.

## The interface

A workspace is a directory holding `glossql.sql` (the opener),
`ws.ducklake` (the lake's catalog) and `ws/` (its data). A call is one
`duckdb` process run from that directory, the opener first, your
statements on stdin in a quoted heredoc:

```sh
duckdb -unsigned -bail -init glossql.sql <<'SQL'
USE employees;
SELECT * FROM GLOSSARY(dept_emp) WHERE state <> 'current';
SQL
```

The opener loads the extension, attaches the lake, activates the grammar,
makes the store where missing and loads the workspace's kits, each standing when its text is unchanged:

```sql
.startup_text none
LOAD '/path/to/glossql.duckdb_extension';                  -- or LOAD glossql, once installed
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
.output /dev/null
PRAGMA glossql;
FROM glossql_kit('/path/to/kits/kpi/kit.sql');            -- every kit the workspace runs
FROM glossql_kit('/path/to/kits/quality/kit.sql');        -- after the KPI kit, whose words its screens read against
.output
```

- **Nothing but the lake survives a call.** The dataset binding, the
  actor and every variable reset with the process, so every call that
  names a dataset's tables opens with `USE <dataset>;`. A qualified
  `dataset.table.column` reaches across datasets from anywhere.
- **The quoted heredoc is the shape.** `$$…$$` bodies, single quotes
  and `"quoted"` names pass through untouched; inside a double-quoted
  shell string `$$` is the shell's own.
- **`-bail` stops the call at the first refused statement.** What
  ran before it stays done; what came after never ran. Without it
  the rest still runs.
- **`-unsigned`** until the extension is signed by a repository.
- **The kits replay at start-up.** A DuckDB extension is loaded per
  process, so a kit that needs one installs and loads it in its first
  lines, and the opener's replay is what loads it next time. A kit run
  again is silent: unchanged declarations are no-ops, no kits row is stored.

Through MCP the interface is the tool `glossql(statements)` on one connection
the server holds open: the same statements, a markdown table per read,
an outcome line per write, a refusal as the tool's error, DuckDB's class
ahead of the `gloss:` message, with what ran before it above. There
`USE`, variables and the actor persist from call to call. Everything
below holds unchanged.

## What comes back

The CLI renders for a coding agent when it finds one in the environment
(`CLAUDECODE` among others; `.help agent` says the rest):

- a read is a markdown table, `name:TYPE` headers, a footer with the
  row count and a hash of the whole result. A result over 1000 rows or
  10,000 bytes comes as its first and last rows with the count: refine,
  never read a cut result as complete. `.maxrows N` on a call lifts it.
- a statement answers with one `outcome` row saying what it did:
  `declared dataset fin (schema ws.fin)`, `stored fin.orders: 300024
  rows, 0 dropped (new)`, `imported …`, `glossed …`. An extraction
  answers with the measurement row (`function, subject, aspect, value,
  computed`); twenty extractions in one call are twenty small tables,
  and `measurements` holds the lot afterwards.
- a refused statement is one JSON line on stderr and exit status 1.
  The language's refusals open with `gloss:` and name what was wrong;
  the parser's and the binder's are DuckDB's own.

## Who speaks

You are `agent`. A person rules through their own call with `SET
VARIABLE glossql_actor = 'human';` ahead of their statements (and `SET
VARIABLE glossql_actor_id = '<who>';` for the catalog); never set it
yourself. The collapsed read prefers the human's value and judges a
disagreement through the test's detector; a red band is a
disagreement to resolve by re-glossing with grounds, not to overwrite.

## The statement set

| statement | does |
|---|---|
| `USE fin;` | bind the statements after it in this call to a dataset (a schema of the lake); DuckDB's own `USE` |
| `DECLARE DATASET fin SET (purpose: '…');` | create a dataset: a schema plus a row; `purpose` is required |
| `DECLARE SOURCE erp SET (type: parquet, location: '/root');` | register a source: `type` (a word for the kind: `parquet`, `csv`, `json`, `shapefile`, …, or `relational_db` for an attached database), `location` (a directory, a URL prefix, or an attached database's name), `via` optional; relative file paths in a recipe resolve under the location for DuckDB's own readers |
| `PREVIEW erp AS $$SELECT … LIMIT 5$$;` | run recipe-shaped SQL at the source, load nothing: the rehearsal |
| `DECLARE RECIPE orders ON fin FROM erp [SET (key: id)] AS $$sql$$;` | land the table the SQL produces, now, as `fin.orders`; a key (`id`, or `'a, b'`) makes every later import merge by it, and the result carries each key once or the load refuses by name; an unchanged re-declaration is a no-op |
| `IMPORT orders;` | run the recipe again over the source as it holds now: a new snapshot of the lake |
| `DECLARE RELATIONSHIP dept_emp.emp_no -> employees.emp_no;` | declare a join edge (`<->` both ways; `a.(x, y) -> b.(x, y)` composite) between stored columns |
| `DECLARE ASPECT name WITH $$json-schema$$ AS FACT\|MEASUREMENT\|VIEW [ON TABLE, COLUMN, DATASET, SOURCE, RELATIONSHIP [WHEN aspect = 'value' \| WHEN TYPE = 'T' \| WHEN TYPE IN ('T', 'U')]];` | add a word; the schema is checked on every body; `ON` is the grain, `WHEN` narrows what is required: to subjects whose sibling aspect says so, or to columns of a data type (the base type, `DECIMAL` for `DECIMAL(18,2)`; `ON COLUMN` alone) |
| `GLOSS aspect ON subject AS $$json$$;` | speak a value into your value; a VIEW aspect is glossed on the dataset (`GLOSS revenue ON fin AS …`) and then serves as the view `fin.revenue` |
| `DELETE FROM glossary WHERE subject = '…' AND aspect = '…';` | take rows out of the catalog for good; what the newest remaining rows say then serves; `main.glossary` and the lake's full name spell the same rows |
| `SELECT … FROM GLOSSARY(subject);` | the collapsed context, one row per subject and aspect |
| `SELECT … FROM GLOSSARY(subject, all => true);` | every value, raw |
| `SELECT … FROM TESTS(subject \| subject::aspect);` | the tests' results |
| `DECLARE FUNCTION f FOR fin\|GLOBAL AS $$sql$$ [RETURNS aspect];` | a measurement (`RETURNS`: a table macro over `subject_column($subject)`) or a detector (no `RETURNS`: runs at read over the test's `slots` with `$threshold`) |
| `SELECT f() FROM orders.amount;` | extract: compute and land the measurement, or serve the stored row while what it read is unchanged (`computed` says which) |
| `DECLARE TEST w ON aspect [BY (AGENT, HUMAN)] [DETECTOR f THRESHOLD x];` | admit actors, wire the scoring |
| `FROM glossql_kit('path-or-url');` | run a kit's declarations, one row each, and land a kits row with the text's hash; run again unchanged, as the opener does at every start-up, the kit holds in one row and only its LOAD lines run; changed, it runs whole |
| `FROM glossql_lineage($$sql$$);` | every output column of a SELECT down to the base scans, `(column, derivation, complete)` |

A body rides `$$…$$` as written: plain double quotes inside, nothing
escaped, closing `}$$;`. A metric definition's `sql` is one line inside its JSON
string, single quotes for its literals. An unquoted name folds to
lowercase; a `"Quoted"` one keeps its case. A declaration cannot change
once anything is glossed on it.

## Subjects

A subject is a bare path, never a string: `orders`, `orders.amount`,
`fin.orders.amount`. Under `USE fin`, one name is the dataset if one
exists by that name, else a declared source, else a table of the bound
dataset; two names are `table.column` when the bound dataset's table
holds that column, else `dataset.table`; three names are
`dataset.table.column`. A dataset-level aspect is read as `GLOSSARY(fin)`
or narrowed as `GLOSSARY(fin::revenue)`. A gloss outside its aspect's
declared grains is refused, never stored. A source's glosses are
workspace rows: the subject is the bare name (`GLOSS conventions ON
erp`), every dataset's `GLOSSARY()` owes and serves them, `GLOSSARY(erp)`
reads the one source, and a gloss from any dataset supersedes.

## Reading live state

- `GLOSSARY(subject)` serves `(subject, aspect, value, band, score,
  state)`; `state` is `current | stale | conflict | missing`; a
  value in conflict is withheld; every gap is a row. `GLOSSARY(subject,
  all => true)` is a different shape: `(subject, aspect, kind, test,
  actor, body, written_at, current)`.
- `TESTS(subject)` serves `(subject, aspect, test, band, score,
  computed_at, current, error)`.
- the store, as views in the lake's `main` schema, bare from any
  dataset in use (`ws.main.*` from elsewhere); every column, nothing else: `datasets (seq, name,
  settings, declared_at)` · `sources (seq, name, settings,
  declared_at)` · `recipes (seq, dataset, table_name, source, settings,
  sql, declared_at)` · `imports (seq, dataset, table_name, mode, rows,
  dropped_rows_count, lineage, snapshot_id, imported_at)` ·
  `relationships (seq, dataset, left_side, op, right_side,
  declared_at)` · `aspects (seq, name, kind, grains, condition, schema,
  declared_at)` · `functions (seq, name, scope, script, returns,
  declared_at)` · `tests (seq, name, aspect, actors, detector,
  threshold, declared_at)` · `glossary (seq, dataset, subject, aspect,
  actor_kind, actor_id, body, snapshot_id, written_at)` ·
  `measurements (seq, dataset, function, subject, aspect, pin, value,
  computed_at, reads)` · `kits (seq, name, location, statements,
  loaded_at)`. History is rows ordered by `seq`; the newest row per key
  is what holds.
- the stored schema: `SELECT table_name, column_name, data_type FROM
  information_schema.columns WHERE table_catalog = 'ws' AND
  table_schema = '<dataset>'`; `DESCRIBE <name>` for one; `SUMMARIZE
  <table>` for the shape of the data.
- plain SELECT over the tables for the data itself; the lake's history
  as `FROM fin.orders AT (VERSION => n)`.

## Measure, then judge

A kit's measurements over-produce evidence; call them before claiming
(`SELECT column_profile() FROM orders.amount;`), cite them in the
gloss's `grounds`, and declare only what the data supports. A question
the measurements settle — what a column holds, a date's grain, stock or
flow, which columns join — is your work. What remains askable is
judgment: definitions, conventions, business meaning, a choice between
readings. Ask it in chat; the person's review decision is stored through their call.

What the words mean, the order of a round, and the claims a metric's
view carries on its columns: the KPI kit's skill (`kits/kpi/SKILL.md`).
Which screens find structure before you declare it, and how to write a
check of your own: the quality kit's (`kits/quality/SKILL.md`).
