# glossql

The [glossql](https://github.com/glossdb/glossql) context language as a
DuckDB 2.0 extension, built on the PEG parser's grammar extension API.

Status: every statement and read of `grammar.ebnf` parses and runs; the semantics of `SPEC.md` are implemented where DuckDB
has the primitive: the section-by-section account is `CONFORMANCE.md`.
`SPEC.md`, `grammar.ebnf` and the corpus (`corpus/`: transcriptions of real
artifacts, the language's record) live here with the parser;
`test/sql/glossql_corpus.test` holds every block of the record to the grammar.
The workspace is a DuckLake: datasets are its schemas, landings its
snapshots, and the store lives in its catalog, so a gloss carries the
snapshot it was written at,
a measurement the versions of what it read, and each reads `stale` once
that changed. The core ships no vocabulary; a kit does (`kits/`: KPI,
quality and spatial, loaded with `glossql_kit()`). The research behind it,
with what each round
confirmed, is `duckdb-2.0-research.md`. The suites under `test/sql/` are
the acceptance record (fourteen suites, 1453 assertions).

```sql
LOAD glossql;
PRAGMA glossql;                                   -- the grammar for this connection, the store where missing   -- opt-in per connection
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');   -- the workspace is a DuckLake
USE ws;
PRAGMA glossql;                            -- the store: glossql_* tables in the lake's catalog, read as ws.glossql.*

DECLARE DATASET fin SET (purpose: 'working-capital analysis');
USE fin;                                        -- a dataset is a schema of the lake; DuckDB's USE binds it
DECLARE SOURCE erp SET (type: parquet, location: 's3://lake/erp');
PROBE erp AS $$SELECT id, amount FROM read_parquet('orders/*.parquet') LIMIT 5$$;
DECLARE RECIPE orders ON fin FROM erp SET (key: id)
  AS $$SELECT id::INTEGER AS id, amount::DOUBLE AS amount FROM read_parquet('orders/*.parquet')$$;
IMPORT orders;                                  -- the recipe runs again; keyed tables merge; every landing is a snapshot

DECLARE ASPECT behavior WITH $${"type": "object", "properties": {"value": {"enum": ["stock", "flow"]}}}$$ AS FACT ON COLUMN;
DECLARE FUNCTION behavior_entropy FOR fin AS $$ SELECT subject, ... AS band, ... AS score FROM slots GROUP BY subject $$;
DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN) DETECTOR behavior_entropy THRESHOLD 0.7;

GLOSS behavior ON orders.amount AS $${"value": "flow"}$$;
SET VARIABLE glossql_actor = 'human';           -- the actor rides the connection
GLOSS behavior ON orders.amount AS $${"value": "stock"}$$;

SELECT * FROM GLOSSARY(orders);                 -- (subject, aspect, value, band, score, state): current | contested | stale | unassessed
SELECT * FROM ATTEST(orders.amount::behavior);  -- the standard attest schema
SELECT * FROM GLOSSARY(orders.amount, all => true);

DECLARE ASPECT min_max WITH $${"type": "object", "required": ["min", "max"]}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
SELECT min_max() FROM orders.amount;            -- extraction lands a measurement with what it read; a repeat serves it while that is unchanged
FROM glossql.min_max('fin.orders.amount');      -- the same function, as the table macro it is

DECLARE ASPECT revenue WITH $${"title": "revenue"}$$ AS QUERY;
GLOSS revenue ON fin AS $${"sql": "SELECT sum(amount) AS revenue FROM fin.orders"}$$;
FROM fin.revenue;                               -- a grounding is a view, and a table of the dataset
GLOSS behavior ON revenue.revenue AS $${"value": "flow"}$$;  -- its columns are subjects: a reader of the numbers finds its words there
```

The session above declares its own words. A fresh workspace takes them
from a kit instead, and the kit's `SKILL.md` teaches the actor what they
mean:

```sql
FROM glossql_kit('kits/kpi/kit.sql');           -- vocabulary, functions, witnesses: one row per statement
FROM glossql_kit('kits/quality/kit.sql');       -- the screens over a dataset's cells, after the KPI kit
```

## How glossql maps onto DuckDB

| glossql | DuckDB |
|---|---|
| statements (`DECLARE …`, `GLOSS`, `PROBE`, `IMPORT`, `DELETE FROM glossary`) | rules prepended to `Statement`; each transform builds an `ExtensionStatement` whose plan is one table function |
| `DELETE FROM [lake.][glossql.]glossary [WHERE …]` | the same, run on the store's table in the lake's metadata catalog: `glossql.glossary` is a view over it, and DuckDB deletes through a table only; the result is a DELETE's row count, and the reads' version moves with it |
| `GLOSSARY()`, `ATTEST()` | rules prepended to `InnerTableRef`, transformed into table-function references whose `bind_replace` serves the dataset's rows, kept by the instance in DuckDB's object cache under the store's version and computed again on the second connection when the store or the lake moved (`glossql_cached`), narrowed to the subject and aspect in SQL |
| `SELECT f() FROM subject` (extraction) | the same rule shape as plain SQL; a custom terminal matcher marks the end of the statement, and the binder decides: an extraction when `f` is declared, the plain SQL it looks like otherwise |
| the workspace | an attached DuckLake, one per connection (`SET VARIABLE glossql_workspace` picks among several) |
| a dataset | a schema of the lake; `USE` is DuckDB's own; unprefixed subjects resolve against the connection's default schema |
| a source | a directory or URL prefix (file sources, resolved through `file_search_path`) or a database DuckDB has `ATTACH`ed (postgres, mysql, sqlite, duckdb, …); its SET pairs, like a dataset's and a recipe's, land as one JSON `settings`, checked at parse against the schema the statement holds; as a subject it is its bare name, a workspace row every dataset's read owes and serves |
| a recipe, an import | `CREATE OR REPLACE TABLE … AS` in the lake at the source, one snapshot per landing; a keyed import is `MERGE INTO`; the table's history is the lake's (`AT (VERSION => n)`) |
| `snapshot_id`, `stale` | a gloss carries the lake's snapshot at write; the read joins the lake's column history and marks the slot stale once its column was retyped or dropped, or its table lost one, or the lineage behind its column moved between the landing that stood at the write and the newest; a function voice is stale once what it read moved |
| lineage | `glossql_lineage(sql)`: every output column of a SELECT resolved through its bound plan, before any optimizer, down to the base scans, a leaf being `scan.column:type`; a landing records its recipe's lineage per column on the imports row, so the row-to-row diff is the drift record of what a source sends |
| a measurement's `pin`, `reads` | `reads` names what the body's bound plan scans, lake tables as `dataset.table` and store relations as `glossql.relation`, `**` when a scan cannot be named (a file, a catalog function, another database); `pin` is every leg at its version, a table's the last snapshot of the lake's change log that wrote it, a relation's its highest `seq`; the next extraction serves the row while the named legs match, and `computed` says which happened |
| a measurement function | a table macro in the dataset's schema (`glossql` for GLOBAL); `$subject` is its parameter, spelled `"$subject"` so no column of the body collides with it |
| a detector | a stored SQL body ATTEST runs over the witness's `slots`, `$threshold` bound |
| `$subject`, `$threshold`, the actor | DuckDB 2.0 session variables (`SET VARIABLE`); `glossql_actor` is the kind supersession keys on, `glossql_actor_id` the writer, and a gloss lands with both |
| JSON Schema admission | `json_schema_check(schema, document)`, a scalar function over the yyjson DuckDB ships |
| a dataset's cells | `dataset_cells(subject[, views])`: every scalar column of a dataset's tables, or of one table, as one relation (`table`, `column`, `ci`, `column_type`, `rn`, `val`), generated at bind like `subject_column`; a screen that reads a whole dataset is SQL over it (`kits/quality/`) |
| a dataset's shape | `glossql_columns(subject[, views])`: every column of a dataset's tables, or of one table, and of its views unless `views` is false (`table_name`, `column_name`, `column_type`, `ordinal_position`, `table_type`), the dataset's own relations walked by name — where `information_schema` binds every view of every catalog for its columns; what the reads' owed subjects, the cells and the kits read a dataset's shape from |
| a grounding | a view in the dataset's schema, and a table of the dataset: its columns are subjects, and what a reader of its rows needs is glossed on them; the body meets the grounding contract (`sql` or `stopped`, `assumptions` disclosed), then any schema a kit declared for a body key of its own, an aspect flavored `x-kind: grounding` |
| a kit | a script of declarations; `glossql_kit(path)` runs it statement by statement on a second connection and serves one row each; the path is anything DuckDB's file system opens, a URL included once `httpfs` is loaded |
| the store | `glossql_<relation>` tables in the lake's metadata catalog, beside the `ducklake_*` tables; the columns are the extension's own, sharpened to what the reads need, with the glossql engine's record as the teacher: settings and conditions as JSON, `dataset` on a gloss and a relationship, `mode` on an import, a supersession key per declaration relation; history is rows ordered by `seq`, the reads collapse; `ws.glossql.<relation>` reads each as a typed view |

Source: `src/glossql_grammar.cpp` (rules and transforms), `src/glossql_store.cpp`
(the store, admission, the reads), `src/glossql_runner.cpp` (statements that
run SQL, on a second connection, and the kit loader), `src/glossql_schema.cpp`
(the validator), `src/glossql_lineage.cpp` (the plan walk). Kits: `kits/README.md`; the KPI kit's words: `kits/kpi/SKILL.md`; the
quality kit's screens: `kits/quality/SKILL.md`; the spatial kit's:
`kits/spatial/SKILL.md`.

Not built on purpose: the `read.` prefix. A grounding is its view,
reached by the bare name under `USE` or as `dataset.metric`, which SPEC
§5 says plans the same way; a lake-wide `read` schema would be a second
door to it.

## The CLI door

Claude Code, the released `duckdb` and this extension are a door: no
wrapper, no server. A workspace is a directory holding an opener,
`glossql.sql`, the lake's catalog `ws.ducklake` and its data `ws/`. A call
is one `duckdb` process run there, the opener first, the statements on
stdin:

```sh
duckdb -unsigned -bail -init glossql.sql <<'SQL'
USE employees;
SELECT * FROM GLOSSARY(dept_emp) WHERE state <> 'current';
SQL
```

The opener loads the extension, attaches the lake and runs `PRAGMA glossql`
silently (`skills/glossql/SKILL.md` has it). The CLI renders for a coding
agent when it finds one in the environment: markdown tables, a hash
footer, JSON errors on stderr (`.help agent`). `skills/glossql/SKILL.md`
teaches an agent the door and the language on DuckDB, `kits/kpi/SKILL.md`
the KPI kit's words; both are linked under `.claude/skills/`, so a Claude
Code session in this repository has them, and a project of your own
copies or links the two directories into its `.claude/skills/`.

## The MCP door

The workspace behind one tool, on one connection. `examples/mcp/glossql_mcp.py`
is a stdio MCP server that opens the workspace through the `duckdb`
package once, running the opener's SQL (its dot commands skipped), and
keeps that connection for its life. Its one tool, `glossql(statements)`,
runs each call's statements there and renders what each returns: a
markdown table with `name:TYPE` headers per read, the outcome line per
write; a refused statement is the tool error, what landed before it
above the refusal. `USE`, variables and the actor persist from call to
call, as in any session. The two skills are its resources
(`skill://glossql/SKILL.md`, `skill://glossql-kpi/SKILL.md`) and its prompts,
the opener a resource. Nothing else: no brief, no auth, the lake is the
state.

```sh
uv run examples/mcp/glossql_mcp.py /path/to/workspace         # fastmcp and duckdb 2.0.0, pinned in the file
claude mcp add glossql -- uv run --quiet "$PWD/examples/mcp/glossql_mcp.py" /path/to/workspace
uv run --with pytest --with fastmcp pytest -q test/python    # the door's integration test, on a workspace it makes
```

`GLOSSQL_WORKSPACE` stands in for the argument.

## Building

```sh
git submodule update --init --depth 1      # duckdb at v2.0.0, extension-ci-tools
brew install croaring                       # ducklake's one dependency; the pipeline takes it from vcpkg.json
GEN=ninja make                              # build/release/duckdb, build/release/extension/glossql/, ducklake beside it
./build/release/test/unittest "test/sql/glossql*"
./build/release/duckdb -unsigned           # LOAD 'build/release/extension/glossql/glossql.duckdb_extension'
make lint-tools                            # once: a project .venv with clang-format and clang-tidy, pinned (uv)
GEN=ninja make lint                         # the pipeline's format and tidy checks plus unused includes; `make lint-fix` applies the formatter
```

DuckLake is built with the extension, from the commit DuckDB 2.0.0 pins,
as a loadable in the build's extension repository, where the test runner
looks. The Makefile names the build `v2.0.0`, the release the submodule
pins, so the released `duckdb` loads the loadable; a build named after a
commit is refused by it.

The extension is a C++ (ABI `CPP`) extension: it is built per DuckDB
release, like every grammar-level extension, because the grammar API is C++
only (`duckdb_v2.h` exposes a tokenizer, not the grammar). The CLI parses a
`-c` batch before executing it, so the grammar setting applies from the next
batch; stdin and the test runner go statement by statement.
