# How glossql maps onto DuckDB

Each part of the language, and the DuckDB mechanism it is built on.

| glossql | DuckDB |
|---|---|
| statements (`DECLARE …`, `GLOSS`, `PREVIEW`, `IMPORT`, `DELETE FROM glossary`) | rules prepended to `Statement`; each transform builds an `ExtensionStatement` whose plan is one table function |
| `DELETE FROM [lake.][main.]glossary [WHERE …]` | the same, run on the store's table in the lake's metadata catalog: `main.glossary` is a view over it, and DuckDB deletes through a table only; the result is a DELETE's row count, and the reads' version moves with it |
| `GLOSSARY()`, `TESTS()` | rules prepended to `InnerTableRef`, transformed into table-function references whose `bind_replace` serves the dataset's rows, kept by the instance in DuckDB's object cache under the store's version and computed again on the second connection when the store or the lake moved (`glossql_cached`), narrowed to the subject and aspect in SQL |
| `SELECT f() FROM subject` (extraction) | the same rule shape as plain SQL; a custom terminal matcher marks the end of the statement, and the binder decides: an extraction when `f` is declared, the plain SQL it looks like otherwise |
| the workspace | an attached DuckLake, one per connection (`SET VARIABLE glossql_workspace` picks among several) |
| a dataset | a schema of the lake; `USE` is DuckDB's own; unprefixed subjects resolve against the connection's default schema |
| a source | a directory or URL prefix (file sources, resolved through `file_search_path`) or a database DuckDB has `ATTACH`ed (postgres, mysql, sqlite, duckdb, …); its SET pairs, like a dataset's and a recipe's, land as one JSON `settings`, checked at parse against the schema the statement holds; as a subject it is its bare name, a workspace row every dataset's read owes and serves |
| a recipe, an import | `CREATE OR REPLACE TABLE … AS` in the lake at the source, one snapshot per load; a keyed import is `MERGE INTO`; the table's history is the lake's (`AT (VERSION => n)`) |
| `snapshot_id`, `stale` | a gloss carries the lake's snapshot at write; the read joins the lake's column history and marks the value stale once its column was retyped or dropped, or its table lost one, or the lineage behind its column moved between the load current at the write and the newest; a function's value is stale once what it read moved |
| lineage | `glossql_lineage(sql)`: every output column of a SELECT resolved through its bound plan, before any optimizer, down to the base scans, a leaf being `scan.column:type`; a load records its recipe's lineage per column on the imports row, so the row-to-row diff is the drift record of what a source sends |
| a measurement's `pin`, `reads` | `reads` names what the body's bound plan scans, lake tables as `dataset.table` and store relations as `glossql.relation`, `**` when a scan cannot be named (a file, a catalog function, another database); `pin` is every leg at its version, a table's the last snapshot of the lake's change log that wrote it, a relation's its highest `seq`; the next extraction serves the row while the named legs match, and `computed` says which happened |
| a measurement function | a table macro in the dataset's schema (`glossql` for GLOBAL); `$subject` is its parameter, spelled `"$subject"` so no column of the body collides with it |
| a detector | a stored SQL body TESTS runs over the test's `slots`, `$threshold` bound |
| `$subject`, `$threshold`, the actor | DuckDB 2.0 session variables (`SET VARIABLE`); `glossql_actor` is the kind supersession keys on, `glossql_actor_id` the writer, and a gloss lands with both |
| JSON Schema admission | `json_schema_check(schema, document)`, a scalar function over the yyjson DuckDB ships |
| a dataset's cells | `dataset_cells(subject[, views])`: every scalar column of a dataset's tables, or of one table, as one relation (`table`, `column`, `ci`, `column_type`, `rn`, `val`), generated at bind like `subject_column`; a screen that reads a whole dataset is SQL over it (`kits/quality/`) |
| a dataset's shape | `glossql_columns(subject[, views])`: every column of a dataset's tables, or of one table, and of its views unless `views` is false (`table_name`, `column_name`, `column_type`, `ordinal_position`, `table_type`), the dataset's own relations walked by name — where `information_schema` binds every view of every catalog for its columns; what the reads' owed subjects, the cells and the kits read a dataset's shape from |
| a metric definition (a `VIEW` aspect's value) | a view in the dataset's schema, and a table of the dataset: its columns are subjects, and what a reader of its rows needs is glossed on them; the body meets the definition's contract (`sql` or `stopped`, `assumptions` disclosed), then any schema a kit declared for a body key of its own, an aspect flavored `x-kind: grounding` |
| a kit | a script of declarations; `glossql_kit(path)` runs it statement by statement on a second connection and serves one row each, writing a kits row with the text's hash; run again unchanged it holds, one row and only its LOAD lines, since what it created is in the lake; the path is anything DuckDB's file system opens, a URL included once `httpfs` is loaded |
| the store | `glossql_<relation>` tables in the lake's metadata catalog, beside the `ducklake_*` tables; the columns are the extension's own, sharpened to what the reads need, with the glossql engine's record as the teacher: settings and conditions as JSON, `dataset` on a gloss and a relationship, `mode` on an import, a supersession key per declaration relation; history is rows ordered by `seq`, the reads collapse; `ws.main.<relation>` reads each as a typed view, bare from any dataset in use, since the lake's `main` is on every search path |

Source: `src/glossql_grammar.cpp` (rules and transforms), `src/glossql_store.cpp`
(the store, admission, the reads), `src/glossql_runner.cpp` (statements that
run SQL, on a second connection, and the kit loader), `src/glossql_schema.cpp`
(the validator), `src/glossql_lineage.cpp` (the plan walk). Kits: `kits/README.md`; the KPI kit's words: `kits/kpi/SKILL.md`; the
quality kit's screens: `kits/quality/SKILL.md`; the spatial kit's:
`kits/spatial/SKILL.md`.
