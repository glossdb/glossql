# Conformance: SPEC.md against this extension

What of `SPEC.md` this extension implements, what it softens
because DuckDB already has the thing in another shape, what stays outside
the plugin by design, and what is not built yet. The grammar,
`grammar.ebnf`, is implemented in full: every statement and read parses.
The acceptance record is `test/sql/`.

Status words: **holds** as specified; **softened** with the DuckDB shape
named; **outside** the plugin, a wrapper's or a kit's; **not yet**;
**proposed**, a departure written back as a SPEC proposal.

## §3 Sources and datasets

| SPEC | Status | Here |
|---|---|---|
| `DECLARE SOURCE` with `type`, `location`, `via`; other types refused | holds | the SET pairs land as one JSON `settings`, checked at parse against the schema the statement holds (`type` an enum of the four, `location` required, `via` recorded, no other key); secrets are DuckDB's `CREATE SECRET` |
| a relational source executes the recipe in its own dialect | softened | a relational source is a database DuckDB has `ATTACH`ed; the recipe runs over it in DuckDB SQL, pushdown being the scanner's |
| file sources resolve paths under the location | holds | `file_search_path` set to the location for the statement |
| `PREVIEW` rehearses the recipe, load nothing, schema always carried | holds | the result streams with its schema |
| the recipe carries the casts; unchanged re-declaration is a no-op | holds | |
| keyed recipes merge, unkeyed replace | holds | `MERGE INTO` by key, `CREATE OR REPLACE TABLE … AS` |
| one commit per load | holds | a load is one snapshot of the lake; its `imports` row follows as the store's own commit, the lake and its catalog being two attached databases |
| `dropped_rows_count` per import | softened | the recipe is the load, so nothing is lost between source and table; the count is the CSV reader's rejects when the recipe stores them (`store_rejects`), NULL when it ignores errors without storing them, else 0; in the outcome and in `glossql.imports`; which rows is the reader's `reject_errors`, at the source |
| loads versioned, `stale` on re-derived columns | holds | every load is a snapshot and the table's history is the lake's (`AT (VERSION => n)`); a gloss reads `stale` once its column was retyped or dropped, or its table lost one, or the lineage behind its column moved: each load records what computed every column, read off the recipe's bound plan down to the source's columns and their types, and the rule compares the load that held at the write with the newest; a column added or a filter changed re-derives nothing; a source that sends a column under another type moves the leaf under an unchanged recipe |
| `DROP TABLE` refuses while the table holds data or glosses | outside | host DDL is DuckDB's; a wrapper's allowlist |
| the substrate SQL allowlist | outside | a wrapper's |
| `DECLARE DATASET`, `USE`, path resolution | holds | a dataset is a schema of the workspace's lake; `USE ws.fin`, or `USE ws` then `USE fin`, is DuckDB's; a loaded table wins over a same-named dataset |
| derived views closed | softened | views are DuckDB's and count as tables: a view and its columns are subjects, glossed, measured and required like a table and its columns; its definition is the shape its glosses age by and a leg of what is measured over it |

## §4 Subjects and relationships

| SPEC | Status | Here |
|---|---|---|
| subjects: dataset, table, column, pair path, tuple endpoints | holds | |
| tables come from recipes | softened | any table of the schema is a subject, including one made by `CREATE TABLE` |
| an endpoint names a stored column or the declaration is refused | holds | the table and every column of the key, looked up in the catalog; views count |
| relationships detected, verified, declared; no negative form | holds | a RELATIONSHIP gloss takes a declared pair path |

## §5 The glossary

| SPEC | Status | Here |
|---|---|---|
| aspects: name, JSON Schema, kind FACT, VIEW, MEASUREMENT; grains; `WHEN` | holds | `json_schema_check()` over the yyjson DuckDB ships |
| a VIEW aspect's grain is the dataset | holds | a grain clause is refused at parse |
| identical re-declaration no-op; change under glosses refused | holds | |
| every body JSON, dollar-quoted | holds | |
| the metric definition contract is fixed and open; the claims a reader needs are glosses on the view's columns | holds | the contract (`sql` or `stopped`, `assumptions`) validates every VIEW gloss, then each aspect flavored `x-kind: grounding` a kit declares; a reader of the view takes the period, the measure and its `behavior`, the slices and the row identity off the view's columns (fixture 28) |
| a metric definition is a view; a stop ends it; a loaded table's name refused | holds | `CREATE OR REPLACE VIEW` in the dataset's schema; the same sql glossed again keeps the view and its version, so the glosses and measurements on its columns stand |
| `read.<metric>()` | softened | the view is reached by its bare name under `USE` or as `FROM dataset.metric`, which §5 says plans the same way; no `read` schema, since a schema is lake-wide and metrics are per dataset, and it would be a second interface to the one view |
| supersession key (subject, aspect, actor kind) | holds | the newest value per key serves |
| two VIEW glosses of one aspect on different tables coexist | softened | the subject of a VIEW gloss is the dataset, as §5.2 also says; one serving view per metric |
| `DELETE FROM glossary` | holds | the grammar's statement, `DELETE FROM [lake.][main.]glossary [alias] [WHERE …]`: the lake reads the store as views, and DuckDB deletes through a table only, so it runs on the store's table in the lake's metadata catalog, where every write goes, and comes back as a DELETE does, the rows removed; the reads' version moves with it |
| `snapshot_id`, `stale` | holds | every glossary row carries the lake's snapshot at write, NULL for a dataset subject or a pair path; staleness is a join against `ducklake_column`, and against `ducklake_view` for a view or its column, never a guess |
| `GLOSSARY()` collapsed and raw; `missing`, `conflict`, `current` | holds | `glossql_glossary()` plans a subquery over the store |
| `stale` | holds | served and marked: a gloss against its subject's shape, a function value against what it read |

## §6 The function library

| SPEC | Status | Here |
|---|---|---|
| `DECLARE FUNCTION FOR dataset or GLOBAL AS body [RETURNS aspect]` | holds | |
| a measurement is one read-only SQL query; `$subject`, `subject_column()` | holds | a table macro in the dataset's schema; callable by any SQL |
| the shape rule: scalar, object, array | holds | |
| output validated against the aspect's schema at extraction | holds | |
| a FACT aspect returned by a function is a value | holds, untested | a measurement row serves as actor `function` |
| extraction is stored in `measurements` with `pin` and `reads`; served while unchanged | holds | `reads` is read off the body's bound plan: lake tables as `dataset.table`, store relations as `glossql.relation`, `**` for a scan that has no name (a file, a catalog function, another database), empty for a body that reads nothing; `pin` is the glossql engine's: every leg at its version, sorted, `name:version`, a table's version being the last snapshot of the lake's change log that inserted, deleted, altered or dropped it (compaction is not a write), a relation's its highest `seq`; the next extraction serves the newest row while the legs its reads name match, `**` while every leg does, and never from a body the function has since re-declared; the row says `computed`; `ducklake_expire_snapshots` can lower a version it never touched, which reads as a move |
| a top-level `summary` serves at extraction | holds | the whole value is stored and reads back through `GLOSSARY(subject::aspect)` |
| a detector plans over `slots`, `$threshold` bound, test-results contract per row | holds | run at read by `TESTS()` |

## §7 Tests

| SPEC | Status | Here |
|---|---|---|
| `BY`, `DETECTOR`, `THRESHOLD`; one of the clauses; `BY` refused on MEASUREMENT | holds | |
| the test-results schema; result per test; detectors at read; `TESTS()` sweeps the dataset | holds | |
| a detector that cannot answer serves `error` with the text | holds | the detector runs at read on a second connection; a plan or run it refuses, or a row outside the test-results schema, is the result `error` with the failure's first line, one row per subject holding values; the collapse never withholds on it |
| `current` on a result | holds | every function value the result read holds at the read's pin; the detector sees `current` per value |
| judgment never written into data | holds | results are computed at read, never stored |

## §8 Skills, and the actor

| SPEC | Status | Here |
|---|---|---|
| agents learn through skills, the interface tells | outside | a kit ships its `SKILL.md`; the interface is the wrapper's (`README.md`: the CLI and MCP interfaces) |
| the actor comes from the interface | softened | `SET VARIABLE glossql_actor` is the kind, `glossql_actor_id` the writer, both set by the wrapper on the actor's connection; a gloss is stored with `actor_kind` and `actor_id` |

## Beyond SPEC

The workspace is an attached DuckLake, one per connection (`SET VARIABLE
glossql_workspace` picks among several). `PRAGMA glossql` makes the
store: the relations as `glossql_<relation>` tables in the lake's metadata
catalog, beside the `ducklake_*` tables, and the views `ws.main.<relation>`
that read them typed. The columns are the extension's own, sharpened to what
the reads need: a declaration's SET pairs are one JSON `settings`, an
aspect's WHEN one JSON `condition`, a gloss and a relationship carry their
`dataset`, an import its `mode`, every row its `seq` and a timestamp.
`glossql_kit(path)` loads a kit and records it in `glossql.kits`.
`json_schema_check(schema, document)`, `subject_column(subject)` and
`dataset_cells(subject[, views])`, a dataset's or a table's cells as one
relation, are plain functions. `glossql_lineage(sql)` serves the derivation of each
column of a SELECT as the session would plan it, a leaf being the scan,
the column and its type; an imports row carries the same for its
load as `lineage`, so the row-to-row diff is the drift record. A measurement is a table macro any SQL may call, its one
parameter spelled `"$subject"`. An extraction's row carries `computed`,
false when the stored row served. None of these is grammar; each is a
DuckDB function the language's statements plan into.
