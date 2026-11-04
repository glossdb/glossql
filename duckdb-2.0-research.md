# glossql on DuckDB 2.0 — research note

Scope: can the glossql grammar (`../glossql/grammar.ebnf`, `SPEC.md`) be
implemented as a DuckDB 2.0 extension, and what would the rest cost.
Facts below are read from the DuckDB source at tag `v2.0.0`
(`d54cbbe3df`) and checked against the local `duckdb` 2.0.0 binary.
Nothing here was built; the build spike is the proposed next step.

## 1. The extension mechanism as shipped

| Piece | Where (v2.0.0 source) | What it is |
|---|---|---|
| `GrammarExtension` | `src/include/duckdb/parser/grammar_extension.hpp` | name + description + `GetChanges() -> vector<GrammarChange>`; registered with `GrammarExtension::Register(DatabaseInstance&, shared_ptr<…>)` from a C++ extension entry (`DUCKDB_CPP_EXTENSION_ENTRY`). Merged in PR #24919 (2026-09-04). |
| `GrammarChange` | `src/include/duckdb/parser/grammar_change.hpp` | `AddRule(peg_text, transform, suppress_suggestions)`, `AddChoice` / `PrependChoice(rule, choice, cursor)`, `RemoveChoice`, `ReplaceChoice`, `ReplaceRule`, `SetTransformProcess`, `AddTerminalRuleOverride(rule, matcher_factory)` for custom matchers. |
| The grammar | `src/parser/peg/grammar/statements/*.gram` (41 files), inlined as `inlined_grammar.gram` | Every rule of DuckSQL is addressable by name: `Statement`, `SelectAtom`, `InnerTableRef`, `TableFunction`, `Expression`, `ReservedKeyword`, `UnreservedKeyword`, … |
| Activation | `src/common/settings.json` (`active_grammar_extensions`, scope **local**), `duckdb_grammar_extensions()` | Opt-in per connection: `SET active_grammar_extensions = ['glossql']`. Grammar is compiled once per distinct active set and cached. Opt-in exists because PEG ordered choice lets extensions conflict (PR text). |
| Transform | `src/include/duckdb/parser/peg/transformer/peg_transformer.hpp`, `transform_result.hpp` | A rule's transform is `arena_ptr<TransformProcess>(PEGTransformer&, ParseResult&)`. Simplest form: `MakeProcess<FinalizeTransformProcess>(…, FinalizeFn)`; `FinalizeFn` walks the `ParseResult` tree and calls `transformer.Transform<T>(child)` to reuse DuckDB's own transforms (`Expression` → `ParsedExpression`, `TableRef`, `SelectStatement`, `GroupByNode` …). A `Statement` alternative returns `MakeResult<unique_ptr<SQLStatement>>`. Result types carry stable names so this works across the loadable-extension boundary. |
| Keywords | `matcher/keyword_matcher.hpp:75`, `create_table.gram:76` | A quoted literal in a rule matches token text case-insensitively; `PlainIdentifier <- !ReservedKeyword …`, so a new word stays usable as an identifier unless added to `ReservedKeyword`. Declare category with `AddChoice("UnreservedKeyword", "'GLOSS'")`. |
| Tokenizer | `src/parser/peg/tokenizer/base_tokenizer.cpp:341-371` | `$$…$$` and `$tag$…$tag$` are native string tokens. `->`, `<->`, `=>`, `::` are single operator tokens. `$name` is a `ColLabelParameter` and resolves to a `SET VARIABLE` in 2.0. |

Production users in 2.0.0: the CLI's own `shell` grammar extension
(`tools/shell/shell_grammar_extension.cpp`, bare `tbl` / expression
statements, activated by the CLI with `SET active_grammar_extensions`),
and the CONNECT passthrough dialect (`passthrough_dialect.hpp`). In-tree
loadable example: BigQuery pipe syntax, `test/extension/loadable_grammar_extension_demo.cpp`,
loaded with `LOAD` in `test_loadable_grammar_extension.test`.

Two older mechanisms still exist beside it (`parser_extension.hpp`):
`ParserExtension.parse_function` is now a token-stream fallback that runs
only after PEG fails on a statement; `parser_override` replaces the whole
parser. `DialectExtension` (`peg/dialect_extension.hpp`) is a named
dialect with its own tokenizer/keyword helper/grammar changes, selected
by `current_dialect`.

## 2. Executing a new statement

A grammar transform must produce a `unique_ptr<SQLStatement>`. Two vehicles:

- **`ExtensionStatement`** (`statement/extension_statement.hpp`,
  `planner/binder/statement/bind_extension.cpp`): the binder calls the
  extension's `plan_function(info, context, parse_data)`, which returns a
  `TableFunction` + parameters + `modified_databases` +
  `requires_valid_transaction` + `return_type`, planned as a scan of that
  table function. The executor is an ordinary table function with
  `ClientContext`: catalog, transaction, settings. The public ctor takes a
  `ParserExtension` + parse data; `parse_function` may stay null. Building
  it from a grammar transform is not demonstrated in-tree — to verify in
  the spike.
- **Desugar to stock AST**: e.g. a `CallStatement`
  `CALL glossql_gloss('unit', 'orders.amount', '{…}')`. Simpler, but no
  control over statement properties.

## 3. glossql syntax against the stock 2.0 grammar (empirical)

Run with `duckdb` 2.0.0; "catalog" = parsed, failed only at binding.

| glossql form | stock 2.0 |
|---|---|
| `GLOSSARY(orders.amount)`, `GLOSSARY(orders.amount::unit)`, `GLOSSARY()` | catalog |
| `GLOSSARY(orders.customer_id -> customers.id)`, `… <-> …` | catalog |
| `ATTEST(fin::dso)`, `read.dso()` | catalog |
| `SELECT outliers() FROM orders.amount` (extract) | catalog (schema `orders`) |
| `$${"value": "EUR"}$$`, `$g$…$g$` | ok |
| `SET VARIABLE subject = …; … subject_column($subject)` | ok |
| `DELETE FROM glossary WHERE …`, `USE fin` | catalog (DuckDB's own USE) |
| `GLOSSARY(orders.amount, all => true)` | **parser error** — `all` is reserved; `NamedFunctionArgument` needs a `TypeFuncName` |
| `DECLARE …`, `GLOSS …`, `PROBE …`, `IMPORT segments`, `IMPORT fin.segments` | **parser error** — need grammar rules |

Note: stock 2.0 still binds an unquoted `orders.amount` argument as the
string `'orders.amount'`, but as deprecated behaviour behind
`table_function_identifier_conversion`. Not a base to build on.

## 4. How the grammar would map

- **Statements** (`DECLARE SOURCE/RECIPE/DATASET/RELATIONSHIP/ASPECT/FUNCTION/WITNESS`,
  `GLOSS`, `PROBE`, `IMPORT`): one `AddRule` each + `PrependChoice("Statement", …)`.
  `IMPORT` must exclude DuckDB's `IMPORT DATABASE` (`'IMPORT' !'DATABASE' …`).
  Bodies are the native dollar-string token. Subjects and pair paths are
  own rules over `Identifier`, `'.'`, `'->'`, `'<->'`.
- **Extract** (`SELECT f(), g() FROM subject`): a prepended `Statement`
  alternative with the same whole-production constraint glossql uses
  today (`&(';' / EndOfInput)`; PEG `&`/`!` predicates are in use in the
  grammar). Same ambiguity as today for `count()`-style calls.
- **Reads** (`GLOSSARY`, `ATTEST`): `PrependChoice("InnerTableRef", "GlossaryRead")`
  with a rule that parses the subject/`::aspect`/`all => true` itself and
  transforms into a `TableFunctionRef` with constant string arguments, so
  the stock binder never sees a column reference. `read.<metric>()`,
  `metric_series()`, `misfit.<frame>()` are ordinary table-function syntax;
  groundings as views + `CREATE MACRO read.dso() AS TABLE …` would make
  `read.dso()` a catalog object, no grammar needed. `bind_replace`
  (`table_function.hpp:347`) returns a `TableRef` and gives plan-time
  expansion with nesting.
- **`USE`**: if a dataset is a DuckDB schema, DuckDB's `USE` does the job and
  the extension reads the search path; otherwise a prepended rule shadows
  DuckDB's `USE` for every connection with the extension active. Design
  decision, not a mechanism question.
- **Autocomplete** comes with the rules (unless `suppress_suggestions`).
  `enable_legacy_parser = true` bypasses all grammar extensions.

## 5. What is not there

- **No C API for the grammar.** `duckdb_v2.h` (16.5k lines) exposes only
  `duckdb_v2_tokenize_sql` + token types (unstable). Grammar changes,
  transforms and the AST (`SQLStatement`, `TableRef`, `ParsedExpression`)
  are C++ with `std::function` and `arena_ptr`. The extension is ABI type
  `CPP`, built per DuckDB version. Nothing named "AST SDK" exists; the
  AST is those classes plus `json_serialize_sql` (SELECT only).
- The API is labelled preview in the 2.0 blog; the PR keeps `ReplaceRule`
  under question and plans `<extension>:<grammar>` namespacing.

## 6. Beyond the grammar: what glossql needs from the host

| glossql today (DataFusion, Rust, ~41k lines src) | DuckDB equivalent |
|---|---|
| parser crate, ~715 lines + sqlparser tokenizer | ~2–3k lines C++ of rules + transforms + plan functions (estimate) |
| `RelationPlanner` + async pre-pass serving `GLOSSARY`, store relations, pinned tables as `MemTable`/`ViewTable` (`session/src/reads.rs`, `prepass.rs`) | table functions with `bind`/`bind_replace`; bind has `ClientContext`, so no compute-ahead pass is needed |
| store relations in SQLite/Postgres via sqlx, DuckLake-shaped catalog + parquet | DuckDB tables in an extension schema; landed tables as DuckLake (reference impl ships in 2.0); pinned reads = `AT (VERSION => n)` |
| `$subject` AST substitution, `$threshold` param | `SET VARIABLE` + `$name` (native) |
| recipes at relational sources over ADBC | `postgres`/`mysql` ATTACH or 2.0 `CONNECT` pushdown |
| JSON Schema validation (jsonschema crate) | not in DuckDB; vendor a C++ validator |
| session crate: cube, misfit, forecast (candle), search, detectors — 23k lines | **the real port cost**; or stays a client process driving DuckDB |
| doors: MCP, Arrow IPC HTTP, app, skills | outside DuckDB either way; actor as a session variable the extension reads |

## 7. Spike results (2026-10-28)

Built as a loadable C++ extension on the v2.0.0 extension template
(`src/glossql_grammar.cpp`, `src/glossql_store.cpp`; suite
`test/sql/glossql.test`, 73 assertions, all passing; the same statements run
through the CLI with `LOAD`). Confirmed:

- **Statements through `ExtensionStatement`.** A `Statement` alternative's
  transform may construct an `ExtensionStatement` directly; the binder calls
  the `plan_function`, which returns a table function that appends through
  `InternalAppender` inside the statement's transaction. `DECLARE ASPECT`,
  `DECLARE RELATIONSHIP`, `GLOSS` run this way; CHECK constraints and the
  json extension's cast refuse bad rows (`$$not json$$` → Malformed JSON).
- **Reads through `InnerTableRef`.** `GLOSSARY(subject[::aspect][, all => true])`
  is prepended to `InnerTableRef` and transformed into a `TableFunctionRef` to
  `glossql_glossary(...)`, whose `bind_replace` plans a subquery over the
  store. Aliases, joins, filters, aggregates compose; `json_serialize_sql`
  shows the desugared call. `all => true` parses although `all` is reserved,
  because the rule owns it.
- **Subjects and pair paths** parse as their own rules: `orders.amount`,
  `orders.customer_id -> customers.id`, `invoices.order_id <-> orders.id`,
  `txn.(business_id, account) -> coa.(business_id, code)`; `->`, `<->`, `::`,
  `=>` are single operator tokens, as read off the tokenizer.
- **Dollar-quoted bodies** arrive unquoted from the `StringLiteral` transform.
- **New words stay identifiers**: `gloss`, `aspect`, `glossary`, `fact` as
  column and table names, and the JSON `->` operator, are untouched.
- **Per-connection opt-in** behaves as documented: before
  `SET active_grammar_extensions = ['glossql']` and after `RESET`, a glossql
  statement is a parser error; `duckdb_grammar_extensions()` lists it.
- **The actor rides the connection** as `SET VARIABLE glossql_actor`; the
  collapsed read picks human over agent, newest first.

Two parse-tree facts the demos do not show: a rule whose body is one macro
call (`R <- Parens(List(X))`) wraps the macro's list in a list of its own,
and `X*` arrives as `Optional(Repeat)` while `X+` is a bare `Repeat`.
`CALL` invokes table functions only; a pragma query function
(`PRAGMA glossql`) runs the multi-statement DDL. The CLI parses a `-c`
batch before executing it, so the grammar setting applies from the next
batch; stdin and the test runner go statement by statement.

Not covered by the spike: `legacy_parser` interplay (not built), the
remaining statements of `grammar.ebnf` (same mechanisms), JSON Schema
validation of bodies, and the dataset/`USE` design question of §4.

## 8. Round two (2026-10-28): the whole grammar

Every statement and read of `grammar.ebnf` now parses and runs (five
suites, 389 assertions; README has the mapping). What this round added to
the record about the mechanism:

- **No predicates.** The grammar's expression language has LITERAL,
  REFERENCE, FUNCTION_CALL, SEQUENCE, CHOICE, OPTIONAL, REPEAT,
  OPTIONAL_REPEAT and REGEX (`peg/peg_parser.hpp`); `!ReservedKeyword` is
  a special case, and a `!X` in a rule definition is silently mis-parsed.
  An end-of-statement lookahead is a custom terminal matcher
  (`AddTerminalRuleOverride`, an `AtomicMatcher` that succeeds without
  consuming), which is what the extract rule uses.
- **`ExtensionStatement` plans one scan.** `Binder::Bind(ExtensionStatement&)`
  casts the plan to `LogicalGet`, so a `bind_replace` that returns a
  subquery is unusable there. A statement whose meaning depends on the
  catalog (extraction vs plain SELECT) is transformed into a stock
  `SelectStatement` over a table function instead; its `bind_replace` sees
  the catalog and returns either the subquery or the extraction scan.
- **`Parser(context)` uses the connection's grammar**, glossql rules
  included; SQL the extension generates is parsed with
  `ParserOptions::Builtin()` plus `CompiledGrammar::DefaultGrammar()`.
- **Table macros bind their body at creation**: a body over a CTE the caller
  provides (`slots`) is refused, and `$name` parameters are not supported in
  macros. Measurements are macros (their `$subject` becomes the parameter);
  detectors stay scripts that ATTEST inlines with `slots` as a CTE.
- **A second connection** (`Connection(*context.db)`) runs SQL while the
  calling connection's context lock is held — DuckDB's own pattern
  (`external_resources_manager.cpp`). Recipes, probes, imports, macro
  creation and the catalog lookups behind admission use it; those writes
  commit on their own, not with a transaction the caller may have open.
- **Reads generated at bind.** GLOSSARY and ATTEST build their SQL from the
  witnesses, detectors and grains as they stand, including the unassessed
  subjects owed per grain from `information_schema` and the WHEN condition.
- The sqllogictest runner wants `{TEST_DIR}`, not `__TEST_DIR__`, and
  expected rows are whitespace-trimmed.

What stays outside the plugin by design: doors and identity, the cube and
its analyses, the substrate-SQL allowlist. What waits for DuckLake:
versioned landings, snapshot-pinned reads, `stale`.

## 9. Round three: kits, the loader, the grounding schema

- **A kit loads as statements, not as a pragma.** A `PragmaCall` query
  function's text is re-parsed with `Parser(context)`, so the connection's
  grammar extensions apply (`src/planner/statement_preprocessor.cpp`),
  but the expansion runs as one transaction, and the store's lookups on a
  second connection would not see the kit's own earlier declarations.
  `glossql_kit(path)` is a table function: it reads the script through
  `FileSystem::GetFileSystem(context)` (local paths; URLs once `httpfs` is
  loaded), sets `active_grammar_extensions` on a second connection,
  `ExtractStatements()` the script, and runs each statement with
  `Connection::Query(unique_ptr<SQLStatement>)`, committing one by one;
  `statement->query` carries each statement's own text, which the row
  reports. A failing statement stops the load with its error.
- **A flavor the engine reads.** `schema->>'x-kind'` on the JSON column
  is the key read; an aspect flavored `grounding` has its schema applied
  to every QUERY body after the contract, is never glossed, takes no
  witness, and is re-declared only when every current QUERY slot still
  matches. The one convention the core knows is the `x-` key the SPEC
  already uses for flavors.
- **A detector body may open with `WITH`.** The verdict query puts the
  script under the `slots` CTE as a derived table, `SELECT * FROM
  (<script>) AS q`, so a body's own CTEs compose.
- **Ported functions.** `slot_entropy`, `column_profile` and
  `outlier_profile` port to DuckDB SQL as written in the glossql
  library, with `quantile_cont`, `median`, `mad` and `FILTER` for what
  DataFusion's `profile()` aggregate computed; `distinct` needs quoting
  as a column alias.

## 10. Round four: the three gaps

What the conformance map still had as *not yet* among the reads and the
landing, closed on DuckDB primitives; each mechanism verified.

- **The verdict `error` (SPEC §7.2).** The detector runs at read, on the
  second connection, inside `bind_replace`: `Connection::Query` of the
  wrapped verdict query, its rows embedded as `VALUES` in the planned
  subquery. A failure of plan or run is the witness's verdict, one row
  per subject holding slots, band `error`, score 0, the first line of
  `QueryResult::GetError()` as the text (`Binder Error: Referenced column
  "x" not found in FROM clause!`; the candidate bindings on the next line
  are dropped). A row outside the attest schema, a band off the four
  colours or a score off 0..1, is the same verdict. Two things the
  embedding needed: a decimal literal in `VALUES` is typed `DECIMAL`
  (`0.6666666666666666` becomes `DECIMAL(17,16)`), so a score travels
  as text and is cast `::DOUBLE` outside; and the collapse's pick orders
  `band = 'error'` last, so an error never outranks a verdict and never
  withholds, as the SPEC says.
- **Relationship endpoints (SPEC §4).** `ResolveSubject` leaves a tuple
  key whole, `main.txn.(business_id, account)`, so a split on `.` gives
  dataset, table and key; the table is looked up in
  `information_schema.tables` and each column of the key in
  `information_schema.columns`, views counting as tables. A gloss on a
  pair path takes a declared relationship, since only declared ones exist.
- **Dropped rows (SPEC §3).** In this host the recipe is the landing:
  the SQL that yields the rows is the SQL that lands them, so nothing is
  lost in between and the only reader that drops rows is one told to
  `ignore_errors`. When it also stores its rejects (`store_rejects =
  true`), the scan creates `temp.main.reject_errors` and
  `temp.main.reject_scans` on the scanning connection, one row per
  error, not per line, so the count is `count(DISTINCT (scan_id,
  file_id, line))`; temporary tables are per connection and the landing
  opens a fresh one, so what it holds is this landing's. Without
  `store_rejects` the reader drops silently and the count is NULL, the
  Rust engine's answer too. One mechanism to respect: a failing
  statement inside the landing's transaction aborts it (`Current
  transaction is aborted`), so the rejects table is looked up in
  `information_schema.tables` under catalog `temp`, never probed.

## 11. Round five: the workspace is a DuckLake

The persistence round, built on the layout the glossql engine's record
already uses: one catalog database holding the `ducklake_*` tables and
the store relations as `glossql_<relation>` beside them. Each mechanism
verified against the running binary.

- **Installed, not built.** `INSTALL ducklake` fetches by version key:
  a release calls itself `v2.0.0`, a shallow submodule clone without
  tags calls itself `v2.0.0-dev1` with the hash as key, and the
  repository has nothing under the hash (404). Fetching the tag, or
  `OVERRIDE_GIT_DESCRIBE=v2.0.0`, makes the build a release in name,
  and the published DuckLake loads. The test runner satisfies `require
  ducklake` from the build's own extension repository
  (`build/release/repository/<version>/<platform>/`), which every build
  regenerates, so `make lake` copies the installed file in after the
  build; a `LOAD ducklake` inside a test cannot see the user's home,
  which the runner sandboxes. The lake writes parquet, so a suite also
  requires `parquet`; a landing whose parquet write failed left a table
  whose scan bind crashed inside DuckLake, a thing to know when a
  symptom looks like ours.
- **The store in the metadata catalog.** DuckDB creates, writes and
  reads our own tables in `__ducklake_metadata_<alias>` (`main` for a
  DuckDB-file catalog); DuckLake ignores them, no snapshot moves, and
  they survive detach and re-attach. The hidden catalog is reachable by
  name and absent from `duckdb_databases()`, `duckdb_schemas()` and
  `information_schema`; a user-named one (`METADATA_CATALOG`) is
  visible, so discovery tries information_schema first and the default
  name second. `ATTACH 'ducklake::memory:'` is accepted and serves the
  suites.
- **Views in the lake.** A view in the lake's `glossql` schema reads a
  store table through the metadata catalog's name, which an alias
  change breaks; `duckdb_views()` lists lake views with their SQL,
  re-serialized without quotes, so init re-makes a view whose text does
  not name the current catalog and leaves the rest, each view being a
  snapshot.
- **One transaction, one database.** A transaction that wrote the lake
  cannot write its catalog (`a single transaction can only write to a
  single attached database`): a landing is the lake's commit and the
  `imports` row the store's. A failing statement inside an open
  transaction aborts it, so nothing is probed inside one.
- **The column history.** `ducklake_column` rows live from
  `begin_snapshot` to `end_snapshot`, the end exclusive. `CREATE OR
  REPLACE TABLE` gives a new `table_id` with every column re-begun;
  `DELETE` plus `INSERT` keeps column ids; `ALTER COLUMN TYPE` only
  widens; `DROP COLUMN` ends the row. Staleness therefore compares
  shape, not identity: the column's type live at the gloss's snapshot
  against its type live now, dropped or retyped being stale, a table
  being stale when any then-column is. Column rows join their table row
  by `table_id` within the table row's snapshot range.
- **Portable columns.** Text and numbers only, as the Rust record
  writes them: bodies and schemas as VARCHAR cast `::JSON` in the views
  and reads (`->>` takes the cast), lists as JSON text read back with
  `from_json(col, '["VARCHAR"]')`, a `seq` the writer fills from
  `max(seq) + 1` in place of `rowid`. DuckLake tables and views accept
  `CREATE OR REPLACE MACRO`, `CREATE OR REPLACE VIEW` and `MERGE INTO`.
- **Time travel** takes an expression: `AT (VERSION =>
  getvariable('v'))` binds.

## 12. Round six: pins and reads

What the measurement round relied on, each verified against the source
or a CLI probe.

- **The bound plan names its tables.** `LogicalGet::GetTable()` goes
  through `function.get_table_entry`, which DuckDB's own scan sets and
  DuckLake's `ducklake_scan` sets too (`src/storage/ducklake_scan.cpp`),
  so a walk over `Planner::CreatePlan`'s plan — run inside
  `ClientContext::RunFunctionInTransaction` on the second connection,
  before any optimizer — finds every lake table and every store table a
  body scans, views and `subject_column()` having expanded at bind. A
  `LogicalGet` without an entry is a table function: `range` and its
  kin read nothing, anything else makes the reads `**`.
- **The lake's change log.** `ducklake_snapshots(lake)` serves
  `changes MAP(VARCHAR, VARCHAR[])`: `tables_created`, `views_created`,
  `table_macros_created`, `schemas_created` carry names;
  `tables_inserted_into`, `tables_deleted_from`, `tables_altered`,
  `tables_dropped` carry table ids; small writes are inlined by default,
  in file lakes too, and arrive as `inlined_insert`, `inlined_delete`;
  `ducklake_merge_adjacent_files` records `merge_adjacent`, which is not a
  write. A table's version is therefore the greatest of its
  `begin_snapshot` and the last snapshot whose id-keyed lists name it.
  `ducklake_expire_snapshots` removes snapshot rows, so a version can
  fall; the metadata table behind the function is
  `ducklake_snapshot_changes.changes_made`, a text the function parses.
  Two `unnest` calls in one SELECT zip, which turns the map into rows.
- **Lambdas** are `lambda p: expr` in 2.0; the arrow form is a binder
  error ("Deprecated lambda arrow").
- **A macro parameter cannot share a column's name.** `CREATE MACRO
  f(subject) AS TABLE SELECT … FROM glossary` fails with "Conflicting
  column names for column subject"; the parameter is spelled
  `"$subject"`, an identifier no column takes, and `$subject` in a body
  becomes it.
- **The second connection's search path** starts at the default catalog,
  not the caller's: a body naming `glossql.glossary` failed to bind at
  `CREATE MACRO` until `Inner` copied `ClientData::catalog_search_path`
  from the caller (`SET search_path`).
- **2.0 types names as `Identifier`** (`GetIdentifierName()`), wraps a
  scan's function in `BoundTableFunction` (`GetDefinition()->name`), and
  keeps `ClientContext::GetParserOptions` private; `ParserOptions::Builtin()`
  with the default grammar parses plain SQL.

## 13. Round seven: the store's own columns

The store relations stopped mirroring the Rust record and took the
columns the reads need, the record being the teacher rather than the
template. What held in DuckDB:

- **A declaration's SET pairs as one JSON object, checked at parse.**
  The grammar transform builds `{"type":"csv","location":"…"}` from the
  pairs (a string or a name is a JSON string, a number stands as written,
  a key given twice is refused) and runs the yyjson validator over it
  against a schema the statement holds; a `ParserException` carries the
  violation (`settings at $: property 'owner' is not allowed`). No
  connection is needed, so the check sits where the old allowed-key list
  did. The engine reads the keys it needs back with `settings::JSON->>'type'`;
  the composite recipe key is one `key: 'a, b'`, split on commas, as the
  Rust session spells it.
- **A supersession key per relation** (`name`; `dataset, table_name`;
  `name, scope`; `aspect`) generates the one newest-row subquery that the
  reads used to spell by hand.
- **`dataset` on a gloss and a relationship** turns the dataset-wide read's
  `subject LIKE 'fin.%'` into an equality; the owed set keeps the subject
  predicate, having no dataset column of its own.
- **`actor_kind` and `actor_id`.** The kind comes from `glossql_actor` as
  before; the id from `glossql_actor_id`, NULL when the wrapper set none.
  The raw read keeps the SPEC's `actor` column, the kind.

## 14. Round eight: lineage off the bound plan

The re-derivation rule needed what computes each column of a recipe, and
a drift record wanted the same per landing. Both come from one walk of
the logical plan the planner makes before any optimizer runs
(`Planner::CreatePlan` under `RunFunctionInTransaction`, as the reads do).

- **The walk.** Bottom-up over `LogicalOperator`, a
  `column_binding_map_t<string>` from binding to derivation text: a
  `LogicalGet` leaf is `scan.column:type` (`GetColumnIds()`,
  `GetColumnName()`, `returned_types`; `GetTable()` names a catalog
  table, else the function); a projection, aggregate (`group_index`,
  `aggregate_index`), window (`window_index`) and unnest write their
  expressions' texts under their own index; a set operation joins the
  sides' texts under `table_index`; a materialized CTE is walked
  definition first so `LogicalCTERef::cte_index` resolves; filters,
  limits, orders, distinct and joins pass bindings through, a MARK join
  adding `mark_index`. The planner's `names` give the output columns.
- **Expression text without names.** 2.0 keeps the bound classes'
  members private: `Binding()`, `GetValue()`, `Function()`,
  `Identifier()`, `GetReturnType()`, `GetExpressionType()`. A cast is a
  `BoundFunctionExpression` typed `OPERATOR_CAST`; comparisons and
  between are functions too, the `LEGACY_BOUND_*` classes staying in
  the enum. `ExpressionIterator::EnumerateChildren` walks every child
  including a window's partitions and orders, so the text is the
  function or expression-type name over the children's texts, which
  carries no binding index and reads the same under a different filter,
  order or limit. The binder drops a no-op cast, so `amount::DOUBLE`
  over a DOUBLE source is the leaf itself; constants carry their
  implicit casts, typed by the column they meet.
- **Settings on the second connection.** `ClientConfig::file_search_path`
  is gone; `ClientContext::TryGetCurrentSetting(Identifier, Value)` reads
  it, and `Inner` now mirrors it beside the search path.
- **Per landing, not per declaration.** Recording the lineage at each
  landing is what makes source drift visible: under `SELECT *` or a
  cast-free column, a CSV whose values change type moves the leaf while
  the landed column keeps its type, which the lake's column history
  cannot see.
