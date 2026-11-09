# glossql

**The language for writing data harnesses.** It declares what must be
known about data, how it is measured and tested, and who may say it,
and keeps the record of what was said.

A coding harness has types, a compiler and tests. glossql gives data
the same three: a schema of what must be known, validation on write,
and measurements and tests on the live data. glossql is a DuckDB
extension. It adds a handful of statements and two table functions to
DuckDB SQL, and keeps its catalog in a DuckLake beside the lake's own
tables, so what is known about the data is versioned with the data.

Every word glossql uses, beside the industry's: [terms](docs/terms.md).

## Declare what must be known

```sql
-- with the kpi kit loaded, which declares `role`
DECLARE ASPECT currency WITH $${"type": "object", "required": ["value"],
  "properties": {"value": {"type": "string", "pattern": "^[A-Z]{3}$"}}}$$
  AS FACT ON COLUMN WHEN role = 'measure';           -- required on every measure column
DECLARE TEST currency_test ON currency BY (AGENT, HUMAN)
  DETECTOR slot_entropy THRESHOLD 0.7;              -- the test: who may annotate it; how disagreement is scored
DECLARE ASPECT min_max WITH $${"type": "object", "required": ["min", "max"]}$$
  AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS
  $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
```

- An **aspect** is a property of the harness schema: a JSON Schema for
  its values, the assets it applies to (dataset, table, column,
  relationship, source), and when it is required. It is asserted
  (`FACT`), computed (`MEASUREMENT`), or a view (`VIEW`): a metric
  definition, a scalar, or a derived relation.
- A **function** is one SQL query: a measurement (profiling,
  relationship discovery, quality screens) or a detector that scores a
  test.
- A **test** is declared on an aspect: which actors may
  annotate it, and which detector scores their agreement.
- **Grants** (planned) give each actor the statements and datasets it
  needs, as in SQL.
- Sources, load queries (`RECIPE`), datasets and relationships are
  declared the same way. A **kit** packages a domain's declarations
  with a skill that teaches them.

## Record what is

A **gloss** is an annotation: one actor's value for an aspect on an
asset.

```sql
GLOSS currency ON orders.amount AS $${"value": "EUR"}$$;  -- validated, stored with its actor and the data version
SELECT min_max() FROM orders.amount;                     -- a measurement, stored with the versions it read
IMPORT orders;                                           -- a load: a new DuckLake snapshot with its row count and column lineage
```

## Read it

```sql
SELECT * FROM GLOSSARY(orders);  -- what is known: one row per asset and aspect, state current | stale | conflict | missing
SELECT * FROM TESTS(orders);    -- the test results: a band and a score per asset and aspect
```

`stale` means the data changed after the annotation; `conflict`, that
actors disagree beyond the test's threshold; `missing`, that the
aspect is required and nobody has annotated it. Everything else is
plain SQL over the catalog tables.

## Kits

The extension ships three kits: `kpi` (the semantic layer's
vocabulary), `quality` (data quality screens), and `spatial`, a small
example of a domain kit. Each kit's `SKILL.md` teaches an agent what
its words mean ([kits](kits/README.md)).

```sql
FROM glossql_kit('kits/kpi/kit.sql');           -- vocabulary, functions, tests: one row per statement
FROM glossql_kit('kits/quality/kit.sql');       -- the screens over a dataset's cells, after the KPI kit
```

## A whole session

From an empty lake to a served metric, declaring its own words instead
of loading a kit:

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');   -- the workspace is a DuckLake
USE ws;
PRAGMA glossql;                                   -- the grammar for this connection, and the store: glossql_* tables in the lake's catalog, read as views in ws.main

DECLARE DATASET fin SET (purpose: 'working-capital analysis');
USE fin;                                        -- a dataset is a schema of the lake; DuckDB's USE binds it
DECLARE SOURCE erp SET (type: parquet, location: 's3://lake/erp');
PREVIEW erp AS $$SELECT id, amount FROM read_parquet('orders/*.parquet') LIMIT 5$$;
DECLARE RECIPE orders ON fin FROM erp SET (key: id)
  AS $$SELECT id::INTEGER AS id, amount::DOUBLE AS amount FROM read_parquet('orders/*.parquet')$$;
IMPORT orders;                                  -- the recipe runs again; keyed tables merge; every load is a snapshot

DECLARE ASPECT additivity WITH $${"type": "object", "properties": {"value": {"enum": ["semi-additive", "additive"]}}}$$ AS FACT ON COLUMN;
DECLARE FUNCTION additivity_entropy FOR fin AS $$ SELECT subject, ... AS band, ... AS score FROM slots GROUP BY subject $$;
DECLARE TEST additivity_test ON additivity BY (AGENT, HUMAN) DETECTOR additivity_entropy THRESHOLD 0.7;

GLOSS additivity ON orders.amount AS $${"value": "additive"}$$;
SET VARIABLE glossql_actor = 'human';           -- the actor rides the connection
GLOSS additivity ON orders.amount AS $${"value": "semi-additive"}$$;

SELECT * FROM GLOSSARY(orders);                 -- (subject, aspect, value, band, score, state): current | conflict | stale | missing
SELECT * FROM TESTS(orders.amount::additivity);  -- the test results
SELECT * FROM GLOSSARY(orders.amount, all => true);

DECLARE ASPECT min_max WITH $${"type": "object", "required": ["min", "max"]}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
SELECT min_max() FROM orders.amount;            -- stored with what it read; a repeat serves it while that is unchanged
FROM min_max('fin.orders.amount');              -- the same function, as the table macro it is

DECLARE ASPECT revenue WITH $${"title": "revenue"}$$ AS VIEW;
GLOSS revenue ON fin AS $${"sql": "SELECT sum(amount) AS revenue FROM fin.orders"}$$;
FROM fin.revenue;                               -- a metric definition is a view, and a table of the dataset
GLOSS additivity ON revenue.revenue AS $${"value": "additive"}$$;  -- its columns are assets: annotate them like a table's
```

## Status

| Feature | Status |
|---|---|
| Every statement and read of `grammar.ebnf` | stable |
| The catalog in DuckLake, versioned with the data | stable |
| Validation, measurements with provenance, tests | stable |
| Load queries over files and attached databases, with column lineage | stable |
| The `kpi`, `quality` and `spatial` kits | stable |
| The CLI and the example MCP server | stable |
| Grants per actor on statements and datasets | planned |

What of `SPEC.md` is implemented, section by section:
[CONFORMANCE.md](CONFORMANCE.md).

## Documentation

- [SPEC.md](SPEC.md) — the language, normative.
- [grammar.ebnf](grammar.ebnf) — the grammar, the source of truth for
  syntax.
- [CONFORMANCE.md](CONFORMANCE.md) — what this extension implements.
- [The documentation](docs/README.md) — installation, usage, functions,
  kits and the catalog's specification, one page per table.
- [Terms](docs/terms.md) — every word, beside the industry's.
- [Interfaces](docs/interfaces.md) — the CLI and the MCP server.
- [How glossql maps onto DuckDB](docs/internals/duckdb.md) — each part of the
  language and the DuckDB mechanism under it.
- `corpus/` — real artifacts written in glossql: the language's
  acceptance tests, held to the grammar by `test/sql/glossql_corpus.test`.

The glossdb server builds a harness for agents on this language:
[glossdb/glossdb](https://github.com/glossdb/glossdb).

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

## License

MIT.
