# glossql

glossql is a language for a company's data intelligence layer: what
the data means, how it is loaded, and whether statements about it hold.
It is a DuckDB extension. It adds a few statements and two table
functions to DuckDB SQL.

It gives you three things:

- **A business glossary stored in the lake.** Annotations on datasets,
  tables and columns are stored in a DuckLake beside the data, so each
  one belongs to a version of the data.
- **Views.** A metric, a single value or a table, defined in SQL and
  served as a view of its dataset.
- **Tests.** Measurements and tests run on the data. A result is marked
  `stale` when the data it read has changed.

It is for data engineers who build that layer, and for agents that
build it with them. [glossdb](https://github.com/glossdb/glossdb) is
the server around it. The idea behind it:
[the data harness](docs/harness.md).

## First use

Build the extension ([installation](docs/installation.md)), then:

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');   -- the workspace is a DuckLake
USE ws;
PRAGMA glossql;                                  -- turns the grammar on and creates the catalog

DECLARE DATASET shop SET (purpose: 'order analysis');
USE shop;

-- load: a source, then a load query that creates the table shop.orders
DECLARE SOURCE exports SET (type: csv, location: 'exports');
DECLARE RECIPE orders ON shop FROM exports SET (key: id)
  AS $$SELECT id::INTEGER AS id, placed_on::DATE AS placed_on, amount::DOUBLE AS amount FROM read_csv('orders.csv')$$;

-- describe: declare a property, then annotate a column with it
DECLARE PROPERTY unit WITH $${"type": "object", "required": ["value"]}$$ AS FACT ON COLUMN;
GLOSS unit ON orders.amount AS $${"value": "EUR"}$$;

-- measure: a function over the data, stored with the inputs it read
DECLARE PROPERTY min_max WITH $${"type": "object", "required": ["min", "max"]}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS
  $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
SELECT min_max() FROM orders.amount;

-- define a metric: SQL, served as the view shop.revenue
GLOSS revenue ON shop AS $${"kind": "metric", "sql": "SELECT placed_on, sum(amount) AS revenue FROM shop.orders GROUP BY placed_on"}$$;
FROM shop.revenue;

-- read: one row per asset and property, with its state
SELECT * FROM GLOSSARY(orders);
IMPORT orders;                                   -- load again; every load is a snapshot of the lake
```

`GLOSSARY()` gives each row a state: `current`, `stale` (the data
changed after the value was written), `conflict` (an agent and a person
disagree beyond a test's threshold) or `missing` (the property is
required and nobody has recorded it).

## Kits

A kit is a script of declarations for one domain, with a `SKILL.md`
that teaches an agent its words. Four ship with the extension:
`lineage` (a dataset's lineage and the read functions a function's
body reads through), `kpi` (the semantic model's vocabulary), `quality`
(data quality checks) and `spatial` (geometry columns). `PRAGMA glossql`
loads `lineage`; load the others. See [kits](docs/kits.md).

```sql
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');
```

## Status

| Feature | Status |
|---|---|
| The statements and reads of `grammar.ebnf` | stable |
| The catalog in DuckLake, versioned with the data | stable |
| Loads from files and attached databases, with column lineage | stable |
| Annotations, measurements, tests, views | stable |
| The `kpi`, `quality` and `spatial` kits | stable |
| The example MCP server (`examples/mcp/`) | stable |
| `glossql_service()`, calls to a model service | preview |
| Grants per actor on statements and datasets | planned |

Limits:

- The extension is built for one DuckDB release, today 2.0.0, and is
  loaded unsigned.
- A connection works on one workspace.
- The extension does not restrict plain SQL. What an actor may run is
  for the application around it to decide.

## Documentation

- [docs/](docs/README.md): installation, usage, the functions, the
  kits and the catalog's tables.
- [SPEC.md](SPEC.md): the language, normative.
- [grammar.ebnf](grammar.ebnf): the syntax.
- `corpus/`: real artifacts written in glossql, the language's
  acceptance tests.

## License

MIT.
