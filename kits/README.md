# Kits

The extension ships the language and the store and no vocabulary. A kit
is the vocabulary: a script of glossql declarations, aspects, functions
and tests, with a `SKILL.md` beside it that teaches an actor the
words. The core validates what a kit declares; the kit decides what is
said. A workspace runs kits of its own beside these, loaded after them.

| Kit | Brings |
|---|---|
| `kpi/` | column and table vocabulary (`meaning`, `role`, `additivity` with `ratio` and its halves, `unit`, `dimension`, `entity`), source `conventions`, the `definitions` handbook, the `slot_entropy` detector, the `column_profile` and `outlier_profile` measurements, and the tests that bind them; a metric's view carries the same words on its columns |
| `spatial/` | the geometry vocabulary: `shape` on a geometry column (`WHEN TYPE = 'GEOMETRY'`), the `crs` its coordinates are in, and the `geometry_profile` measurement; its first lines install and load DuckDB's spatial extension |
| `quality/` | how the data holds together, over `dataset_cells()`: `relationship_candidates`, `relationship_checks`, `hierarchy_candidates`, `derivation_candidates`, `grounding_collisions` and `additivity_anchors` as table macros, and the six measurements that land them (`detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations`, `detect_grounding_collisions`, `additivity_evidence`, the stock/flow evidence over declared edges), the `temporal` profile, and the `rate_tolerance` detector that bands an authored expectation against a check value; loaded after `kpi/` |

## Loading

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');   -- the workspace
USE ws;
PRAGMA glossql;
FROM glossql_kit('kits/kpi/kit.sql');
FROM glossql_kit('kits/quality/kit.sql');     -- after the KPI kit, whose words its screens read against
```

`glossql_kit(path)` reads the script through DuckDB's file system, so the
path is anything DuckDB opens: a local path, or a URL once `httpfs` is
loaded (`INSTALL httpfs; LOAD httpfs;` then a raw GitHub, S3 or HTTPS
URL). It runs the statements one by one on a second connection under the
glossql grammar and serves one row per statement, `(statement, outcome)`.
Each statement commits on its own, as it does from a script: a failing
statement stops the load with its error, and the statements before it
stand. The load is recorded in `kits` with its path and count.

Loading a kit again with the same text runs only its `LOAD` and
`INSTALL` lines: what it declared is already in the lake, and one row
says so. A changed text runs whole and writes a new `kits` row: an
unchanged aspect is a no-op, a function is re-declared, each test
(`TEST`) writes a new row, and a changed aspect that has glosses under
it is refused, as the language says.

## Writing one

A kit is statements in order: aspects first, then the functions, then the
tests that name both.

- An aspect's `WITH` schema is a JSON Schema; the core validates every
  FACT body against it and every measurement's output against its
  aspect's. Annotations such as `title` or `x-kind` ride beside the
  structure.
- The core validates every VIEW gloss against the metric definition contract
  (`sql` or `stopped`, `assumptions`). What a reader of the view's rows
  needs — the period, the measure and its additivity, the slices, the row
  identity — is glossed on the view and its columns under the kit's
  aspects, never keyed in the body. A kit whose readers need a body key
  of their own declares a schema for it, an aspect flavored `"x-kind":
  "grounding"`: the core validates every VIEW gloss against each such
  schema after the contract; it is never glossed, no test names it,
  and re-declaring it is refused while a current VIEW value would stop
  matching.
- A function's body is DuckDB SQL. A measurement (`RETURNS aspect`)
  becomes a table macro in the dataset's schema, or `glossql` for
  `GLOBAL`; `$subject` is its parameter and `subject_column($subject)` the
  subject's column as a relation named `v`. Names in the body resolve as
  they would in the declaring session. A detector (no `RETURNS`) runs
  at read over its test's `slots` with `$threshold` bound, and returns
  `(subject, band, score)`. A body may open with its own `WITH`. A detector
  that fails serves its test's result `error`, never a failed read.
- What a measurement scans is what it is pinned to: the lake's tables and
  the store's relations, by name, read off the bound plan. A later
  extraction serves the stored row while those are unchanged and computes
  once one moved; the row says which (`computed`). A body that reads a
  file, a catalog function or another attached database has reads `**`
  and holds only until anything in the dataset or the store moves, so a
  kit's measurement should read tables and relations. A top-level
  `summary` object in the result is what extraction returns; the whole
  value is stored and reads back through `GLOSSARY(subject::aspect)`.
- Conditions narrow what is required: `AS FACT ON COLUMN WHEN role =
  'measure'` is required only where the sibling's winning value says so.

The `SKILL.md` is the half the engine cannot check: what each word means,
the order of a round, what a reader expects of a key.

## Wrapping it

The extension has no server. Identity, the SQL allowlist and the cube stay
in the wrapper, which holds one connection per actor and sets the actor
on it; the extension records what that connection says.

```python
import duckdb  # the package's DuckDB version must match the extension's

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'build/release/extension/glossql/glossql.duckdb_extension'")
con.execute("SET active_grammar_extensions = ['glossql']")
con.execute("ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/')")  # or a Postgres-backed lake every client shares
con.execute("USE ws")
con.execute("PRAGMA glossql")
con.execute("FROM glossql_kit('kits/kpi/kit.sql')")

def say(statement: str, kind: str, who: str) -> list[tuple]:
    """The door: the wrapper knows who speaks, the statement does not."""
    con.execute("SET VARIABLE glossql_actor = ?", [kind])      # human | agent: what supersession keys on
    con.execute("SET VARIABLE glossql_actor_id = ?", [who])    # the writer, for the record
    return con.execute(statement).fetchall()
```

An MCP server exposes `say` as one tool and the kit's `SKILL.md` as a
resource; an HTTP endpoint does the same per route. Nothing in the
statement names the actor, so a wrapper that authenticates the caller is
the whole identity story.
