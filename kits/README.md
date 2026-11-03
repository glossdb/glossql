# Kits

The extension ships the language and the store and no vocabulary. A kit
is the vocabulary: a script of glossql declarations, aspects, functions
and witnesses, with a `SKILL.md` beside it that teaches an actor the
words. The core validates what a kit declares; the kit decides what is
said. The glossql server brings its own kits beside these, loaded after
them, in `kits/` of its repository with their suites: the metrics kit,
whose series read serves the worlds its doors answer from, and the
futures kit, which calls the model service. A kit whose words depend on
a door or a service lives with that door or service; the kits here
depend on the language alone.

| Kit | Brings |
|---|---|
| `kpi/` | column and table vocabulary (`meaning`, `role`, `behavior` with `ratio` and its halves, `unit`, `dimension`, `entity`), source `conventions`, the `definitions` handbook, the `slot_entropy` detector, the `column_profile` and `outlier_profile` measurements, and the witnesses that bind them; a metric's view carries the same words on its columns |
| `spatial/` | the geometry vocabulary: `shape` on a geometry column (`WHEN TYPE = 'GEOMETRY'`), the `crs` its coordinates are in, and the `geometry_profile` measurement; its first lines install and load DuckDB's spatial extension |
| `quality/` | how the data holds together, over `dataset_cells()`: `relationship_candidates`, `relationship_checks`, `hierarchy_candidates`, `derivation_candidates` and `grounding_collisions` as table macros, and the five measurements that land them (`detect_relationships`, `relationship_coherence`, `detect_hierarchies`, `detect_derivations`, `detect_grounding_collisions`), the `temporal` profile, and the `rate_tolerance` detector that bands an authored expectation against a check voice; loaded after `kpi/` |

## Loading

```sql
LOAD gloss;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');   -- the workspace
USE ws;
PRAGMA gloss;
FROM gloss_kit('kits/kpi/kit.sql');
FROM gloss_kit('kits/quality/kit.sql');     -- after the KPI kit, whose words its screens read against
```

`gloss_kit(path)` reads the script through DuckDB's file system, so the
path is anything DuckDB opens: a local path, or a URL once `httpfs` is
loaded (`INSTALL httpfs; LOAD httpfs;` then a raw GitHub, S3 or HTTPS
URL). It runs the statements one by one on a second connection under the
glossql grammar and serves one row per statement, `(statement, outcome)`.
Each statement commits on its own, as it does from a script: a failing
statement stops the load with its error, and the statements before it
stand. The load is recorded in `glossql.kits` with its path and count.

Loading a kit twice is a no-op for every unchanged aspect, a re-declare
for functions, and a new row per witness; a changed aspect that has
glosses under it is refused, as the language says.

## Writing one

A kit is statements in order: aspects first, then the functions, then the
witnesses that name both.

- An aspect's `WITH` schema is a JSON Schema; the core validates every
  FACT body against it and every measurement's output against its
  aspect's. Annotations such as `title` or `x-kind` ride beside the
  structure.
- The core validates every QUERY gloss against the grounding contract
  (`sql` or `stopped`, `assumptions`). What a reader of the view's rows
  needs — the period, the measure and its behaviour, the slices, the row
  identity — is glossed on the view and its columns under the kit's
  aspects, never keyed in the body. A kit whose readers need a body key
  of their own declares a schema for it, an aspect flavored `"x-kind":
  "grounding"`: the core validates every QUERY gloss against each such
  schema after the contract; it is never glossed, no witness names it,
  and re-declaring it is refused while a current QUERY slot would stop
  matching.
- A function's body is DuckDB SQL. A measurement (`RETURNS aspect`)
  becomes a table macro in the dataset's schema, or `glossql` for
  `GLOBAL`; `$subject` is its parameter and `subject_column($subject)` the
  subject's column as a relation named `v`. Names in the body resolve as
  they would in the declaring session. A detector (no `RETURNS`) runs
  at read over its witness's `slots` with `$threshold` bound, and returns
  `(subject, band, score)`. A body may open with its own `WITH`. A detector
  that fails serves its witness's verdict `error`, never a failed read.
- What a measurement scans is what it is pinned to: the lake's tables and
  the store's relations, by name, read off the bound plan. A later
  extraction serves the landed row while those are unchanged and computes
  once one moved; the row says which (`computed`). A body that reads a
  file, a catalog function or another attached database has reads `**`
  and stands only until anything in the dataset or the store moves, so a
  kit's measurement should read tables and relations. A top-level
  `summary` object in the result is what extraction returns; the whole
  value lands and reads back through `GLOSSARY(subject::aspect)`.
- Conditions narrow what is owed: `AS FACT ON COLUMN WHEN role =
  'measure'` is owed only where the sibling's winning slot says so.

The `SKILL.md` is the half the engine cannot check: what each word means,
the order of a round, what a reader expects of a key.

## Wrapping it

The extension has no door. Identity, the SQL allowlist and the cube stay
in the wrapper, which holds one connection per actor and sets the actor
on it; the extension records what that connection says.

```python
import duckdb  # the package's DuckDB version must match the extension's

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'build/release/extension/gloss/gloss.duckdb_extension'")
con.execute("SET active_grammar_extensions = ['gloss']")
con.execute("ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/')")  # or a Postgres-backed lake every client shares
con.execute("USE ws")
con.execute("PRAGMA gloss")
con.execute("FROM gloss_kit('kits/kpi/kit.sql')")

def say(statement: str, kind: str, who: str) -> list[tuple]:
    """The door: the wrapper knows who speaks, the statement does not."""
    con.execute("SET VARIABLE gloss_actor = ?", [kind])      # human | agent: what supersession keys on
    con.execute("SET VARIABLE gloss_actor_id = ?", [who])    # the writer, for the record
    return con.execute(statement).fetchall()
```

An MCP server exposes `say` as one tool and the kit's `SKILL.md` as a
resource; an HTTP endpoint does the same per route. Nothing in the
statement names the actor, so a wrapper that authenticates the caller is
the whole identity story.
