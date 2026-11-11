# Installation

glossql is built for one DuckDB release at a time, because DuckDB's
grammar API is C++. This build targets DuckDB 2.0.0. The released
`duckdb` CLI loads it with `-unsigned`.

## Build from source

```sh
git submodule update --init --depth 1      # duckdb at v2.0.0, extension-ci-tools
brew install croaring                       # DuckLake's dependency; CI takes it from vcpkg.json
GEN=ninja make                              # build/release/duckdb, the extension, and DuckLake
make test                                   # the suites under test/sql
```

The build also produces DuckLake as a loadable extension. The Makefile
names the build `v2.0.0`, so the released `duckdb` 2.0.0 loads it.
`make lint` runs the format and tidy checks (`make lint-tools` once
before it).

## Open a workspace

A workspace is a DuckLake. `PRAGMA glossql` turns the grammar on for
the connection and creates the catalog in the lake if it is missing:
the `glossql_*` tables beside the `ducklake_*` tables, and one view per
table in `ws.main`.

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
PRAGMA glossql;
```

Run `PRAGMA glossql` after the `ATTACH`. Without an attached lake it
is refused.

A connection works on one workspace. With several DuckLakes attached,
name the one glossql uses:

```sql
SET VARIABLE glossql_workspace = 'ws';
```

## Set the actor

Each annotation is stored with the kind of actor that wrote it and,
when set, the actor's id. Both are session variables. No statement
names the actor.

```sql
SET VARIABLE glossql_actor = 'human';
SET VARIABLE glossql_actor_id = 'ada@example.com';
```

- `glossql_actor` is `agent` or `human`. One value is kept per asset,
  property and kind, so a person's value and an agent's are stored side
  by side. `GLOSSARY()` serves the person's.
- `glossql_actor_id` names the writer. It is stored in `actor_id` and
  in the `declared_by` column of a declaration.

An application that authenticates its users sets both on the user's
connection.

## The CLI

Put the opening statements in a file, `glossql.sql`, beside the lake:

```sql
LOAD 'build/release/extension/glossql/glossql.duckdb_extension';
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
PRAGMA glossql;
```

Then run each call with it, the statements on stdin:

```sh
duckdb -unsigned -bail -init glossql.sql <<'SQL'
USE staff;
SELECT * FROM GLOSSARY(employees) WHERE state <> 'current';
SQL
```

Each call is a new process, so `USE` and the session variables are set
again in every call. The CLI parses a `-c` batch before it runs it, so
the grammar applies from the next batch. Use stdin or `-init`.

`skills/glossql/SKILL.md` teaches an agent the CLI and the statements.

## Python

The `duckdb` package's version must match the extension's DuckDB
version.

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'build/release/extension/glossql/glossql.duckdb_extension'")
con.execute("ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/')")
con.execute("USE ws")
con.execute("PRAGMA glossql")
```

## The example MCP server

`examples/mcp/glossql_mcp.py` is a stdio MCP server for one workspace.
It opens one connection, runs the SQL of the workspace's `glossql.sql`
on it and keeps the connection, so `USE`, variables and the actor stay
set from call to call.

- One tool, `glossql(statements)`. A read comes back as a markdown
  table with `name:TYPE` headers and a row count. A write comes back as
  its acknowledgement line. A statement that fails stops the call and
  is the tool's error, with the output of the statements before it.
- Resources and prompts: `skill://glossql/SKILL.md` and one
  `skill://glossql-<kit>/SKILL.md` per kit. The opener is the resource
  `workspace://glossql.sql`.

```sh
uv run examples/mcp/glossql_mcp.py /path/to/workspace
claude mcp add glossql -- uv run --quiet "$PWD/examples/mcp/glossql_mcp.py" /path/to/workspace
uv run --with pytest --with fastmcp pytest -q test/python    # its test
```

`GLOSSQL_WORKSPACE` replaces the argument.
