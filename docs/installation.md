# Installation

glossql is built per DuckDB release, like every extension that adds
grammar, because DuckDB's grammar API is C++ only. This build targets
DuckDB 2.0.0. The released `duckdb` CLI loads it unsigned.

## Build from source

```sh
git submodule update --init --depth 1      # duckdb at v2.0.0, extension-ci-tools
brew install croaring                       # DuckLake's one dependency; CI takes it from vcpkg.json
GEN=ninja make                              # build/release/duckdb, the extension, DuckLake beside it
./build/release/test/unittest "test/sql/glossql*"
```

DuckLake is built with the extension, from the commit DuckDB 2.0.0
pins, as a loadable in the build's extension repository. The Makefile
names the build `v2.0.0`, so the released `duckdb` loads it; a build
named after a commit is refused by it.

Linting:

```sh
make lint-tools                            # once: a project .venv with clang-format and clang-tidy, pinned (uv)
GEN=ninja make lint                        # format and tidy checks plus unused includes; `make lint-fix` formats
```

## Load it and open a workspace

A workspace is a DuckLake. `PRAGMA glossql` turns the grammar on for
the connection and creates the catalog in the lake, if it is not there.

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
PRAGMA glossql;
```

Run `PRAGMA glossql` after the `ATTACH`: before it, the pragma has no
lake to make the catalog in and is refused.

Start the CLI with `-unsigned` to load a locally built extension:

```sh
./build/release/duckdb -unsigned
```

From Python, the `duckdb` package's version must match the extension's
DuckDB version:

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'build/release/extension/glossql/glossql.duckdb_extension'")
con.execute("ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/')")
con.execute("USE ws")
con.execute("PRAGMA glossql")
```

## An opener for the CLI

Put the four statements in a file, `glossql.sql`, beside the lake, and
run every call with it:

```sh
duckdb -unsigned -bail -init glossql.sql <<'SQL'
USE employees;
SELECT * FROM GLOSSARY(dept_emp) WHERE state <> 'current';
SQL
```

The CLI parses a `-c` batch before running it, so the grammar applies
from the next batch; stdin and the test runner go statement by
statement. More on the CLI and the example MCP server:
[interfaces](interfaces.md).
