# Interfaces

Two interfaces ship with the extension: the DuckDB CLI and a small MCP
server. The glossdb server adds the rest.

## The CLI

Claude Code, the released `duckdb` and this extension are enough: no
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
teaches an agent the CLI and the language on DuckDB, `kits/kpi/SKILL.md`
the KPI kit's words; both are linked under `.claude/skills/`, so a Claude
Code session in this repository has them, and a project of your own
copies or links the two directories into its `.claude/skills/`.

## The MCP server

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
uv run --with pytest --with fastmcp pytest -q test/python    # the integration test, on a workspace it makes
```

`GLOSSQL_WORKSPACE` stands in for the argument.
