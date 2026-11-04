"""The MCP door, opened over stdio the way a client opens it, on a workspace
made for the test: a fresh lake, the built extension, the opener with a dot
command to skip.

    uv run --with pytest --with fastmcp pytest -q test/python
"""

import asyncio
from pathlib import Path

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

REPO = Path(__file__).resolve().parents[2]
DOOR = REPO / "examples/mcp/glossql_mcp.py"
EXTENSION = REPO / "build/release/extension/glossql/glossql.duckdb_extension"


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    if not EXTENSION.is_file():
        pytest.skip(f"no built extension at {EXTENSION}")
    (tmp_path / "ws").mkdir()
    (tmp_path / "glossql.sql").write_text(
        ".startup_text none\n"
        f"LOAD '{EXTENSION}';\n"
        "ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');\n"
        "USE ws;\n"
        "PRAGMA glossql;\n"
    )
    return tmp_path


def test_the_door(workspace: Path) -> None:
    async def run():
        config = {"mcpServers": {"glossql": {"command": "uv", "args": ["run", "--quiet", str(DOOR), str(workspace)]}}}
        async with Client(config) as client:
            assert [t.name for t in await client.list_tools()] == ["glossql"]
            kits = sorted(d.name for d in (REPO / "kits").iterdir() if (d / "SKILL.md").is_file())
            assert {str(r.uri) for r in await client.list_resources()} == {
                "skill://glossql/SKILL.md",
                *(f"skill://glossql-{kit}/SKILL.md" for kit in kits),
                "workspace://glossql.sql",
            }
            assert sorted(p.name for p in await client.list_prompts()) == sorted(
                ["glossql", *(f"glossql-{kit}" for kit in kits)]
            )

            async def glossql(statements: str, **arguments) -> str:
                result = await client.call_tool("glossql", {"statements": statements, **arguments})
                return result.content[0].text

            # a write answers with its outcome line; a $$ body may hold a semicolon
            out = await glossql(
                "DECLARE DATASET fin SET (purpose: 'the door'); USE fin;"
                " CREATE TABLE fin.orders AS SELECT 1 AS id;"
                ' DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT ON COLUMN;'
                ' GLOSS note ON orders.id AS $${"text": "x; y"}$$;'
            )
            assert "declared dataset fin" in out
            assert "glossed note on fin.orders.id as agent" in out

            # the connection persists: the USE above binds this call; a read is a name:TYPE table
            out = await glossql("SELECT subject, aspect, state FROM GLOSSARY(orders.id);")
            assert "| subject:VARCHAR | aspect:VARCHAR | state:VARCHAR |" in out
            assert "| fin.orders.id | note | current |" in out
            assert out.endswith("1 rows")

            # a refusal stops the call, names its place, and carries what landed before it
            with pytest.raises(ToolError) as refused:
                await glossql("SELECT 1 AS before; USE nowhere; SELECT 2 AS after;")
            refusal = str(refused.value)
            assert "| before:INTEGER |" in refusal
            assert 'statement 2 refused: Catalog Error: SET schema: No catalog + schema named "nowhere"' in refusal
            assert "after" not in refusal

            # a large result shows its first and last rows and the true count
            out = await glossql("SELECT i FROM range(9) t(i);", max_rows=4)
            assert "| 0 |\n| 1 |\n| … |\n| 7 |\n| 8 |\n9 rows" in out

            # a long cell is cut with the count of what is missing; max_cell 0 serves it whole
            out = await glossql("SELECT repeat('x', 600) AS long_cell, NULL AS nothing;")
            assert "| long_cell:VARCHAR | nothing:NULL |" in out
            assert "| " + "x" * 500 + "…(+100 chars) | NULL |" in out
            out = await glossql("SELECT repeat('x', 600) AS long_cell;", max_cell=0)
            assert "| " + "x" * 600 + " |" in out

            # a refusal inside an explicit transaction rolls the whole transaction back
            with pytest.raises(ToolError):
                await glossql("BEGIN TRANSACTION; CREATE TEMP TABLE t_tx AS SELECT 1 AS x; USE nowhere;")
            with pytest.raises(ToolError, match="t_tx does not exist"):
                await glossql("SELECT x FROM t_tx;")

            # the opener is a resource, read with its dot command intact
            opener = await client.read_resource("workspace://glossql.sql")
            assert opener[0].text.startswith(".startup_text none")

    asyncio.run(run())
