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
DOOR = REPO / "examples/mcp/gloss_mcp.py"
EXTENSION = REPO / "build/release/extension/gloss/gloss.duckdb_extension"


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    if not EXTENSION.is_file():
        pytest.skip(f"no built extension at {EXTENSION}")
    (tmp_path / "ws").mkdir()
    (tmp_path / "gloss.sql").write_text(
        ".startup_text none\n"
        f"LOAD '{EXTENSION}';\n"
        "ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');\n"
        "USE ws;\n"
        "PRAGMA gloss;\n"
    )
    return tmp_path


def test_the_door(workspace: Path) -> None:
    async def run():
        config = {"mcpServers": {"gloss": {"command": "uv", "args": ["run", "--quiet", str(DOOR), str(workspace)]}}}
        async with Client(config) as client:
            assert [t.name for t in await client.list_tools()] == ["gloss"]
            assert {str(r.uri) for r in await client.list_resources()} == {
                "skill://gloss/SKILL.md",
                "skill://gloss-kpi/SKILL.md",
                "workspace://gloss.sql",
            }
            assert [p.name for p in await client.list_prompts()] == ["gloss", "gloss-kpi"]

            async def gloss(statements: str, **arguments) -> str:
                result = await client.call_tool("gloss", {"statements": statements, **arguments})
                return result.content[0].text

            # a write answers with its outcome line; a $$ body may hold a semicolon
            out = await gloss(
                "DECLARE DATASET fin SET (purpose: 'the door'); USE fin;"
                " CREATE TABLE fin.orders AS SELECT 1 AS id;"
                ' DECLARE ASPECT note WITH $${"type": "object"}$$ AS FACT ON COLUMN;'
                ' GLOSS note ON orders.id AS $${"text": "x; y"}$$;'
            )
            assert "declared dataset fin" in out
            assert "glossed note on fin.orders.id as agent" in out

            # the connection persists: the USE above binds this call; a read is a name:TYPE table
            out = await gloss("SELECT subject, aspect, state FROM GLOSSARY(orders.id);")
            assert "| subject:VARCHAR | aspect:VARCHAR | state:VARCHAR |" in out
            assert "| fin.orders.id | note | current |" in out
            assert out.endswith("1 rows")

            # a refusal stops the call, names its place, and carries what landed before it
            with pytest.raises(ToolError) as refused:
                await gloss("SELECT 1 AS before; USE nowhere; SELECT 2 AS after;")
            assert "| before:INTEGER |" in str(refused.value)
            assert 'statement 2 refused: Catalog Error: SET schema: No catalog + schema named "nowhere"' in str(refused.value)
            assert "after" not in str(refused.value)

            # a large result shows its first and last rows and the true count
            out = await gloss("SELECT i FROM range(9) t(i);", max_rows=4)
            assert "| 0 |\n| 1 |\n| … |\n| 7 |\n| 8 |\n9 rows" in out

            # the opener is a resource, read with its dot command intact
            opener = await client.read_resource("workspace://gloss.sql")
            assert opener[0].text.startswith(".startup_text none")

    asyncio.run(run())
