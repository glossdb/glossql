# /// script
# requires-python = ">=3.11"
# dependencies = ["fastmcp>=4,<5", "duckdb==2.0.0"]
# ///
"""The MCP door: a gloss workspace behind one tool, on one DuckDB connection.

A workspace is a directory holding the opener gloss.sql, the lake's
catalog and its data. The server opens one connection through the duckdb
package, runs the opener's SQL on it (the CLI's dot commands skipped) and
keeps it for its life: the tool `gloss` runs each call's statements there,
one after the other, and renders what each returns. The two skills are
resources and prompts; the opener is a resource.

    uv run examples/mcp/gloss_mcp.py /path/to/workspace

GLOSS_WORKSPACE stands in for the argument. The duckdb package is pinned
to the release the extension is built for.
"""

import os
import sys
from pathlib import Path

import duckdb
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

REPO = Path(__file__).resolve().parents[2]
SKILLS = {"gloss": REPO / "skills/gloss/SKILL.md", "gloss-kpi": REPO / "kits/kpi/SKILL.md"}
WORKSPACE = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("GLOSS_WORKSPACE", ".")).resolve()
OPENER = WORKSPACE / "gloss.sql"
CELL = 500


def open_workspace() -> duckdb.DuckDBPyConnection:
    """One connection, the opener run on it: the extension loaded, the lake attached, the grammar on."""
    if not OPENER.is_file():
        sys.exit(f"gloss: no opener gloss.sql in {WORKSPACE}")
    os.chdir(WORKSPACE)
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    sql = "\n".join(line for line in OPENER.read_text().splitlines() if not line.lstrip().startswith("."))
    for statement in con.extract_statements(sql):
        con.execute(statement)
    return con


def cell(value: object) -> str:
    text = "NULL" if value is None else str(value)
    return text if len(text) <= CELL else text[:CELL] + "…"


def render(con: duckdb.DuckDBPyConnection, max_rows: int) -> str:
    """A result as a markdown table with name:TYPE headers and a row-count footer; a
    large one as its first and last rows; DuckDB's empty Success result as nothing."""
    description = con.description or []
    rows = con.fetchall()
    if not rows and [d[0] for d in description] == ["Success"]:
        return ""
    head = "| " + " | ".join(f"{d[0]}:{d[1]}" for d in description) + " |\n|" + "---|" * len(description)
    if len(rows) > max_rows > 0:
        shown = rows[: max(max_rows // 2, 1)] + [None] + rows[-max(max_rows // 2, 1) :]
    else:
        shown = rows
    body = "\n".join("| … |" if r is None else "| " + " | ".join(cell(v) for v in r) + " |" for r in shown)
    return f"{head}\n{body}\n{len(rows)} rows" if body else f"{head}\n0 rows"


CON = open_workspace()

mcp = FastMCP(
    "gloss",
    instructions=(
        "A gloss workspace: DuckDB, a DuckLake and the glossql context language on one connection, "
        "behind one tool, `gloss`, which runs glossql statements and plain SQL. Read "
        "skill://gloss/SKILL.md first (the door, the language, the reads, the store), then "
        "`SELECT * FROM glossql.datasets;`. skill://gloss-kpi/SKILL.md is the KPI kit's vocabulary, "
        "for a workspace that loaded the kit."
    ),
)


# one connection, one statement at a time: the tool runs on the server's loop, so calls queue
@mcp.tool(run_in_thread=False)
def gloss(statements: str, max_rows: int = 1000) -> str:
    """Run glossql statements and plain SQL in the workspace, `;`-separated, on the server's one connection.

    The connection lives as long as the server: `USE <dataset>`, variables and the
    actor persist from call to call, and the lake keeps what landed. A read comes
    back as a markdown table with `name:TYPE` headers and a row-count footer, whole
    up to `max_rows`, else its first and last rows; a write comes back as its
    outcome line. A refused statement stops the call and is the error, naming its
    place: what landed before it stayed landed, what came after never ran.
    """
    out = []
    for index, statement in enumerate(CON.extract_statements(statements), 1):
        try:
            CON.execute(statement)
        except Exception as error:
            try:
                CON.rollback()
            except Exception:
                pass
            raise ToolError("\n".join(out + [f"statement {index} refused: {error}"]).strip()) from None
        text = render(CON, max_rows)
        if text:
            out.append(text)
    return "\n".join(out) or "(no output)"


@mcp.resource("skill://gloss/SKILL.md", mime_type="text/markdown")
def gloss_skill() -> str:
    """The door and the language on DuckDB: the call, the statement set, the reads, the store."""
    return SKILLS["gloss"].read_text()


@mcp.resource("skill://gloss-kpi/SKILL.md", mime_type="text/markdown")
def kpi_skill() -> str:
    """The KPI kit's vocabulary: each aspect, the order of a round, a grounding's keys."""
    return SKILLS["gloss-kpi"].read_text()


@mcp.resource("workspace://gloss.sql", mime_type="text/plain")
def opener() -> str:
    """The workspace's opener: the extension it loads and the lake it attaches."""
    return OPENER.read_text()


@mcp.prompt(name="gloss")
def gloss_prompt() -> str:
    """Work in the gloss workspace: the door skill as the opening message."""
    return SKILLS["gloss"].read_text()


@mcp.prompt(name="gloss-kpi")
def kpi_prompt() -> str:
    """Gloss with the KPI kit: its skill as the opening message."""
    return SKILLS["gloss-kpi"].read_text()


if __name__ == "__main__":
    mcp.run(show_banner=False)
