# Connect

Three doors, one listener. Who is speaking rides the door: `/mcp`
speaks as an agent actor, `/query` and `/app` speak as the human — and
since a human slot outranks an agent slot at every read, choosing a
door is choosing a rank.

## `/mcp` — the agent door

Streamable-HTTP MCP, stateless, one tool: `glossql`. Its `statements`
argument takes a statement or a semicolon-separated sequence; the
result is a JSON array, one outcome per statement:

- a read — `{"columns": [...], "rows": [...], "row_count": n,
  "truncated": bool}`. `columns` carries the result's shape with
  engine types even at zero rows (a `LIMIT 0` rehearsal returns the
  schema, which is its point). Data rows cap at `--row-cap`;
  `truncated: true` means refine the query, not that the result is
  complete. Metadata reads — `GLOSSARY()`, `ATTEST()`, the store
  relations — sent as their own single statement arrive whole.
- a write — `{"affected": n}` or `{"done": true}`.
- a refusal — a tool error whose text names what was wrong and, in a
  sequence, its place: what landed stayed landed, the rest was never
  attempted.

The agent actor's id is the client name from the initialize handshake;
a call that named no client speaks as `--agent`. `USE <dataset>;`
survives between calls — the server keeps one session per actor and
dataset.

The door also asks. While human-judgment questions stand open, a call
that reads the record carries one question form (MCP elicitation);
the answer lands as a human gloss and the question retires. A client
without form support gets nothing — the agent relays questions in
chat instead. `--round-wait` bounds how long a form waits.

With Claude Code:

```bash
claude mcp add --transport http glossql http://127.0.0.1:8080/mcp
```

Agent knowledge — the grammar, the flows, the judgment — ships as the
agent skills in this repository (`glossql`, `glossql-metrics`,
`glossql-functions`, `glossql-apps`); the door itself only tells
outcomes.

## `/query` — the Arrow door

Plain HTTP, no client library. POST the statement text as the body:

- **One read** answers as an Arrow IPC stream
  (`application/vnd.apache.arrow.stream`), straight from the engine —
  no row cap, one batch in memory, and hanging up cancels the work.
- **Anything else** — sequences, declarations, writes — answers in the
  same JSON outcome shape as the MCP door.
- A refused statement answers `422` with `{"error": "…"}`; the text is
  the refusal.

```python
import urllib.request
import pyarrow.ipc

req = urllib.request.Request(
    "http://127.0.0.1:8080/query",
    data=b"SELECT metric, period, value FROM metric_series() WHERE dimension = ''",
)
with urllib.request.urlopen(req) as resp:
    table = pyarrow.ipc.open_stream(resp).read_all()
```

`/query` speaks as the workspace's human actor by design.

## `/app` — the door for people

Server-rendered data apps. `http://127.0.0.1:8080/app/docket` is the
built-in: what stands open for a human to judge, what has been
settled, what waits on an act, with the metric surfaces and the record
behind them. The URL is the whole state — a filtered view is a link
someone can send — and the door takes exactly one write, the docket's
ruling form. A workspace's own apps serve beside it at `/app/<name>`,
one directory per app under `apps/`.
