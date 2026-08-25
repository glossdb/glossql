# Connect

Three doors, one listener. `/query` and `/app` carry the dataset in the
path — `/<dataset>/query`, `/<dataset>/app` — because a browser is
pointed at one and stays there. `/mcp` is a single endpoint, because an
agent is pointed at a workspace and moves between its datasets. `/` is
the workspace itself: which datasets there are, and the way into each.

Who is speaking rides a bearer token from the workspace's issuer: its
subject is the actor id. With which standing is the door's — `/mcp`
is the agent door, the others are human doors — see
[`install.md`](install.md#tokens).

## `/mcp` — the agent door

Streamable-HTTP MCP at revision `2026-07-28` — genuinely stateless, no
sessions of any kind. One tool: `glossql`. Its `statements` argument
takes a statement or a semicolon-separated sequence; the result is a
JSON array, one outcome per statement:

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

The agent actor's id is the token's subject, never the name the client
gives itself in the handshake — a name a caller picks for itself proves
nothing. A call without a token is answered 401 with the discovery
pointer an OAuth-capable client follows.

**`USE <dataset>;` opens every call that touches dataset-scoped
names.** MCP has no session to hold a binding, so the statements carry
it: `USE` moves the statements after it and expires with the call.
Nothing on the server remembers where you were. A call that names no
dataset is workspace-scoped — which is what `SELECT * FROM datasets`
and a source-grain gloss both want — and a dataset the workspace does
not hold yet is not an error here: `DECLARE DATASET` is what brings the
name into being.

The door also asks. While human-judgment questions stand open, a call
that reads the record carries a round of forms (MCP elicitation); the
answers arrive on the client's retry of the same call and land as
human glosses, witnessed by the server. Anything that is not an answer
defers: the question stays open and is asked again.

With Claude Code:

```bash
set -a; source .env; set +a      # the same application the server is registered as
MCP_CLIENT_SECRET=$GLOSSQL_CLIENT_SECRET claude mcp add --transport http \
  --client-id $GLOSSQL_CLIENT_ID --client-secret --callback-port 3118 \
  glossql http://127.0.0.1:8080/mcp
claude mcp login glossql
```

The callback port is the loopback redirect the issuer's application
must list (`http://localhost:3118/callback`) — any port but the
server's own. `login` opens the issuer's sign-in in a browser and
stores the token; Claude Code refreshes it on its own.

One entry serves the whole workspace; the agent picks its dataset with
`USE`.

Agent knowledge — the grammar, the flows, the judgment — ships as the
agent skills in this repository (`glossql`, `glossql-metrics`,
`glossql-functions`, `glossql-apps`); the door itself only tells
outcomes.

## `/<dataset>/query` — the Arrow door

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
    "http://127.0.0.1:8080/fin/query",
    data=b"SELECT metric, period, value FROM metric_series() WHERE dimension = ''",
)
with urllib.request.urlopen(req) as resp:
    table = pyarrow.ipc.open_stream(resp).read_all()
```

A dataset the workspace does not hold answers `404`, naming the ones
it does. This door reads and writes; it does not bring datasets into
being.

## `/<dataset>/app` — the door for people

Server-rendered data apps. `http://127.0.0.1:8080/fin/app/docket` is
the built-in: what stands open for a human to judge, what has been
settled, what waits on an act, with the metric surfaces and the record
behind them. The URL is the whole state — a filtered view is a link
someone can send. A workspace's own apps serve beside it at
`/<dataset>/app/<name>`, one directory per app under `apps/`.

An app names no dataset; the URL does, so the same app serves every
dataset and the picker in the header is a link that rewrites the first
segment. The writes are human acts, signed with the token's subject:
this is a human door, so every caller that reaches it has human
standing.
