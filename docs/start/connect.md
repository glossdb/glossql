# Connect

Four doors, one listener. `/query`, `/app` and `/mcp` carry the
dataset in the path — `/<dataset>/query`, `/<dataset>/app`,
`/<dataset>/mcp` — so a caller is pointed at one dataset and stays
there. `/mcp` without a dataset is the workspace's agent door, for the
agent that declares sources and brings datasets into being. `/` is the
workspace itself: which datasets there are, and the way into each.

The actor rides a bearer token from the workspace's issuer: the
token's subject is the actor id. The door sets the standing — the two
`mcp` doors are agent doors, the others are human doors — see
[`install.md`](install.md#tokens).

## `/<dataset>/mcp` and `/mcp` — the agent doors

Streamable-HTTP MCP at revision `2026-07-28` — stateless, no sessions
of any kind. One tool: `glossql`. Its `statements` argument
takes a statement or a semicolon-separated sequence; the result is a
JSON array, one outcome per statement:

- a read — `{"columns": [...], "rows": [...], "row_count": n,
  "truncated": bool}`. `columns` carries the result's shape with
  engine types even at zero rows (a `LIMIT 0` rehearsal returns the
  schema, which is its point). Data rows cap at `--row-cap`;
  `truncated: true` means refine the query, not that the result is
  complete. Metadata reads — `GLOSSARY()`, `ATTEST()`, the store
  relations — arrive whole, alone or inside a sequence.
- a write — `{"affected": n}` or `{"done": true}`.
- a refusal — a tool error whose text names what was wrong and, in a
  sequence, its place: what landed stayed landed, the rest was never
  attempted.

The agent actor's id is the token's subject, never the name the client
gives itself in the handshake — a name a caller picks for itself proves
nothing. A call without a token gets a 401 with the discovery
pointer an OAuth-capable client follows.

**A dataset's door opens every call on that dataset.** Its tables and
columns resolve unprefixed, another dataset's with the dataset's name
in front, and the result closes with the dataset's `next` line. A
dataset the workspace does not hold is a 404 naming what it does hold.

**At the workspace door, `USE <dataset>;` opens every call that
touches dataset-scoped names.** MCP has no session to hold a binding,
so the statements carry it: `USE` moves the statements after it and
expires with the call. Nothing on the server remembers where you were.
A call that names no dataset is workspace-scoped — which is what
`SELECT * FROM datasets` and a source-grain gloss both want — and a
dataset the workspace does not hold yet is not an error here:
`DECLARE DATASET` creates the name.

The door also asks. While human-judgment questions are open, a call
that reads the record carries a round of forms (MCP elicitation); the
answers arrive on the client's retry of the same call and land as
human glosses, witnessed by the server. Anything that is not an answer
defers: the question stays open and is asked again.

With Claude Code:

```bash
set -a; source .env; set +a      # the same application the server is registered as
MCP_CLIENT_SECRET=$GLOSSQL_CLIENT_SECRET claude mcp add --transport http \
  --client-id $GLOSSQL_CLIENT_ID --client-secret --callback-port 3118 \
  glossql http://127.0.0.1:8080/fin/mcp
claude mcp login glossql
```

The callback port is the loopback redirect the issuer's application
must list (`http://localhost:3118/callback`) — any port but the
server's own. `login` opens the issuer's sign-in in a browser and
stores the token; Claude Code refreshes it on its own.

One entry per dataset the agent works on. An entry at `/mcp` serves
the whole workspace, and there the agent picks its dataset with `USE`.

Agent knowledge — the grammar, the flows, the judgment — ships as the
agent skills in this repository (`glossql`, `glossql-metrics`,
`glossql-functions`, `glossql-apps`); the door itself only reports
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
it does. This door reads and writes; it does not create datasets.

## `/<dataset>/app` — the door for people

Server-rendered data apps. `http://127.0.0.1:8080/fin/app` opens the
docket, the dataset's page: what is open for a human to judge, what
is settled, what waits on an act; the metrics; the quality of the
data (each landing's account, the checks, the column coverage); the
column lineage; and under Export every landed table and every served
read with a CSV and a Parquet download (`/fin/app/export/orders.csv`,
`/fin/app/export/read.dso.parquet`). The URL is the whole state — a
filtered view is a link someone can send. A workspace's own apps
serve beside it at `/<dataset>/app/<name>`, authored as glosses over
the agent door.

An app names no dataset; the URL does, so the same app serves every
dataset. The bar reads as the URL — a dataset picker, an app picker,
then the app's pages as tabs — and each picker rewrites one path
segment. The writes are human acts, signed with the token's subject:
this is a human door, so every caller that reaches it has human
standing.

A browser needs no setup: open `http://127.0.0.1:8080/` — the
workspace root, every dataset at a glance with the way into each —
and the server sends it to the issuer to sign in, then brings it back
with the token in a cookie
(`/auth/login`, `/auth/callback`; `/auth/logout` clears it). The person
signing in is the token's subject — the same name an agent's connection
carries when that person is behind it.
