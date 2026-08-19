# The doors

One binary, one listener, three doors:

```
serverd --workspace <dir> [--addr <ip:port>] [--agent <id>]
        [--row-cap <n>] [--round-wait <secs>]
```

Defaults: `127.0.0.1:8080`, agent fallback id `agent`, row cap 200,
round wait 120 s. The workspace directory holds `catalog.sqlite`, the
`warehouse/` (created at boot), `apps/`, and the band model's
`weights/`. A fresh workspace receives the shipped system — the
measurement library and the KPI kit — before any door opens.

The doors are unauthenticated while governance stays a held-open
question; door choice is rank choice. `/mcp` speaks as an agent;
`/query` and `/app` speak as the one anonymous `human` actor — human
standing is unsigned, and human > agent > function holds at every
read.

## `/mcp` — the agent door

MCP streamable HTTP, stateless, JSON responses. One tool: `glossql`,
taking `statements` (string) — declarations, `USE`, `GLOSS`,
extraction, probes, and plain SQL. Live state (datasets, functions,
aspects, witnesses, sources, glossary, measurements, imports) reads as
plain tables through the tool; skills teach the grammar and the flows.

- **Actor rides the connection.** The handshake's `clientInfo` name is
  the agent actor; on the sessionless lifecycle the per-request
  `_meta` stamp carries it. A call no initialize named falls back to
  `--agent`.
- **The row cap bounds engine work.** A tool result ships at most
  `--row-cap` rows and declares `truncated`; the stream terminates
  early, so what the agent won't see is never computed. Metadata reads
  — `GLOSSARY()`, `ATTEST()`, the store relations — are exempt: the
  map must be whole, and the store bounds it.
- **The question round.** While `open_questions` derives rows, calls
  that read the record carry one question each (an owed enum becomes a
  choice form, a loose assumption confirm/correct) — landings and
  judging queries run uninterrupted. On the sessionless lifecycle the
  answer arrives on the client's retry and lands as the human gloss;
  on a session lifecycle the ask rides the call's own stream and
  silence past `--round-wait` reads as a decline. A decline defers the
  question for the server run, and a writing call re-opens what was
  deferred.
- **Sessions are reaped between calls.** A reaped session answers a
  bare `Not Found`; the client re-inits and replays `USE`.

## `/query` — the Arrow door

`POST /query` with statements as the body. A single read streams
Arrow IPC (`application/vnd.apache.arrow.stream`) straight from the
engine — batches encode as they arrive, memory rides one batch, no
cap, and a client that hangs up cancels the work upstream. An error
after bytes flowed breaks the stream, which is the truth. Everything
else — statement sequences, declarations, writes — answers in the wire
JSON shape; a refused statement is 422 with the reason. pyarrow reads
the stream directly.

## `/app` — the app door

Server-rendered data apps (see [`apps.md`](apps.md) for authoring).
Routes: `/app` (the app list), `/app/{app}`, `/app/{app}/p/{page}`,
`/app/{app}/frames/{frame}`, `/app/{app}/specs/{spec}`,
`/app/assets/{file}` (the vendored assets), and the one write,
`POST /app/{app}/rule`.

- **Frames are Arrow IPC.** URL params bind as plan placeholders —
  `$from` in the frame SQL, `?from=…` on the URL — typed values
  through the plan, never text spliced into SQL. Everything arrives as
  Utf8; the frame SQL casts explicitly. A placeholder nobody bound
  fails at execution, telling the author what the URL owed. `$dataset`
  is reserved: always the app's bound dataset, never overridable from
  the URL.
- **Frames only read.** They run as the Human actor `app:<name>` on
  the plane's channel for the app's dataset, fixed at channel
  construction.
- **The one write is a ruling.** The post is accepted only while
  `open_questions` still derives that exact (subject, aspect, key) —
  a stale tab is refused (409), a dead question cannot be re-ruled,
  and the recorded prose comes from the derivation, never from the
  browser. Success is 204 with `HX-Trigger: glossql:written`; the
  browser's frame store hears it, drops its caches, and every
  connected component refetches in place. The ruling lands in the
  human's own slot, witnessed by this server.
