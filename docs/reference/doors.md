# The doors

One binary, one listener.

```
/                          the workspace — which datasets there are
/mcp                       the agent door
/<dataset>/query           the Arrow door
/<dataset>/app             the app door
/assets/<file>             the app door's embedded assets
/.well-known/oauth-protected-resource
```

**The two door kinds scope differently because their callers do.** A
browser is pointed at a dataset and stays there, so `/query` and `/app`
carry it in the path and a link is shareable. An agent is pointed at a
workspace and moves between its datasets, so `/mcp` is one endpoint and
the dataset arrives in the statements.

```
serverd --workspace <dir> [--addr <ip:port>]
        [--row-cap <n>] [--cube-cache <megabytes>] [--require-token]
        [--public-key <key.pem> --issuer <iss>] [--audience <uri>]
```

Defaults: `127.0.0.1:8080`, row cap 200, cube cache 2048 MB. The workspace directory holds `catalog.sqlite`, the
`warehouse/` (created at boot), `apps/`, the band model's `weights/`,
A fresh workspace receives
the shipped system, the measurement library and the KPI kit, before any
door opens.

## The dataset arrives with the call

No door keeps a cursor. `/query` and `/app` are bound by their URL;
`/mcp` opens unbound and `USE` binds it. Either way the binding lives
as long as the call and no longer, so a restart cannot lose it and two
callers working two datasets cannot steer each other. Full
`dataset.table.column` paths still resolve across datasets; the binding
decides what an unprefixed name means.

**Over `/mcp`, every call that touches dataset-scoped names begins with
`USE`.** MCP has no session to hold it — the 2026-07-28 revision removed
protocol-level sessions outright, and requires that anything spanning
requests "be referenced by an explicit identifier the client passes on
each request". `USE` is that identifier. A call that names no dataset is
workspace-scoped, which is what reading `datasets` and writing a
source-grain gloss both want.

A name the workspace does not hold is a 404 on `/query` and `/app`,
naming what it does hold. Over `/mcp` it is not an error: `DECLARE
DATASET` is what brings the name into being.

## Who is speaking

Every door is behind one gate. A request carries a bearer token —
`Authorization: Bearer <jwt>` from a machine, the same string in a
`glossql_token` cookie from a browser — verified against a public key.
Its claims are the actor: `sub` is the id, `kind` (`human` | `agent`)
is the actor kind, and `aud` must be this server's canonical URI
(RFC 8707 §2). glossql is an OAuth 2.1 **resource server** and never an
authorization server: it verifies, it does not issue, and there is no
login flow or user table inside a workspace.

`kind` being signed is the point. Human outranks agent at every read
and the supersession key is (subject, aspect, actor kind), so an agent
that could claim human standing would outrank every human. It cannot,
because it cannot sign.

**No private key comes near this process.** The server is given
`--public-key` (a public key in PEM, not a certificate), `--issuer` and
`--audience`, and whoever holds the matching private half does the
minting — an IdP in a deployment, and in `dev/` a keypair that was used
once and discarded, leaving a public key and two long-lived tokens in
the repository.

A machine carries the token in `Authorization: Bearer`; a browser
carries the same string in a `glossql_token` cookie (`HttpOnly;
SameSite=Lax`).

Without `--public-key` there is no gate: nothing to verify against, no
resource-metadata document to serve, and every request is the door's
own default — the anonymous `human`, or an agent over `/mcp`. That is
how a fresh workspace is opened. With one, `--require-token` refuses a
request that brings none instead of falling back.

Standing the server *witnesses* is separate and is not governed by a
token: an answer elicited mid-call lands with human standing over an
agent's connection, because the server saw the act (SPEC.md §1).

An invalid or missing token answers 401 with `WWW-Authenticate: Bearer
resource_metadata="…"`, pointing at the RFC 9728 document the MCP
authorization spec requires. That document sits outside the gate: it is
where a client learns how to authenticate, so requiring a token to read
it would point the client at itself.

## `/mcp` — the agent door

MCP streamable HTTP, protocol revision `2026-07-28` and nothing behind
it: no sessions, no `Mcp-Session-Id`, no GET stream, no resumability.
An older client is refused with an `UnsupportedProtocolVersionError`
naming what this door speaks, rather than served under semantics it no
longer implements. JSON responses.

One tool: `glossql`, taking `statements` (string) — declarations,
`USE`, `GLOSS`, extraction, probes, and plain SQL. Live state
(datasets, functions, aspects, witnesses, sources, glossary,
measurements, imports) reads as plain tables through the tool; skills
teach the grammar and the flows.

- **The actor is the token's subject.** Without one the call writes as
  `agent`, with agent standing. The request's own `clientInfo` name is
  not used: a caller names itself on every request, so the string
  proves nothing, and the record's actor column is where it says who
  spoke.
- **The row cap bounds engine work.** A tool result ships at most
  `--row-cap` rows and declares `truncated`; the stream terminates
  early, so what the agent won't see is never computed. Metadata reads
  — `GLOSSARY()`, `ATTEST()`, the store relations — are exempt: the
  map must be whole, and the store bounds it.
- **A refusal keeps what landed.** A refused statement is a tool error
  whose text is the refusal; in a sequence it names its place, and a
  second block carries the outcomes of the statements that stood
  (`{"landed": […]}`, the usual shape) — they landed, and the
  statements after the refusal were never attempted.
- **The question round.** While `open_questions` derives rows, calls
  that read the record carry a round of forms — landings and judging
  queries run uninterrupted. The answers arrive on the client's retry
  of the same call and land as human glosses, witnessed here. Anything
  that is not an answer is a defer: the question stays open and is
  asked again.

## `/<dataset>/query` — the Arrow door

`POST /<dataset>/query` with statements as the body. A single read
streams Arrow IPC (`application/vnd.apache.arrow.stream`) straight from
the engine — batches encode as they arrive, memory rides one batch, no
cap, and a client that hangs up cancels the work upstream. An error
after bytes flowed breaks the stream, which is the truth. Everything
else — statement sequences, declarations, writes — answers in the wire
JSON shape; a refused statement is 422 with the reason, and a refused
sequence carries the outcomes of the statements that stood under
`landed`. pyarrow reads the stream directly.

## `/<dataset>/app` — the app door

Server-rendered data apps (see [`apps.md`](../concepts/apps.md) for
authoring). Routes: `/<dataset>/app` (the app list),
`/<dataset>/app/{app}`, `/<dataset>/app/{app}/p/{page}`,
`/<dataset>/app/{app}/frames/{frame}`,
`/<dataset>/app/{app}/specs/{spec}`, and two writes,
`POST /<dataset>/app/{app}/rule` and `.../remeasure`. The vendored
assets are not dataset-scoped and serve at `/assets/{file}`.

- **An app names no dataset.** The URL binds it, so one app serves
  every dataset in the workspace and the header's picker is a link that
  rewrites the first path segment.
- **Frames are Arrow IPC.** URL params bind as plan placeholders —
  `$from` in the frame SQL, `?from=…` on the URL — typed values
  through the plan, never text spliced into SQL. Everything arrives as
  Utf8; the frame SQL casts explicitly. A placeholder nobody bound
  fails at execution, telling the author what the URL owed. The dataset
  is not among them: the channel is bound to the one the path named, so
  a frame reads it as `current_dataset` and a query string has nothing
  to override.
- **Frames only read.** They run as the Human actor `app:<name>` on the
  plane's channel for that dataset, fixed at channel construction.
- **The writes are human acts.** A token carrying agent standing is
  refused (403) rather than downgraded. A ruling is accepted only while
  `open_questions` still derives that exact (subject, aspect, key) in
  the bound dataset — a stale tab is refused (409), a dead question
  cannot be re-ruled, another dataset's open question on a same-named
  subject cannot admit one, and the recorded prose comes from the
  derivation, never from the browser.
  Success is 204 with `HX-Trigger: glossql:written`; the browser's
  frame store hears it, drops its caches, and every connected component
  refetches in place. The ruling lands in the human's own slot,
  witnessed by this server.
