# The doors

One binary, one listener.

```
/                          the workspace — which datasets there are
/mcp                       the agent door
/<dataset>/query           the Arrow door
/<dataset>/app             the app door
/assets/<file>             the app door's embedded assets
/auth/login, /auth/callback, /auth/logout
                           the browser's way to a token
/.well-known/oauth-protected-resource
```

**The two door kinds scope differently because their callers do.** A
browser is pointed at a dataset and stays there, so `/query` and `/app`
carry it in the path and a link is shareable. An agent is pointed at a
workspace and moves between its datasets, so `/mcp` is one endpoint and
the dataset arrives in the statements.

```
serverd --workspace <dir> [--addr <ip:port>] [--row-cap <n>]
        [--cube-cache <megabytes>] [--memory-limit <megabytes>]
        [--tls-cert <pem> --tls-key <pem>]
```

The authorization arrangement — `GLOSSQL_ISSUER`, `GLOSSQL_AUDIENCE`,
`GLOSSQL_CLIENT_ID`, `GLOSSQL_CLIENT_SECRET` — is read from `.env` or
the environment, never from flags ([install](../start/install.md)).

With `--tls-cert` and `--tls-key` (both or neither) the doors serve
https — what a desktop MCP client requires of a remote server. The
repo's `certs/` holds a self-signed pair for `localhost` and the
loopback addresses, and the test suite checks it against the server;
regenerate it with

```
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout certs/localhost-key.pem -out certs/localhost.pem \
  -days 3650 -nodes -subj "/CN=glossql dev" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=digitalSignature" -addext "extendedKeyUsage=serverAuth"
```

(`CA:FALSE` matters: a client's webpki refuses a CA certificate
serving as the endpoint's own). A deployment that terminates TLS at
its edge does not pass the flags. The default `GLOSSQL_AUDIENCE`
follows the served scheme.

Defaults: `127.0.0.1:8080`, row cap 200, cube cache 2048 MB, memory
limit 4096 MB. The cube cache and the memory limit are two budgets, not
one: the cache holds its bytes outside the engine, so a deployment is
sized for their sum. The workspace directory holds `catalog.sqlite`, the
`warehouse/` (created at boot), `apps/`, and the band model's
`weights/`. A fresh workspace receives
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
DATASET` is what creates the name.

## Who is speaking

Every door is behind one gate. A request carries a bearer token —
`Authorization: Bearer <jwt>` from a machine, the same string in a
`glossql_token` cookie (`HttpOnly; SameSite=Lax`) from a browser —
verified against the keys its issuer publishes. glossql is an OAuth 2.1
**resource server** (MCP authorization, revision 2026-07-28) and never
an authorization server: it verifies, it does not issue, and there is
no login flow, no client registration and no user table inside a
workspace. Those are the issuer's.

**Identity is the token's; standing is the door's.** The token's `sub`
is the actor id. The actor kind is which door the request came
through: `/mcp` is the agent door, `/`, `/query` and `/app` are human
doors — the actor rides the transport (SPEC.md §1). Nobody signs a
standing; the supersession key's third leg, (subject, aspect, actor
kind), is settled by where the request arrived.

The gate is configured by the arrangement in `.env`. `GLOSSQL_ISSUER`
is the authorization server's URL: its OpenID configuration is read at
boot and names the key set (`jwks_uri`), which is fetched then and
again only when a token names a key not in it. `GLOSSQL_AUDIENCE` is
this server's canonical URI, which a token's `aud` names (RFC 8707 §2)
— a token minted for another resource does not open this one.
An issuer that does not honour `resource=` cannot mint a token this
server accepts, because nothing else in a token says which resource it
was meant for. `GLOSSQL_CLIENT_ID` is the application registered at the
issuer for this server, which the browser login signs in and exchanges
its code as; it binds nothing. Signature (by the key
the token's `kid` names, with the algorithm that key admits), `iss`,
`exp` and the binding are checked on every request; a refusal is
logged with its reason, never with the token. A server that cannot
reach its issuer does not start.

The one way around the gate is explicit. `GLOSSQL_INSECURE_OPEN=true`
serves every door without authentication: no login and no discovery
document — with no 401 to answer, a client is never sent to
authenticate — and every caller is recorded as `insecure_dev_mode`,
still with the door's standing. The name is the warning, and the
server says so at start. A laptop trying the server out,
never a deployment.

Standing the server *witnesses* is separate and is not governed by the
token: an answer elicited mid-call lands with human standing under the
same subject over the agent's connection, because the server saw the
act (SPEC.md §1).

A missing or invalid token answers 401 with `WWW-Authenticate: Bearer
resource_metadata="…"`, pointing at the RFC 9728 document at
`/.well-known/oauth-protected-resource` (also under any path):
`resource` (the audience) and `authorization_servers` (the issuer). A
browser navigating to a door — a GET that asks for HTML — is sent to
`/auth/login` instead and brought back afterwards.

**The browser's token.** A machine obtains its token itself; a browser
is walked through it: `/auth/login` sends it to the issuer's sign-in
(authorization code with PKCE, RFC 7636, the resource named per RFC
8707), `/auth/callback` exchanges the code at the issuer's token
endpoint as the registered application, verifies the token with the
same gate every door uses, and sets it as the `glossql_token` cookie.
The issuer must list `<audience>/auth/callback` as a redirect URI. The
login in progress — state, PKCE verifier, where to go back to — rides a
ten-minute cookie scoped to `/auth`; the server holds no session.
`/auth/logout` clears the cookie.

Three things sit outside the gate: `/auth`, where a browser goes
because it holds no token; the discovery document, which is
where a client learns how to authenticate; and `/assets`, the app
door's own script and styles, which hold no data.

## `/mcp` — the agent door

MCP streamable HTTP. The door speaks protocol revision `2026-07-28`
first and negotiates down to the library's floor (`2025-11-25` today)
for older clients — served statelessly whatever the revision: no
sessions, no `Mcp-Session-Id`, no GET stream, no resumability. A
request that stamps no version marker at all is served at the server's
own revision, which is what the spec has a server assume for an absent
header — some real clients stamp only part of their traffic. A client
below the floor is refused, naming what the door speaks. JSON
responses.

One tool: `glossql`, taking `statements` (string) — declarations,
`USE`, `GLOSS`, extraction, probes, and plain SQL. Its description is
the contract for every call — the `USE` rule, the outcome shapes, the
refusal shape and the round's cadence — because a client carries the
tool list on every turn and fetches a resource once, if at all. Live
state (datasets, functions, aspects, witnesses, sources, glossary,
measurements, imports) reads as plain tables through the tool; skills
teach the grammar and the flows, and the door serves them itself:
each skill is a resource (`skill://<name>/SKILL.md`) and a prompt of
the same name, its references are sibling resources
(`skill://<name>/references/<page>.md`, listed by their title line,
which says when to read each), the `docs/` pages are
`doc://docs/<section>/<page>.md`, and the language spec and grammar
are `doc://SPEC.md` and `doc://grammar.ebnf` — what an agent working
in the repository reads is what a connected one is served. The
initialize instructions are the map of the objects and say which page
to read at which moment. All of it is embedded at compile time, so
what a build serves is what its suite tested.

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
- **A grounding's write answers with its fact.** `GLOSS` on a QUERY
  aspect returns the metric's row in the `metric_axes()` shape at the
  pin the write moved to — whether the SQL plans, the verb and its
  basis, the admitted axes, and each served column not admitted with
  the road back in — in place of `{"done"}` (the reads reference has
  the columns). Every other gloss answers `{"done"}`.
- **A refusal keeps what landed.** A refused statement is a tool error
  whose text is the refusal; in a sequence it names its place, and a
  second block carries the outcomes of the statements that stood
  (`{"landed": […]}`, the usual shape) — they landed, and the
  statements after the refusal were never attempted.
- **The brief.** The initialize instructions close with one composed
  line over live counts, and the same line rides any tool result whose
  call moved them: what is owed first — rulings awaiting their fold-in,
  approved recipe changes awaiting their re-declare, judgment questions
  open for the human — then the record's size, how many human writings
  stand and when the latest landed. A count of writings is presence,
  never work; `owed` and `open_questions` are the reads behind the
  first part. The instructions then name where to begin — a workspace
  before its first dataset has no brief to sweep, and reads
  `workspace_next`; one with a dataset opens with the brief, once,
  as a read and never a gate. That opening rides initialize only.
- **The question round.** While `open_questions` derives rows, calls
  that read the record carry a round of forms — landings and judging
  queries run uninterrupted, and a question served once waits for the
  human while the work goes on. The answers arrive on the client's
  retry of the same call and land as human glosses, witnessed here.
  Anything that is not an answer is a defer: the question stays open
  and is asked again on the next record read.

## `/<dataset>/query` — the Arrow door

`POST /<dataset>/query` with statements as the body. A single read
streams Arrow IPC (`application/vnd.apache.arrow.stream`) straight from
the engine — batches encode as they arrive, memory rides one batch, no
cap, and a client that hangs up cancels the work upstream. The status
is set on the first batch, not on the plan: a read the engine cannot
start is 422 with the reason. An error after bytes flowed ends the
body without its terminating chunk; the client's reader sees a
truncated stream, and the server logs the reason. Everything
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
