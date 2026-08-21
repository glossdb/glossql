# Install and run

`serverd` is one binary serving one workspace — the directory that
holds your data lake and everything declared over it.

## Build and start

```bash
cargo build --release -p glossql-serverd
./target/release/serverd --workspace ~/acme
```

The server prints its doors and listens:

```
glossql tokens in /Users/you/acme/tokens — agent.jwt for an MCP client's headers
  open http://127.0.0.1:8080/?token=eyJ… (the door swaps it for a cookie)
  a request with no token is served as the anonymous human (agent over /mcp) — --require-token to refuse it instead
serverd on 127.0.0.1:8080 — / (datasets), /<dataset>/mcp, /<dataset>/query, /<dataset>/app
```

## Flags

| flag | default | meaning |
|---|---|---|
| `--workspace <dir>` | required | the workspace directory; created content lands here |
| `--addr <ip:port>` | `127.0.0.1:8080` | where the doors listen |
| `--agent <id>` | `agent` | fallback agent actor id for MCP calls whose initialize named no client |
| `--row-cap <n>` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--cube-cache <megabytes>` | `2048` | the byte budget for the cube cache — every metric's cells held in memory, evicted least-recently-used past it; the `cube` aspect bounds one cube, this bounds them all |
| `--require-token` | off | refuse a request that carries no token, instead of serving it as the door's default identity |
| `--issuer-key <pem>` | — | a configured issuer's **public key** in PEM (not a certificate). With it, the workspace mints nothing and every request must bring a token |
| `--issuer <iss>` | — | the issuer the token's `iss` must name; required with `--issuer-key` |
| `--audience <uri>` | `http://<addr>` | this server's canonical URI, which every token's `aud` must name (RFC 8707 §2) |

## Tokens

Who is speaking is a signed claim, not a door's assumption: `sub` is
the actor id and `kind` (`human` | `agent`) is the actor kind. Human
outranks agent at every read, so an agent that could claim human
standing would outrank every human — it cannot, because it cannot
sign.

glossql is an OAuth 2.1 resource server and never an authorization
server. It verifies; it does not issue, and there is no login flow or
user table inside a workspace.

Without `--issuer-key`, the workspace holds its own key. First boot
writes an Ed25519 keypair into `keys/` (the private half `0600`) and
mints one long-lived token per actor kind into `tokens/`:

- `tokens/agent.jwt` goes into an MCP client's configured headers.
- `tokens/human.jwt` reaches a browser through the startup link. The
  door swaps it into an `HttpOnly; SameSite=Lax` cookie and redirects
  to the bare path, so the credential does not stay in the address bar
  or the history.

Until `--require-token`, a request that brings none is still served —
as the anonymous `human`, or as an agent over `/mcp`. That is how a
fresh workspace is opened before anyone holds a token.

## What boot does

Opening a workspace creates `warehouse/` if absent and opens the
catalog. A fresh workspace then receives the shipped system before any
door opens: the measurement library and the KPI kit (the semantic
vocabulary and its witnesses) are declared into the store — as
ordinary declarations, readable back through the `functions` and
`aspects` relations like anything an agent writes. The bootstrap is
idempotent; every boot calls it and it declares only into a workspace
where none of it stands. Nothing is written outside the workspace
directory.

## Workspace anatomy

```
acme/
  catalog.sqlite     the Iceberg catalog
  warehouse/         the lake — every table and every declared
                     relation lives here as Iceberg data
  apps/              optional: workspace apps, one directory per app;
                     a workspace app named like a built-in shadows it
                     whole
  weights/           optional: the band model's weights, verified by
                     digest at load (a sibling ../weights directory
                     is also searched)
```

The lake is the whole store. There is no separate database for the
glossary: glosses, functions, witnesses, measurements — every relation
is an Iceberg table under `warehouse/`, and the workspace directory is
the complete, copyable state of the system.
