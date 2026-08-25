# Install and run

`serverd` is one binary serving one workspace — the directory that
holds your data lake and everything declared over it.

## Build and start

```bash
cargo build --release -p glossql-serverd
cp .env.example .env            # then fill it in — see Tokens below
./target/release/serverd --workspace ~/acme
```

The server reads `.env`, reads the issuer's keys, prints its doors and
listens:

```
glossql verifying https://issuer.example tokens for http://127.0.0.1:8080 (application a1b2c3…)
serverd on 127.0.0.1:8080 — / (datasets), /mcp, /<dataset>/query, /<dataset>/app
```

## Flags

| flag | default | meaning |
|---|---|---|
| `--workspace <dir>` | required | the workspace directory; created content lands here |
| `--addr <ip:port>` | `127.0.0.1:8080` | where the doors listen |
| `--row-cap <n>` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--cube-cache <megabytes>` | `2048` | the byte budget for the cube cache — every metric's cells held in memory, evicted least-recently-used past it; the `cube` aspect bounds one cube, this bounds them all |
| `--memory-limit <megabytes>` | `4096` | the engine's memory ceiling for the whole process. A plan that would exceed it is refused by name; nothing spills, because a container is the wrong place to be writing overflow. Separate from `--cube-cache`, whose bytes sit outside the engine — size a deployment for the sum |

The authorization arrangement is not a flag. It is read from `.env` in
the working directory, or from the environment (a set variable wins
over the file, which is how a container is configured without one):

| variable | meaning |
|---|---|
| `GLOSSQL_ISSUER` | the authorization server's issuer URL; its OpenID configuration names the keys tokens are verified against |
| `GLOSSQL_AUDIENCE` | this server's canonical URI, the API identifier registered at the issuer and the `aud` a token must name (RFC 8707 §2); defaults to `http://<addr>` |
| `GLOSSQL_CLIENT_ID` | the application registered at the issuer for this server — what a token minted for it carries as `azp` |
| `GLOSSQL_CLIENT_SECRET` | that application's secret, used by the browser login on `/app` |

`.env.example` at the repository root lists them; `.env` is never
committed.

## Tokens

Who is speaking is the token's subject; with which standing is the
door's: `/mcp` writes as an agent, the other doors as a human. glossql
is an OAuth 2.1 resource server and never an authorization server — it
verifies against the keys the issuer publishes, it does not issue, and
there is no login flow, client registration or user table inside a
workspace. There is no open mode: a request without a valid token is
answered 401, and a server without an issuer does not start.

The issuer is any OpenID Connect provider. Register this server there
as an API whose identifier is `GLOSSQL_AUDIENCE`, and one confidential
application (`GLOSSQL_CLIENT_ID`) with two redirect URIs:
`<audience>/auth/callback` for the browser login, and the loopback
redirect of the MCP client that will sign in with it (Claude Code:
`http://localhost:3118/callback`, see [`connect.md`](connect.md)). The
provider's discovery document
(`<issuer>/.well-known/openid-configuration`) is everything the server
needs. Tokens must be RS256, ES256/384 or EdDSA, name their key
(`kid`), and carry `iss`, `sub`, `exp`.

A token is bound to this server by its `aud` naming the audience. MCP
clients ask for that with the RFC 8707 `resource` parameter; an issuer
that fills `aud` only from a parameter of its own mints `aud: []` for
them, and such a token is bound instead by the application it was
minted for —
`azp`, or `client_id` as RFC 9068 spells it — which must be
`GLOSSQL_CLIENT_ID`. A token for another application, or naming another
resource, is refused either way.

How a client obtains a token is the client's flow with the issuer —
[`connect.md`](connect.md) shows Claude Code's.

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
