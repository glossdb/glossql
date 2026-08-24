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
glossql open — no --public-key, so no token is verified and every request writes as the anonymous human (agent over /mcp).
  Development tokens and the key that verifies them are in dev/.
serverd on 127.0.0.1:8080 — / (datasets), /mcp, /<dataset>/query, /<dataset>/app
```

## Flags

| flag | default | meaning |
|---|---|---|
| `--workspace <dir>` | required | the workspace directory; created content lands here |
| `--addr <ip:port>` | `127.0.0.1:8080` | where the doors listen |
| `--row-cap <n>` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--cube-cache <megabytes>` | `2048` | the byte budget for the cube cache — every metric's cells held in memory, evicted least-recently-used past it; the `cube` aspect bounds one cube, this bounds them all |
| `--require-token` | off | refuse a request that carries no token, instead of serving it as the door's default identity; needs `--public-key` and `--issuer` |
| `--public-key <pem>` | — | the **public key** tokens are verified against, in PEM (not a certificate). Without it there is no gate at all |
| `--issuer <iss>` | — | the issuer the token's `iss` must name; required with `--public-key` |
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

**No private key comes near this process.** Whoever holds the private
half mints; the server is given a public key and verifies. In a
deployment that half belongs to your IdP.

For development the repository carries `dev/` — `public.pem` and two
long-lived tokens, `human.jwt` and `agent.jwt`, minted for
`http://127.0.0.1:8080` by a keypair that was used once and thrown
away. They are committed on purpose: there is no secret in them worth
keeping, and a credential you can read is one nobody has to be handed.

```bash
./target/release/serverd --workspace ~/acme \
  --public-key dev/public.pem --issuer glossql-dev
```

The agent token goes into an MCP client's configured headers; the human
token goes into a browser as a cookie named `glossql_token`
(`HttpOnly; SameSite=Lax`).

When they expire, mint a new pair the same way — an Ed25519 keypair, a
JWT per actor kind carrying `iss`, `aud`, `sub`, `kind`, `exp`, `iat`,
and then delete the private key. Nothing in this repository can sign,
which is the property worth keeping.

Without `--public-key` there is no gate: every request is served as the
anonymous `human`, or as an agent over `/mcp`. That is how a fresh
workspace is opened. With one, `--require-token` refuses a request that
brings none instead of falling back.

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
