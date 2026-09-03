# Install and run

`glossql` is one binary serving one workspace — the directory that
holds your data lake and everything declared over it. The binary
carries the band model's regressor; there is nothing else to install.
The server picks the compute device at start: Metal on Apple
Silicon, CUDA where the cuda flavor finds a device, CPU otherwise.

## Install

macOS (Apple Silicon):

```bash
brew install glossdb/glossql/glossql
```

Debian-based Linux, x86_64 (Debian 13 / Ubuntu 24.04 or newer):
download the `.deb` from the [releases
page](https://github.com/glossdb/glossql/releases) and

```bash
sudo apt install ./glossql_<version>_amd64.deb
```

Machines with an NVIDIA GPU take `glossql-cuda_…` instead: the band
model then runs on GPU 0 (compute capability 8.0 — Ampere — or
newer), falling back to CPU when no device answers. That flavor loads
only where the NVIDIA driver and the CUDA 12 runtime libraries
(cudart, cublas, nvrtc, curand) are installed — on machines without
them, `glossql` is the right package.

Both packages install one command, `glossql`.

## Build and start

```bash
cargo build --release -p glossql-serverd
cp .env.example .env            # then fill it in — see Tokens below
./target/release/glossql --workspace ~/acme
```

A source build expects the [tabicl-candle
checkout](https://github.com/glossdb/tabicl-candle) as a sibling
directory and stages its converted weights beside the binary;
`--features embed-weights` bakes the regressor in instead, which is
how the released artifacts are built.

The server reads `.env`, reads the issuer's keys, prints its doors and
listens:

```
2026-09-03T08:04:42.103275Z  INFO verifying tokens issuer=https://issuer.example audience=http://127.0.0.1:8080 application=a1b2c3…
2026-09-03T08:04:42.104551Z  INFO glossql listening — / (datasets), /mcp, /<dataset>/query, /<dataset>/app addr=127.0.0.1:8080 scheme="http"
```

## Flags

| flag | default | meaning |
|---|---|---|
| `--workspace <dir>` | required without a catalog connection | the workspace directory; created content lands here. With `GLOSSQL_CATALOG_URI` set it may be left unnamed — the working directory serves — since it then holds only `apps/` and `weights/` |
| `--addr <ip:port>` | `127.0.0.1:8080` | where the doors listen |
| `--row-cap <n>` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--cube-cache <megabytes>` | `2048` | the byte budget for the cube cache — every metric's cells held in memory, evicted least-recently-used past it; the `cube` aspect bounds one cube, this bounds them all |
| `--memory-limit <megabytes>` | `4096` | the engine's memory ceiling for the whole process. A plan that would exceed it is refused by name; nothing spills, because a container is the wrong place to be writing overflow. Separate from `--cube-cache`, whose bytes sit outside the engine — size a deployment for the sum |

Authorization is not a flag. The server reads it from `.env` in the
working directory, or from the environment (a set variable wins over
the file, which is how a container is configured without one):

| variable | meaning |
|---|---|
| `GLOSSQL_ISSUER` | the authorization server's issuer URL; its OpenID configuration names the keys tokens are verified against |
| `GLOSSQL_AUDIENCE` | this server's canonical URI, the API identifier registered at the issuer and the `aud` a token must name (RFC 8707 §2); defaults to `http://<addr>` |
| `GLOSSQL_CLIENT_ID` | the application registered at the issuer for this server, which the browser login on `/app` signs in and exchanges its code as |
| `GLOSSQL_CLIENT_SECRET` | that application's secret, used by the browser login on `/app` |
| `GLOSSQL_INSECURE_OPEN` | `true` (the literal) serves every door without authentication — no issuer needed, no login served, every caller recorded as `insecure_dev_mode` with the door's standing. The name is the warning: a laptop trying the server out, never a deployment |
| `GLOSSQL_CATALOG_URI` | an Iceberg REST catalog's endpoint. Set, the workspace's catalog is that service rather than the workspace directory's own SQLite file; storage is attached on the catalog's side, and each table load answers with what its FileIO needs (the connection always offers `X-Iceberg-Access-Delegation: vended-credentials`). Unset, the local catalog is used |
| `GLOSSQL_CATALOG_WAREHOUSE` | which warehouse of that catalog this workspace is — required with the URI |
| `GLOSSQL_CATALOG_TOKEN` | a bearer token used as-is: an object-store platform's API token, minted with both its catalog and its storage permissions. Exactly one of token or credential authenticates the connection |
| `GLOSSQL_CATALOG_CREDENTIAL` | `client_id:client_secret`, exchanged for a bearer token at `GLOSSQL_CATALOG_TOKEN_ENDPOINT` (required with it) and exchanged again when the token nears its stated expiry; `GLOSSQL_CATALOG_SCOPE` as the backend's documentation names it |
| `AWS_ACCESS_KEY_ID` … | storage itself needs no glossql variables — table loads answer with what FileIO needs. A dev store that vends nothing is configured through the standard AWS conventions (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_DEFAULT_REGION`, `AWS_ALLOW_HTTP`), read by the storage layer itself |
| `GLOSSQL_LOG` | what the server puts on its record — a `tracing` filter. A bare level (`debug`) is this server's crates at that level, the substrate held at `info` and the MCP library at `warn`; directives (`glossql_session=debug,apache_avro=debug`) are taken as written. `RUST_LOG` is honoured when it is unset; `info` when neither is |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | where an OpenTelemetry collector listens (`http://127.0.0.1:4318`; `/v1/traces` and `/v1/logs` are appended). Set, the record is also exported there — spans as traces, events as logs, OTLP over HTTP, protobuf, batched. Unset, nothing is exported. The exporter's other variables are the SDK's own: `OTEL_EXPORTER_OTLP_HEADERS` for a hosted collector's credentials, `OTEL_RESOURCE_ATTRIBUTES` for what names the deployment beyond `service.name=glossql` |

`.env.example` at the repository root lists them; `.env` is never
committed.

The record goes to stdout: lines for a person when that is a
terminal, JSON otherwise. At `info` the server logs a request at any
door as its method, path and status, and a call as its actor, the
dataset it arrived on, the digest and length of its text, and the
spans of the work it caused — each statement, each read's planning,
each measurement run, each commit — each closing with its busy and
idle time.
A read closes when its client has taken the last row or dropped the
stream, with the engine's own counts: rows served, whether it
completed, the operators, their compute time and spills; at `debug`
the physical plan annotated with those counts, as `EXPLAIN ANALYZE`
prints it.
The text of a call is a `debug` event inside its span and nothing
else, because statement bodies and groundings carry data. A refusal
carries its reason and never the token.

With `OTEL_EXPORTER_OTLP_ENDPOINT` set the same record goes to the
collector as well, under `service.name=glossql`: the spans as traces,
with their events; the events as log records, each carrying the trace
and span id it happened under, so a log line leads to its trace. A
`traceparent` header a client sends makes its request a child of the
client's trace. The export runs on the OpenTelemetry SDK's own
threads, never on the engine's runtime, and is flushed when the server
stops on SIGINT or SIGTERM. Metrics are not exported: a backend derives request rates and
latencies from the spans.

## Tokens

The token's subject says who is speaking. The door sets the standing:
`/mcp` writes as an agent, the other doors as a human. glossql
is an OAuth 2.1 resource server and never an authorization server — it
verifies against the keys the issuer publishes, it does not issue, and
there is no login flow, client registration or user table inside a
workspace. A request without a valid token gets a 401, and a
server without an issuer does not start. The one exception is
explicit: `GLOSSQL_INSECURE_OPEN=true` serves the doors open, every
caller recorded as `insecure_dev_mode` — for a laptop trying the
server out, never for a deployment.

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

A token binds to this server by its `aud` naming the audience. MCP
clients ask for that with the RFC 8707 `resource` parameter, so the
issuer must honour it — register the audience as a resource the
issuer can mint for; that is the whole setup. The server refuses a
token that names another resource, or none.

How a client obtains a token is the client's flow with the issuer —
[`connect.md`](connect.md) shows Claude Code's.

## What boot does

Opening a workspace creates `warehouse/` if absent and opens the
catalog. Before any door opens, boot declares the shipped system into
a fresh workspace: the measurement library and the KPI kit (the
semantic vocabulary and its witnesses) — ordinary declarations,
readable back through the `functions` and `aspects` relations like
anything an agent writes. The bootstrap is idempotent; every boot
calls it, and it declares only into a workspace that holds none of
it. The server writes nothing outside the workspace directory.

## Workspace anatomy

```
acme/
  catalog.sqlite     the Iceberg catalog
  warehouse/         the lake — every table and every declared
                     relation lives here as Iceberg data
  apps/              optional: workspace apps, one directory per app;
                     a workspace app named like a built-in shadows it
                     whole
  weights/           optional: a band-model weights override,
                     verified by digest at load (a sibling ../weights
                     directory is also searched); the released binary
                     carries the regressor itself
```

The lake is the whole store. There is no separate database for the
glossary: glosses, functions, witnesses, measurements — every relation
is an Iceberg table under `warehouse/`, and the workspace directory is
the complete, copyable state of the system.

With `GLOSSQL_CATALOG_URI` set, `catalog.sqlite` and `warehouse/` move
behind the REST catalog and its storage: the workspace directory then
holds only `apps/` and `weights/`, the state of the system is the
catalog's warehouse, and `--workspace` may be left unnamed (the
working directory serves). Everything else is the same lake —
datasets are namespaces, every relation an Iceberg table, whichever
side of the connection they live on.
