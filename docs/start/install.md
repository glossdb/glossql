# Install and run

`glossql` is one binary serving one workspace — the directory that
holds your data lake and everything declared over it. The binary
carries no model. Three doors — the metric-bands walk,
`whatif.<scenario>()` and `misfit.<frame>()` — are served by the
kernel service, `glosskernels`, hosted or run beside the server, named
by `GLOSSQL_TABICL_URL`. Without it those three doors refuse by name
and everything else serves.

## Install

macOS (Apple Silicon):

```bash
brew install glossdb/glossql/glossql
```

The formula installs one command, `glossql`. On Linux, run the
container (below) or build from source.

## Build and start

```bash
cargo build --release -p glossql-serverd
cp .env.example .env            # then fill it in — see Tokens below
./target/release/glossql --workspace ~/acme
```

A source build needs nothing beside the repository.

The server reads `.env`, reads the issuer's keys, prints its doors and
listens:

```
2026-09-03T08:04:42.103275Z  INFO verifying tokens issuer=https://issuer.example audience=http://127.0.0.1:8080 application=a1b2c3…
2026-09-03T08:04:42.104551Z  INFO glossql listening — / (datasets), /mcp, /<dataset>/mcp, /<dataset>/query, /<dataset>/app addr=127.0.0.1:8080 scheme="http"
```

## The container

The same binary as an x86_64 image: `ghcr.io/glossdb/glossql:<version>`,
pushed at each release tag. `docker build .` at a checkout builds the
same. A distroless base — glibc, the C runtime
libraries, the root certificates, no shell and no package manager —
and the binary; no model, no weights, no GPU. It listens on 8080 and
runs as the base's unprivileged `nonroot` user. There is no workspace in a container:
the state is the catalog and the warehouse `GLOSSQL_CATALOG_SQL` and
`GLOSSQL_WAREHOUSE` name, and without both the server refuses to
start, naming them; the apps are the built-ins and the app parts in
the record. A file source is a URL too — a container or bucket path
the server's identity may read — since there is no directory for
files. The configuration is the environment the platform injects,
secrets included — nothing is read from a file, and the image holds no
value of its own. `GET /healthz` answers `ok` outside the gate, for a
platform's probe, and stays off the record. One server owns a
workspace: it holds the store's head in memory, so a deployment runs
one replica per catalog and warehouse
([the store](../architecture/store.md)).

```bash
docker run --rm -p 8080:8080 \
  -e GLOSSQL_ISSUER=https://issuer.example \
  -e GLOSSQL_AUDIENCE=https://glossql.example \
  -e GLOSSQL_CLIENT_ID=… -e GLOSSQL_CLIENT_SECRET=… \
  -e GLOSSQL_CATALOG_SQL=postgres://glossql:…@db.example:5432/glossql \
  -e GLOSSQL_WAREHOUSE=abfss://lake@account.dfs.core.windows.net/warehouse \
  -e AZURE_STORAGE_ACCOUNT_NAME=account -e AZURE_STORAGE_ACCOUNT_KEY=… \
  -e GLOSSQL_TABICL_URL=https://… -e GLOSSQL_TABICL_TOKEN=… \
  ghcr.io/glossdb/glossql:0.1.5
```

The image sizes the server for a box with 8 GiB of memory and an
8 GiB ephemeral disk, two numbers from two facts: the engine's ceiling
and the cube cache at their defaults, 6 GiB tracked and the rest of
the memory to the process and what the engine does not track;
`GLOSSQL_SPILL_LIMIT=6144` for the disk, the rest to the writable
layer. It listens on `0.0.0.0:8080`. A different box injects its own
numbers as variables, beside the rest of its environment.

## Flags

Every flag but `--workspace` has a variable named after it, and the
flag wins where both are set.

| flag | variable | default | meaning |
|---|---|---|---|
| `--workspace <dir>` | — | required when the catalog or the warehouse lives in it | the laptop's shape: the directory holding the catalog and the warehouse. A deployment names both in the environment (or a REST catalog) and runs without a directory |
| `--addr <ip:port>` | `GLOSSQL_ADDR` | `127.0.0.1:8080` | where the doors listen |
| `--row-cap <n>` | `GLOSSQL_ROW_CAP` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--cube-cache <megabytes>` | `GLOSSQL_CUBE_CACHE` | `2048` | the byte budget for the cube cache — every metric's cells held in memory, evicted least-recently-used past it; the `cube` aspect bounds one cube, this bounds them all |
| `--memory-limit <megabytes>` | `GLOSSQL_MEMORY_LIMIT` | `4096` | the engine's memory ceiling for the whole process. A sort or a hash aggregate that outgrows it spills to the OS temp directory, up to `--spill-limit`. Past that bound, or for a shape that cannot spill (a `count(DISTINCT …)` held whole per partition), the plan is refused by name with the shape that fits. Separate from `--cube-cache`, whose bytes sit outside the engine — size a deployment's memory for the sum |
| `--spill-limit <megabytes>` | `GLOSSQL_SPILL_LIMIT` | twice `--memory-limit` | how much of the disk the engine may spill onto, at its temp directory. The disk's own number, set from the box: a container's ephemeral disk, or a disk mounted at the temp directory. Unset, it follows the memory ceiling |

Authorization is not a flag. A run with a workspace reads `.env` in
the working directory, and the environment on top of it (a set
variable wins over the file); a deployment reads no file — its
platform injects the variables, secrets included:

| variable | meaning |
|---|---|
| `GLOSSQL_ISSUER` | the authorization server's issuer URL; its OpenID configuration names the keys tokens are verified against |
| `GLOSSQL_AUDIENCE` | this server's canonical URI: the API identifier registered at the issuer and the `aud` a token must name (RFC 8707 §2), and the host the agent door answers beside loopback — read under the open switch too, so a deployment always names its URL; defaults to `http://<addr>` |
| `GLOSSQL_CLIENT_ID` | the application registered at the issuer for this server, which the browser login on `/app` signs in and exchanges its code as |
| `GLOSSQL_CLIENT_SECRET` | that application's secret, used by the browser login on `/app` |
| `GLOSSQL_INSECURE_OPEN` | `true` (the literal) serves every door without authentication — no issuer needed, no login served, every caller recorded as `insecure_dev_mode` with the door's standing. The name is the warning: a laptop trying the server out, never a deployment |
| `GLOSSQL_CATALOG_SQL` | the workspace's catalog on a Postgres server (`postgres://user:password@host:5432/db`) instead of the workspace directory's own SQLite file — the same catalog in a database that outlives a container. The warehouse stays under `--workspace` unless `GLOSSQL_WAREHOUSE` moves it. Unset, `catalog.sqlite` in the workspace serves |
| `GLOSSQL_WAREHOUSE` | the lake in an object store instead of under `--workspace`: `s3://bucket/prefix`, `gs://bucket/prefix` or `abfss://container@account.dfs.core.windows.net/prefix`. The store's own conventions carry the credentials (`AWS_*`; `AZURE_STORAGE_ACCOUNT_NAME` and `_KEY`, or the managed identity the client reads on Container Apps with nothing set; `GOOGLE_SERVICE_ACCOUNT_PATH`, or the attached service account the client reads on Cloud Run with nothing set). With both this and `GLOSSQL_CATALOG_SQL` named the server needs no workspace directory |
| `GLOSSQL_CATALOG_URI` | an Iceberg REST catalog's endpoint. Set, the workspace's catalog is that service rather than the workspace directory's own SQLite file; storage is attached on the catalog's side, and each table load answers with what its FileIO needs (the connection always offers `X-Iceberg-Access-Delegation: vended-credentials`). Unset, the local catalog is used |
| `GLOSSQL_CATALOG_WAREHOUSE` | which warehouse of that catalog this workspace is — required with the URI |
| `GLOSSQL_CATALOG_TOKEN` | a bearer token used as-is: an object-store platform's API token, minted with both its catalog and its storage permissions. Exactly one of token or credential authenticates the connection |
| `GLOSSQL_CATALOG_CREDENTIAL` | `client_id:client_secret`, exchanged for a bearer token at `GLOSSQL_CATALOG_TOKEN_ENDPOINT` (required with it) and exchanged again when the token nears its stated expiry; `GLOSSQL_CATALOG_SCOPE` as the backend's documentation names it |
| `GLOSSQL_TABICL_URL` | the kernel service behind the metric-bands walk, `whatif.<scenario>()` and `misfit.<frame>()` — the hosted kernel API, or a `glosskernels` service run beside the server. Unset, those three doors refuse by name and everything else serves |
| `GLOSSQL_TABICL_TOKEN` | the bearer that service expects: a key the hosted API issued, or whatever a service of your own was started with |
| `AWS_ACCESS_KEY_ID` …, `AZURE_STORAGE_ACCOUNT_NAME` … | storage itself needs no glossql variables behind a REST catalog — table loads answer with what FileIO needs. A store that vends nothing, a dev rig or the SQL catalog's warehouse, is configured through the store's standard conventions, read by the storage layer itself: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_DEFAULT_REGION`, `AWS_ALLOW_HTTP`; `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_USE_EMULATOR` (with `AZURITE_BLOB_STORAGE_URL` when the emulator is not on the loopback) |
| `GLOSSQL_LOG` | what the server puts on its record — a `tracing` filter. A bare level (`debug`) is this server's crates at that level, the substrate held at `info` and the MCP library at `warn`; directives (`glossql_session=debug,apache_avro=debug`) are taken as written. `RUST_LOG` is honoured when it is unset; `info` when neither is |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | where an OpenTelemetry collector listens (`http://127.0.0.1:4318`; `/v1/traces` and `/v1/logs` are appended). Set, the record is also exported there — spans as traces, events as logs, OTLP, batched. Unset, nothing is exported. The exporter's other variables are the SDK's own: `OTEL_EXPORTER_OTLP_PROTOCOL` picks the transport, `http/protobuf` (the default) or `grpc`; `OTEL_EXPORTER_OTLP_HEADERS` carries a hosted collector's credentials; `OTEL_RESOURCE_ATTRIBUTES` names the deployment beyond `service.name=glossql`. A platform's managed collector injects the endpoint and the protocol itself — Container Apps' agent does, and speaks gRPC |

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
the `mcp` doors write as an agent, the other doors as a human. glossql
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
  catalog.sqlite     the Iceberg catalog (absent with GLOSSQL_CATALOG_SQL:
                     the catalog is then the Postgres server it names)
  warehouse/         the lake — every table and every declared
                     relation lives here as Iceberg data
```

The lake is the whole store. There is no separate database for the
glossary: glosses, functions, witnesses, measurements — every relation
is an Iceberg table under `warehouse/`, and the workspace directory is
the complete, copyable state of the system.

With `GLOSSQL_CATALOG_URI` set, `catalog.sqlite` and `warehouse/` move
behind the REST catalog and its storage, and with `GLOSSQL_CATALOG_SQL`
and `GLOSSQL_WAREHOUSE` to the Postgres server and the object store
they name: the state of the system is then the catalog's warehouse
and a deployment runs without a directory. Everything else is the same
lake —
datasets are namespaces, every relation an Iceberg table, whichever
side of the connection they live on.
