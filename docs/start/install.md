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
serverd on 127.0.0.1:8080 — /mcp (agent door), /query (arrow door), /app (app door)
```

## Flags

| flag | default | meaning |
|---|---|---|
| `--workspace <dir>` | required | the workspace directory; created content lands here |
| `--addr <ip:port>` | `127.0.0.1:8080` | where the doors listen |
| `--agent <id>` | `agent` | fallback agent actor id for MCP calls whose initialize named no client |
| `--row-cap <n>` | `200` | rows an MCP tool result ships before declaring `truncated` (data reads only; metadata reads arrive whole) |
| `--round-wait <secs>` | `120` | how long a question form waits for a person before the silence counts as a decline |

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
