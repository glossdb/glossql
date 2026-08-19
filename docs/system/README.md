# System

The server: one binary, one workspace, three doors. How it is built is
[`../architecture/`](../architecture/README.md); the shipped surface is
[`../reference/`](../reference/README.md); running it is
[`../start/`](../start/README.md).

## What stands

- **The statement spine** — the parser over its corpus acceptance
  suite; every fixture transcribes a real artifact.
- **The store** — every declared relation an Iceberg table in the
  workspace lake; supersession is a read; admission and grain rules
  ([store](../architecture/store.md)).
- **The session** — one per connection, holding actor, dataset, store,
  and engine context: recipe materialization, probe routing, ruling
  composition, the shipped read library, `whatif.<scenario>()` and
  `misfit.<frame>()` ([reads](../reference/reads.md)).
- **Import** — file sources with cast accounting; recipe and probe
  execution at the source over ADBC; source-row counting.
- **Scripts** — the native kernels; the shipped
  function library declared at boot; the band plane over the
  candle-ported in-context model
  ([functions](../reference/functions.md),
  [methods](../methods/README.md)).
- **The doors** — `/mcp` (one `glossql` tool, stateless, row cap),
  `/query` (streaming Arrow IPC), `/app` (directory apps, the built-in
  docket, one write — the ruling form)
  ([doors](../reference/doors.md)). The doors carry no authentication
  while governance stays a held-open question; `/query` speaks as the
  anonymous `human` actor, so door choice is rank choice.
- **Skills** — the glossin plugin (Agent Plugins shape, one skill
  per `skills/*/SKILL.md`), where a door-connected agent learns the
  language; every skill is gated by the server's test suite like
  these pages.
- **Bootstrap** — a fresh workspace receives the shipped system at
  boot; declaration relations read as plain tables.

## Known limits

- No request timeouts, no session eviction — bounded at PoC scale.
- `SELECT _pos` is unreadable in user SQL; the lineage column is the
  format's.
- An MCP session reaped between calls answers a bare `Not Found`; the
  client re-inits and replays `USE`.

## Planned

Actionable units live in the issue tracker; the standing picture:

- **Upstream-dependent.** Physical deletes — strike cleanup, aspect
  re-declare, measurement-write batching — arrive with the Iceberg
  delete write path; snapshot properties on the insert path collapse
  the landing to `INSERT INTO`; a CTE probe in the planner seam
  retires the pre-pass; groundings compile to spec views when catalog
  view operations land.
- **Deployment.** A REST catalog binds through the one-builder seam;
  cloud kernel serving is measured when a deployment target exists;
  Flight SQL is a future door — pyarrow reads the HTTP stream today.
- **Language and doors.** App authoring as statements and a
  data-update verb are held for ruling, corpus-first.
- **Model track, each with its trigger.** Fully native band body;
  the width scaling experiment; frame-limit machinery; what-if product
  design.
