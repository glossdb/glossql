# The channel cut: sessions keyed (actor, dataset), one shared lake mount

Date: 2026-08-07. Ruled by the project lead after the app door exposed
`USE` as the weak point (the mount race under concurrent frames) and the
server question was put directly: a loaded dataset must be re-used, not
re-loaded per session.

## What changed

**One shared mount.** The `Lake` caches a single
`Arc<IcebergCatalogProvider>`; every session mounts by Arc clone. The
per-session `IcebergCatalogProvider::try_new` build — the load that
would have serialized requests per dataset — is gone. The provider
freezes only the namespace list (iceberg-datafusion-0.10.1
catalog.rs:53–73); table lookups inside a namespace go to the catalog
live, so a recipe landing is visible to every session with no rebuild.
The one invalidation point is a namespace create (`DECLARE DATASET`):
writers invalidate, the next touch rebuilds.

**Binding through the substrate.** A session binds to a dataset by
mounting the shared schema and setting
`datafusion.catalog.default_schema` — resolution reads the config per
statement (datafusion-53.1.0 session_state.rs:295), so bare names
resolve through DataFusion's own path. The per-table alias machinery
(`alias()`, the `aliased` bookkeeping, and the deregister/register race
the app door hit) is deleted, not relocated. Staged materialization
batches move to a session-local memory schema (`glossql_stage`) so a
bound session's bare registration can never create a lake table.

**Channels.** The plane keys sessions `(actor, dataset)`. A channel's
binding is fixed at construction; `USE` moves the actor's pointer
between channels and never rebinds a session, so channels serving
concurrent readers hold still under load. Statement sequences run
through `Plane::execute`, which routes the runs between `USE`s to the
pointer's channel. Doors that know their dataset (the app door, from
`app.toml`) ask for the channel directly — no `USE` on any concurrent
path. The grammar is untouched: SPEC.md's `USE` prose ("sets the
resolution context") describes exactly what the pointer does.

**DataFusion's `CREATE DATABASE`/`CREATE SCHEMA` stay refused.** They
create entries in a session's in-memory catalog — process-local,
undurable, invisible to other sessions. Datasets and tables are store +
lake truth (`DECLARE DATASET`, recipes); the DataFusion catalog is a
per-process projection of it. The ruling is to use *more* of the
substrate's catalog machinery (the provider and default-schema seams),
not to open its DDL.

Session-local objects (`CREATE TEMP TABLE`/`TEMP VIEW`) are a seam to
design together if ever wanted; nothing needs it today, so there is no
overlay schema and exactly one resolution path.

## Evidence

- Workspace suite green (32 suites), including new coverage:
  `catalog/tests/lake.rs::provider_is_shared_until_a_namespace_lands`
  (Arc reuse across touches and clones, rebuild on namespace create)
  and `session/tests/channels.rs` (`USE` switches channels without
  rebinding; a table landed by one actor reads bare from another
  actor's fresh channel; the app-door shape with no `USE` statement).
- Live on the fin2 workspace (release, 8115): 60 frame requests
  12-wide, 60× HTTP 200 in 1.14 s; `USE fin2; SELECT` through the
  plane loop, then a bare-name read in a separate call streaming
  Arrow IPC off the pointer-selected channel.
- The diff is net-negative in the session crate: per-session provider
  builds, `alias()`, and the `USE` mount choreography came out.
