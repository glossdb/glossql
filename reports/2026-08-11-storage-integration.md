# Workspace storage: the lake is the store (2026-08-11)

This report records the storage rulings taken for the integration
phase and the evidence they rest on. The outcome in one line: the
workspace's context store moves into the Iceberg catalog and
warehouse; no database file or database server exists anywhere in
the design.

## Rulings (project lead, 2026-08-11)

- **No SQLite, anywhere.** The workspace's only persistence is an
  Iceberg catalog plus its warehouse. `glossary.sqlite` and
  `catalog.sqlite` leave the design. The server holds an
  `iceberg::Catalog` handle; deployment binds a REST catalog. Which
  concrete service (R2, Lakekeeper, …) stays parked.
- **Context tables live in a sibling namespace bound by naming
  convention**: dataset `fin` pairs with `fin_meta`, which holds the
  relations the language already exposes — `glossary`, `aspects`,
  `functions`, `witnesses`, `relationships`, `recipes`, `imports`.
  Schemas are the store's current relation shapes. The language
  surface does not move: `USE fin` + `SELECT * FROM glossary`
  resolves to `fin_meta.glossary`; data statements never see the
  meta namespace.
- **Writes are appends** through the same `INSERT INTO` path recipes
  use. Supersession stays a read rule (latest per subject, aspect,
  actor kind); a strike appends a row that vacates the slot. The
  server is the sole writer of `*_meta`.
- **The cache is not storage.** A cache is defined by two features:
  it survives a restart or it does not, and it is always
  size-capped. Ruled: in-memory bounded map, cold after restart,
  recompute at read — which detector-at-read already models.
  `SELECT * FROM cache` and `DELETE FROM cache` sit on it unchanged.
  It never touches the lake.
- **Rely on the catalog tier, never copy it.** The deployment
  contains exactly one database — the catalog service's own, behind
  the REST protocol. glossql rides that tier for what it is good at
  (atomic commits, the namespace registry, auth) and runs no state
  beside it. serverd is stateless compute plus the bounded cache.
- **No external-reader requirement.** The door is the only reader;
  the design carries no weight for outside engines.
- Maintenance defaults set once, at meta-table creation:
  `write.metadata.delete-after-commit.enabled = true` (bounded
  metadata directory) and snapshot expiry as the retention knob for
  how much supersession history to keep.

## The measured store (2026-08-11, `~/glossql-ws`)

464 rows across every relation: glossary 204 (51 KB of bodies),
cache 179 (98 KB), aspects 26, witnesses 18, functions 13, imports
9, relationships 8, recipes 7 — a 320 KB SQLite file in total.
Kilobytes per dataset. Writes arrive in authoring bursts (a door
turn is a `;`-separated batch), then the workspace goes quiet.
Storage design at this scale is an API and integration question,
not an infrastructure one.

## Precedent (researched 2026-08-11)

- **AWS S3 Metadata** — the adopted shape. S3 writes object metadata
  as managed Iceberg tables in a dedicated system bucket, namespace
  derived from the data bucket by naming convention
  (`b_<bucket-name>`), AWS sole writer, read-only to everyone else.
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-overview.html>
- **Databricks system tables** — the reserved-name variant: a
  catalog named `system` in every metastore, platform sole writer
  (Delta). <https://docs.databricks.com/aws/en/admin/system-tables/>
- **Trino Iceberg materialized views** — storage tables `st_<uuid>`
  beside user tables; originally unhidden, users dropped them by
  accident, and Trino added connector-level hiding
  (<https://github.com/trinodb/trino/issues/12559>). The documented
  failure mode: separation by table prefix in the same schema.
  Separation by namespace avoids it.
- **The catalog tier** (Polaris, Lakekeeper, Nessie, Gravitino,
  Amoro, Unity OSS): every one keeps its own state in an RDBMS or
  KV store. That is the tier this design relies on through REST —
  their database, behind their protocol.
- **No context or metadata engine stores its own state in a
  lakehouse format** — fourteen surveyed, zero hits; the position is
  unoccupied. SQLMesh's explicit warning against lake-resident state
  targets high-frequency OLTP churn
  (<https://sqlmesh.readthedocs.io/en/stable/concepts/state/>),
  which the measured write shape above is not.

## Pin facts (iceberg-rust 0.10.1, verified in crate source)

- A commit is: manifest + manifest list + one new `metadata.json`,
  then a single compare-and-swap on the catalog, retried on conflict
  (`iceberg-catalog-sql/src/catalog.rs:974`,
  `iceberg/src/transaction/mod.rs:175`).
- `fast_append` is the only data-writing action. No row deletes and
  no compaction at the pin — a strike vacates logically today,
  physical cleanup follows upstream row-delete support; both are
  spec-level features awaiting implementation.
- `fast_append` carries custom snapshot properties
  (`iceberg/src/transaction/append.rs:70`) — the Rust equivalent of
  Spark's `CommitMetadata`; candidate home for import provenance on
  landing commits. Our `INSERT INTO` path does not expose it at the
  pin (`iceberg-datafusion/src/physical_plan/commit.rs:249`).
- The REST client is complete for the lifecycle: table create /
  load / drop / purge / rename / register and `update_table` with
  server-side commit application
  (`iceberg-catalog-rest/src/catalog.rs:1005`); OAuth2
  client-credentials with token refresh, static bearer, custom
  headers (`src/client.rs:34`); the `prefix` from
  `/v1/config?warehouse=…` threads into every route
  (`src/catalog.rs:169`) — one endpoint serves several catalogs.
  Gaps at the pin: `update_namespace` unsupported
  (`src/catalog.rs:652`); no multi-table transactions — per-table
  commits, which matches per-statement acks.
- The seam is one builder: `Lake::open` swaps `SqlCatalogBuilder`
  for `RestCatalogBuilder`; `Arc<dyn Catalog>`, the shared provider,
  and the session are untouched (`crates/catalog/src/lib.rs:67`).
  If a hard catalog split is ever wanted, DataFusion mounts multiple
  catalogs (`datafusion-53.1.0/src/execution/context/mod.rs:1785`);
  the sibling namespace suffices now.

## Rejected on the way (recorded so the reasoning survives)

- **Postgres or any second database**: one writer per workspace;
  nothing for a database server to do.
- **Table or namespace properties as the store**: a configuration
  surface — string KV, untyped, not a query shape.
- **An event-log table** (statements or tombstones with read-time
  folding): redundant; Iceberg snapshots already are the history.
- **Whole-table rewrites per turn**: no primitive writes a whole
  table at once, and the choreography was complexity uncalled for
  by kilobytes.

## Also settled this session, integration scope

- Data files ride `object_store` URLs; the pinned `object_store`
  0.13.2 ships aws, azure, and gcp providers natively — Azure needs
  no S3-compatibility layer.
- Function bodies may be carried inline in declarations
  (`$$`-carried), so a remote agent can complete the flow through
  the door alone. Corpus-first when picked up.
- App authoring as statements: `corpus/18-app-authoring.md` (the
  corpus's first `glossql-gap` fixture) records the per-artifact
  form; the publish verb's semantics are held for ruling.
- Skills distribute as static files, via git and the CLI. An API
  surface waits for a consumer.
- Parked: the deployment container, the concrete catalog service,
  otel.
