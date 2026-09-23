# The substrate

glossql is a database built inside a framework: DataFusion is the
query engine, and the data plane is its own parquet scan over files a
catalog of rows names ([storage](storage.md)). It is not a library the
server calls — every mechanism goes through one of its extension
points, and where a shape once worked around it the cost was
concrete: a blocked planner thread, re-entrant planning that needed
its own cycle stack, one stack for the whole nesting.

## The seams in use

- **`RelationPlanner`** — DataFusion's seam for custom FROM elements.
  It sees the raw table factor before default planning, which is what
  makes `GLOSSARY(subject, all => true)` plannable at all: the default
  table-function path rejects named arguments. The store's relations
  and `GLOSSARY()`/`ATTEST()` plan through it, decoded structurally
  from the AST — which is also why the JSON `->` operator never
  collides with pair paths: inside these factors `->` never reaches
  expression planning.
- **The async pre-pass** — DataFusion's own planner resolves every
  table reference asynchronously first and only then runs the
  synchronous plan builder. The server copies that shape: every door a
  statement names — a `read.<aspect>()` grounding, a replayed
  scenario, a subject column — is resolved depth-first before planning
  begins, its SQL fetched from the store and built into a logical
  plan. No blocking calls inside planning, no re-entrancy, and the
  cycle check is the resolution path itself: a door already on the
  path is a cycle, spelled out in the error. AST walks use sqlparser's
  derive-generated visitors, never a hand-written walker, which misses
  positions (scalar subqueries, for one).
- **The shipped reads ride the same resolution.** Each read is one
  `.sql` file embedded in the binary and planned like any served
  grounding — one file serves the door, an app frame, and a skill
  example alike. `current_dataset` is the exception: it serves session
  state, which no `.sql` file can
  reach, so it is a compute door the pre-pass evaluates into a batch.
- **The catalog hierarchy as-is** — `CatalogProviderList` →
  `CatalogProvider` → `SchemaProvider` → `TableProvider`. The mounted
  catalog is a `CatalogProvider` of the workspace's own over the data
  plane's rows, each landed table a `TableProvider` over its files
  whose scan is the engine's `DataSourceExec` over a `FileScanConfig`;
  there is no parallel catalog API. Tables are written through one
  path, `Lake::write` then `Lake::commit`, one transaction per
  landing.
- **One `RuntimeEnv` for the process** — the memory pool, the disk
  manager and the file caches every plan answers to. DataFusion builds
  one per session state when it is handed none, and a channel is built
  per call, so the runtime is created at boot and handed to every
  channel instead — a pool built per call would bound only that call.
  The pool is bounded (`--memory-limit`); a sort or a final-mode hash
  aggregate past it spills to the OS temp directory, the disk manager
  capping the spilled bytes at twice the pool, and a consumer that
  cannot spill is refused with the shape that fits; the three file
  caches are off — the list-files cache defaults to an infinite TTL
  over exactly the source globs a re-import is re-reading because they
  changed.
- **Schema without execution** — a logical plan carries its schema, so
  column names and types are answered without reading a row.

## The rules the seams impose

- **`scan()` runs during planning, not execution.** No IO, no network,
  no heavy computation there — it blocks the planner; the pre-pass
  exists so nothing async is left by the time the planner runs.
- **The zero-greps.** `block_in_place`, `block_on`, `thread_local!`,
  and bare `tokio::spawn` stay at zero in the crates the server owns —
  a hit means something is being built around the framework instead of
  on it. The one named owner is the file reader, which infers a
  schema inside the engine's synchronous table-function call; its
  exact count is pinned by a conformance test. The ADBC driver, whose
  Rust API is synchronous, runs on the runtime's blocking pool and
  hands its batches over a bounded channel.

## The data plane

- **The snapshot is the version.** A statement's reads pin it — every
  scan reads the file list of the pinned version whatever lands after
  — and a version stays a durable key: a measurement's pin carries
  it, a gloss row stores it, and the staleness rule compares it with
  the snapshot the subject's column last changed at.
- **Landed tables only.** The lake holds what recipes land, as parquet
  files. The record — the store's relations — lives in the same
  database ([store](store.md)), where a row is one insert and a read
  one query, and its order is the database's identity column.
- **Facts about a write are rows too**: a landing's are its `imports`
  row, written beside the commit; claims about a subject are rows of
  the record.
- **A commit is one transaction** on the catalog's rows, and the files
  it ends outlive it by a grace, so a reader that pinned them
  finishes its scan.
