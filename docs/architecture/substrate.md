# The substrate

glossql is a database built inside two frameworks: DataFusion is the
query engine, Iceberg v3 is the table format. They are not libraries
the server calls — every mechanism goes through one of their extension
points, and where a shape once worked around them the cost was
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
  `CatalogProvider` → `SchemaProvider` → `TableProvider`. Table names,
  columns, and snapshot ids are answered by the provider chain; there
  is no parallel catalog API. Tables are created through
  iceberg-datafusion's own front door — `SchemaProvider::register_table`
  — and written through one path of the workspace's own,
  `Lake::append_batches`, which is what lets a landing's facts ride the
  snapshot they describe.
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
  on it. The one named owner is the ADBC seam, whose Rust API is
  synchronous today; its exact count is pinned by a conformance test.

## Iceberg

- **The snapshot is the version.** A statement's reads pin it — every
  scan reads the pinned snapshot whatever lands after — and a snapshot
  stays addressable after later commits, which makes it a durable key.
  The catalog-backed provider always reads current, so an unpinned
  pair of scans could straddle a landing.
- **Ordering is the format's.** Iceberg v3 row lineage —
  `_last_updated_sequence_number` and `_row_id`, the commit that last
  touched the row and the row id the commit assigned in order across
  every file it wrote — is a total order over writes, assigned by the
  catalog with no coordination between writers and nothing minted by
  the store. Inside one append, the row id orders the rows: two rows
  sharing a supersession key resolve to the later one, whichever files
  they landed in.
- **One sequence, one commit per relation.** A call's rows land at
  its end, one append per relation they touch, and what stood before
  a refusal lands the same way; replacement is a later row, never an
  update.
- **Facts about a write ride the write** (snapshot properties and
  summary); claims about a subject are rows.
- **Landings read back from snapshot summaries** — one entry per
  append snapshot, its facts taken from the summary it rode. The
  store's lineage columns read through iceberg-rust's own scan; they
  do not cross the SQL surface.
