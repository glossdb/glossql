---
name: glossql-substrate
description: How to build ON DataFusion and iceberg-rust rather than around them — the extension points, the rules that come with them, and where each was verified. Use before writing or reviewing any server code that plans, executes, reads the catalog, or runs a function.
---

# The substrate

glossql is a database. **DataFusion is the query engine and Iceberg v3 is
the catalog standard** — they are not libraries we call, they are the
frameworks we build inside. Every mechanism we wrote that duplicates one
of theirs has cost us: thread contention, blocked planner threads,
copied instead of passed memory, stale caches.

The rule this skill exists to enforce: **either use the extension point,
or write down why it is wrong for us.** "We didn't know" is the failure
mode; this file is so nobody has to rediscover it.

Extending a high-performance Rust framework is hard. There are few right
ways and many wrong ones, so the method is small steps and tests, not
large designs.

## Where the answers live

Read these before designing, not after. They are prose, not source, and
they say what the source only implies.

| topic | reference |
|---|---|
| table providers, the three layers | `docs/source/library-user-guide/custom-table-providers.md` |
| same, as a blog | datafusion.apache.org/blog/2026/03/31/writing-table-providers/ |
| catalogs, schemas, tables | `docs/source/library-user-guide/catalogs.md` |
| SQL-level extension | `docs/source/library-user-guide/extending-sql.md` |
| logical plan building | `docs/source/library-user-guide/building-logical-plans.md` |
| optimizer rules | `docs/source/library-user-guide/query-optimizer.md` |
| expressions | `docs/source/library-user-guide/working-with-exprs.md` |
| cancellation and the tokio contract | datafusion.apache.org/blog/2025/06/30/cancellation/ |
| repartition heuristics | blog 2025-12-15-avoid-consecutive-repartitions |
| pushdown | blogs 2026-07-20-sort-pushdown, 2026-03-10-limit-pruning, 2025-09-10-dynamic-filters |

All under `github.com/apache/datafusion/tree/main/docs` and
`github.com/apache/datafusion-site/tree/main/content/blog`. Both are
readable with `gh api repos/<repo>/contents/<path> --jq '.content' |
base64 -d`.

## The three layers, and where work belongs

From `custom-table-providers.md`:

1. **`TableProvider`** — describes the table and produces a plan. Logical.
2. **`ExecutionPlan`** — describes *how*: partitioning, ordering, children. Physical.
3. **`SendableRecordBatchStream`** — does the work, one `RecordBatch` at a time.

> **`scan()` runs during planning, not execution.** It should return
> quickly. Best practice is to avoid performing I/O, network calls, or
> heavy computation here. A common pitfall is to fetch data or open
> connections in `scan()`. This blocks the planning thread and can cause
> timeouts or deadlocks.

That paragraph names two bugs we shipped: `MemTable::try_new` built from
a store read at plan time, and `block_in_place` + `block_on` inside a
sync planner callback.

**Pick the lightest starting point.** The guide's own table:

| if the data is… | start with |
|---|---|
| already `RecordBatch`es in memory | `MemTable` |
| an async stream of batches | `StreamTable` |
| **a logical transformation of other tables** | **`ViewTable` wrapping a logical plan** |
| a custom source needing full control | all three layers |

`ViewTable` is the one we kept missing: a `read.<aspect>()` grounding is
a logical transformation of other tables, which is exactly a view.

## Resolve async before planning, never inside it

DataFusion's own `statement_to_plan` walks the statement for table
references, awaits each through the catalog into a map, and only then
runs the sync `SqlToRel`
(`datafusion-session/src/session_state.rs`, `statement_to_plan`).

Copy that shape. An async pre-pass that resolves everything the sync
planner will need means:

- no `block_in_place`, no `block_on`, no blocked planner thread;
- no re-entrancy, so no expansion stack and no `thread_local`;
- the cycle check is the traversal path, which you have for free.

Verified: spike 5, 2026-08-17 — plain, nested, diamond, cycle,
self-reference and missing-grounding cases all behave, with the closure
resolved before one planning pass.

**Walk the AST with sqlparser's derive-generated visitors**, never a
hand-written walker: `visit_relations`, `visit_relations_mut`, and the
`VisitorMut` trait's `pre_visit_table_factor` / `pre_visit_query` /
`pre_visit_expr` (`sqlparser/src/ast/visitor.rs`). A hand-written walker
misses positions — scalar subqueries in the SELECT list, in our case.
Note `read.x()` parses as a table **function**, so a rename must clear
`args` too, which only the `TableFactor` carries.

## Concurrency is the engine's, not ours

- CPU work runs **on** the tokio runtime, inline in `poll_next`, bounded
  to one batch. DataFusion uses `spawn_blocking` only for blocking IO,
  never for compute (`datafusion/src/lib.rs`, and `block_in_place`
  appears nowhere in any DataFusion crate).
- Parallelism comes from **partitions**. `execute()` is called once per
  partition; each partition is a task the runtime multiplexes. Expose
  your data's natural partitioning; implement `repartitioned` if the
  engine should be able to ask for a different count.
- `target_partitions` defaults to available parallelism
  (`datafusion-common/src/config.rs`).
- **Never hand-roll a concurrency governor.** N independent plans run
  at once each open their own files and buffers, and the process fd
  budget goes first; one plan the engine schedules over its partitions
  shares them. Waves, semaphores and batch sizes are the symptom of
  running N plans where one would do.
- Cancellation is stream drop. `EnsureCooperative` is in the default
  physical rule set and wraps leaves that declare nothing, so a plan
  built from operators cancels correctly without effort.

Verified: spike 1, 2026-08-17 — 21 plans built up front and driven
concurrently beat 35 re-planned strings in waves of 4 by 2.4×, and
dropping the stream stopped the work immediately.

**Prefer composed sub-plans over one `UNION ALL`** when the arms are
independent. Measured 24 ms against 39 ms: concurrent plans run
concurrently, while a union has to channel its arms through a shuffle to
build one frame. DataFusion only avoids that shuffle when
`can_interleave` holds across arms
(`datafusion-physical-optimizer/src/enforce_distribution.rs`), which
needs matching hash partitioning — different tables per arm will not
have it. When a plan-shape question is not this clear, settle it with
`EXPLAIN` / `EXPLAIN ANALYZE`, not by reasoning.

## Use the SQL DataFusion already has

Before writing a loop that issues one query per candidate, check whether
one statement expresses it:

- `GROUPING SETS` / `ROLLUP` / `CUBE` — a total plus every slice in one
  aggregate pass (`datafusion-sql/src/select.rs`).
- window functions, CTEs, `FILTER (WHERE …)`, `count(DISTINCT …)`.
- Aggregates we do not need to write: `avg`, `stddev`, `variance`,
  `median`, `percentile_cont`, `approx_percentile_cont`,
  `approx_distinct`, `correlation`, `covariance`, `min_max`, `count`.

Two `GROUPING SETS` rules found the hard way (spike 6, 2026-08-17):

- There is **no `GROUPING()` function** in DataFusion, so a NULL in a
  grouped column is ambiguous between "aggregated away" and "a real NULL
  member". Filter NULL members in the base, or you cannot tell a total
  from a slice.
- **The projected expression must be the grouping expression.** Matching
  is syntactic, so `CAST("d" AS VARCHAR)` over a grouping key of `"d"`
  returns NULL in every group — it plans, it runs, and it is wrong. Put
  the cast in the base subquery.

## Schema without execution

A logical plan carries its schema. `ctx.state().statement_to_plan(sql)`
then `plan.schema()` answers column names and types **without reading a
row** — no `LIMIT 1` probe needed. Verified: spike 6 resolved 11
groundings in 13 ms with zero rows executed.

## The catalog hierarchy is the shape

`CatalogProviderList` → `CatalogProvider` → `SchemaProvider` →
`TableProvider` (`catalogs.md`). DDL goes through the catalog API; DML
(`INSERT INTO`) goes through `TableProvider`.

Do not wrap a `CatalogProvider` in a parallel API of your own. If you
need table names, columns, or a snapshot id, the provider chain already
answers — and iceberg-datafusion's `IcebergCatalogProvider` *is* a
`CatalogProvider`.

## Iceberg

- **The snapshot is the version.** Pin it per statement with
  `IcebergStaticTableProvider::try_new_from_table_snapshot`; the
  catalog-backed provider always reads current
  (`iceberg-datafusion/src/table/mod.rs`), so two scans in one query can
  straddle a landing. A pin stays addressable after later commits, so it
  is a durable key. Verified: spike 3.
- **Ordering is the format's, not ours.** Iceberg **v3** row lineage
  gives `_last_updated_sequence_number` (the commit that last touched
  the row) and `_pos` (position in file); together they are a total
  order over writes. Nothing to mint, no coordination between writers.
  Verified: spike 7. `_row_id` read synthesis merged 2026-08-29
  (apache/iceberg-rust#3058, refs #2879) — after our `ffaf049` pin, so
  it arrives with the next pin move; metadata-only projection pruning
  is still open (#3117). `_last_updated_sequence_number` landed in
  PR #2966, merged 2026-08-10, after the 0.10.1 release.
- `format-version` is a **reserved** property — rejected at create. Get
  to v3 with `Transaction::upgrade_table_version().set_format_version(V3)`.
- Metadata columns are readable through **iceberg-rust's own scan**, not
  through iceberg-datafusion's SQL surface.
- **A commit is a transaction.** One row per commit costs ~16.5 ms;
  40 rows in one commit costs ~0.47 ms/row. **Ordering inside one commit
  is only settled by `_pos`, which is per-file** — so a batch is safe
  when no two of its rows share a supersession key (bootstrap, pre-warm),
  and unsafe otherwise until the pin carries `_row_id` (read synthesis
  merged upstream, #3058). Ruled 2026-08-17: one
  statement, one commit; batch only the two paths that cannot tie.
- Facts about a write ride the write (snapshot properties/summary);
  claims about a subject are rows.
- Read landings through `Table::inspect()`, not SQL over
  `table$snapshots` — that path has a projection-pushdown bug
  (`count(*)` fails, `SELECT *` works).

## Before you write a mechanism

Ask, in order:

1. Does DataFusion or Iceberg already do this? Check the guide above.
2. If it does and we are not using it, is there a written reason?
3. If we still build it, does it go through an extension point
   (`TableProvider`, `ExecutionPlan`, `ScalarUDF`, `AggregateUDF`,
   `TableFunctionImpl`, `SchemaProvider`, `OptimizerRule`,
   `ExtensionPlanner`) or beside one?
4. Is there a test that keeps it that way?

The greps that must stay at zero in the crates we own — `block_in_place`,
`block_on`, `thread_local!`, bare `tokio::spawn` — are not style rules.
They are how we notice we have started building around the framework
again.
