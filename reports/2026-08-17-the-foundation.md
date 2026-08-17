# The foundation: what we build, what the engine builds (2026-08-17)

**This replaces `2026-08-16-store-to-the-lake.md`,
`2026-08-17-functions-split.md` and `2026-08-17-one-store-one-pin.md`.**
Their rulings are carried in §5 with a disposition each — kept, revised
or dropped — so nothing is lost by consolidating. Git keeps the three.

Seven spikes and a reading pass sit behind it. The design guidance is
`.claude/skills/glossql-substrate/SKILL.md`, which carries the
references and the rules; this report carries the decisions.

## 1. The rule

**glossql is a database. DataFusion is its query engine and Iceberg v3
is its catalog standard.** They are not libraries we call; they are the
frameworks we build inside.

Every mechanism of ours that duplicates one of theirs has already cost
us something concrete: a blocked planner thread, a hand-rolled
concurrency governor that exhausted the file-descriptor budget, a cache
with no key, a stack overflow. None of those were bad luck. They are
what building beside a framework produces.

So: **either use the extension point, or write down why it is wrong for
us.** The current code does neither, and that — not any single defect —
is what makes it immature.

## 2. The ledger

Each row: what we built, what the foundation already offers, where that
was verified, and the verdict.

| # | ours | the foundation's | verified | verdict |
|---|---|---|---|---|
| 1 | `block_in_place` + `block_on` in the planner, 10 sites | resolve async **before** sync planning, as `statement_to_plan` does | spike 5 | **delete** |
| 2 | `thread_local! EXPANDING` cycle guard | no re-entrancy → nothing to guard; the traversal path is the check | spike 5 | **delete** |
| 3 | `sql_all` waves-of-4 + `tokio::spawn` | partitions and `target_partitions`; the engine schedules | spike 1, 2.4× | **delete** |
| 4 | `db.query` re-entering the engine from rhai | `ScalarUDF`, `AggregateUDF`, `TableFunctionImpl`, `TableProvider` | spikes 0, 1, 2, 6 | **delete** |
| 5 | `MemTable::try_new` at plan time, 3 sites | `scan()` returns a plan; work happens in the stream | guide, "Keep `scan()` lightweight" | **delete** |
| 6 | `Lake`'s parallel API (`table_names`, `table_columns`, `snapshot_id`, `data_version`) | `CatalogProvider` → `SchemaProvider` → `TableProvider` answers all of it | `catalogs.md` | **absorb** |
| 7 | `data_version: AtomicU64` | snapshot ids | spike 3 | **delete** |
| 8 | `cache` + ACCEPTS invalidation edges | snapshot-keyed materialization; Iceberg MVs when upstream has them | spikes 3, 7 | **replace** (§4) |
| 9 | `id INTEGER PRIMARY KEY AUTOINCREMENT` | v3 row lineage: `_last_updated_sequence_number` + `_pos` | spike 7 | **delete** |
| 10 | `imports` / `recipes` / `datasets` relations | `Table::inspect()`, table properties, namespaces | spike 4 | **delete** |
| 11 | SQL string-templating in loops | `LogicalPlanBuilder`, `TreeNode` rewrites — whatif already does this | spikes 1, 6 | **delete** |
| 12 | per-candidate dimension probing | `GROUPING SETS` | spike 6, 7.4× | **delete** |
| 13 | `SELECT * FROM (sql) LIMIT 1` schema probes | `plan.schema()` — no rows read | spike 6 | **delete** |
| 14 | hand-written AST walkers | sqlparser's derive-generated visitors | spike 5 | **never write one** |
| 15 | expansion by re-planning through the same `SessionContext` | `ViewTable` wrapping a logical plan | guide | **adopt** (§4) |

Row 15 is the one the reading pass added and the spikes had missed: a
`read.<aspect>()` grounding is "a logical transformation of other
tables", which is the guide's own definition of a view. Spike 5 solved
it by inlining CTEs; `ViewTable` is what the framework offers, and it is
smaller.

**Two things checked and found already correct**, recorded so they are
not re-opened:

- **`GlossqlParser` wrapping `DFParser` is right.** `extending-sql.md`'s
  extension points are `ExprPlanner` (custom expressions and operators)
  and `TypePlanner` (custom types). Our extensions are *statements*.
  DataFusion has no statement-level hook; `DFParser` is meant to be
  wrapped.
- **`whatif` and `misfit` are already the target shape** — async Rust,
  `statement_to_plan`, a `TreeNode` plan rewrite, no `db.query`, no
  `block_on`. They are the reference implementations, and nobody had
  written that down.

## 3. What stays ours

The language and its semantics — supersession, collapse, admission,
grain, condition, witnesses, actor kinds. No framework has an opinion
about any of it.

Then: the judges' bodies (rhai, invoked through `ScalarUDF`), the app
door, the MCP door, and the shipped searches and statistics as Rust
behind DataFusion's UDF and `TableProvider` points.

Everything else in §2 is a candidate for deletion.

## 4. The architecture that falls out

**One store.** Every input — data and declarations alike — is an Iceberg
table in the workspace catalog. `glossql` holds `sources`, `aspects`,
`functions`, `witnesses` and source-grain glossary rows;
`<dataset>_meta` holds that dataset's `glossary` and `relationships`.
Tables are v3, reached by `upgrade_table_version`.

**One pin.** A statement resolves each table's snapshot once, in the
pre-pass, and plans against `IcebergStaticTableProvider`. This is a
correctness requirement — the catalog-backed provider always reads
current, so two scans can straddle a landing — and the materialization
key is a free by-product of it.

**One plan.** Nothing computes outside a DataFusion plan. The pre-pass
resolves declarations and door closures asynchronously; the sync planner
then runs once over an AST with nothing left to fetch. Doors are
`TableProvider`s, groundings are `ViewTable`s, judges are `ScalarUDF`s,
statistics are `AggregateUDF`s, searches are `TableFunctionImpl`s.

**The read set may depend on declarations, never on data values.** This
is what makes a computation plannable, keyable, parallelisable and
cancellable at once. A data-derived *parameter* over an already-committed
read set is fine; a data-derived *choice of what to read* is not.

**Stream what is unbounded; collect what carries a declared cap.**
`misfit` collects because `ROW_CAP` is declared and refused past;
`GLOSSARY()` over an owed set streams.

**Measurements, not cache.** A `measurements` relation keyed by the pin
digest — the sorted (table → snapshot) list of everything the
computation read, declarations included. Under a complete key there is
no invalidation, only a miss. Old rows are the drift record, not
garbage. Reads never write; the landing warms.

**Duplicate computation is harmless and uncoordinated.** Same key, same
value, because the value is a pure function of the pin. Two agents
asking at once both compute, both append, last write wins. No lock, no
coalescing.

## 5. Disposition of the three reports

**Kept.** Stored-is-what-actors-said / derived-is-computed. Facts about a
write ride the write. The store holds what actors can overwrite. Every
relation into Iceberg, SQLite only as the catalog's own backend.
`<dataset>_meta` naming and the access-rights argument for it. Migration
is wipe and re-bootstrap. `open_functions` goes. A compute door is a
relation, not a scalar verb. The four function shapes (judge, read,
statistic, search) and the extension points they map to. Measurements do
not take part in the collapse hierarchy. `ACCEPTS` as an invalidation
edge goes. The SPEC diff of 2026-08-16 §7 stands.

**Revised.**

- *"The cache is removed; derived values are not stored."* → the defect
  was the key, not the storage. Storage returns keyed by the pin (§4).
- *"Every commit is a snapshot, so the order is already established — no
  column of ours to mint."* → true only under v3 row lineage, and only
  via iceberg-rust's own scan. Right conclusion, wrong reason, and it
  needed main to be true at all.
- *"`imports` becomes a read over `table$snapshots`."* → that SQL path
  has a projection-pushdown bug (`count(*)` fails). Build it from
  `Table::inspect()` inside our own provider.
- *"The door's plan node holds a call list and spawns each call as a
  blocking task."* → if calls are plans there is no custom operator; the
  door composes sub-plans and the engine schedules them.
- *"`behavior_evidence` is the largest port and the biggest risk."* →
  its enumeration is already declaration-bounded and its arithmetic was
  vectorised in 2026-08-06. It is round trips, not algorithm.
- *"One plan" meaning one SQL statement.* → it means one scheduled plan.
  Rust over Arrow qualifies; a single `UNION ALL` was measurably slower
  than N plans driven concurrently.

**Dropped.** Single-flight coalescing on the pin key — never wanted,
invented in error. A `MEASURE` grammar word — it reintroduces
`open_functions`. Retention policy — out of scope. Detectors as a
taxonomy risk — verified fine: `slot_entropy`, `band_breach` and
`rate_tolerance` are the three functions without `RETURNS` and none
touches the door.

## 6. The roadmap is the ledger sorted

Every stage ends green, including the conformance greps, which ratchet
down and never up.

| stage | ledger rows | gate |
|---|---|---|
| **0** capture goldens over the small three (§7) for every function and door; land the conformance test at today's counts | — | suite green, no behaviour change |
| **1** split `store.rs`: rules become pure functions over rows, IO behind a narrow trait | — | goldens green |
| **2** the async pre-pass; `ViewTable` for groundings | 1, 2, 14, 15 | goldens green; `block_in_place`/`thread_local` → 0; the 16 MB stack workaround goes with it |
| **3** store relations onto Iceberg v3, `relationships` first, `glossary` last; absorb `Lake`; drop sqlx | 6, 7, 9, 10 | goldens green |
| **4** compute doors under `execute`; cache removed; `measurements` keyed by the pin | 5, 8 | SPEC + corpus diff lands here |
| **5** function ports, one per commit against its golden; each rhai file deleted as its port passes | 3, 4, 11, 12, 13 | golden per function |
| **6** pre-warm at the landing | — | — |
| **7** scale: land the large corpora (§7) and re-measure | — | the ratios hold, or we learn where they stop |

Stage 7 is deliberately last. Optimising against the current stack would
tune code we are deleting; the numbers only mean something once the
architecture under them is the one we intend to keep.

We are running on `iceberg-rust` main and datafusion 54.1 as of today —
two lines of code (`ScalarUDFImpl::as_any` was removed in 54), arrow
unchanged at 58.4, 48/48 test targets green.

## 7. Ruled on the open items (project lead, 2026-08-17)

- **One statement, one commit. No batching until `_row_id` lands.**
  16.5 ms per write, and the interactive path is the only one that pays
  it. Bootstrap and pre-warm are the exception and need no waiting:
  ordering inside a commit only matters when two rows share a
  supersession key, and neither writes the same (subject, aspect,
  actor kind) twice — so they batch safely today at 0.47 ms/row.
- **`ViewTable` is the answer for `read.<y>()`.** Iceberg materialized
  views are not pursued; the item closes rather than waiting on
  apache/iceberg-rust#55.
- **The suite stays red** on the one stack-overflow test. No
  `RUST_MIN_STACK` in `.cargo/config.toml` — a red test is the reminder
  that stage 2 is owed, and it goes green when the cause is deleted
  rather than when the limit is raised.
- **`UNION ALL` versus N concurrent plans is explained, not open.**
  Concurrent plans run concurrently; a union has to channel its arms
  through a shuffle to build one frame. Nothing to measure — it is a
  reason to prefer composed sub-plans, recorded in the skill.
- **The four workspace skills become deliverables** in their own repo
  once stable, so `glossql-substrate` living beside them is fine.

**`temporal` is settled by reading it, and it is not a search.** Its five
queries are all over the one subject column: a `LIMIT 0` schema probe
(`:22`), an aggregate for presence and span (`:35`), the delta median
(`:45`), significant gaps at `med * 2.0` (`:96`), and an actual-versus-
expected count (`:161`). The last two carry parameters derived from
earlier results — a threshold and a granularity — but **neither changes
what is read**: the read set is `{table.column}`, known from the subject
alone. That is the `behavior_evidence` pattern (§4's rule: parameters may
be data-derived, the read set may not), already proven in spike 2. It
plans as one statement with the median as a scalar subquery, or as two if
that reads badly; two plans over a known read set is not an exception. It
is a per-column statistic, and it ports at stage 5 like the rest.

**The goldens are picked by shape, and used as they are** (ruled
2026-08-17). `~/glossql-ws` alone is 13 clean tables in one dataset and
would let a port pass while breaking every abstention path. Diversity of
*shape* is what protects a port; size is a separate question asked at
the end. Nothing is sliced or preprocessed — a dataset either works as
it stands or it is discussed.

| workspace | covers what the others do not |
|---|---|
| `glossql-ws` (finance generator) | ground truth, real glosses, the working-capital run |
| booksql | broken keys, composite endpoints; already run |
| `rel-f1` | declared-FK truth; **three tables with `time_col: null`** — the borrowed-axis path; two tables carrying three FKs to different parents — the shared-parent alignment path |
| `rel-event` | **keyless junction tables** (`pkey: null` on `event_attendees`, `event_interest`), which no other corpus has; and a column literally named `Unnamed: 0`, which is the identifier-quoting hazard string-templated SQL fails on |

Checked, 2026-08-17: both RelBench schemas are flat, no nested types.
`timestamp_ns` appears where we cast to microsecond, and the date
detection matches any unit.

**Scale is verified at the end, on the new stack, not now.** Every
measurement in this report was taken on 12 MB, where fixed costs
dominate and §2's ratios are untested. `dataraum-eval/corpora/relbench`
holds six larger corpora — `rel-salt` 85 MB through `rel-stack` 993 MB —
plus full booksql, used freely and kept local regardless of licence.
They belong to §6's last stage: confirm the ratios hold, and find the
optimisation potential of the finished architecture rather than of the
one being replaced.

## 8. Still open

- **`SELECT _pos FROM …` in user SQL does not work** — metadata columns
  are readable through iceberg-rust's scan only. Expected not to matter;
  find out rather than design for it.
- **Contributing upstream** — the `_row_id` read path is the obvious
  first contribution, and it is what unblocks batching. Premature until
  this architecture is worth defending.
