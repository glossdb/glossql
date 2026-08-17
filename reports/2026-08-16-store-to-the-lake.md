# The store moves to the lake; derived values stop being stored (2026-08-16)

The rulings of this session and the evidence behind them.

Three changes, taken together because each makes the next smaller. The
store's relations become Iceberg tables. The `cache` relation goes away
entirely — not replaced, removed — and with it the machinery that kept
stored derived values honest. And the read path is rewritten against
the spec rather than adapted, non-blocking, because a pull model puts
the work where the current one cannot carry it.

Four of the ten relations do not survive as tables: `cache`, because
derived values are not stored, and `imports`, `recipes`, `datasets`,
because the lake already records what they hold. Six remain.

## 1. Ruled (project lead, 2026-08-16)

- **Every relation of ours goes into Iceberg.** SQLite is acceptable as
  the Iceberg catalog's own backend and nowhere else. Six tables
  survive the move.
- **The cache is removed**, with all of its machinery. Caching returns
  later, surgically, in the read path only, where a measurement shows
  repeated identical work — `whatif` and `metric_cube` the likely
  first candidates — keyed on snapshot id.
- **Detector verdicts are not cached.**
- **`<dataset>_meta`** for dataset-scoped relations; **source-grain
  glossary rows land in `glossql.glossary`**.
- **`imports`, `recipes` and `datasets` stop being tables of ours** —
  the lake already records a landing, a table's making, and a
  namespace. What it cannot know rides as commit or object properties
  (§6). A dataset's purpose is set at create and not changed
  afterwards; if anyone ever wants that, it is argued then.
- **Migration is wipe and re-bootstrap.** We are building a first
  version, not running a mature service. No export tool, no shim.
- **The read path is rewritten, not adapted** (§8). The current one
  proved the grammar; the server needs a non-blocking implementation
  written against the spec. The old code is retired — git keeps the
  archeology.
- **A compute door is a relation, not a scalar verb** (§8). It is a
  `TableProvider` whose plan node runs the calls. A
  `measure(function, subject)` scalar is refused: an agent already
  extends the system by declaring a function, and a call that answers
  without recording the method is what the grammar exists to prevent.
- **The REST catalog is parked** until this works. Redis is parked.

## 2. The principle this rests on

**Stored is what actors said. Derived is computed at read.**

A gloss is authored. It does not become wrong when data changes — it
becomes old, and the collapse already discloses that by comparing the
snapshot it was written against to the table's current one. Serve and
mark, as SPEC §5.3 has it.

A measurement is a function of data. It *does* become wrong when data
changes. Storing it therefore creates an obligation to maintain it,
and this system has no maintenance mechanism. Everything the cache
grew — the ACCEPTS invalidation edges, the sweeps, the freshness
comparisons — is an attempt to maintain derived data without one.
That is the defect, and removing the storage removes the need.

The design assumption is that **data updates are normal**, not an
event we cause. A derived value computed at read against the current
snapshot cannot be stale. A stored one has no key at all — it is a row
someone has to go and update.

A second line decides *where* the stored half lives: **facts about a
write ride the write; claims about a subject are rows.** `imports`
describes the act that produced a snapshot, so the snapshot carries it
(§6). The glossary is the other side — supersession, actor kind,
witnesses, precedence — and snapshot and table properties are untyped
string KV with no ordering or precedence of their own, so putting any
of it there means rebuilding those semantics on a configuration
surface. That is what the 2026-08-11 report turned down as
properties-as-the-store, and the line is the same one.

Two places already sat on the right side of it before this session:
the read context lists tables and columns from the lake instead of
keeping our own copy (`reads.rs:95-103`), and gloss staleness compares
against the table's current snapshot instead of a flag we maintain.
Three relations join them — `imports`, `recipes` and `datasets` (§6).

A third rule bounds all of this, and it is what keeps shipped material
out of the store: **the store holds what actors can overwrite.** A
function is a row because an actor supersedes it. The built-in app and
a workspace's `apps/<name>/` directory are files because no actor
overwrites them — read-only bundled material, not workspace state.

## 3. What the update assumption breaks today

Not opinion; each is in the code.

- `stamp()` returns `None` for dataset-level subjects
  (`session.rs:713`), so `metric_cube`, `detect_relationships` and
  `whatif` carry no data version. In the measured workspace those are
  nine rows of 71 and **68% of all cached bytes** — the values most
  bound to data are the ones with no link to it.
- The `imports` edge (`store.rs:1074`) fires only when we land the
  data ourselves. An Iceberg commit from anywhere else — the point of
  a lakehouse — fires nothing.
- Detector freshness compares `computed_at` against slot `written_at`
  (`reads.rs:188`) and does not consider data at all, so a verdict over
  changed data is served as current.
- `whatif` is not a declared function, so no ACCEPTS edge reaches it;
  its only invalidation is a hand-written delete on a glossary strike
  (`store.rs:1787`). It also writes to the cache *inside the
  RelationPlanner* (`whatif.rs:175`) — a write during query planning.

## 4. Nothing here is expensive

Measured on the real workspace (`~/glossql-ws`, the 2026-08-15
working-capital run): 302 rows in a 442 KB SQLite file over a 12 MB
warehouse of 13 tables. Cache rows carry `computed_at` and a
function's rows are written in one pass, so the spread is the compute
time:

| function | rows | ms/row |
|---|---|---|
| `behavior_evidence` | 4 | 943 |
| `profile` | 6 | 32 |
| `detect_hierarchies` | 3 | 26 |
| `rate_tolerance` (detector) | 3 | 14 |
| `dimension_relevance` | 6 | 3.8 |
| `slot_entropy` (detector) | 43 | 1.0 |

The whole cache recomputes in about four seconds; nothing in it takes
longer than a second. Detectors are cheap by construction — they are
refused table data outright (`DeniedDoor`, `reads.rs:144`), so their
input is a handful of slots and a threshold.

The cost is CPU in our own scripts and kernels, not IO. That is why
moving the store to Iceberg neither helps nor hurts it, and why the
caching question is a later, measured one. For scale: a full agentic
run over this workspace takes 45 minutes to an hour, so the whole
cache is worth about 0.1% of one run.

### What a read-time sweep costs, and what bounds it

With nothing stored, a `GLOSSARY()` sweep computes the measurements
the vocabulary says are owed. What "owed" means is already decided by
two declarations, and both must be applied or the arithmetic is
nonsense:

- **Grain** — `ON DATASET|TABLE|COLUMN|…` gates which subjects an
  aspect reaches (`admit_grain`, `store.rs:423`).
- **Conditional relevance** (ruled 2026-08-14) — an aspect declaring
  `condition_aspect = condition_value` is owed on a subject only while
  that sibling aspect's winning slot carries the value; absence is
  decisive (`store.rs:1487-1521`, applied at `:1567`).

The measured workspace has 13 tables and 75 columns. Nineteen columns
carry a `role`: 12 `measure`, 7 `dimension`. Fifteen functions fill
slots — five at column grain, two at table, eight at dataset.

Grain alone gives 5 × 75 + 2 × 13 + 8 = **409 calls**, of which
`behavior_evidence` at 943 ms is 75 calls and 71 seconds. That number
is wrong, because it ignores the conditions.

**The gap: the conditions are set on the FACT aspects and not on their
MEASUREMENT counterparts.** `behavior` is conditioned on
`role = measure`; `behavior_evidence`, its measured half, is
unconditioned. Same for `dimension` and `dimension_relevance`. With
the conditions each pair evidently wants:

| function | subjects | cost |
|---|---|---|
| `behavior_evidence` on `role = measure` | 12, not 75 | 11.3 s, not 71 s |
| `dimension_relevance` on `role = dimension` | 7 | 0.03 s |
| `outlier_profile` on `role = measure` | 12 | unmeasured |
| `temporal_profile` on `role = timestamp` | — | unmeasured |
| `column_profile` — produces what the others read, unconditioned | 75 | 2.4 s |

`timestamp` is already in the role vocabulary the skill teaches
(`key · measure · dimension · timestamp · attribute`), so
`temporal_profile` needs no new vocabulary — only the condition.

Running `behavior_evidence` over 75 columns was always doing 63 calls
on columns nobody had called measures.

### Two gaps this exposes, both outside the migration

- **The shipped measurement aspects need the conditions their fact
  counterparts already carry.** This is what bounds read-time
  computation to what the workspace has assigned.
- **Conditions gate the backlog but not admission.** `admit_grain`
  takes grain only; `condition_aspect` is read in exactly one place,
  the unassessed disclosure. So an agent may extract
  `behavior_evidence` on a column whose role is `dimension` or
  unglossed, and may `GLOSS behavior` there too. That is why the agent
  has been deciding relatively freely what it can measure. Gating
  admission on the same rule would give one declaration three uses:
  admission, the backlog, and what a read computes.

`unassessed` is what makes this legible and it is not an artifact: a
row saying an aspect is owed here and nobody has spoken. It is the
backlog, bounded by grain and condition.

**Ruled 2026-08-16: `open_functions` goes rather than moving.** The
surface counts functions with no row in `cache`
(`crates/session/reads/workspace_next.sql:75`) — functions never run.
Under a pull model no function is ever open; it is computed when
asked, a little slower on the first call. `functions` joins the
surfaces where nothing can be owed and `open` reads 0, which is the
rule that read already states in its own header.

That is the whole shipped-surface exposure. `cache` appears in exactly
one other place in the reads and frames — as prose, in
`metric_surfaces.sql:15` and `docket/frames/trend.sql:1`, both of
which read through `metric_series()` and keep working.

## 5. Where the relations live

The schema already carries the split. Five relations have no `dataset`
column — `sources`, `datasets`, `aspects`, `functions`, `witnesses`;
four have one — `glossary`, `recipes`, `relationships`, `imports`.
Four of the ten relations do not survive as tables (§6), leaving six.

- **`glossql`** — `sources`, `aspects`, `functions`, `witnesses`, plus
  source-grain glossary rows: they read and supersede workspace-wide
  (ruled 2026-08-12) and would otherwise have to be hunted across
  every dataset's namespace.
- **`<dataset>_meta`** — that dataset's record: `glossary`,
  `relationships`. `fin` pairs with `fin_meta`, as ruled 2026-08-11.

**Not one flat metadata catalog**, because of access rights when they
come: REST catalogs grant on namespace and table, so `fin` + `fin_meta`
grants a dataset and its record as a unit. A shared namespace with a
`dataset` column turns per-dataset access into row filtering, which no
catalog can express and we would have to build.

**Not nested `fin.meta`**: iceberg-datafusion flattens multi-part
namespaces into separate DataFusion schemas
(`iceberg-datafusion-0.10.1/src/catalog.rs:50-57`), so `fin.meta`
surfaces as two schemas. Single-part with an underscore is what works,
and it matches the AWS S3 Metadata precedent recorded on 2026-08-11 —
managed Iceberg tables in a system namespace derived by naming
convention, single writer, read-only to everyone else. That survey
found no context or metadata engine storing its own state in a
lakehouse format; the pattern is borrowed from S3.

**REST-ready by construction:** create namespace, create table,
append, compare-and-swap. No SQL feature is used. `Lake::open` swaps
the builder (`crates/catalog/src/lib.rs:72`).

## 6. What is deleted

| what | where | lines |
|---|---|---|
| `forward_delete` sweeps + pre-selects | `store.rs:1743-1890` | 148 |
| detector freshness at read | `reads.rs:150-247` | 98 |
| cache accessors | `store.rs:1638-1730` | 94 |
| `invalidate()` | `store.rs:1020-1043` | 24 |

Plus the ACCEPTS-as-invalidation-edge concept (`accepts_relation`),
the `cache` relation itself, and the plan-time `cache_put` in the
whatif planner. Measurements, verdicts, whatif and misfit compute at
read.

**Use what Iceberg already records.** Supersession orders by
`id INTEGER PRIMARY KEY AUTOINCREMENT`. Every write is a commit and
every commit is a snapshot, so the order is already established by the
store — no column of ours to mint.

**Ruled 2026-08-16: `imports` stops being a table of ours.** The lake
already records a landing. `table$snapshots` serves `committed_at`,
`snapshot_id`, `parent_id`, `operation`, `manifest_list` and a
`summary` map (`iceberg-0.10.1/src/inspect/snapshots.rs:48-77`), and
the summary carries `added-records` and `total-records`, maintained by
the writer without being asked
(`src/spec/snapshot_summary.rs:198,280`). Against our columns:

| `imports` column | where it comes from |
|---|---|
| `dataset`, `table_name` | the namespace and table |
| `landed_rows` | `summary['added-records']` |
| `imported_at` | `committed_at` |
| `source_scans` | ours — the source side |
| `dropped_rows_count` | ours — the recipe's shape |
| `cast_failures` | ours — the cast attempts |

The last three are not derived and not knowable from the lake: they
are facts about the import *act*, on the source side, and they are why
the relation exists at all (the 2026-08-15 fix, after a summed count
reported 16,817 phantom dropped rows). They ride the landing commit as
snapshot properties — `fast_append` carries them
(`src/transaction/append.rs:41-66`) and the `summary` map serves them
straight back.

**One requirement this creates.** DataFusion's insert path commits
`tx.fast_append().add_data_files(data_files)` and never sets snapshot
properties (`iceberg-datafusion-0.10.1/src/physical_plan/commit.rs:248`),
so a landing that must carry them commits through iceberg-rust's
transaction API instead of `INSERT INTO`. That is the import path,
which already holds those three facts at the moment it writes.

The `imports` relation survives as a name — a read over
`table$snapshots` plus the properties, in the same column shape agents
already read. It gains something on the way: it shows every landing,
including ones we did not record ourselves.

At the pin the metadata tables are `snapshots` and `manifests` only
(`src/inspect/metadata_table.rs:32-37`).

**Ruled 2026-08-16: `recipes` becomes table properties.** A recipe is
one per (dataset, table) — exactly the primary key — and it is two
strings, `source` and `sql`. It has no actor, no witness, no
precedence, and the language never asks what a previous recipe said;
replacing one is outright replacement. `UpdatePropertiesAction::set`
carries it (`iceberg-0.10.1/src/transaction/update_properties.rs:57`).

The code already shows the two stores drifting apart. The recipe is
written *after* materialization (`session.rs:472`), so the table always
exists by then — yet admission has to cross-check the lake anyway:

```rust
Some(lake) if admission == RecipeAdmission::Unchanged
    && lake.table_exists(dataset, table).await? =>
```

That guard exists only because a recipe row can name a table that is
not there, and `drop_table_records` deletes the row on `DROP TABLE`
for the same reason. On the table, the fact cannot outlive or precede
what it describes, and both disappear. The cost: `SELECT * FROM
recipes` becomes N table-metadata reads instead of one scan — 13
tables in the measured workspace — and the no-lake path
(`session.rs:436`) has no table to hold a property, so that mode needs
an answer when the code is written.

**Ruled 2026-08-16: `datasets` becomes the namespace.** `DECLARE
DATASET` already does both in one act (`session.rs:424-426`): it
writes the row *and* creates the namespace, so we keep a parallel copy
of a list the catalog maintains. `create_namespace` takes properties
and the settings are a single key — `{"purpose": "…"}` in the measured
workspace. `update_namespace` is implemented in SqlCatalog
(`iceberg-catalog-sql-0.10.1/src/catalog.rs:579`) but unsupported in
iceberg-rust's REST client, hence set-at-create above. The dataset
list reads from `list_namespaces`, filtered by the `_meta` convention.

**Where it stops.** The remaining six have no Iceberg object that owns
them: `sources` describes a connection outside the lake entirely;
`aspects` and `witnesses` are vocabulary attached to no table or
namespace; `functions` likewise, and the bodies settle it
independently — `behavior_evidence` is a 32 KB script, which is not
what a properties map is for; `relationships` spans two tables, so no
single object owns it, and it is a set per dataset while properties
are flat KV; `glossary` carries actor, supersession and precedence.

One tempting candidate that fails, recorded so it is not proposed
again: column glosses into an Iceberg schema field's `doc`. A column
gloss looks exactly like column documentation, but it has an actor, a
kind, supersession and a possible contested state, none of which
survives being flattened to one string.

**One consistent view per statement.** `IcebergTableProvider::scan`
reloads the table from the catalog on every scan
(`iceberg-datafusion-0.10.1/src/table/mod.rs:135`), so two scans of
one table in one query can straddle a landing. Resolving the snapshot
once per statement and computing everything derived against it fixes
that as a side effect of the rewrite.

## 7. The SPEC diff

Seven places, and the net is a shrink. Nothing here adds prose.

**§3, re-land and `DROP TABLE`.** "the old landing and its cached
evidence are dropped" and "`DROP TABLE` removes a table whole (the lake
table, the recipe, the cached evidence, the import records)" — there is
no cached evidence, the recipe is a table property, and the import
records are the table's snapshots. All three go with the table. The
sentence becomes: `DROP TABLE` drops the table, and refuses while it
holds data or glosses. `drop_table_records` disappears with it.

**§5.1, MEASUREMENT.** "its value is the bound function's cached JSON
output (§6, §7), served by `GLOSSARY()` beside facts and groundings,
from the `cache` relation (§6)" → its value is the bound function's
output, computed when a read needs it and served by `GLOSSARY()`
beside facts and groundings.

**§6, `ACCEPTS`.** "The declaration relations `relationships` and
`imports` may ride the list too, as invalidation edges only … a write
to the relation kills the cache like an aspect value would" — with
nothing cached there is no edge, so the clause goes whole, and
`accepts_relation` with it. A script that wants those relations reads
them as tables, which it could already do without naming them.

**§6, `RETURNS`.** "each is a data-grounded *voice* whose cached output
joins the spoken slots (§7). Results land in the `cache` relation
below." → whose output joins the spoken slots, computed at read.

**§6, the cache section.** "The first run computes and caches; later
selects read the cache", the `cache` relation definition and its
column shape, the `DELETE FROM cache` example, and the whole "Writes
invalidate, reads recompute, judgment only supersedes" paragraph —
all of it goes. What survives from that section is the output-shape
rule, which was never about caching: a body carrying a top-level
`summary` serves the summary at extraction and reads back whole
through `GLOSSARY(subject::aspect)` (ruled 2026-08-14). "Functions
never write the glossary" stays, and is now unconditional.

**§7.2, detectors.** "Detectors run **at read**: a verdict missing or
older than the newest slot write recomputes when `ATTEST()` or a
collapsed `GLOSSARY()` read needs it, and caches like any function
result — `DELETE FROM cache` still forces it." → **Detectors run at
read.** The qualification was the caching; without it the sentence is
shorter and truer. "Detail lives in the value function's own cached
output" → its own output.

**§9, `metric_series()`.** "serves the cached `metric_cube` measurement
as long rows … cached-only, nothing computes at read" → serves the
`metric_cube` measurement as long rows; it computes when read.

**Corpus.** Fixtures using `DELETE FROM cache` or selecting from the
`cache` relation change with the language. The parser suite is the
check: every ```sql block in SPEC.md parses and every fixture behaves
as tagged.

**Two related rulings, recorded here because they are language, not
implementation.** A function is never "open" — a function is called,
and there is no state in which it stands unfinished; `workspace_next`
loses `open_functions` and gains nothing in its place. A function
whose `ACCEPTS` inputs are not glossed returns an error naming them,
which is an answer, not a condition to track.

## 8. The read path is rewritten, not adapted

**Ruled 2026-08-16.** The current read path proved the grammar and ran
it over several datasets; that was its job. The implementation the
server needs is non-blocking, and it is written against the spec
rather than adapted from what proved the grammar. Learnings carry
forward, the old code is retired, git keeps the archeology.

The change is narrower than it sounds, because the seam is already
right. `plan_relation` returns
`LogicalPlanBuilder::scan(name, provider_as_source(…))` — it just
hands it a `MemTable` built at plan time. `provider_as_source` takes
any `Arc<dyn TableProvider>`, and `TableProvider::scan` is async and
receives `filters` and `limit`
(`datafusion-catalog-53.1.0/src/table.rs:166,285`). So each door swaps
an eagerly-built batch for a provider that works in `scan`/`execute`.
The `RelationPlanner` seam stays: it is what lets `all => true` and
pair paths (`a.b <-> c.d`) decode from the raw AST.

**The line between the doors: expansion at plan time is correct,
computation at plan time is not.** `read.<aspect>()` and the shipped
reads expand a grounding into a subplan and do no work — unchanged.
`GLOSSARY()`, `ATTEST()`, `metric_series()`, `whatif.<x>()`,
`misfit.<x>()` and the store relations compute, and their computation
moves under `execute`. Three `MemTable::try_new` sites carry all of it
(`reads.rs:325,361,426`).

**What the layers become.** `Lake` is a thin wrapper today: a catalog
handle, a cached provider, small metadata reads, and `data_version` —
an `AtomicU64` bumped on materialization, `DROP TABLE` and namespace
create (`crates/catalog/src/lib.rs:46`). That counter was a stand-in
for snapshot ids the SQLite store had no access to; with the relations
on the lake and a statement resolving its snapshot once, it has
nothing left to say and goes. `Store` becomes row IO behind the narrow
trait plus the pure rules. Computation lives in one place — under
`execute` — rather than being spread across a planner callback, a
store read and a wrapper.

What this buys, beyond not blocking a planner thread: a filtered read
stops paying for what it discards. Today the batch is built before the
`WHERE` is applied, so `GLOSSARY() WHERE subject = 'x'` computes the
whole scope. Under a pull model that is the difference between calling
one function and calling twelve — the argument for doing this *with*
the cache removal rather than after it.

### The contract we are writing against

`ExecutionPlan::execute` states three obligations
(`datafusion-physical-plan-53.1.0/src/execution_plan.rs:265-330`) and
no others:

1. Do no work before the first poll — `execute` returns a `Stream`.
2. Do not hold the CPU without yielding back to the runtime.
3. Raw `tokio::spawn` is disallowed; use `SpawnedTask`, `JoinSet` or
   `RecordBatchReceiverStreamBuilder`, so dropping the stream cancels
   the work behind it.

The second is the subject of DataFusion's cancellation post
(2025-06-30) and it costs us nothing. Explicit yield points are needed
only by source operators that use no tokio resources and by
exchange-like operators that pass data outside tokio's channels
(`coop.rs:34-38`). `SchedulingType` defaults to `NonCooperative`
(`execution_plan.rs:1043`), and `EnsureCooperative` is in the default
physical rule set
(`datafusion-physical-optimizer-53.1.0/src/optimizer.rs:153`), so a
leaf that declares nothing is wrapped in `CooperativeExec` by the
optimizer. The tokio task budget is not ours to manage.

`target_partitions` defaults to available parallelism
(`datafusion-common-53.1.0/src/config.rs:506`), so repartition,
aggregation and joins already fan one query across that many tasks.

The path from a request: the axum listener takes it, the door resolves
the actor's channel from the plane, `query_stream_with_params` plans
(`statement_to_plan`) and executes
(`execute_logical_plan().execute_stream()`, `session.rs:899-909`), and
batches stream back. `execute_stream` yields one output stream over a
plan that may fan out beneath it.

**CPU work belongs on the runtime.** This is the fact that decides the
rest, and it is unconventional enough that DataFusion's own blog opens
by saying so: it "uses the Rust async system and the Tokio task
scheduler for CPU intensive processing" (2025-06-30). Sorts, joins and
aggregations run inline in `poll_next`, bounded to one batch, and
nothing is offloaded. The crate documentation states the negative
directly (`datafusion-53.1.0/src/lib.rs:644-651`):

> DataFusion does not use `tokio::task::spawn_blocking` for
> CPU-bounded work, because `spawn_blocking` is designed for blocking
> **IO**, not designed CPU bound tasks. Among other challenges,
> spawned blocking tasks can't yield waiting for input (can't call
> `await`) so they can't be used to limit the number of concurrent CPU
> bound tasks or keep the processing pipeline to the same core.

Checked against the source rather than the doc: `block_in_place`
occurs **nowhere** in any DataFusion crate, and every internal
`spawn_blocking` is blocking IO — spill files (`spill/mod.rs:119,193`),
`StreamTable`'s fifo (`catalog/src/stream.rs:384,424`), and the JSON
source bridging a sync `Reader` over a channel
(`datasource-json/src/source.rs:401`, pattern drawn at
`utils.rs:392-418`). `RecordBatchReceiverStreamBuilder::spawn_blocking`
is the escape hatch for a foreign sync IO source. It is not the
pattern for compute.

**So plan time does not block either.** Today it does, through
`block_in_place` + `block_on` (`reads.rs:443-448`, `:515-519`), because
`RelationPlanner` is a sync trait. DataFusion has the same problem and
solves it by resolving asynchronously *before* planning:
`statement_to_plan` walks the statement for table references, awaits
each one through the catalog into a map, and only then runs the sync
`SqlToRel` (`session_state.rs:494-514`).

The same shape works here. An async pre-pass over the parsed AST
collects every door reference, fetches what the sync planner will need
— a grounding's SQL, a frame's schema, the owed set — and stashes it;
the planner then reads from memory. No `block_in_place` anywhere. The
pre-pass must chase groundings transitively to reach closure, so the
cycle guard falls out of it as a plain graph walk (§8, below).

**The candle work and the column kernels fit the model as they are.**
Pure CPU over Arrow, no re-entrancy: they run inline in `poll_next`,
one call per poll, cancellable between calls. `compute_pool()`
(`crates/scripts/src/lib.rs:86`) stays, for a reason that is not
scheduling: `candle-core` depends on rayon
(`candle-core-0.9.2/Cargo.toml:231`) and would otherwise fan into the
global pool, so concurrent calls would oversubscribe the machine
between them. `GLOSSQL_CANDLE_THREADS` caps a third-party library's
internal parallelism. That is the only pool we own.

**The unresolved seam is `db.query`, not the CPU.** A rhai script is
CPU work that re-enters the engine mid-flight, and rhai 1.25 cannot
`await`, so a running script cannot yield — which the model above has
no place for. Three ways out and no fourth: a thread
(`spawn_blocking`, the IO escape hatch used for compute); a stackful
coroutine (weighed and rejected earlier); or removing the re-entrancy,
so that a function's data is fetched before its body runs and the
script is pure CPU over Arrow like the kernels beside it.

The third is where it went: `2026-08-17-functions-split.md` reads the
shipped library and finds that only one of its thirteen functions
crunches data at all. The rest compose SQL in loops, which is a
question about what a function *is* rather than about where it runs.

**Memory is declared, not anticipated.** The substrate carries the
mechanism: `RuntimeEnvBuilder::with_memory_limit` and
`with_memory_pool` (`runtime_env.rs:374,401`), backed by
`GreedyMemoryPool`, `FairSpillPool` or `TrackConsumersPool`
(`memory_pool/pool.rs:65,138,330`); an operator that wants accounting
registers a `MemoryConsumer` and calls `try_grow`
(`memory_pool/mod.rs:244,322,454`), and receives an error rather than
an OOM. PyIceberg's DataFusion proposal (apache/iceberg-python#3554,
open) reaches for exactly this — a default budget with spill —
instead of estimating what an operation will hold. If a script's
materialized input ever needs a bound, that is where it is set.

### Where a function call lives

**Ruled 2026-08-16: the door is a relation and the call is a plan
node.** A leaf `ExecutionPlan` built from
`RecordBatchReceiverStreamBuilder` holds a list of (function, subject)
calls, spawns each as a blocking task, and streams batches as they
finish. Every compute door — `GLOSSARY()`, `ATTEST()`,
`metric_series()`, `whatif.<x>()`, `misfit.<x>()` — builds the same
node with a different call list. One implementation, written once.

A `measure(function, subject)` scalar was considered and refused. The
substrate would carry it: `AsyncScalarUDFImpl` has
`invoke_async_with_args` and `ideal_batch_size`
(`datafusion-expr-53.1.0/src/async_udf.rs:22-38`), and the physical
planner inserts `AsyncFuncExec` above projections
(`physical_planner.rs:2902`), filters (`:1120`) and aggregate inputs
(`:1041`) without being asked — so the refusal is not about
feasibility. It is refused because an agent already extends the system
with a function: it declares one, and the declaration carries the
body. A scalar verb adds no capability, it adds a second way to invoke
what is already declarable — and the second way returns an answer
without leaving a record. The grammar exists so that an agent closes
its information gap *and* persists the method it used; a call that
computes inside a `SELECT` and records nothing is the shape the system
is built to prevent. It would also route around grain and condition,
which §4 already notes are weakly enforced.

So the door is a `TableProvider` whose async `scan` reads the store,
applies grain and condition, and returns the node. The rules stay
Rust, applied at the one site that can build a call list, and
filtering is contractual rather than hoped for: a
`supports_filters_pushdown` of `Exact` means the predicate reaches
`scan` (`datafusion-expr-53.1.0/src/table_source.rs:46-50`) and
shrinks the list before anything runs.

**The rough edges this will surface**, named now rather than found
later:

- **`Exact` is a promise.** A provider that claims `Exact` and does
  not fully apply the filter returns silently wrong rows — DataFusion
  drops the `Filter` above it. `Inexact` is the safe claim and it
  costs a `LIMIT`: with any inexact filter pushed down, limit
  pushdown is refused
  (`datafusion-catalog-53.1.0/src/table.rs:155-163`), so `--row-cap`
  and a `WHERE` interact.
- **A schema must exist before `scan`.** `misfit.<frame>()`'s output
  schema is the declared frame's schema, and a `TableProvider` answers
  `schema()` synchronously. That is what the async pre-pass is for: it
  resolves the frame — as `plan_serve` resolves a grounding's SQL
  today (`reads.rs:456-459`) — before the sync planner runs.
- **Context assembly belongs in the stream, not in `scan`.** `scan`
  decides *which* calls; each call's `ACCEPTS` context is a store read
  awaited as the stream reaches that call. Assembling it in `scan`
  would rebuild the plan-time batch under a new name.
- **`PlanProperties` is unforgiving boilerplate.** Partitioning,
  `EmissionType`, `Boundedness` and `EquivalenceProperties` are all
  constructor arguments (`execution_plan.rs:1028-1035`), and
  `with_new_children` must be right. Metrics are opt-in: without an
  `ExecutionPlanMetricsSet` our node shows nothing under `EXPLAIN
  ANALYZE`.
- **Cancellation granularity is one call.** A cancelled query stops
  consuming at once; the call in flight still finishes, because
  neither a `poll_next` body nor a script can be interrupted. Under a
  second, per §4.
- **Receiver capacity is lookahead.** If the node buffers N batches
  ahead, it computes up to N the consumer may never read; small is
  right.
- **Tests change shape.** They collect today; they will drive streams,
  and cancellation deserves one of its own — drop the stream
  mid-flight, assert the work stopped.

**Repeated expansion is not a problem to solve here.** DataFusion's
common-subexpression elimination is expression-level — "only common
sub-expressions within a single `LogicalPlan` are eliminated" — and
materialized views are `not_impl_err!`
(`datafusion-sql-53.1.0/src/statement.rs:608`). So a door reached
twice in one plan expands twice. That has never shown up in a run, and
the durable answer is upstream: Iceberg views are already in
iceberg-rust's spec module (`src/spec/view_metadata.rs`,
`view_version.rs`), materialized views follow the format.

### The cycle guard falls out of the pre-pass

`GlossqlReads` is registered once per session (`session.rs:308`) and a
nested expansion re-plans through the *same* `SessionContext`
(`reads.rs:506-519`), so DataFusion calls back into the same object
with no way to know where it is. That is what the `EXPANDING`
thread-local stands in for, and it holds today only because
`block_in_place` keeps the whole nesting on one thread.

With resolution moved ahead of planning it needs no mechanism at all.
The pre-pass already walks door references transitively — a
grounding's SQL may name another door, which must be fetched before
the planner runs — so it is a graph traversal in plain async Rust,
and a traversal carries its own path. A repeat on that path is a
cycle; the planner never sees one, and the thread-local, the chain
field and the child `SessionState` all go.

**The path is not the set of everything expanded.** A grounding
reached through two branches appears twice in the tree and never twice
on one path, so it expands normally; only a genuine ancestor repeat is
refused. A door name refers to the current grounding, so
self-reference has no base case and nothing to terminate on — it is an
authoring mistake, caught at read because catching it at write would
mean expanding every other grounding in the workspace on each `GLOSS`.
The path is the error message.

## 9. Staging

1. **Split `store.rs`** — supersession, collapse, precedence, grain and
   condition admission become pure functions over rows; IO behind a
   narrow trait. Behaviour-preserving, suite green. The rules need no
   runtime and no IO, which is what lets them be tested without a
   store.
2. **Remove the cache and move the compute doors under `execute`**,
   together, for the reason in §8. The SPEC and corpus diff lands
   here. Conditions gate admission, so read-time computation is
   bounded by what the workspace has assigned.
3. **`imports`, `recipes`, `datasets` become reads** over snapshot
   summaries, table properties and the namespace list; the import path
   commits through iceberg-rust so it can stamp its three source-side
   facts.
4. **Implement the IO trait over Iceberg** for the six remaining
   relations, one first — `relationships`, small and without
   supersession — measuring the workspace-fixture cost before the rest
   follow. `glossary` last.
5. **Delete the SQLite implementation** and our direct sqlx
   dependency; `iceberg-catalog-sql` keeps its own.

## 10. Deferred: caching

After all of the above lands, and only then: measure where a read
repeats identical work. `whatif` and `metric_cube` are the named
candidates; the collapse may join them once measurements compute at
read. Whatever is added is in the read path, bounded, in memory, and
keyed by the snapshot ids the computation read — so a data update
yields a different key rather than a stale hit. Not storage.
