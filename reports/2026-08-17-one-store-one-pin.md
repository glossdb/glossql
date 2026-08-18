# One store, one pin; the plan line under verification (2026-08-17)

The third report of this arc. `2026-08-16-store-to-the-lake.md` moved the
relations to Iceberg and removed the cache;
`2026-08-17-functions-split.md` read the library and found four shapes
under one name. Both are right about their halves and neither states the
design they share. This report states it, records what is ruled, revises
one 2026-08-16 ruling, and defines the prototypes that decide the rest.

Four spikes were run before writing, all on the measured workspace
(`~/glossql-ws`, 13 tables, 12 MB); their numbers are §4, §11 and §13.
All four pass. §14 is what they did not settle.

## 1. Ruled (project lead, 2026-08-17)

- **One store, one pin.** Every input — data and declarations alike — is
  an Iceberg table, and a statement resolves each table's snapshot once
  and computes everything derived against that set.
- **The read set may depend on declarations. Never on data.** This is
  the rule that decides what may be a function (§5).
- **"One plan" is about scheduling, not about SQL.** Every computation
  is a node the engine schedules, cancels and parallelizes. Rust over
  Arrow qualifies; the core analyses stay in Rust. What is forbidden is
  computing at plan time, re-entering the engine from code that cannot
  `await`, and a read set that depends on data values.
- **Stream what is unbounded; collect what carries a declared cap**
  (§6).
- **Prototype the complex functions before writing the implementation.**
  `temporal`, `metric_cube`, `coherence` and `behavior_evidence` are
  essential and must work; the design is not settled by argument about
  them. `coherence` and `behavior_evidence` were spiked and both pass
  (§13); `temporal` and `metric_cube` are held as the same two shapes.

Proposed here and not ruled: pin-keyed materialization (§7), pre-warm at
the landing (§8), no new grammar word (§9).

**This revises one 2026-08-16 ruling.** That report ruled "the cache is
removed, with all of its machinery" and "derived values are not stored".
The removal of the machinery stands. The prohibition on storage does
not: §3 shows the defect was the key, not the storage, and §7 proposes
what replaces it. The 2026-08-16 reasoning was correct for the key it
had.

## 2. The three lines

| line | what it says | what it removes |
|---|---|---|
| **one store** | every input is an Iceberg table | the production DB dependency |
| **one pin** | a statement resolves each snapshot once; derived values are keyed by that set | invalidation |
| **one plan** | every computation is a node the engine schedules | the parallelism and cancellation gaps |

They are not three projects. Each makes the next available: one store is
what lets the key be complete (§3), and one pin is what lets a plan be
consistent (§4).

## 3. The cache's defect was the key, not the storage

`cache` was not wrong because it held derived values. It was wrong
because it could not name what those values were derived from, in two
ways the earlier reports found separately and never joined.

**Half the subjects had no data version.** `stamp()` returns `None` for
dataset-level subjects (`session.rs:715`), so `metric_cube`,
`detect_relationships` and `whatif` carried no link to the data at all —
nine rows of 71 and 68% of cached bytes in the measured workspace
(2026-08-16 §3).

**Half the inputs had no version that could exist.** A measurement does
not read only data; it reads declarations. `coherence.rhai:25` reads
`relationships` and then joins two data tables. `metric_cube.rhai:54`
joins `glossary` against `aspects` and hand-writes the supersession
collapse as a `NOT EXISTS`. Those inputs lived in SQLite. There was no
snapshot id to put in the key even in principle, so the freshness
comparisons reached for `written_at` (`reads.rs:188`) and the sweeps
reached for hand-declared edges (`store.rs:1074`, and for `whatif` a
hand-written delete at `store.rs:1787`).

Every piece of machinery 2026-08-16 deletes is compensation for a key
that could not be written down. **Moving the store into the lake is what
completes the key**, and that is the whole reason the two reports are
one design: after the move, every input of every measurement — data and
declaration — is a table with a snapshot id.

Under a complete key there is no invalidation, only a miss. A re-import
produces a different key; the old entry is not stale, it is unreachable.
Deleting it becomes a space policy, which may be wrong without anything
breaking.

**One thing to ship from this section regardless.** `metric_cube.rhai:54`
restates the store's collapse rules — latest per (subject, aspect),
human over agent — as SQL text inside a script. The collapse belongs in
the read library as a relation (`glossary_current` or similar). That
removes a duplicated rule and is what makes the declarations-join-data
shape expressible without a script at all.

## 4. The pin holds — measured

`IcebergTableProvider::scan` reloads the table from the catalog on every
scan and always reads the current snapshot
(`iceberg-datafusion-0.10.1/src/table/mod.rs:139-142`), so two scans of
one table in one query can straddle a landing. That is a correctness
exposure independent of caching, and the fix is the pin.

`IcebergStaticTableProvider::try_new_from_table_snapshot(table,
snapshot_id)` (`:279`) is the mechanism. Spike 3 (throwaway example,
`glossql-catalog`) landed three rows, pinned the snapshot, landed two
more, and asked:

| | live provider | pinned provider |
|---|---|---|
| after the second landing | 5 | 3 |
| two scans in one query | 10 | 6 |
| re-pin of S1 from a table loaded at S2 | — | 3 |

All three pass. The third is the one that matters beyond consistency: a
snapshot stays addressable after later commits, so a pin is a durable
key — a materialization can be verified or recomputed at the snapshot it
claims, not merely trusted.

**What the pin costs.** A statement plans against static providers built
by the async pre-pass, not against the shared `IcebergCatalogProvider`.
The shared provider stays for discovery and namespace listing; the pin
is per statement. `Lake::data_version` — the `AtomicU64` stand-in for
snapshot ids the SQLite store could not see (`crates/catalog/src/lib.rs:46`)
— has nothing left to say and goes, as 2026-08-16 §8 already had it.

## 5. The read set rule

> **The read set may depend on declarations. Never on data.**

"Read set" means the set of tables and columns read — not every
parameter. A value derived from data that only selects *how* to reduce
an already-committed read (which `date_trunc` grain, which threshold)
keys fine, because the pin already covers what it read. §13.2 is where
this distinction was forced.

The rule decides what may be a function, and it subsumes three problems
that looked separate:

- **Keying.** A computation whose inputs are chosen at runtime from
  values it has already read cannot be keyed before it runs, so it
  cannot be looked up and cannot be pre-warmed.
- **Re-entrancy.** rhai 1.25 cannot `await`, so a script that re-enters
  the engine must block. Rust can await, so the same computation written
  in Rust composes sub-plans legally.
- **Parallelism.** A known read set is a set of plans that can be built
  and run at once; a discovered one is a serial chain.

`db.query` in a loop breaks the rule. Rust versus SQL does not enter
into it — a Rust node that reads `relationships`, builds N join plans
and reduces them in Arrow obeys the rule exactly as well as one SQL
statement does.

**Gates become columns, not branches.** Where a script queries in order
to decide whether to query again, the probe is pruning, not discovery.
`behavior_evidence` is explicit about this: its anchors come from
declared relationships only, and its probes — grain
(`behavior_evidence.rhai:214,332`) and viability (`:404`) — exist to
avoid scanning wide. Computing the gate as a column beside the result,
and letting the judge read it, restores a knowable read set. The cost is
real and is not assumed away: work that a gate would have skipped is
performed. Whether the plan's shared scans and parallelism pay for it is
what spike 2 measures.

## 6. whatif and misfit are already the architecture

This was not written down, and it changes the risk picture.

Both doors are Rust, async, with no rhai, no `db.query` and no
`block_on`. `whatif` resolves the scenario and every QUERY grounding
through `collapsed_read`, computes its support worlds from the
declaration, builds each series through `ctx.state().statement_to_plan()`
(`whatif.rs:593`), rewrites the plan with a `TreeNode` transform rather
than string templating (`apply_overrides`), executes, and hands bounded
Arrow to the band kernel. `misfit` does the same with one plan and a
declared cap.

They obey §5 exactly: by the time either touches data, the full set of
plans is known. **The "one plan" line is therefore verified for the
hardest analysis in the system.** What is unverified is whether the four
remaining rhai crunchers fit the same shape — which is what §13 spikes.

**The cap rule falls out of them.** `misfit` collects because
`ROW_CAP = 2000` is declared and anything past it is refused by name
(`misfit.rs:37`); `whatif` collects because the series is at most 24
months. Neither is cheating on the `execute` contract: a leaf that does
its bounded work on the first poll does no work before it. So — stream
what is unbounded, collect what carries a declared cap. That resolves
the tension in 2026-08-16 §8 between "no work before the first poll" and
"the kernel needs the whole frame".

**What they still need**, none of it architectural:

- `whatif.rs:175` writes the cache during planning. It goes with the
  pin. Its key is well-formed once the glossary is an Iceberg table: the
  scenario's slot, every QUERY grounding's slot, and the pinned data
  tables. Today its only invalidation is the hand-written delete at
  `store.rs:1787`.
- Both loop `.await` serially over concepts and worlds. That is the
  parallelism gap in its most concrete form, and `buffered`/`JoinSet`
  over the worlds is an afternoon.
- `misfit`'s output schema is the declared frame's schema, and a
  `TableProvider` answers `schema()` synchronously — the async pre-pass
  is what supplies it.

## 7. Measurements, not cache — proposed

The shape does not change from `cache`; the key does, and the name
should follow it. A `measurements` relation in `<dataset>_meta`, holding
what `GLOSSARY()` serves:

```
(function, subject, aspect, pin_digest, pin, value, computed_at)
```

`pin` is the sorted (table → snapshot_id) list of everything the
computation read, declarations included; `pin_digest` is the lookup key.
A single `snapshot_id` is not enough — a dataset-grain measurement reads
many data tables plus `glossary` and `aspects`.

**Only aspect values land.** Searches and statistics stay inside a plan
and recompute; they are single plans now rather than 148 round trips,
and materializing intermediates is the discipline 2026-08-16 §10 already
set — only where measurement shows repeated identical work.

**Reads never write.** A read that misses computes and answers without
appending; writing during a read is the `whatif.rs:175` defect under a
new name, and an Iceberg commit is metadata-heavy and optimistically
concurrent, so a per-miss append would both contend and cost. Writes are
acts, batched into one commit (§8).

**The old rows are the drift record** (project lead, 2026-08-17). A
measurement kept at the pin it was computed against is a point in a
series over snapshots, so the relation answers "what did this look like
three landings ago" and "how far has it moved" without anything further
being built. That inverts the retention question: rows are not garbage
awaiting collection, they are history, and dropping them is a policy
about how much past a workspace keeps — argued on its own terms, not as
a consequence of staleness. It is also the second reason the relation
belongs in the catalog rather than in memory: a process restart must not
lose it.

## 8. Pre-warm at the landing — proposed

Pre-warming implies invalidation only when the warm is keyed on nothing.
Keyed, a value computed against snapshot S stays correct forever; after
S′ lands it is simply never asked for. The failure mode is wasted work,
never a wrong answer.

That makes the trigger obvious: **the landing is the warm.** The import
path mints the new pin and knows which tables moved, so it computes what
grain and condition say is owed against the pin it just created. There
is no window in which a warm entry is stale, because the warm is
downstream of the write that would have staled it. The measured
workspace recomputes everything in about four seconds (2026-08-16 §4),
so frequent updates are affordable.

This depends on the gap 2026-08-16 §4 named: the shipped measurement
aspects need the conditions their FACT counterparts already carry, or
the owed set is 409 calls rather than what the workspace has assigned.

## 9. No new grammar word — proposed

`MEASURE` was considered as a top-level verb and is refused for one
reason: it reintroduces `open_functions`. If measuring is an act that
can be declined, a function can stand un-run, and `workspace_next` has
to count it again — the state 2026-08-16 §7 ruled out. The landing is
the act, the function declaration is the persisted method, and a
materialization row carries function and pin, which is the whole
provenance. A statement adds no information and one more thing to
forget.

The other half of the question — whether measurement results join in
other SQL — is yes, and is already `2026-08-17-functions-split.md` §4:
`ACCEPTS` was function composition through the only channel available.
Once a measurement is a relation, composition is a join.

## 10. What falls away from the 2026-08-16 door design

2026-08-16 §8 ruled the compute door a `TableProvider` whose plan node
holds a call list and spawns each call as a blocking task, because a
call was a script. If a call is a plan, the door's `scan` returns a
`UNION ALL` — a lookup where the key hits, a sub-plan where it misses —
and there is no custom operator.

Which retires most of that section's named rough edges: no
`PlanProperties` boilerplate, no receiver-capacity lookahead, no
per-call cancellation granularity, no `spawn_blocking` for compute.
`EnsureCooperative` wraps the leaves and the cancellation contract is
met by not writing an operator.

What survives: the `Exact`/`Inexact` pushdown promise and its
interaction with `--row-cap`; the schema-before-`scan` requirement; and
the two bounded leaves that whatif and misfit legitimately need (§6).

## 11. Spike 0: the judge costs 0.2 µs; the envelope costs 164

The runtime already builds one engine and caches ASTs per script
(`crates/scripts/src/lib.rs:722-737`); per call it is `Scope::new()` +
`to_dynamic(context)` + `eval_ast_with_scope` + `to_value(result)`
(`:751-775`). Benchmarked in release on the real `band_breach` body:

| | per call |
|---|---|
| `invoke band_breach`, 1 metric × 24 points | 18.5 µs |
| `invoke band_breach`, 4 metrics | 44.2 µs |
| `invoke band_breach`, 16 metrics | 164.3 µs |
| — of which `to_dynamic(context)` | 46.0 µs |
| — of which `eval_ast_with_scope` | 134.5 µs |
| **row judge: scalars in, scalar out** | **0.20 µs** |
| — of which `Scope::new()` + 2 pushes | 0.06 µs |

One-off: `Engine::new_raw()` 1.5 µs, `compile(band_breach)` 714 µs,
cached per script.

**The interpreter is not the cost; the JSON envelope is.** A per-row
rhai `ScalarUDF` at 0.20 µs is about five million calls per second on
one core, before DataFusion partitions it — a million rows costs 0.2 s.
So the judge extension point is safe as a per-row scalar **provided the
judge receives scalars rather than a serialized context**. Roughly a
third of that 0.20 µs is scope churn, which a batch-shaped UDF reusing
one scope would remove; there is no need to reach for it yet.

This also retro-explains part of the library's slowness that the split
report attributed to round trips alone: every function call today pays
serialization of its whole context, and that cost grows with the context
rather than with the work.

**Caveat.** The row judge benchmarked is four comparisons. A judge with
a per-row loop body costs more, though the shipped judges are small by
construction — the point of the split is that they stay that way.

## 12. What `behavior_evidence` inherited

The stock/flow discriminator came from v0.3's lineage reconcile
(`behavior_evidence.rhai:1-3`, transcribed 2026-08-05). Read at the
source, that origin explains the shape:

- `analysis/lineage/reconcile.py` is 249 lines of **pure Python** —
  `math` and `statistics.median` over `Sequence[float]`. No numpy, no
  vectorization.
- `analysis/lineage/processor.py:110-146` issues **one DuckDB query per
  slice** (`duckdb_conn.execute(sql).fetchall()`), and reads its
  declarations from a separate SQLAlchemy database (`:52`).

So query-per-cell inside a deep loop was the natural shape *there*: an
unvectorized language and a metadata store outside the data engine. The
rhai port transcribed the structure faithfully, round trips included.
The 943 ms/row is a 2024-era architecture carried across a language
change, not an authoring mistake.

Both conditions are gone here. There is no separate metadata store once
the store is in the lake, and the substrate is vectorized.

**Half of this port already happened, and it worked.** The 2026-08-06
rework replaced v0.3's Python convention enumeration with the single
`reconcile` kernel call — "one matrix product over the stacked entity
series" (`behavior_evidence.rhai:36-42`). The arithmetic is already
ahead of v0.3. What remains is the enumeration and probe plumbing, which
is the same move applied to the other half of the same function.

**The inverse case is worth recording.** v0.3's `hierarchies` *was*
vectorized — polars plus numpy, with g3 computed by
`np.maximum.reduceat` (`analysis/hierarchies/stats.py:72-88`). Our port
turned it into 148–593 SQL round trips. Either the SQL of
`2026-08-17-functions-split.md` §8 or an Arrow kernel recovers what was
lost; the point is that vectorization was there first and the port
removed it.

## 13. Spikes 1 and 2: the two hard functions

Both pass. Both were run against the measured workspace and thrown away.

### 13.1 `coherence` in whatif's shape

Three paths over the same 14 declared relationships:

| | | |
|---|---|---|
| **PHASE 1** | 14 relationships, 13 tables, 16 date pairs | **1 ms, 0 data scans** |
| A — today | 35 query strings, waves of 4 | 56 ms |
| B — port: 21 plans built up front, all concurrent | 3 ms to build | **24 ms (2.4×)** |
| C — one plan: 2 `UNION ALL`s | 3 ms to build | 39 ms (1.5×) |

- **(a) passes decisively.** The read set closes in 1 ms with no data
  read. Every query below that line is derivable from `relationships`
  plus Iceberg schema metadata. `coherence` was *already* read-set
  declarative; it was written in rhai with string building, which hid
  it.
- **(b) passes**, 2.4×. Part of that is a better algorithm the plan form
  made writable: today's two queries per relationship (`count` plus a
  `NOT IN` semi-join) become one pass over a distinct-parent left join.
  Stated because it means the speedup is not all plumbing.
- **Correctness: 14/14 relationships agree** between today's algorithm
  and the port, filled and orphans both. This was checked because the
  `NOT IN`/`LEFT JOIN` rewrite has different NULL semantics if written
  carelessly.
- **(c) passes.** First batch at 13 ms, dropping the stream returns
  immediately, against a 39 ms full run.
- **Output is two relations, not one.** Relationship facts
  (14 rows) and temporal pair facts (16 rows) have different shapes, so
  the nested `temporal: [...]` array flattens into a second relation.
  Same finding as §13.2 — measuring produces long rows, and the nesting
  was an artifact of returning JSON.

**C is slower than B, and that matters.** Forcing the arms into one
`UNION ALL` lost 60% against driving the same plans concurrently. So
"one plan" as a literal target can cost throughput; the ruled line —
engine-scheduled, not one statement — is the right one, and §14 carries
the unexplained half.

### 13.2 `behavior_evidence`'s recall half

Enumerated for each of the four columns the function actually ran on:

| subject | axes | alignments | candidates | phase 1 | probes A→B | ms A→B |
|---|---|---|---|---|---|---|
| `journal_lines.net_amount` | 1 | 2 | 21 | 4 ms | 9→2 | 62→24 (2.6×) |
| `ar_invoices.amount` | 2 | 3 | 42 | 2 ms | 18→4 | 41→21 (1.9×) |
| `account_balances.ending_balance` | 1 | 1 | 7 | 1 ms | 5→1 | 19→4 (4.6×) |
| `inventory_positions.value` | 1 | 2 | 7 | 2 ms | 5→1 | 19→5 (3.6×) |

- **The candidate space is declaration-bounded and small** — 7 to 42
  anchors, every coordinate (event, alignment, measure axis, event axis,
  grain, scope) derived from `imports`, `relationships` and schema in
  1–4 ms with no data read. The 943 ms/row was never fan-out.
- **The schema is fixed** — thirteen columns, one row per candidate, no
  optional keys. What made it look variable was measuring and judging in
  one function, exactly as `2026-08-17-functions-split.md` §7 argued.
- **The gate-as-column trade goes the right way, and §5 was too
  pessimistic.** I expected to pay for work a gate would have skipped.
  Instead the probe count collapses 4–5× and the time with it, because
  one aggregate answers viability for *every* grain at once where today
  issues one query per grain. Computing more per query beat computing
  less per query many times.
- **The gates were never read-set discovery.** Both the grain probe
  (`:214,332`) and the viability probe (`:404`) read the same tables the
  measurement already reads; they select a `date_trunc` argument and
  prune candidates. That refines §5: what must be declaration-derived is
  the set of tables and columns read, not every parameter. A
  data-derived parameter over an already-committed read set keys fine.

**Consequence.** `behavior_evidence` was the named largest risk in
`2026-08-17-functions-split.md` §10 and it is not one. Its enumeration
is declarative today, its arithmetic was vectorized in 2026-08-06, and
what remains between them is round trips.

The spike leaves out the two-hop alignment family (`:271-285`), so the
candidate counts are lower bounds.

`temporal` and `metric_cube` are still not spiked, deliberately: they
are the same two shapes, and spiking four functions is how a prototype
becomes the implementation.

## 14. Risks

What the spikes did not settle, worst first.

**Scale is the big one.** Every number in this report comes from a 12 MB
workspace where whole runs take 20–60 ms — a regime where fixed costs
dominate and the ratios may not survive. Specifically: spike 1 path B
drove 21 plans at unbounded concurrency, and the wave-of-4 cap in
`session.rs:227-236` exists because 16 concurrent dataset-grain queries
exhausted the process fd budget on the booksql run (2026-08-07), each
parquet scan holding roughly `target_partitions` files open. **The port
must bound concurrency, and it must do it through the engine's own knobs
— `target_partitions`, the memory pool — rather than a hand-counted wave
size.** A hand-rolled governor outside the engine is the same defect in
a new place.

**C being slower than B is unexplained.** Before committing to a shape,
find out whether `UNION ALL` arms serialize, whether the union collapses
output partitioning, or whether it is an artifact of this size. The
answer changes whether a compute door emits one union or N driven
sub-plans.

**The pin costs a catalog round trip per table per statement.** Building
static providers means N `load_table` calls before planning. Thirteen
tables against local SQLite is free; N HTTP calls against the parked
REST catalog is not, and the REST catalog is the stated destination.
Unmeasured.

**Ports that change algorithms need differential testing, not unit
tests.** Spike 1's orphan rewrite agreed 14/14 — on one workspace. The
mitigation is concrete: keep the rhai implementation runnable through
the port and diff its output against the ported one across the corpus
workspaces, rather than asserting expected values written by the same
hand that wrote the port.

**Materialization write contention is unmeasured.** Warm-at-landing
gives one data commit plus one `measurements` commit per landing, which
is fine alone. Concurrent agents against concurrent landings means
optimistic-concurrency retries on a single-writer table, and the S3
precedent this borrows from (2026-08-11) assumes a single writer.

**The four extension points do not cover detectors.** §11 measures a
per-row judge at 0.20 µs, but `band_breach` is not row-shaped — it reads
`context.slots[].body.metrics[].points[]`, which is the 164 µs envelope
case. That is affordable (43 detector rows in the workspace) but the
taxonomy in `2026-08-17-functions-split.md` §6 needs the distinction:
row judges take scalars, slot judges take the collapse and stay as they
are.

**Unbounded growth of `measurements` is now a feature with a cost.**
Keeping the drift series means the relation only grows, so compaction
and a retention policy are owed — as a decision about how much past a
workspace keeps, not as a cache eviction.

## 15. Open

- **Pin-keyed materialization is proposed, not ruled** (§7), as are
  pre-warm at the landing (§8) and the refusal of a new grammar word
  (§9).
- **`temporal`** remains classed on shape alone; its dtype branch is
  schema, but whether the span/gap/granularity phases collapse into one
  pass is unread.
- **The no-lake path** (`session.rs:436`) still has no answer for where
  a recipe property lives, from 2026-08-16 §6.
- **Whether the straddle actually occurs** was not demonstrated — spike
  3 proves the pin holds, and the exposure is read from
  `iceberg-datafusion` source rather than reproduced.
