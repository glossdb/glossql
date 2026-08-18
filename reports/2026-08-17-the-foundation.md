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
| **2** the async pre-pass; groundings as pre-resolved plans | 1, 2, 14, 15 | goldens green; the expansion path stops blocking; the 16 MB stack workaround goes with it |
| **3** declarations and records onto the lake; absorb `Lake` | 6, 7, 9, 10 | goldens green or argued |
| **4** compute doors under `execute`; cache removed; `measurements` keyed by the pin | 5, 8 | SPEC + corpus diff lands here |
| **4½** `glossary` crosses; sqlx drops | 9 | goldens green; the strike ruling (§8) gates it |
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

## 7a. What stage 0 found: composite endpoints are declared and then ignored

The goldens made an asymmetry visible that nothing was holding in place.

- The **detector** handles composites. `relationships.rhai:192,218` calls
  `pair_keys` (`crates/scripts/src/lib.rs:583`), which fnv1a-hashes two
  columns' cells into one key per row — "the composite rescue's pair
  domain" — and computes its overlap statistics over that.
- The two functions that **consume** declared relationships skip them.
  `behavior_evidence.rhai:136` and `coherence.rhai:35` both drop any
  endpoint containing `(`.

So the system can find a composite edge, an agent can declare one — the
tuple is the key, ruled 2026-08-05, fixture 14 — and then the two
measurements that read declared relationships pretend it is not there.

Neither skip is a ruling. `behavior_evidence` says "for now";
`coherence`'s was added on 2026-08-13 to stop a tuple from being quoted
as one impossible column name, which is a crash guard, not a decision.

**On booksql this is total.** Fixture 14 records that every surviving
edge there is composite, so both functions are blind to the entire
declared graph. That is why booksql's golden holds 70 computed values
against rel-f1's 224, and it is exactly the kind of thing a golden set
exists to surface.

**It closes during stage 5, not before.** The consumers do not need the
hash: a composite join is `ON a.c1 = b.c1 AND a.c2 = b.c2`, plain SQL,
and the port is already rewriting both functions to build joins rather
than template strings. Fixing it in rhai first would change the goldens
that were just captured, and for no lasting code. The rule this follows:
the golden diff at stage 5 is *expected and argued*, not discovered.

## 7b. Stage 3 landed (2026-08-17), and the roadmap reordered

The original stage 3 ended "`glossary` last; drop sqlx" — impossible
before stage 4, because a glossary strike executes raw SQL against
sqlite and every gloss write feeds the cache sweeps stage 4 deletes.
The order above is the correction: sqlite now holds exactly `glossary`
and `cache`, and both leave together after the cache machinery is gone.

What landed, and the decisions it carries:

- **One namespace, `glossql`; the dataset is a key column.** The
  2026-08-16 §5 `<dataset>_meta` pairing was built and reversed the same
  day: a workspace holds many datasets, which scopes rows by a key, not
  by a namespace layout — the physical per-dataset split is the format's
  own identity partition, declared per relation. The pairing's whole
  justification is per-dataset REST grants; access rights are held open,
  and if that ruling wants namespace grants, the pairing returns by
  re-bootstrap.
- **`sources`, `aspects`, `functions`, `witnesses` are lake rows**,
  latest-per-name by `(seq, pos)`. Every cross-relation SQL join in the
  store decomposed into one typed scan plus a rule — the shape stage 1
  started, finished.
- **`datasets` are the namespaces, `recipes` are table properties,
  `imports` are the snapshots.** The landing commits through
  iceberg-rust's `fast_append` with its three source-side facts as
  snapshot properties — DataFusion's INSERT cannot carry them. Two
  consequences, taken with eyes open: a dataset's settings are
  set-at-create (as ruled), and a re-land starts the import record over,
  because the substrate has no overwrite — replacing the table replaces
  its record. The old "history survives re-land" behavior was the sqlite
  shadow's artifact.
- **`Lake` absorbed:** `data_version` and the version-keyed read cache
  deleted; the read context rebuilds per read from the catalog. Sessions
  take the lake from the store — the lakeless session mode and
  `with_lake` are gone, and a test fixture registers tables through the
  same landing path a recipe takes.
- **The fin fixture migrated in place**: its authored vocabulary rides
  `goldens/fin/setup.glossql`, idempotently per capture. Its baseline
  was re-captured — the migration's re-declares sweep cached rows once —
  and the other three corpora reproduce byte-identically on the new
  stack.

## 7c. Stage 4 landed (2026-08-17)

The cache is gone whole — relation, invalidation edges, sweeps, detector
freshness, `forward_delete`'s re-aiming machinery, `DELETE FROM cache` —
and what replaced it is the pin.

- **The pin is per statement.** The pre-pass resolves every table of the
  bound dataset once (`IcebergStaticTableProvider`), so two scans can no
  longer straddle a landing, and the same resolution set — data tables,
  the crossed declaration relations, and the glossary at its sqlite
  write head until 4½ — is the measurement key. The glossary-head
  component is the bridge that keeps gloss writes visible to the key
  before the relation crosses.
- **`measurements` holds what extraction lands**: `(dataset, function,
  subject, aspect, pin_digest, pin, value, computed_at)`, append-only,
  partitioned by dataset. Extraction is the compute act — a hit at the
  pin serves, a miss computes, validates and lands. Reads never write:
  `GLOSSARY()` serves measurement slots at the read's pin and discloses
  misses as `unassessed`; detector verdicts compute at every read and
  are never stored; `whatif.` and `misfit.` replay per read.
- **An abstention naming `missing_aspects` never lands** — found by the
  suite an hour in: landed, it hit forever at its own pin, and the
  producer's later run could not heal it. It is an answer about the
  context, exactly as ruled on 2026-08-04; the next extraction
  recomputes.
- **A declaration whose current row already says this writes nothing.**
  Found by the golden gate: every idempotent re-declare appended a row,
  moved that relation's snapshot, and staled every key in the
  workspace. Statement-identity-is-content now holds for every
  declaration write, which is what makes the pin usable at all.
- **No query runs during plan time** (ruled 2026-08-17). Every compute
  door — `glossary()`, `attest()`, `metric_series()`, `whatif.`,
  `misfit.`, the store relations — evaluates in the async pre-pass,
  keyed by the factor's own rendering; the planner is a lookup. The
  ratchet came down: `block_in_place` 6→5, `block_on` 4→3; what remains
  is the `db.query` bridge (stage 5) and the sync ADBC driver.
- **`ACCEPTS` lost its relation entries** — the invalidation-edge
  meaning died with the cache, so the shipped declarations dropped
  `(glossary, imports, relationships)` from their lists; a script reads
  those as tables, which needs no naming. The SPEC diff of 2026-08-16
  §7 is applied; `metric_series` serves the cube at the read's pin
  rather than computing at page load, which revises that diff's one
  line — flagged for review.
- **Goldens:** rel-f1 and booksql reproduce byte-identically — their
  baselines were always the cold-run pattern, dependency-ordered
  abstentions included, and the pin reproduces it exactly. fin's
  `_values` shed the rows that no longer store (verdicts, the whatif
  read) and was re-accepted; two consecutive captures are
  byte-identical. One observation recorded for stages 5–6: the capture
  calls functions alphabetically, so cold corpora golden the
  `dimension_relevance`/`outliers` abstentions where a warmed workspace
  goldens values — pre-existing, and the pre-warm orders by dependency
  when it lands.

## 7d. Stage 4½ landed (2026-08-17): one store, and sqlx leaves

The glossary crossed — the last relation — and with it the sqlite pool,
the schema, the `NOT EXISTS` supersession SQL, the LIKE-escaped scope
predicate and the forwarded-SQL injection guards left whole: there is
no raw SQL to guard because nothing executes any. `Store::open(lake)`
takes exactly one argument now, and a workspace's `glossary.sqlite` is
dead weight. The strike refuses by name
(`Error::StrikeParked`, pointing at the 0.11 item and the delete
report), and the judge flow closes contests by concession until then.

What the crossing forced, and what it taught:

- **The ReadContext is the statement's store snapshot.** The first
  post-crossing capture ran 45× slower than before (383 s against 8.5),
  because every read re-scanned every relation from the lake. The
  architecture's own words — one statement, one resolution — were the
  fix: `Store::read_context` reads each relation once, and the read
  rules (`slots`, `raw_read`, `collapsed_read`, `measurement_in`)
  became pure functions over it, no IO at all.
- **The context reuses on pin identity** — the exact shape 2026-08-16
  §10 sanctioned: in-memory, read-path only, keyed by the snapshots the
  computation reads. Same pin, same contents, no invalidation to write;
  a session forgets its own context after landing a measurement (the
  one write the pin cannot see), and another channel's landing staying
  invisible until the pin moves is the ruled harmless-duplicate case.
- **Measurement reads push the digest into the format's scan**
  (`scan_where`, `Reference::equal_to` → `TableScan::with_filter`), so
  the drift record's history is never decoded to serve today. Warm
  capture after all three: **5.3 s** — faster than the sqlite-era 8.5.
- **The fixture migrated by a scratch tool, once**: 119 glossary rows
  read from `glossary.sqlite` in id order and landed as one commit, so
  `_pos` carries the original history order. The tool was never
  committed — it preserved a fixture, it is not a migration feature.

Goldens: booksql, rel-f1 and rel-event byte-identical through the whole
crossing; fin moved by six eighth-digit rounding flips from the one
recompute at the new pin and was re-accepted.

## 7e. Stage 5 revised (ruled 2026-08-17): a measurement is a query

The fused-native plan (eleven rhai orchestrations become eleven Rust
functions) was presented and replaced by the project lead's own
question — how would agents extend this? The answer is the final state:

- **A measurement's body is SQL.** The declaration stays
  `DECLARE FUNCTION … RETURNS aspect AS $$…$$`; the body plans through
  the same pre-pass, pin and read-only guard as every statement, the
  result validates against the RETURNS aspect's schema and lands at the
  pin. The engine is the runtime; there is no function runtime for
  measurements. Role by shape decides the body language with no marker:
  RETURNS present → SQL, absent → a judge, rhai, pure over its input.
- **The four shapes stand as authored/shipped**: searches and
  statistics are shipped Rust behind the engine's points
  (`hierarchy_candidates('t')`, `profile(col)`, `mad`, `entropy`);
  reads are the agent's home for complex SQL (a QUERY gloss, nothing
  new); judges are small rhai; a measurement composes them in one
  statement. Heavy walks (`metric_bands`) stay native with a thin true
  SQL body. Agents are bounded to non-heavy functions by design —
  accepted.
- **Values land, relations replay.** The landed measurement stays one
  JSON body — pin-keyed, schema-validated, the drift record — never a
  persisted frame. Chaining is inline SQL at compute time or a read
  over the `measurements` table with the JSON functions; a result that
  wants to be a table is a read, replayed at the pin. The result-shape
  rule is fixed and dumb: one row × one column → the value; one row →
  an object of its columns; many rows → an array of row objects; NULL
  keys are omitted (checked: no golden body carries a null-valued key,
  so the rule is byte-compatible with the record).
- **Abstentions shrink**: `missing_aspects` was composition through
  stored intermediates; inline composition removes the absent input.
  `applicable: false` stays a durable finding, expressible in SQL.
- **Table functions take names, not tables** — DataFusion has no
  polymorphic `TABLE t` argument (`datafusion-sql/src/relation/mod.rs:290`);
  a search resolves its subject argument through the statement's pin
  inside the pre-pass, the same door discipline as `read.<aspect>()`.
- **Migration**: same one-function-per-commit cadence against the same
  goldens. Native recall pieces land first with their own tests, then
  each declaration's body rewrites rhai→SQL gated on its golden; a
  shipped rhai measurement runs on the legacy door path until its body
  crosses, and the door dies with the last one. The composite fix rides
  `coherence` and `behavior_evidence` as the two argued diffs. SPEC's
  "functions are scripts with JSON contracts" line is proposed as a
  diff after the first body survives its golden — corpus first.

## 7f. Stage 5 landed (2026-08-18): the door is dead, the engine is the runtime

Thirteen shipped measurements crossed, one commit each against its
golden; the door died with the last one. What stands:

- **A measurement is a query.** Twelve declarations carry SQL bodies;
  `metric_bands`' walk and the searches are Rust doors the bodies read
  as relations — `subject_column`, `derivation_candidates`,
  `hierarchy_candidates`, `relationship_candidates`,
  `relationship_checks`, `grounding_collisions`, `metric_band_walk`,
  `metric_cube_slices`, `behavior_anchors` — computed in the pre-pass
  over the statement's pins like every compute door. The judgment
  constants live in the bodies where anyone reads them; the doors
  optimize recall. `profile`, `mad` and `entropy` register as
  aggregates through the runtime seam, so a measurement body and an
  agent's own SQL name the same functions.
- **The script surface is judges.** `SqlDoor`, `CtxDoor`, the
  block-in-place bridge, the waves-of-4 governor, `db.query`, and the
  query kernels (Table/Col/KeyVec) are gone; a script receives subject
  and context and computes. The model kernels ride the runtime trait
  (`band_point`, `reconcile` joined `band_grid`/`misfit_scores`).
  Ratchet: block_in_place 5→3, block_on 3→1, spawn 4→3 — every
  remainder named (the sync ADBC driver; serving fire-and-forget).
- **The composite fix (§7a) landed in both consumers**: coherence joins
  every leg of a tuple endpoint (booksql's golden now holds the
  measurement the corpus was built for — orphan rates 0.997/0.99999/
  0.739 against 810k tuples), and behavior_evidence enumerates tuple
  edges (booksql unchanged there, honestly: its dimension tables carry
  no time axis).
- **Abstentions shrank as ruled**: `missing_aspects` went with the
  stored intermediates (outliers and dimension_relevance compose their
  profile inline and land at first ask — 213 cold-corpus subjects
  flipped from abstention to value); `applicable: false` stays a
  durable finding.
- **The argued diffs, complete**: the abstention→value flips; rel-f1's
  one gap-sample tie order (deterministic now); rel-event's
  detect_relationships computing where the rhai interpreter's 50M-op
  backstop refused (1876 candidates, declared-truth edges on top), and
  its nine >64-terms kernel refusals re-rendered; cube rows as records
  (a tuple is a script-ism arrow cannot carry) with metric_series
  reading the record form; fin re-baselined whole on the rebuilt
  fixture.
- **The fixture rebuild** (its own commit) surfaced: the sqlite-era
  sources/recipes never crossed; replay order is supersession order;
  a workspace does not relocate (absolute paths in iceberg metadata);
  re-landing shifts float sums in the last digits, so the golden pairs
  with the standing fixture; and outliers' cast needed the display
  bridge on date columns.

Still owed from the stage: the SPEC diff ("functions are scripts with
JSON contracts" → measurement bodies are SQL, judges are scripts) is
proposed to the project lead, not applied; stage 6 (pre-warm at the
landing) and stage 7 (scale) follow.

## 8. Still open

- ~~**A pinned table shadows a same-named CTE.**~~ Closed 2026-08-18.
  The planner seam (`RelationPlanner`) sees every table factor before
  DataFusion's own CTE lookup (datafusion-sql 54.1,
  `src/relation/mod.rs:190`), and `RelationPlannerContext` exposes no
  CTE scope to ask (datafusion-expr 54.1, `src/planner.rs:400`) — so
  the pin and batch arms silently inverted SQL's precedence. Cure: the
  pre-pass collects the query's CTE names (`ctes_in`, per body scope)
  and the seam declines those names, letting the engine's CTE lookup
  win — no error, the same silent shadowing SQL itself does, just in
  the ruled direction. Shipped read names stay reserved over both
  (2026-08-14). The shipped bodies keep their `mc_`/`be_` prefixes
  (changing a body supersedes a declaration, which existing workspaces
  would not receive). Upstream, `RelationPlannerContext` growing a CTE
  probe would make the skip unnecessary — worth an issue.
- **`SELECT _pos FROM …` in user SQL does not work** — metadata columns
  are readable through iceberg-rust's scan only. Expected not to matter;
  find out rather than design for it.
- **The strike and batch commits are one parked item: iceberg-rust
  0.11** (ruled 2026-08-17 — "none of the 3 mentioned [strike] flows is
  mission critical for the immediate future of this prototype"). The
  full grounding is `reports/2026-08-17-delete-in-iceberg-v3.md`: the
  checkout reads deletes but cannot commit one, and upstream tracks the
  gap (#2580 DV writer + RowDelta, #2792 DV read, #2879 `_row_id`).
  Until 0.11: `glossary` crosses anyway and `DELETE FROM glossary`
  refuses loudly, naming this item — a contest closes by concession,
  and a glossed aspect's re-declaration waits or rebuilds the
  workspace. Check upstream main every few days; contribute when this
  architecture is worth defending.
