# glossql — workspace rules

The context language (SPEC.md) and its server. Current phase: **PoC server
build-out** (started 2026-08-03; the language was agreed by the project lead
after the same-day simplification pivot — the 2026-07 draft lives in git
history, and the stack and storage decisions are recorded in `reports/`).
Milestone 1 is the statement spine; corpus fixture 11 is the PoC acceptance
test. Grammar changes still follow the corpus-first process below.

## The one-document rule

**SPEC.md is the only normative prose.** No satellite design docs, no
assumption files, no per-topic notes. Open questions live in SPEC.md §9 and
get folded into the body when decided, not appended as history.

Four non-prose artifacts are first-class — fixtures and machinery, not
documents:

- `grammar.ebnf` — the machine-readable grammar; the source of truth for syntax.
- `crates/parser/tests/corpus/` — transcriptions of **real** artifacts:
  the predecessor system's, and — from fixture 14 on — our own test runs
  (` ```glossql ` must parse; ` ```glossql-gap ` documents a gap and must
  fail). Fixtures 11–12 model the system's operational flows as statement
  sequences; fixture 14 records the composite ruling (2026-08-05: a
  composite endpoint is a tuple, the view cure retired).
- `crates/` — the Rust PoC server, a Cargo workspace at the repo root.
  Directories unprefixed, package names `glossql-*`; datafusion moves in
  lockstep with iceberg-datafusion, sqlx with iceberg-catalog-sql (see the
  workspace `Cargo.toml` comment and the M2/M3 reports). Built: `parser`
  (GlossqlParser wrapping DataFusion's DFParser; the corpus is its
  acceptance suite) · `glossary` (sqlx store, supersession, admission,
  collapse states, ACCEPTS-invalidation, glossary-delete verdict
  invalidation — 2026-08-05, aspect grain — `ON DATASET|TABLE|COLUMN|
  RELATIONSHIP|SOURCE` gates glosses and RETURNS and bounds `unassessed`
  disclosure, ruled 2026-08-05; SOURCE grain ruled 2026-08-12, its slots
  read and supersede workspace-wide — imports counters) · `session`
  (SessionContext assembly, RelationPlanner reads, statement router with
  the substrate allowlist, recipe materialization, probe routing,
  DROP TABLE lifecycle, detector-at-read; the READ LIBRARY under
  `crates/session/reads/` — a shipped `.sql` read is a bare relation
  planned through the same expansion `read.<aspect>()` uses, so one
  file serves the door, an app frame and a skill example alike; the
  names are reserved and shadow both a table and a CTE, so the set
  stays small (ruled 2026-08-14); the plane — channels keyed
  (actor, dataset), `USE` selects the channel and never rebinds a
  session, bare names resolve through the substrate's default-schema
  config, ruled 2026-08-07) · `catalog` (the workspace Lake:
  iceberg-rust SqlCatalog on SQLite + warehouse dir; datasets are
  namespaces; one shared IcebergCatalogProvider, invalidated only when
  a namespace lands) · `import` (recipe and probe execution over file sources,
  try_to_date/try_to_timestamp, source-row counting; ADBC executor
  planned) · `scripts` (rhai runtime behind FunctionRuntime, zero-copy
  column kernels, the reference function library and its bootstrap
  declarations under `crates/scripts/functions/`; abstentions name absent
  ACCEPTS inputs — `missing_aspects`, ruled 2026-08-04; the kernel-mirror
  test keeps the kernel list in the glossql-metrics skill honest; the band plane, ruled
  2026-08-11 — `tabicl_bands` native kernel over the sibling-linked
  `../tabicl-candle` port, weights digest-verified from the
  workspace's `weights/`, `metric_bands` + `band_breach` in the
  library, rulings in `reports/2026-08-11-tabicl-integration.md`). Typing is authored in recipes (ruled
  2026-08-04) — no derived views, no raw twin, no typing functions.
  RETURNS mirrors ACCEPTS (ruled 2026-08-04): functions reference aspects
  on both sides, the aspect schema is the one validated contract, a
  function without RETURNS is a detector, and witnesses gate actors and
  adjudicate only — function voices ride the RETURNS binding.
  · `apps` (the app door at `/app`, mounted by serverd — server-rendered
  data apps: an app is a workspace directory (`app.toml`, tera pages,
  `frames/*.sql`, `specs/*.vl.json`), frames stream Arrow IPC through the
  one-query path with URL params bound as plan placeholders, the browser
  holds each frame once (htmx + vega-lite + arrow-js, vendored, the only
  JS), the URL is the only state, drill is navigation; authors write
  declarative artifacts, never code — stack ruled 2026-08-07; the door
  takes exactly ONE write, the docket's ruling form (ruled 2026-08-15,
  because a human who steps away had no way back into the record: the
  MCP round can only ask while they watch, and an agent may never speak
  for them) — composed and written by `glossql-session`'s `rulings`,
  shared with the MCP round, gated on the question still deriving; gl-rows
  renders row surfaces through author templates, display logic computed
  in frame SQL — 2026-08-10. One app ships in the binary, the docket
  (`crates/apps/builtin/docket/`, ruled 2026-08-15, replacing the
  separate model and metrics apps): what stands open for a human to
  judge, what has been settled, what waits on an act, with the metric
  surfaces and the record behind it. A workspace
  `apps/<name>/` shadows the built-in — forking is copying the
  directory out — and an app.toml without a `dataset` pin binds to the
  workspace's sole dataset at request time; every built-in frame
  parses under the test suite)
  · `serverd` (the doors, M5: one axum listener — the MCP shim at `/mcp`,
  rmcp streamable HTTP, stateless per the 2026-07-28 revision, one
  `glossql` tool; the Arrow IPC query door at `/query`. Reads stream end
  to end via `Session::query_stream` — one batch in memory, the MCP row
  cap (`--row-cap`) terminates the stream early so it bounds engine work.
  Sessions live in the plane as channels keyed (actor, dataset); actor
  rides the connection via initialize clientInfo with a boot-flag
  fallback. The door tells,
  skills teach — agent knowledge is statically written into
  `.claude/skills/` — TWO skills since 2026-08-15, down from nine:
  `glossql` (the language, the shipped reads, the outcome shape, the
  substrate's sharp edges) and `glossql-metrics` (the work — land what
  the topic needs, judge the structure, gloss the vocabulary, ground
  the cohort, validate, close with the question round). The seven
  flow-shaped skills went with the staged arc they encoded: order now
  derives from the `workspace_next` read, so an agent asks the record
  what this workspace affords instead of following a manual. Both are
  under the standing invariant (`crates/serverd/tests/skills.rs`) —
  every fenced example parses, and every read example plans against a
  bootstrapped workspace, so a skill cannot teach a column that does
  not exist. The judge pattern — measurements optimize recall, the
  agent judge removes false positives — is taught in the core skill.
  A fresh
  workspace receives the shipped system at boot — embedded bootstrap:
  reference scripts + the measurement library's declarations, vertical
  excluded; the declaration relations (functions, aspects, witnesses,
  sources, relationships) read as plain tables. Flight SQL cut from
  M5: a future door, pyarrow reads the HTTP stream).
- `reports/` — pivot records, review verdicts, and evaluation records;
  `reports/notes/` holds draft flow notes (the old `feedback/` folder,
  merged 2026-08-14).
- `docs/` — coverage inventories (built / not-built per concern; the
  not-built half is the lightweight backlog — `docs/quality/` first).
  Operational only, never normative language prose.

**Standing invariant:** workspace `cargo test` passes — every ```sql block
in SPEC.md parses and every corpus fixture behaves as tagged (the
`glossql-parser` suite), and the store and session suites hold the execution
semantics. A grammar edit that breaks it doesn't land. (The Python harness
retired 2026-08-03 when the parser suite replaced it.)

**Ideation before prose:** no idea enters SPEC.md until it has survived a
corpus test — write competing statement forms for the same real artifact,
check them against grammar and the real table shapes, present the forks to
the project lead. Only the surviving fork becomes a SPEC.md diff, and the
diff should shrink or hold the spec, never grow it by essay. An open §9
question closes only by a transcription verdict, never by argument.

## Grounding

The predecessor production system retired as a reference 2026-08-14: the
corpus fixtures are the empirical record of the vocabulary's origins, and
coverage or semantics questions are settled against this repo's own code
and runs.

## Decided so far — work in progress, not settled

The project lead may reopen any of it; nothing below is sign-off:

- language before implementation · one dataset per workspace (binding in the
  app) · everything-context is JSON against JSON Schemas · the aspect
  trichotomy (`AS MEASUREMENT | FACT | QUERY`) with one uniform `GLOSS`
  statement · supersession key (subject, aspect, actor kind) · actor rides
  the connection, no BY clause · functions are scripts with JSON contracts ·
  witness slot model with detector adjudication (band + score) · judgment in
  detectors and read policy, never in results · authored prose is opaque ·
  `GLOSS` is the write verb, `GLOSSARY()`/`ATTEST()` are the reads.
- Dropped by design (see `reports/2026-08-03-simplification.md`): calibration
  and pooling, serving/curated-context constructs, pack envelopes and
  portability, negative/rejected forms, declarative metric expressions.

## Held open (do not decide in passing)

Persistence backend · engine substrate and its mapping (tech-stack briefing
by the project lead is upcoming) · governance and access rights · actor
transport mechanics · cross-workspace portability.

## Design authority

- The language design has a single owner: the project lead. Every grammar
  change is reviewed by them. Propose as SPEC.md edits with rationale; don't
  let the grammar drift through implementation convenience.
- Sober docs voice: definition before significance, claims sized to named
  mechanisms, no selling.
