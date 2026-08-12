# System — what is built, what is not

Coverage inventory for the server across concerns. The language itself
is SPEC.md's business; onboarding has `../onboarding/`, quality has
`../quality/`. Evidence: `../../reports/`.

## Built

- **Statement spine and parser** — the corpus is the acceptance suite;
  fixtures 11–20 all transcribe except the app-authoring gap (below).
- **Store** — supersession (subject, aspect, actor kind), admission,
  collapse states, ACCEPTS-invalidation, strike-invalidation of
  detector verdicts, witness-keyed verdict cache, aspect grain
  (`ON DATASET|TABLE|COLUMN|RELATIONSHIP`).
- **Catalog** — iceberg-rust SqlCatalog on SQLite + warehouse;
  datasets as namespaces; one shared provider, invalidated only when
  a namespace lands; snapshot ids stamp gloss and cache writes.
- **Session** — channels keyed (actor, dataset); substrate allowlist;
  recipe materialization with supersede-and-reland; probe routing;
  detector-at-read; the `read.*` serve door over every QUERY gloss
  (`metric.*` folded in, no alias — pre-rename workspace apps must
  update frame SQL by hand); `whatif.<scenario>()` as plan-rewrite
  replay with bracketed band grids; `misfit.<frame>()` ranked reads,
  uncached by design, capped at 2000 rows / 16 usable columns.
- **Import** — file sources with cast accounting; the ADBC executor
  for relational sources (key harvest as judge evidence only);
  source-row counting.
- **Scripts** — rhai runtime, zero-copy kernels, the 13-function
  measurement library declared at boot; `tabicl_bands` native kernel
  over the sibling `../tabicl-candle` port (Metal by default, CPU
  fallback, capped candle pool), `metric_bands` + `band_breach`;
  abstentions name absent inputs.
- **Doors** — `/mcp` (stateless, one tool, row cap bounds engine
  work), `/query` (streaming Arrow IPC), `/app` (directory apps:
  tera pages, frame SQL, vega-lite specs; URL params bind as typed
  plan placeholders; gl-rows row surfaces; the model app ships in the
  binary, workspace apps shadow it).
- **Skills** — the eight in `.claude/skills/`, the one teaching layer.
- **Bootstrap** — a fresh workspace receives the shipped system;
  declaration relations read as plain tables.
- **Hardening** — the 2026-08-06 adversarial review's confirmed
  defects all fixed same day (verdict keying, forwarded-delete
  injection, probe/recipe allowlist, re-land ordering, LIKE escaping,
  WAL + indexes, caps pushed into the engine).

## Deliberately not built (ruled)

- Calibration and pooling, serving/curated-context constructs, pack
  envelopes, negative forms, declarative metric expressions
  (2026-08-03).
- Enriched views, column eligibility, run-versioned read views /
  snapshot heads / property graph, LLM prompt infrastructure,
  validation induction as engine machinery, readiness rollups,
  Benford, graph topology (2026-08-06).
- Persistent view objects — grain-checked joins are the construct
  (2026-08-06); a composite endpoint is a tuple (2026-08-05).
- Sentinel lists — none may exist (2026-08-06).
- Quality-layer exclusions: see `../quality/README.md`.

## Known debts — the backlog

Left open by the 2026-08-06 review, deliberately, none structural:

- No request timeouts, no session eviction (bounded at PoC scale).
- No transactions around multi-write flows — a `GLOSS` is several
  separately fsynced commits (moot if the storage move below lands).
- Collapsed `GLOSSARY()` read amplification (O(W·F + W·S) queries).
- Import buffers the whole landing and re-scans sources to count rows.
- `distinct()` allocates a `String` per cell; `extract` runs sync IO
  on the async executor.

Newer:

- The per-file landing count mis-sums across multi-file recipes.
- SPEC §9's band half (agents sweeping `state != 'current'` and
  respecting bands) has still not actually been tested.
- The fk-shuffled fault class remains open product work: the
  settlement-coherence check proved its power but not its standing
  wiring; whether such checks get written blind is an open question
  (2026-08-11 monitoring evaluation).
- The adversarial review of the post-2026-08-07 additions ran
  2026-08-12 (`../../reports/2026-08-12-adversarial-review-recent.md`);
  all 13 findings fixed same day
  (`../../reports/2026-08-12-review-fixes.md`, 19 regression tests).
  Still open from it: the plural-witness ruling, and the second-scan
  import miscount.

## Planned — flagged, with triggers where ruled

Storage and deployment:

- **Context store moves into Iceberg** (ruled 2026-08-11, unbuilt):
  no SQLite anywhere; `<dataset>_meta` sibling namespaces hold the
  store relations; writes are appends, supersession stays a read
  rule; the cache becomes in-memory only; deployment binds a REST
  catalog through the one-builder seam. Concrete catalog service,
  container, and otel are parked. Until this lands, the cache and
  catalog-metadata handling stays deliberately straightforward
  (project lead, 2026-08-12) — no partial moves.
- **Cloud kernel serving** (trigger: first deployment target): Metal
  is Apple-only, so cloud means candle CUDA or capped-CPU sizing —
  neither measured.
- **Flight SQL door** (future; pyarrow reads the HTTP stream today).
- **Upstream-dependent**: Iceberg row deletes (physical cleanup of
  struck rows), multi-table transactions, `update_namespace`. The
  branch/WAP publication seam (session-as-branch, publish =
  fast-forward, apache/iceberg-rust PR 2709) has been post-PoC since
  2026-08-03 and is not required by the 2026-08-11 storage ruling;
  revive only if audit-gated publication becomes a product need.
- **Import provenance in snapshot properties** (`fast_append` carries
  them; our INSERT path does not expose them yet).

Language and doors:

- **App authoring as statements** (fixture 18, the corpus's first
  gap fixture): per-artifact `DECLARE APP/FRAME/PAGE/SPEC` forms and
  the publish verb's semantics are held for ruling.
- **Inline `$$`-carried function bodies** so a remote agent completes
  the flow through the door alone (corpus-first when picked up).
- **A data-update verb** (future since 2026-08-04); the restatement
  watch and row-anomaly-on-import reads depend on it.
- **Value-at-read leftovers**: the conformed-group structured field
  (fixture 15, fork open); axis additivity (named, not built until a
  drill-shaped consumer).

Model track (each with its ruled trigger):

- **Ensemble port** for what-if point reads on sparse support.
- **Row anomaly ranking at import** — deferred by ruling; triggers:
  data updates exist, background jobs exist, semantics specified,
  eval designed.
- **Width scaling experiment** — gates any wide-surface feature.
- **Fork B** (fully native band function) — trigger: protocol
  stability in real installations.
- Frame-limit machinery (thresholds from the width/cost curves).

Pulled-when-a-target-demands (v0.3 remainder, 2026-08-06 rulings):

- UI drill-down set: drivers, composition layer, drill-axis ordering,
  additivity classifier.
- Metric-target set: period/boundary resolvers, validity scope,
  concept reconciliation, derived formulas, business cycles,
  surrogate mint.
- Named wishes: join-path ambiguity sweep, unit-token extraction,
  Benford (audit target).
- Sign partition + ΔBIC tiebreak in the reconcile kernel (ruled in
  2026-08-06; land when behavior evidence next moves).
