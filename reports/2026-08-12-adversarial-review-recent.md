# 2026-08-12 — Adversarial review of the post-2026-08-07 additions

Scope: everything the 2026-08-06 review predates — the channel cut,
the app door and its interaction layer, the `read.*` rename sweep,
`whatif.`, `misfit.`, the `tabicl_bands` kernel and its runtime
changes, and the recent import/glossary edges. Method: three
independent reviewers tracing full paths in source (substrate claims
grounded in the pinned DataFusion 53.1 and iceberg crates, kernel
claims in the sibling `../tabicl-candle`), each finding refuted hard
before reporting. One finding was reproduced live from an isolated
scratchpad binary; the rest are source-traced. CONFIRMED means the
full path is traced and certain; PLAUSIBLE means a live repro is
still owed. Nothing was fixed — findings only.

The 2026-08-06 assessment named the defect shape: **a decision keyed
by less state than it semantically depends on.** It recurs in most of
what follows, now on the freshness/invalidation side.

## Findings, ranked

**1. HIGH · CONFIRMED (reproduced live) — `whatif.` panics when the
ensemble meets a constant feature column.** `band_grid` generates
ensemble members over the pre-filter column count
(`crates/scripts/src/lib.rs:791`), but the kernel asserts the shuffle
length equals the post-unique-filter kept count
(`../tabicl-candle/src/ensemble.rs:184`), and the preprocessor
silently drops constant columns. Legitimate trigger: a scenario whose
`from` month equals the last recorded month — one post month makes
the month-index feature constant (`crates/session/src/whatif.rs:403`
refuses only an empty post set). Second trigger: every bracketed
world for one override skipped by the roster check leaves that factor
column constant. Reproduced: the assert fires on a candle pool
thread and kills the read instead of refusing. The suite only ever
walks six post months, never one.

**2. HIGH · CONFIRMED — deletion invalidates less than a write does.**
The 2026-08-05 strike ruling was applied to one edge; three are open:
(a) a `DELETE FROM glossary` striking a gloss does not invalidate
functions that `ACCEPTS` the struck aspect, though a *write* to the
same aspect would (`crates/glossary/src/store.rs:780` vs
`:1421-1435`) — a measurement keeps serving a value computed from a
deleted gloss; (b) `DELETE FROM cache` striking a disputed function
voice makes the slot set older, so the cached verdict passes the
freshness check and a `contested` withholding survives the disputed
slot's removal — the exact hazard the glossary-side fix closed,
unapplied to the cache target; (c) `whatif` cache rows sit outside
every sweep (`function = 'whatif'` is not in the functions table), so
striking a superseding scenario gloss serves a read computed under
the struck scenario.

**3. MEDIUM-HIGH · CONFIRMED — `SELECT … INTO` slips through the
streaming read path.** `query_stream_with_params` gates read-only on
the statement variant alone (`crates/session/src/session.rs:773-778`)
and never calls the `selects_into` guard the execute path carries
(`:849-851`). A top-level `SELECT … INTO t` is a `Query` to the
parser and a `CreateMemoryTable` to the planner (traced through
datafusion-sql 53.1), so the door path creates a session-lifetime
table — the recipe-only invariant defeated — and `collect_partitioned`
materializes the whole source result before the row cap applies: an
unbounded-memory request from `/query`, `/mcp`, or a frame. Bounded:
it cannot clobber existing names, so no shadowing or rank escalation.
The existing invariant test covers only the execute path.

**4. MEDIUM-HIGH · CONFIRMED — the `whatif.` cache is keyed on
scenario writes alone.** Freshness compares against the scenario
aspect's own slots (`crates/session/src/whatif.rs:142-155`) while the
computation replays the collapsed current grounding of every QUERY
concept (`:289-339`). Supersede a concept grounding and the door
serves bands from the old SQL, marked current, indefinitely —
`read.<concept>()` disagrees with `whatif.<scenario>()` from that
moment. The existing test supersedes only the scenario, so the suite
is blind. Found independently by two reviewers.

**5. MEDIUM · CONFIRMED — per-channel `read_cache` is never
cross-invalidated.** Bind, materialize and drop clear only the acting
session's cache (`crates/session/src/session.rs:502, 562, 943`);
channels are distinct sessions. Channel A re-lands a table; channel B
(the app door is the shipped long-lived case) still pins the old
snapshot and reports `state = 'current'` for glosses whose data
moved — the serve-and-mark staleness contract defeated for every
reader that is not the writer. The disclosure universe staleness
(new/dropped tables) rides the same cache.

**6. MEDIUM · CONFIRMED — `metric_bands` never sees new data land.**
It is declared `ACCEPTS (glossary)` only
(`crates/scripts/functions/bootstrap.glossql:187-189`) — unlike
`detect_relationships`, `behavior_evidence` and `detect_derivations`,
which all list `imports` — and its dataset-grain subject carries no
snapshot stamp, so a re-land neither invalidates nor stales it. In
steady state (data lands monthly, nobody glosses) the walk and the
`band_breach` verdicts stand still silently. For the band plane, new
data is the primary event; this is the one edge it lacks.

**7. MEDIUM · CONFIRMED — two witnesses on one aspect cross-wire in
the collapse.** Nothing refuses a second witness per aspect;
`ensure_verdicts` correctly computes one verdict per witness (the
2026-08-06 fix holds), but `collapsed_read` keys verdicts by
(subject, aspect) — last witness in name order wins — while the
threshold comes from the first (`crates/glossary/src/store.rs:
1125-1153`). Witness B's score is compared against witness A's
threshold. Either refuse plural witnesses per aspect or key the
collapse by witness.

**8. MEDIUM · PLAUSIBLE — `whatif.` bypasses the kernel row bound and
its support accounting can misstate.** The 2000-row cap lives only at
the misfit door; `band_grid` receives worlds × post-months rows uncapped
(four overrides over ten years ≈ 2,500 rows into quadratic
attention, each world a full replay query). Beside it: worlds whose
replay changes the month roster are skipped silently yet `basis`
still reports the full grid — the served support claim can be false —
and roster identity is checked by length alone, so equal counts of
different months compare positionally.

**9. LOW-MEDIUM · CONFIRMED, narrow trigger — provider generations
split after a namespace create.** A session pins the shared provider
generation it first mounted (`crates/session/src/session.rs:593-611`);
`DECLARE DATASET` invalidates the shared slot; a later channel's land
registers the table on the new generation's DashMap. The pinned
channel then can't resolve the new table while its own disclosure
grid lists it. Needs a runtime namespace create interleaved between
two channels' first mounts — unlikely at one dataset per workspace,
but the mechanism is certain and undefended.

**10. LOW · PLAUSIBLE — no cycle guard on the `whatif.`/`misfit.`
doors.** The `read.` door guards self-referential groundings with an
expansion stack (`crates/session/src/reads.rs:380-401`); the two
newer doors re-enter the planner with no equivalent, so a grounding
composing `FROM whatif.<same>()` recurses to stack overflow from door
input.

**11. LOW · PLAUSIBLE — the misfit door can serve all-NaN scores.**
Complementary null patterns make a conditioning column all-NaN on a
conditional's training subset; the impute mean is NaN, NaN survives
the unique filter, and every row's density is NaN — served without
refusal, against the door's refuse-or-abstain contract.

**12. LOW · CONFIRMED — import row accounting, two new variants.**
Beside the known join mis-sum (source rows summed across every
scanned provider): a fan-out join makes the `imports` relation serve
a negative `dropped_rows_count` while the statement outcome
saturates to 0 — two doors, two answers; and the counts come from a
second scan after landing, so files appearing between the scans
miscount what was read.

**13. LOW · CONFIRMED — `GLOSSQL_CANDLE_THREADS=0` uncaps the pool.**
Rayon reads 0 as "all logical CPUs"; the one value an operator might
use to mean "minimal" hands the model the whole machine.

## Came back clean

Path traversal on app/page/frame/spec names (`safe_segment` plus leaf
re-checks, asset allowlist) · frame params bound as typed
`ScalarValue` through `ParamValues`, no text splicing anywhere ·
tera context never compiled from request data · frames cannot write
(no execute fallback) · the sole-dataset binding re-resolves per
request and mis-binds nowhere · misfit's cap has no off-by-one and no
collect-before-trim · `reads_only_metadata` correctly keeps the new
two-segment doors on the capped path · the witness-keyed verdict
cache (the 2026-08-06 top fix) holds · the OnceLock weights-load
retry is correct · parallel chain-rule conditionals are bit-identical
to sequential (noise drawn up front, order-preserving collect, exact
f64 loop order) · digest verification is real sha256 on every load ·
aspect grain gates hold on both producer paths · no reachable panics
from door input in the new session files beyond finding 1 · the
dollar-quote guard on forwarded deletes is intact.

One author-side note, low: `gl-rows` substitutes data values into
attributes without scheme filtering, so a frame binding a raw data
column into an `href` can carry a `javascript:` value. The shipped
builtin always constructs links as literal-prefixed relative URLs in
frame SQL; the runtime does not enforce that convention on workspace
apps.

## Assessment

The statement spine, the doors' input handling, and the kernel's
determinism story survived. What did not is the freshness layer the
new reads stand on: findings 2, 4, 5 and 6 are all one sentence —
**the write path's invalidation discipline was not carried to the
delete path, the new caches, or the cross-channel topology.** The
serve-and-mark contract is the product's core promise, and these are
the places it currently lies. Findings 1 and 3 are door-hardening in
the established pattern; the rest are edges with named triggers.
