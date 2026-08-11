# TabICL integration: feature map, limits, and exposure (2026-08-11)

The fidelity gate on the candle port (`../dataraum-tabicl`) is closed:
forward parity, wrapper parity (regressor and classifier), and
statistical parity against the evaluation harness (`../tfmeval`, the
evidence archive). This report records the integration analysis and
the rulings taken on it, so the reasoning is available when the
deferred pieces come up.

## Rulings (project lead, 2026-08-11)

- The glossql workspace links the sibling repo (`../dataraum-tabicl`);
  no vendoring. The repo stays the self-contained publishable unit.
- Weights follow the existing policy: digest-verified local cache;
  container images bake weights at build time, never fetch at start.
- TabICL reads enter the language as **declared detector functions**.
  The FunctionRuntime's native column kernels are the precedent;
  declarations, witnesses, and ACCEPTS-invalidation govern them like
  every other function. The statement forms still go through the
  corpus-first process. Refined same day to **Fork A**: the protocol
  (metric selection, series assembly, walk policy) lives in a shipped
  rhai script an installation can read and adapt; only the forward is
  a native kernel (`tabicl_bands`). A fully native body (Fork B)
  waits for data points from real installations.
- **Frame limits**: be careful with very long and very wide frames;
  enforce a maximum where necessary — an actor can shape the frame to
  fit what we support. On input, batch; for very large updates, skip
  the synchronous analysis and/or offer a background job. Start
  narrow, learn from it, and defer explicitly — documented, which is
  this report's deferral section.
- **The serve door is `read.*`** (ruled 2026-08-11): one generic
  serving prefix over every QUERY gloss — `read.revenue()`,
  `read.march_suspects()`, any declared aggregation — because serving
  declared SQL is one operation whatever flavor sits behind it.
  `metric.*` folds in completely, no alias; the metric flavor lives
  in `x-kind` and the skills, not the call syntax. Analyses (the
  misfit read) stay their own operation-named doors — one prefix per
  operation, never dispatch on the aspect's kind. The sweep landed
  same day (planner, tests, corpus 15/16, SPEC §7 note, three
  skills); workspace apps written before the rename must update
  their frame SQL by hand — there is no alias to catch them.

## Feature map

Four reads, each with its evidence, implementation status, and
measured cost. "Recorded" figures are the tfmeval archive; parity
grades are the dataraum-tabicl test suite.

**1. Metric expectation bands** (evidence: E2.1, reproduced).
Calibrated expected ranges and a median for a metric's next points.
Port complete and graded end to end; the pinned single member matches
the recorded 8-member ensemble within one standard error on every
figure. Cost: ~160 ms per fit CPU, ~80 ms Metal at the protocol's
context sizes (≤80 rows). Interactive-class. Ready.

**2. Row anomaly ranking** (evidence: E1.2s3, reproduced). Rows
ranked by joint implausibility against a reference, mixed
numerical/categorical columns through the chain-rule read. Pinned
single member reproduces the recorded 4-member figures on all three
referential-integrity variants (shuffled AUROC 0.9343 vs 0.9338;
distinct/repeated 0.9932 vs 0.991). Cost model: permutations ×
columns × forward(context+batch); measured 1.4 s CPU / 0.8 s Metal
per conditional at a 600-row forward; the full-protocol run
(15–30k-row forwards) was a ~12 min/variant torch-MPS job. Batch-
class. The port is complete; the sklearn defaults (context = entire
reference, permutations = column count) are what is expensive, and
both are API inputs.

**3. What-if / counterfactual values** (evidence: E4, recorded, not
yet reproduced). Point values for a metric under a scenario factor —
a per-row point read. Recorded: in-support median APE 0.31 % (revenue,
factor 1.15), degrading honestly out of support (4.2 % at 1.6, 20.9 %
at 2.0, bands widening with the extrapolation). The port already
computes the quantities (median + bands); only the read is ungraded.

**4. Categorical prediction** (evidence: E2.2 diagnosis, recorded,
not yet reproduced). Rate-vs-mix cause classification through the
classifier wrapper (wrapper graded to 2.4e-6).

The E4 and E2.2 recorded runs completed in seconds to minutes — they
are small-context fits, so their reproductions are ~a day each of the
established pattern (protocol extraction → pinned-vs-recorded run →
committed fixture → Rust test) and run entirely in the sibling repos,
parallel to integration work.

## The ensemble question

Multi-member ensembling was shown unnecessary for every calibration
and ranking read: those are aggregate or ordinal, and member variance
washes out. The E4 reproduction (run 2026-08-11, same day) answered
the remaining case — per-row point error — with a split: on dense
support (7-factor grid, 42 train rows) the pinned member matches the
recorded 8-member run within thousandths of median APE; on sparse
support (3-factor grid, 18 rows) the ensemble buys 2–3× lower point
error and consistently better effect recovery. The one demonstrated
case where ensembling earns its cost is therefore point reads on
sparse support — a decision for when a feature quotes point values
from sparse grids, not implemented preemptively.

Ruled same day: **what-if ships with the ensemble.** Most workspace
metrics stand on fewer than six inputs, so sparse support is the
normal what-if regime, not the edge case. The port grows the recorded
ensemble configuration (power normalization, feature shuffles, member
aggregation), graded against the recorded 8-member E4 run. Calibration
and ranking reads keep the pinned single member.

## The workspace-support answer (E4b, run 2026-08-11)

E4's factor grid does not exist in a workspace, so the support
experiment re-asked E4.1 under workspace shapes (tfmeval
`experiments/e4b_workspace.py`; the full figures in its FINDINGS.md):
train only on the baseline's pre-activation history, the driver
measured from the data as an agent's SQL would (monthly
revenue-weighted unit price), truth the same-seed levered corpus.

- **Pure metric history is disqualified** — flat or trended, 6 or 18
  train months: negative effect recovery (the read gets the *sign* of
  a +15% price move wrong), coverage 0 at nominal 80, narrow bands —
  confidently wrong, the E4.2 pattern. Structural, not statistical:
  history has no driver variation independent of time, and the
  ensemble is immaterial to it (pinned matches to three decimals).
- **A judged dimension carries the read.** The panel shape —
  (product_group × month) cells, cross-sectional price span ~15×,
  scenario read per cell summed to totals — recovers the effect:
  mape 7.6 %, effect recovery 1.07, coverage 80/90 both 100 % at 18
  months of cells. Six months of cells is not enough (recovery 0.11)
  — the dimension does not excuse short history.
- **The ensemble earns its keep where ruled**: on the working panel,
  ensemble 7.6 % vs pinned 10.7 % mape, recovery 1.07 vs 0.88.

The panel result was then re-read under the project lead's challenge
(why a model where arithmetic is exact?) and completed by **E4c
(propagation, same day)**: the one candidate role left for the
conditional read — propagating a revenue move into ar_balance and
cash_balance through collection-lag structure no formula carries —
fails too. Trained on the baseline's own co-movement and fed the
exact scenario revenue path, the model misses the effect entirely
(cash effect recovery −0.11; the no-lag-structure control at least
overshoots in the right direction) for the same measured reason as
the history regimes: the effect is out of support by construction,
and an in-context read over ~15 monthly rows shrinks it away. The
panel's apparent success was the model re-learning a linear identity
the formula computes exactly.

**Verdict, closing the what-if model question (E4/E4b/E4c):** the
counterfactual read is out of what-if. Fidelity was proven (E4, grid
support); value on workspace-shaped data is disproven — monthly
histories cannot support causal claims. What-if is built as:
declared scenario overrides, deterministic recomposition through the
formula graph, the baseline's uncertainty quoted from the
`metric_bands` corridor, downstream propagation quoted only where a
formula or a user-pinned assumption covers it, refused with the
reason otherwise. The TFM's standing roles are the ones proven in
support: the corridor (E2.1, shipped), cause classification (E2.2,
reproduced), row ranking (E1.2s3, ported). The ensemble port keeps
its parity grade and currently has no consuming feature — recorded
plainly, not justified retroactively.

## The fp32 chaos finding and score semantics

On near-deterministic conditionals (amount given amount_inv) the
row-level log density is chaotic in any fp32 implementation: the
spike's quantile gaps sit at the same ~1e-4 order as legitimate fp32
forward jitter (torch fp32 is 1.6e-4 from its own fp64 reference
there; the port is 1.3e-4 from the same reference), so per-row NLL
moves ~0.3 log units between equally valid implementations while
rank-based reads stay put (AUROC moved 8e-4).

Consequence, proposed for the statement forms: detector scores are
ordinal by construction — for bands, the quantile at which the actual
lands in the predicted distribution; for density, the row's NLL rank
within the scored batch, with thresholds always recomputed same-run
from the reference. Raw NLL levels are never persisted or compared
across runs.

## Exposure: one mechanism, two doors

The reads register as native functions in the session's DataFusion
context. Through the one-query path both doors serve them with no new
surface:

- **Agents** (`/mcp`): the declarations read as plain tables in the
  glossary; a skill (extending glossql-metrics for bands) teaches
  when to reach for them; the judge pattern closes the loop — the
  reads optimize recall, the agent judges the flags, verdicts land as
  GLOSS statements through the existing witness adjudication.
- **Humans** (`/app`): bands are a frame — frame SQL calls the
  function, Arrow streams the range table, a vega-lite layer draws
  the band with breaches marked. Density is a queue — the world-model
  app's pattern (ranked rows, dossiers, contest-as-statement).
  What-if, once graded, is URL-parameter-shaped: the factor is a
  param, the frame recomputes, drill is navigation.

First experienced increment: the band read registered in the session,
one monitoring frame showing a KPI inside its expected range, and the
skill teaching the breach-and-judge loop — feature 1 end to end
through both doors, forcing the declaration shapes through
corpus-first immediately.

## Deferred — documented, each with its trigger

- **E4 reproduction** — done 2026-08-11 (dataraum-tabicl `tests/e4.rs`,
  21 fits at 5.7e-5 vs the pinned oracle). Feature 3's evidence gap is
  closed; the ensemble split (see above) leaves one open decision:
  whether sparse-support point features ship with multi-member
  ensembling, constrained support, or documented degraded error.
- **E2.2 reproduction** — done 2026-08-11 (dataraum-tabicl
  `tests/e22.rs`): the pinned classifier lands the recorded ensemble
  accuracies exactly (full 18/24, blind 13/24), the Rust wrapper
  matches the pinned oracle at ~7e-6. Feature 4's evidence gap is
  closed; its product design still waits for what-if (shared
  dimensions-as-drivers vocabulary).
- **Row anomaly ranking (feature 2)** — deferred by ruling
  (2026-08-11). It is a data-*update* story and the system does not
  yet support data updates: on initial seed imports the read would
  have to be skipped entirely, or a millions-of-rows seed adds tens
  of minutes to every test and eval run. It cannot be made cheap by
  sampling — a read whose claim is "this row does not fit" undercuts
  itself by sampling rows away — and "does not fit" is itself
  unspecified (in which sense: distributional, referential,
  temporal?). Triggers, all required before any build: data updates
  exist as a system concept; background jobs exist; the read's
  semantics are specified; test scenarios and eval runs are designed
  so the cost and behavior are understood before it touches an
  import path.
- **Width scaling**: the evaluation's 8-column surfaces were a harness
  artifact, not a model or platform bound; the wrapper has no column
  cap. Unestablished at width: single-member sufficiency (shuffles
  interact with feature grouping) and read accuracy; known at width:
  cost grows (row attention quadratic in columns; the density
  default's permutation count equals the column count → O(C²) fits,
  so a permutation budget is mandatory). Experiment: pinned vs
  ensemble at C ∈ {16, 64, 128} on ground-truthed wide surfaces
  (finance generator joined wide, or RelBench declared-FK truth),
  grading density AUROC, band calibration, and cost curves. Trigger:
  gates any wide-surface feature.
- **Long frames / large updates**: per the frame-limits ruling —
  enforced maximums, input batching, skip-sync or background jobs for
  large updates. The concrete thresholds come from the width/cost
  curves; not designed yet.
- **Density budget defaults** (context size, permutation count):
  subsumed by the feature-2 deferral above; decided as part of that
  read's specification when its triggers are met.
- **Fork B (fully native band function)**: bands shipped as Fork A so
  the protocol stays readable and adaptable per installation.
  Trigger: data points from real installations showing the protocol
  has stabilized.
- **Stocks in the band walk** — done 2026-08-11: an authored
  `"behavior": "stock"` key in the grounding body switches the walk
  to last-per-month; unmarked groundings read as flows. The
  behavior-*measurement* wiring (deriving the marker from
  behavior_evidence instead of authorship) stays open, trigger: real
  workspaces showing authors forget the marker.

## The plan (2026-08-11, project lead approved the sequence)

Chunk-sized, run in order with 2 and 3 as fillers beside 1:

1. **Ensemble port** (sibling, 2–4 days): power normalization,
   feature shuffles, member aggregation; graded against the recorded
   8-member E4 run. Point reads only — calibration and ranking reads
   keep the pinned member.
2. **Stocks in the band walk** (glossql, small): authored stock
   marker, last-per-month path in metric_bands.
3. **E2.2 reproduction** — done 2026-08-11 (see the deferral list
   entry): recorded accuracies reproduced exactly, wrapper at ~7e-6.
   Feature 4 is the diagnosis read (rate-vs-mix cause after a metric
   moved), not what-if; its product design comes after what-if lands
   — they share the dimensions-as-drivers vocabulary.
4. **What-if support experiment** — done 2026-08-11, extended to E4c
   (propagation) the same day under the project lead's challenge.
   Final verdict in the workspace-support section: the counterfactual
   read is out of what-if; the model's standing roles are corridor,
   diagnosis, and row ranking.
5. **What-if build** (glossql, ~2 chunks, needs ruling): scenario as
   declared overrides recomposed through the formula graph, corridor
   bands on the baseline, refusal-with-reason where propagation is
   uncovered; statement forms through corpus-first. No model kernel.
6. **Density on demand** (glossql, ~2 chunks, needs ruling only):
   re-scoped 2026-08-11 — triggered **only on a signal** (band
   breach, red check, user doubt), never per-import; the frame is
   authored, not passed as SQL-in-SQL; statement-form forks (sample
   as plain QUERY aspect + convention vs a first-class kind) go
   through corpus-first. The serving-prefix question rides the same
   round: one generic serve door over every QUERY gloss (any declared
   aggregation becomes a reusable, judged, versioned read — not only
   metrics), with `metric.*` folding in or staying an alias; analyses
   like the misfit read stay their own operation-named doors — one
   prefix per operation, never dispatch on the aspect's kind.
7. **A real execution** (project lead triggers): feeds Fork B and
   the protocol-stability question; needs only a provisioned
   `weights/` beside the workspace.

Parked with triggers, unchanged: width scaling, frame-limits
machinery, Fork B, feature 4's product design.

## Records

- Port and gate: `../dataraum-tabicl` (README carries the three-stage
  gate with all parity tables).
- Evidence archive: `../tfmeval/output/results/`.
- The candle CPU argsort offset bug found during stage 3 is filed
  upstream: https://github.com/huggingface/candle/issues/3874 (fixed
  locally by materializing before sorting, with a regression test).
