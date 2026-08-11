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
- TabICL reads enter the language as **declared detector functions**
  with native bodies — not rhai scripts. The FunctionRuntime's native
  column kernels are the precedent; declarations, witnesses, and
  ACCEPTS-invalidation govern them like every other function. The
  statement forms still go through the corpus-first process.
- **Frame limits**: be careful with very long and very wide frames;
  enforce a maximum where necessary — an actor can shape the frame to
  fit what we support. On input, batch; for very large updates, skip
  the synchronous analysis and/or offer a background job. Start
  narrow, learn from it, and defer explicitly — documented, which is
  this report's deferral section.

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

Multi-member ensembling was shown unnecessary for every *graded* read:
calibration summaries and rank-based reads are aggregate or ordinal,
and member variance washes out. The one place it plausibly matters is
per-row point error — exactly feature 3 — so the E4 reproduction
doubles as the deciding experiment. Until then the port stays one
member, norm "none", fp32.

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

- **E4 reproduction** (~1 day): gates feature 3 and answers the point-
  read ensemble question. Trigger: before any feature quotes point
  values.
- **E2.2 reproduction** (~1 day): gates feature 4.
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
  product decision informed by the width experiment.

## Records

- Port and gate: `../dataraum-tabicl` (README carries the three-stage
  gate with all parity tables).
- Evidence archive: `../tfmeval/output/results/`.
- The candle CPU argsort offset bug found during stage 3 is filed
  upstream: https://github.com/huggingface/candle/issues/3874 (fixed
  locally by materializing before sorting, with a regression test).
