# Bands — expectation ranges from an in-context model

`metric_bands` walks each grounded metric's recent months and asks,
for every walked point, what range that month should have landed in
given everything before it. It runs at dataset grain
(`SELECT metric_bands() FROM fin`) and reports per point the band
quantiles (p05–p95) and the PIT — the quantile at which the actual
landed, 0..1 and ordinal by construction. The `band_breach` detector
adjudicates PITs; the measurement only reports.

## Why an in-context model

With 1–2 years of monthly history, a curve fitted to the series
cannot produce bands that mean what their nominal coverage claims.
TabICL — the in-context tabular model behind the kernel — conditions
on the walk's own feature recipe per point instead, and the PIT
construction keeps the read honest: if the
bands are calibrated, PITs are uniform; systematic drift shows up as
mass at the edges, which is exactly what the detector scores.

## Mechanism

The `metric_band_walk` door owns the walk protocol — the monthly
verb, the feature recipe, the point-in-time fills (no future value
leaks into a walked point). The model call is one kernel behind the
runtime seam, running the hand-ported
model on candle: Metal by default, CPU fallback, weights
digest-verified at load. The `band_breach` detector
(`crates/scripts/functions/band_breach.sql`) sees only the
measurement's slots: displacement is `|2·pit − 1|` — 0.0 at the
median, 0.8 at a nominal-80 edge — and the score is the worst
displacement across the latest point of every monitored metric,
banded green/yellow/orange with the witness threshold as the red
line (default 0.98).

## Serving the model

The model is the sibling candle port, called through one kernel
behind the runtime seam: weights load lazily from the workspace's
`weights/`, digest-verified; Metal when the machine has it, CPU
fallback; the CPU work runs on a capped pool so the model never
starves the engine. Raw densities never leave the kernel, and the
walk's feature recipe and point-in-time fill match the graded
protocol it was evaluated against before it shipped.

## Limits

- Bands inform; they never verdict. Whether a movement is surprising
  is the actor's judgment, informed by the evidence.
- A series that repeats values — a ratio over a fixed field, a count
  — moves on a grid, and the smallest gap between two of its values is
  its resolution. A corridor narrower than that, with the actual
  inside one step of the median, is the model's noise around a value
  the series takes exactly; the point serves its band and actual and
  withholds the PIT, `withheld` naming why, and the detector reads
  nothing breached. An actual further off than a step keeps its PIT:
  that move is real whatever the corridor's width. A series that never
  repeats has no grid, and a tight corridor on it is earned.
- The walk covers the six most recent months, and a walked point
  conditions on at least five preceding ones — a metric with too
  short a history is served inapplicable with the reason.
