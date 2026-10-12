# Bands — expectation ranges from a forecast service

`metric_bands` walks each grounded metric's recent months and
computes, for every walked point, the range that month should have
landed in given everything before it. It runs at dataset grain
(`SELECT metric_bands() FROM fin`) and reports per point the band
quantiles (p05–p95) and the PIT — the quantile at which the actual
landed in the model's own distribution, 0..1 and ordinal by
construction. The `band_breach` detector adjudicates PITs; the
measurement only reports.

## Why a forecast service

With 1–2 years of monthly history, a curve fitted to the series
cannot produce bands that mean what their nominal coverage claims. A
pretrained forecast model conditions on the series as it stands, and
the PIT construction is its own check: if the bands are calibrated,
PITs are uniform; systematic drift shows up as mass at the edges,
which is what the detector scores. The service measured its coverage
one period ahead at 0.78 to 0.84 for a nominal 0.80 on monthly and
weekly data, held-out sets included.

## Mechanism

The `metric_band_walk` door owns the walk's protocol on this side —
the monthly read at the grounding's verb, the extract's shape, the
`withheld` and `partial` rules. It sends every metric's monthly
series as one table — the metric as `member`, the month as `period`,
one `value` column — with the specification `{"targets": ["value"],
"origins": 6, "horizon": 1, "levels": [0.05, 0.1, 0.5, 0.9, 0.95]}`.
The service takes each of the last six months as an origin, forecasts
the month after it from the history up to the origin, and answers one
row per metric, walked month and level, with the actual and the PIT.
The bands are the model's own; the service reads no record. A metric
with fewer months than a full walk walks the origins it has; one with
fewer than seven is inapplicable before any call; one the service
cannot walk at all is inapplicable in the service's own words. The
`band_breach` detector (`crates/scripts/functions/band_breach.sql`)
sees only the measurement's slots: displacement is `|2·pit − 1|` —
0.0 at the median, 0.8 at a nominal-80 edge — and the score is the
worst displacement across the latest point of every monitored metric,
raised to the number of metrics scored: the worst of k calibrated
displacements sits under an edge with probability the edge to the k,
so the power keeps the edges' base rate at any metric count. The
score is banded green/yellow/orange with the witness threshold as the
red line (default 0.98).

## The record

The PITs of every walk land with the measurement, one per metric and
month, and `band_points()` serves them as rows: how good past
forecasts were is read from the record, never from the service. The
service's `width` field — a factor on every quantile's distance from
the median, for a dataset whose record says the intervals run narrow
or wide — is not sent: on records of one to two years it does not
pay.

## Serving the model

The server carries no model. The walk's call goes to the service the
forecast point names, over HTTP as one Arrow IPC stream each way —
column names and types with the rows, the specification and the
answer in the schema's metadata — at `GLOSSQL_FORECAST_URL`
([install](../start/install.md)). Without it the walk refuses by
name. Every answer names the service and its version, and the
measurement records it as `served_by`.

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
  repeats has no grid, and a tight corridor on it is valid.
- The walk covers the six most recent months, and a walked point
  conditions on at least six known values before it — a metric with
  too short a history is served inapplicable with the reason, and one
  shorter than a full walk walks the origins it has.
- The record is the dataset's, every metric's PITs together. A record
  older than a re-grounding still counts; the drift it carries is what
  the detector reads.
- A month the extract stops inside is partial, and its short sum
  against a corridor fitted on whole months reflects the calendar, not
  a move. Where the extract lands finer than monthly — some month
  holds more than one distinct day — the newest point is served
  `partial` while the extract's horizon falls before the month's last
  day: it keeps its bands and its actual so far, withholds the PIT,
  and the detector scores the month before it. The walk reads that
  shape from the extract, not from the column's judged cadence. A
  monthly-dated series is whole at its one row.
- A metric whose series the engine refuses at execution — a
  timestamp the month bucketing cannot hold, a date past 2262 — is
  served inapplicable with the engine's reason; the other metrics
  walk.
- A NULL date is no period: the rows it dates bucket nowhere.
