-- Metric expectation bands: for every grounded metric, walk the recent
-- months and ask the forecast service what range each month should
-- have landed in, given everything before it. Runs at dataset grain
-- (`SELECT metric_bands() FROM fin`). The walk protocol — the monthly
-- verb, the extract's shape, the `withheld` and `partial` rules — is
-- the door's (`metric_band_walk`); the model call is one table call
-- behind the runtime seam: the series go to the service as they stand,
-- and it builds its own rows. Each walked point records its bands and
-- its PIT — the quantile at which the actual landed in the model's own
-- distribution, 0..1 and ordinal by construction — or, where the
-- corridor is narrower than the series' own resolution, no PIT and
-- `withheld` saying why. The newest month is `partial` while the
-- extract's horizon falls inside it: it serves its bands and its actual
-- so far, withholds the PIT, and the detector scores the month before
-- it. The band_breach detector adjudicates PITs; this measurement only
-- reports.
--
-- `axis` names the date column the walk anchored on and `axis_judged`
-- says whether a temporal_profile verdict named it. The cadence is
-- month either way: the axis is a choice of column and never of grain.
-- `aggregation` names what one month's value is: `sum` for a flow,
-- `latest-sum` for a stock, `ratio-of-sums` for a ratio. `served_by`
-- names the service that answered and its version.
WITH m AS (
  SELECT seq, metric, applicable, reason, grain, aggregation, trained_on,
         axis, axis_judged, served_by,
         CASE WHEN applicable THEN coalesce(array_agg(named_struct(
           'period', period, 'actual', actual,
           'p05', p05, 'p10', p10, 'p50', p50, 'p90', p90, 'p95', p95,
           'pit', pit, 'withheld', withheld, 'partial', partial
         ) ORDER BY point_seq) FILTER (WHERE period IS NOT NULL), []) END AS points
  FROM metric_band_walk($subject)
  GROUP BY seq, metric, applicable, reason, grain, aggregation, trained_on,
           axis, axis_judged, served_by
)
SELECT
  count(metric) > 0 AS applicable,
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'grain', grain, 'aggregation', aggregation, 'trained_on', trained_on,
    'axis', axis, 'axis_judged', axis_judged, 'served_by', served_by,
    'points', points
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics
FROM m
