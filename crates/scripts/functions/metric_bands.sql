-- Metric expectation bands: for every grounded metric, walk the recent
-- months and ask the TabICL forward what range each month should have
-- landed in, given everything before it. Runs at dataset grain
-- (`SELECT metric_bands() FROM fin`). The walk protocol — the monthly
-- verb, the feature recipe, the point-in-time fills — is the door's
-- (`metric_band_walk`), graded against the published protocol; the
-- model call is one kernel behind the runtime seam. Each walked point
-- records its bands and its PIT — the quantile at which the actual
-- landed, 0..1 and ordinal by construction. The band_breach detector
-- adjudicates PITs; this measurement only reports.
--
-- `axis` names the date column the walk anchored on and `axis_judged`
-- says whether a temporal_profile verdict named it. The cadence is
-- month either way: the feature recipe is month-shaped, so the axis is
-- a choice of column and never of grain.
WITH m AS (
  SELECT seq, metric, applicable, reason, grain, aggregation, trained_on,
         axis, axis_judged,
         CASE WHEN applicable THEN coalesce(array_agg(named_struct(
           'period', period, 'actual', actual,
           'p05', p05, 'p10', p10, 'p50', p50, 'p90', p90, 'p95', p95,
           'pit', pit
         ) ORDER BY point_seq) FILTER (WHERE period IS NOT NULL), []) END AS points
  FROM metric_band_walk($subject)
  GROUP BY seq, metric, applicable, reason, grain, aggregation, trained_on,
           axis, axis_judged
)
SELECT
  count(metric) > 0 AS applicable,
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'grain', grain, 'aggregation', aggregation, 'trained_on', trained_on,
    'axis', axis, 'axis_judged', axis_judged,
    'points', points
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics
FROM m
