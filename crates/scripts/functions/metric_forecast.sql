-- The forecast as a measurement at an origin: for every applicable
-- metric, the model's quantiles for the periods ahead from the last
-- period with an actual, and for a flow the totals of the quarter and
-- the year to go. Runs at dataset grain (`SELECT metric_forecast()
-- FROM fin`). The door (`metric_forecast_ahead`) reads each metric as
-- a whole from the cube at its own resolution, sends the history and
-- the periods ahead to the forecast service as one table per grain,
-- and serves one row per metric and point; this body folds the points
-- into the measurement. No actual and no PIT land here: neither is
-- known at the origin. `forecast_points()` joins the actual at read
-- and says where it landed; `forecast_coverage()` counts that over
-- every run the record holds.
--
-- `grain` is the metric's own resolution, `origin` the last period
-- with an actual, `served_by` the service that answered and its
-- version. A point is a step (one period past the origin) or a span
-- (`span` set: the total of the first so many steps, `to_go` naming
-- the quarter, the year or the month it runs to). `service_80` and
-- `service_90` are the service's held-out share of actuals inside the
-- 80% and the 90% band at that grain, step and span, null where it
-- measured none.
WITH m AS (
  SELECT seq, metric, applicable, reason, grain, origin, served_by,
         CASE WHEN applicable THEN coalesce(array_agg(named_struct(
           'step', step, 'span', span, 'to_go', to_go, 'period', period,
           'p05', p05, 'p10', p10, 'p50', p50, 'p90', p90, 'p95', p95,
           'service_80', service_80, 'service_90', service_90
         ) ORDER BY point_seq) FILTER (WHERE period IS NOT NULL), []) END AS points
  FROM metric_forecast_ahead($subject)
  GROUP BY seq, metric, applicable, reason, grain, origin, served_by
)
SELECT
  count(metric) > 0 AS applicable,
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'grain', grain, 'origin', origin, 'served_by', served_by,
    'points', points
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics
FROM m
