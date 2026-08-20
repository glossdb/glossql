-- The picked slice: one admitted dimension's members at the viewer's
-- grain, at the grounding's own verb — the cube's member cells,
-- re-bucketed on the server, the span clipped here. The page renders
-- this tile only when the URL carries a dim.
SELECT period, member AS series, value, num, den, behavior
FROM (
  SELECT period, member, value, num, den, behavior,
         dense_rank() OVER (ORDER BY period DESC) AS recency
  FROM metric_series(grain => $grain)
  WHERE metric = CAST($metric AS VARCHAR) AND dimension = CAST($dim AS VARCHAR)
)
WHERE CAST($span AS VARCHAR) = 'all' OR recency <= TRY_CAST($span AS INT)
ORDER BY period, series
