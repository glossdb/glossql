-- One metric's story at the viewer's grain: the metric's own series
-- (dimension '' — the total at the grounding's own verb) and the
-- disclosed rival beside it where the grounding names one (dimension
-- 'alternative'). The cells are the cube's at the metric's resolution;
-- the grain re-buckets them by the verb on the server, and the span
-- clips the newest periods here — the frame is the filter, and a
-- grain finer than the metric's resolution serves nothing. `$grain`
-- and `$span` are page params; the reference on the page carries the
-- defaults.
--
-- The series carry their own names: the chosen one wears the metric's
-- title, the rival says it is one.
SELECT s.period,
  arrow_cast(CASE WHEN s.dimension = ''
    THEN coalesce(json_get_str(a.schema, 'title'), CAST($metric AS VARCHAR))
    ELSE 'rival: ' || s.member END, 'Utf8') AS series,
  s.value, s.num, s.den, s.behavior
FROM (
  SELECT period, dimension, member, value, num, den, behavior,
         dense_rank() OVER (ORDER BY period DESC) AS recency
  FROM metric_series(grain => $grain)
  WHERE metric = CAST($metric AS VARCHAR) AND dimension IN ('', 'alternative')
) s
LEFT JOIN aspects a ON a.name = CAST($metric AS VARCHAR)
WHERE CAST($span AS VARCHAR) = 'all' OR s.recency <= TRY_CAST($span AS INT)
ORDER BY s.period, series
