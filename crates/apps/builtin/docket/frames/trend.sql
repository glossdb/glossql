-- One metric's story at the viewer's grain: the metric's own series
-- (dimension '' — the total at the grounding's own verb) and the
-- disclosed rival beside it where the grounding names one (dimension
-- 'alternative'). The cells at the grain are a plan over the metric's
-- head, folded by the verb each row carries and windowed to the
-- grain's rung on the server; the span clips the newest periods here
-- — the frame is the filter, and a grain finer than the metric's
-- resolution serves nothing. `$grain` and `$span` are page params;
-- the reference on the page carries the defaults.
--
-- The forecast world rides ahead of the actuals: the newest
-- `metric_forecast` run's medians for the periods ahead, served at the
-- metric's own resolution only, so another grain shows none. The span
-- is the history's and never clips it.
--
-- The series carry their own names: the chosen one wears the metric's
-- title, the rival says it is one, the forecast says `forecast`; the
-- world tells the chart which stroke to draw.
WITH story AS (
  SELECT period, dimension, member, value, num, den, behavior, world,
         dense_rank() OVER (ORDER BY period DESC) AS recency
  FROM metric_series(grain => $grain)
  WHERE metric = CAST($metric AS VARCHAR) AND dimension IN ('', 'alternative')
),
ahead AS (
  SELECT period, value, num, den, behavior, world
  FROM metric_series(grain => $grain, world => 'forecast')
  WHERE metric = CAST($metric AS VARCHAR) AND dimension = ''
)
SELECT s.period,
  arrow_cast(CASE WHEN s.dimension = ''
    THEN coalesce(json_get_str(a.schema, 'title'), CAST($metric AS VARCHAR))
    ELSE 'rival: ' || s.member END, 'Utf8') AS series,
  s.value, s.num, s.den, s.behavior, s.world
FROM story s
LEFT JOIN aspects a ON a.name = CAST($metric AS VARCHAR)
WHERE CAST($span AS VARCHAR) = 'all' OR s.recency <= TRY_CAST($span AS INT)
UNION ALL
SELECT period, arrow_cast('forecast', 'Utf8') AS series, value, num, den, behavior, world
FROM ahead
ORDER BY period, series
