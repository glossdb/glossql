-- One metric's monthly story from the measured cube: the metric's own
-- series (dimension '' — the total at the grounding's own verb), and
-- the disclosed rival beside it where the grounding names one
-- (dimension 'alternative'). What moves between the readings is a
-- chart, not an argument. Empty until the measurement runs.
--
-- The series carry their own names. The chosen one used to read
-- "(chosen)", which tells a reader nothing they wanted: it is the
-- metric, so it wears the metric's title, and the rival says it is
-- one.
SELECT s.period,
  arrow_cast(CASE WHEN s.dimension = ''
    THEN coalesce(json_get_str(a.schema, 'title'), CAST($metric AS VARCHAR))
    ELSE 'rival: ' || s.member END, 'Utf8') AS series,
  s.value
FROM metric_series() s
LEFT JOIN aspects a ON a.name = CAST($metric AS VARCHAR)
WHERE s.metric = CAST($metric AS VARCHAR) AND s.dimension IN ('', 'alternative')
ORDER BY s.period, series
