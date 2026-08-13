-- One metric's monthly story from the cached cube: the chosen reading
-- (dimension '' — the total series at the grounding's own verb), and
-- the disclosed rival beside it where the grounding names one
-- (dimension 'alternative'). What moves between the readings is a
-- chart, not an argument. Empty until the measurement runs.
SELECT period,
  arrow_cast(CASE WHEN dimension = '' THEN '(chosen)' ELSE member END, 'Utf8') AS series,
  value
FROM metric_series()
WHERE metric = CAST($metric AS VARCHAR) AND dimension IN ('', 'alternative')
ORDER BY period, series
