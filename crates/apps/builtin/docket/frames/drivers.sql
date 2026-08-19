-- Why the metric moved: every admitted dimension's member cells, all
-- dimensions at once — the browser ranks members by their own move
-- between the last two windowed periods. The cells are the landed
-- cube's; nothing recomputes here, and a window change refetches
-- nothing.
SELECT dimension, member, period, value, num, den, behavior
FROM metric_series()
WHERE metric = CAST($metric AS VARCHAR)
  AND dimension NOT IN ('', 'alternative')
ORDER BY dimension, member, period
