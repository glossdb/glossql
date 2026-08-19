-- One metric's recent story at day grain, computed at read from the
-- grounding itself (`metric_days` — the cube stays monthly; this is a
-- SELECT over the last 90 observed days at the metric's own verb, on
-- the same judged time axis). The viewer's day and week windows read
-- here; week derives from day in the browser. A ratio row carries its
-- summed halves so the week can re-derive the division.
SELECT d.period,
  arrow_cast(coalesce(json_get_str(a.schema, 'title'),
    CAST($metric AS VARCHAR)), 'Utf8') AS series,
  d.value, d.num, d.den, d.behavior
FROM metric_days($metric) d
LEFT JOIN aspects a ON a.name = CAST($metric AS VARCHAR)
ORDER BY d.period
