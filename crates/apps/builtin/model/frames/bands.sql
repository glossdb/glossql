-- The walked band read for one metric, unnested from the dataset's
-- metric_bands measurement. The inline json_as_text/json_get nesting is
-- deliberate: the substrate's json functions take one dynamic path
-- element at a time, and an extraction parked in a CTE column comes
-- back null -- so every scalar rebuilds its path in place. Stated caps,
-- both mirrored from the measurement itself: 6 walked points per
-- metric, the first 16 monitored metrics. An empty result means the
-- measurement has not run -- SELECT metric_bands() FROM the dataset.
SELECT
  arrow_cast(json_get_str(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'period'), 'Utf8') AS period,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'actual') AS actual,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'p05') AS p05,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'p10') AS p10,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'p50') AS p50,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'p90') AS p90,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'p95') AS p95,
  json_get_float(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'pit') AS pit
FROM GLOSSARY(all => true) g
CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15)) AS mi(i)
CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5)) AS pi(i)
WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  AND json_get_str(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'metric') = CAST($metric AS VARCHAR)
  AND json_get_str(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), mi.i)), 'points')), pi.i)), 'period') IS NOT NULL
ORDER BY period
