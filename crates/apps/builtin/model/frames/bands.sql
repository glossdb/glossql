-- The walked band read for one metric, unnested from the dataset's
-- metric_bands measurement. The static unroll is deliberate, measured
-- 2026-08-12: the substrate's json functions null a dynamic-index
-- extraction (json_get(x, i.i)) the moment it crosses a projection
-- boundary, and the common-subexpression optimizer drops the base
-- column when dynamic chains repeat across two unnest joins — so the
-- array positions unroll as static literals instead. The caps mirror
-- the measurement itself: 16 monitored metrics, 6 walked points.
-- An empty result means the measurement has not run (or its cache was
-- invalidated by a later gloss — it ACCEPTS the glossary):
-- SELECT metric_bands() FROM the dataset.
WITH m AS (
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 0)), 'Utf8') AS mo FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 1)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 2)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 3)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 4)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 5)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 6)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 7)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 8)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 9)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 10)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 11)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 12)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 13)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 14)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
  UNION ALL
  SELECT arrow_cast(json_as_text(json_get(json_as_text(json_get(g.body, 'metrics')), 15)), 'Utf8') FROM GLOSSARY(all => true) g WHERE g.aspect = 'metric_bands' AND g.kind = 'measurement'
),
p AS (
  SELECT json_get_str(mo, 'metric') AS mname, arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 0)), 'Utf8') AS pt FROM m
  UNION ALL
  SELECT json_get_str(mo, 'metric'), arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 1)), 'Utf8') FROM m
  UNION ALL
  SELECT json_get_str(mo, 'metric'), arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 2)), 'Utf8') FROM m
  UNION ALL
  SELECT json_get_str(mo, 'metric'), arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 3)), 'Utf8') FROM m
  UNION ALL
  SELECT json_get_str(mo, 'metric'), arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 4)), 'Utf8') FROM m
  UNION ALL
  SELECT json_get_str(mo, 'metric'), arrow_cast(json_as_text(json_get(json_as_text(json_get(mo, 'points')), 5)), 'Utf8') FROM m
)
SELECT arrow_cast(json_get_str(pt, 'period'), 'Utf8') AS period,
       json_get_float(pt, 'actual') AS actual,
       json_get_float(pt, 'p05') AS p05,
       json_get_float(pt, 'p10') AS p10,
       json_get_float(pt, 'p50') AS p50,
       json_get_float(pt, 'p90') AS p90,
       json_get_float(pt, 'p95') AS p95,
       json_get_float(pt, 'pit') AS pit
FROM p
WHERE mname = CAST($metric AS VARCHAR) AND pt IS NOT NULL
ORDER BY period
