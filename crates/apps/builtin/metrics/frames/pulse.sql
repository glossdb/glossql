-- The pulse: every declared metric surface with its latest cube month,
-- the month-over-month move, the axes the cube admitted, and the
-- validation chip (open questions first, then human-ruled, then merely
-- grounded — the same derivation the model app's surfaces list runs).
-- Values come from the cached metric_cube measurement; a metric with
-- no cube rows shows an em dash until `SELECT metric_cube() FROM` the
-- dataset runs. The list is the navigation — each row opens the
-- metric's dossier.
WITH latest AS (
  SELECT metric, period, value,
         value - lag(value) OVER (PARTITION BY metric ORDER BY period) AS delta
  FROM metric_series() WHERE dimension = ''
  QUALIFY row_number() OVER (PARTITION BY metric ORDER BY period DESC) = 1
),
axes AS (
  SELECT metric, string_agg(dimension, ' · ') AS dims
  FROM (SELECT DISTINCT metric, dimension FROM metric_series()
        WHERE dimension NOT IN ('', 'alternative'))
  GROUP BY metric
),
loose AS (
  SELECT g.aspect, count(*) AS n
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
  GROUP BY g.aspect
),
ruled AS (
  SELECT aspect, max(written_at) AS at
  FROM glossary
  WHERE actor_kind = 'human' AND subject = CAST($dataset AS VARCHAR)
  GROUP BY aspect
)
SELECT
  coalesce(json_get_str(a.schema, 'title'), a.name) AS title,
  a.name,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  coalesce(json_get_str(a.schema, 'x-unit'), '') AS unit,
  arrow_cast(coalesce(l2.period, ''), 'Utf8') AS period,
  arrow_cast(coalesce(CAST(round(l2.value, 1) AS VARCHAR), '—'), 'Utf8') AS latest,
  arrow_cast(coalesce(CASE
    WHEN l2.delta IS NULL OR l2.value IS NULL THEN ''
    WHEN l2.delta >= 0 THEN '+' || CAST(round(100.0 * l2.delta / nullif(l2.value - l2.delta, 0), 1) AS VARCHAR) || '%'
    ELSE CAST(round(100.0 * l2.delta / nullif(l2.value - l2.delta, 0), 1) AS VARCHAR) || '%'
    END, ''), 'Utf8') AS delta,
  arrow_cast(coalesce(x.dims, 'no axes admitted'), 'Utf8') AS axes,
  arrow_cast(CASE
    WHEN coalesce(l.n, 0) > 0 THEN CAST(l.n AS VARCHAR) || ' open'
    WHEN r.at IS NOT NULL THEN 'human-ruled ' || substr(r.at, 1, 10)
    WHEN q.aspect IS NULL THEN 'nothing recorded'
    ELSE 'grounded' END, 'Utf8') AS status,
  CASE
    WHEN coalesce(l.n, 0) > 0 THEN 'warn'
    WHEN r.at IS NOT NULL THEN 'ok'
    WHEN q.aspect IS NULL THEN 'warn'
    ELSE 'ok' END AS scls,
  arrow_cast('?metric=' || a.name, 'Utf8') AS link
FROM aspects a
LEFT JOIN (SELECT DISTINCT aspect FROM GLOSSARY(all => true) WHERE kind = 'query') q
  ON q.aspect = a.name
LEFT JOIN latest l2 ON l2.metric = a.name
LEFT JOIN axes x ON x.metric = a.name
LEFT JOIN loose l ON l.aspect = a.name
LEFT JOIN ruled r ON r.aspect = a.name
WHERE a.kind = 'query'
ORDER BY CASE WHEN coalesce(json_get_str(a.schema, 'x-kind'), '') = 'metric'
              THEN 0 ELSE 1 END, a.name
