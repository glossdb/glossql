-- The declared metric surfaces: every QUERY aspect with its standing
-- formula, and the validation chip the sketch ruled in (2026-08-13):
-- open questions first, then human-ruled with its date, then merely
-- grounded. Every chip is a derivation — the list is the navigation,
-- each row opens the metric's dossier.
WITH loose AS (
  SELECT g.aspect, count(*) AS n
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    -- the winning slot only, as in frames/queue.sql
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
  GROUP BY g.aspect
),
ruled AS (
  -- scoped to the bound dataset: a metric gloss's subject IS the
  -- dataset, and the plain glossary table is workspace-wide — without
  -- the guard this chip would claim another dataset's ruling
  SELECT aspect, max(written_at) AS at
  FROM glossary
  WHERE actor_kind = 'human' AND subject = CAST($dataset AS VARCHAR)
  GROUP BY aspect
)
SELECT
  coalesce(json_get_str(a.schema, 'title'), a.name) AS title,
  a.name,
  coalesce(json_get_str(a.schema, 'x-unit'), '') AS unit,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  arrow_cast(coalesce(json_get_str(json_get(f.value, 'formulas'), a.name),
           'a base concept — grounded as an extract, no formula'), 'Utf8') AS formula,
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
-- the collapsed read: one winning formulas slot, human outranking agent
LEFT JOIN GLOSSARY() f ON f.aspect = 'formulas'
LEFT JOIN loose l ON l.aspect = a.name
LEFT JOIN ruled r ON r.aspect = a.name
WHERE a.kind = 'query'
ORDER BY CASE WHEN coalesce(json_get_str(a.schema, 'x-kind'), '') = 'metric'
              THEN 0 ELSE 1 END, a.name
