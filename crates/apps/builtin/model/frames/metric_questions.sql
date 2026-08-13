-- One metric's open questions (the sketch's "your check" box,
-- 2026-08-13): the judged assumptions below full confidence on the
-- winning slot — the same derivation the door asks as forms, rendered
-- where the metric lives. Answered rows stop deriving; nothing is
-- stored or dismissed.
SELECT
  json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
  arrow_cast(coalesce(json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension'), '-'), 'Utf8') AS dim,
  arrow_cast(json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption'), 'Utf8') AS what,
  arrow_cast(coalesce(json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis'), 'unstated'), 'Utf8') AS basis
FROM GLOSSARY(all => true) g
CROSS JOIN generate_series(0, 19) AS i(i)
WHERE g.kind = 'query'
  AND g.aspect = $metric
  AND i.i < json_length(g.body, 'assumptions')
  AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
  -- the winning slot only, as in frames/queue.sql
  AND (EXISTS (SELECT 1 FROM glossary me
               WHERE me.subject = g.subject AND me.aspect = g.aspect
                 AND me.actor_id = g.actor AND me.actor_kind = 'human')
       OR NOT EXISTS (SELECT 1 FROM glossary h
                      WHERE h.subject = g.subject AND h.aspect = g.aspect
                        AND h.actor_kind = 'human'))
ORDER BY conf ASC
