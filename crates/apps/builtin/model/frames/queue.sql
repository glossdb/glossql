-- The one queue (informative since 2026-08-13: the app carries no
-- write — answers travel through a session and land as human slots):
-- what the model does not yet hold firmly. Two sources: claims the
-- model's own rules owe and nobody wrote, and judged metric
-- assumptions below full confidence. Glyphs and links are the
-- frame's job; the template stays dumb. No cap — the queue is the
-- whole of what is owed.
WITH owed AS (
  SELECT '–' AS glyph, 'g-una' AS gcls,
         CAST(NULL AS DOUBLE) AS conf,
         c.subject AS subj, c.aspect AS asp,
         CASE c.aspect
           WHEN 'behavior' THEN 'a measure with no summability claim — nothing under any sum of it'
           ELSE 'a measure with no unit' END AS what,
         '?subject=' || c.subject AS link
  FROM GLOSSARY() c
  JOIN (SELECT subject FROM GLOSSARY(all => true)
        WHERE aspect = 'role' AND json_get_str(body, 'value') = 'measure') r
    ON r.subject = c.subject
  WHERE c.state = 'unassessed' AND c.aspect IN ('behavior', 'unit')
),
loose AS (
  SELECT '◐' AS glyph, 'g-jud' AS gcls,
         json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
         g.aspect AS subj,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS asp,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
         '?metric=' || g.aspect AS link
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    -- the winning slot only: a human body's assumptions replace the
    -- agent's at every read, so only the governing ones queue
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
)
SELECT * FROM (SELECT * FROM owed UNION ALL SELECT * FROM loose)
ORDER BY conf ASC NULLS FIRST, subj, asp
