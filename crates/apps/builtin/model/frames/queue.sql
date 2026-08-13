-- The one queue (informative since 2026-08-13: the app carries no
-- write — answers travel through a session and land as human slots):
-- judgment only. Judged metric assumptions below full confidence —
-- conventions and definitions the data cannot arbitrate. Unassessed
-- witnessed claims (behavior, unit) are NOT here (ruled 2026-08-13):
-- a claim a measurement can settle is the agent's backlog — the
-- coverage tile counts it — and no human is ever asked for a
-- statistic. Glyphs and links are the frame's job; the template
-- stays dumb. No cap — the queue is the whole of what is owed.
SELECT '◐' AS glyph, 'g-jud' AS gcls,
       json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
       g.aspect AS subj,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS asp,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
       arrow_cast('/app/metrics?metric=' || g.aspect, 'Utf8') AS link
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
ORDER BY conf ASC, subj, asp
