-- The one queue (informative since 2026-08-13: the app carries no
-- write — answers travel through a session and land as RULINGS, ruled
-- 2026-08-14): judgment only. Judged metric assumptions below full
-- confidence in the AGENT's current body — conventions and definitions
-- the data cannot arbitrate. Never a frozen human copy (the freeze
-- re-asked every answered question), never a dimension the function
-- map owns (behavior, sign, grain — statistics are agent work), and a
-- standing ruling holds its question closed — matched on the
-- assumption's declared `key`, never on its prose (ruled 2026-08-14) —
-- until the agent's fold-in raises that key to full confidence. An
-- assumption disclosed without a key cannot close, so it is not
-- queued: a known, accepted gap. Unassessed witnessed claims are
-- NOT here: the coverage tile counts the agent's backlog, and no human
-- is ever asked for a statistic. Glyphs and links are the frame's job;
-- the template stays dumb. No cap — the queue is the whole of what is
-- owed.
WITH ruled AS (
  SELECT r.subject AS subject,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key
  FROM glossary r
  CROSS JOIN generate_series(0, 199) AS rj(j)
  WHERE r.aspect = 'ruling' AND r.actor_kind = 'human'
    AND NOT EXISTS (SELECT 1 FROM glossary r2
                    WHERE r2.subject = r.subject AND r2.aspect = 'ruling'
                      AND r2.actor_kind = 'human' AND r2.written_at > r.written_at)
    AND rj.j < json_length(r.body, 'rulings')
),
open_assumptions AS (
  SELECT g.subject AS subject, g.aspect AS aspect,
         json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dim,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what
  FROM glossary g
  JOIN aspects a ON a.name = g.aspect AND a.kind = 'query'
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.actor_kind = 'agent'
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND i.i < json_length(g.body, 'assumptions')
)
SELECT '◐' AS glyph, 'g-jud' AS gcls,
       o.conf,
       o.aspect AS subj,
       o.dim AS asp,
       o.what,
       arrow_cast('/app/metrics?metric=' || o.aspect, 'Utf8') AS link
FROM open_assumptions o
WHERE o.conf < 1.0 AND o.key IS NOT NULL
  AND coalesce(o.dim, '-') NOT IN ('behavior', 'sign', 'grain')
  AND NOT EXISTS (SELECT 1 FROM ruled r
                  WHERE r.subject = o.subject AND r.aspect = o.aspect
                    AND r.key = o.key)
ORDER BY o.conf ASC, subj, asp
