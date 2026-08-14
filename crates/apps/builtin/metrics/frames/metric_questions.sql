-- One metric's open questions (the sketch's "your check" box,
-- 2026-08-13): the judged assumptions below full confidence in the
-- AGENT's current body — the same derivation the door asks as forms,
-- rendered where the metric lives. Three gates (ruled 2026-08-14):
-- derive from the agent's live grounding, never a frozen human copy;
-- skip the dimensions the function map owns (behavior, sign, grain —
-- statistics are agent work); and a standing ruling holds a question
-- closed by content match until the agent's fold-in rewrites the body.
-- Answered rows stop deriving; nothing is stored or dismissed.
WITH ruled AS (
  SELECT r.subject AS subject,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'assumption') AS assumption
  FROM glossary r
  CROSS JOIN generate_series(0, 199) AS rj(j)
  WHERE r.aspect = 'ruling' AND r.actor_kind = 'human'
    AND NOT EXISTS (SELECT 1 FROM glossary r2
                    WHERE r2.subject = r.subject AND r2.aspect = 'ruling'
                      AND r2.actor_kind = 'human' AND r2.written_at > r.written_at)
    AND rj.j < json_length(r.body, 'rulings')
),
open_assumptions AS (
  SELECT g.subject AS subject,
         json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dim,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis') AS basis
  FROM glossary g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.aspect = $metric AND g.actor_kind = 'agent'
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND i.i < json_length(g.body, 'assumptions')
)
SELECT o.conf,
       arrow_cast(coalesce(o.dim, '-'), 'Utf8') AS dim,
       arrow_cast(o.what, 'Utf8') AS what,
       arrow_cast(coalesce(o.basis, 'unstated'), 'Utf8') AS basis
FROM open_assumptions o
WHERE o.conf < 1.0
  AND coalesce(o.dim, '-') NOT IN ('behavior', 'sign', 'grain')
  AND NOT EXISTS (SELECT 1 FROM ruled r
                  WHERE r.subject = o.subject AND r.aspect = $metric
                    AND r.assumption = o.what)
ORDER BY o.conf ASC
