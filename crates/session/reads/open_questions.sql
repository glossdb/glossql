-- open_questions — what still stands open for a human to judge.
--
-- The one derivation. The door's round serves these rows as questions,
-- the app's docket renders them, and the skills name this read instead
-- of describing it again; before this file existed the same CTEs lived
-- three times, in three languages.
--
-- Derived from the agent's CURRENT body — never a frozen copy (the
-- 2026-08-14 run: deriving from the winning human slot re-asked every
-- answered question, because the copy kept the stale confidences).
-- Four gates beyond "below full confidence": the aspect is a grounding
-- (query kind); the assumption carries a `key`, its declared identity
-- (an unkeyed assumption cannot be closed, so it is never asked — a
-- known, accepted gap); the dimension is not one the function map owns
-- (`behavior`, `sign`, `grain` are statistics, ruled 2026-08-13 — no
-- human is asked for a number); and no standing ruling names the same
-- (aspect, key). A ruling holds its question closed until the agent's
-- fold-in raises that key to full confidence, at which point the row
-- drops out on its own.
--
-- `sibling` carries what the human already ruled on the SAME key under
-- a different aspect, so the form can say so while asking. It used to
-- be a second query with the subject and key spliced into SQL text;
-- here it is a join.
--
-- No cap and no display: the rows are the whole of what is owed, and
-- glyphs, links and ordering are the caller's business. Filters ride
-- WHERE, like every other read here.
--
-- No ORDER BY, deliberately. A read expands as a derived relation, and
-- an inner ordering does not survive planning — measured, not assumed:
-- the door's round served the wrong question first until it ordered at
-- the call site. Order where you consume.
WITH open_assumptions AS (
  SELECT g.subject AS subject, g.aspect AS aspect, i.i AS idx,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dimension,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS assumption,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis') AS basis,
         json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf
  FROM glossary g
  JOIN aspects a ON a.name = g.aspect AND a.kind = 'query'
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.actor_kind = 'agent'
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND i.i < json_length(g.body, 'assumptions')
)
SELECT o.subject, o.aspect, o.idx, o.dimension, o.key, o.assumption, o.basis, o.conf,
       max(s.stance || ' on ' || s.aspect) AS sibling
FROM open_assumptions o
LEFT JOIN ruling_entries s
  ON s.subject = o.subject AND s.key = o.key AND s.aspect <> o.aspect
WHERE o.conf < 1.0 AND o.key IS NOT NULL
  AND coalesce(o.dimension, '-') NOT IN ('behavior', 'sign', 'grain')
  AND NOT EXISTS (SELECT 1 FROM ruling_entries r
                  WHERE r.subject = o.subject AND r.aspect = o.aspect
                    AND r.key = o.key)
GROUP BY o.subject, o.aspect, o.idx, o.dimension, o.key, o.assumption, o.basis, o.conf
