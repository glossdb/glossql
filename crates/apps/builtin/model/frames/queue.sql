-- The investigate half of the one queue (ruled 2026-08-12: "needs
-- judgement" and "pin" merged): what the model does not yet hold
-- firmly AND the agent has not composed an answer for. Rows here
-- carry no pin action — their affordance is the dossier. Two
-- sources: claims the model's own rules owe and nobody wrote, and
-- judged metric assumptions below full confidence — minus any
-- assumption already covered by an open agenda question on the same
-- (subject, aspect), which renders as a pinnable card instead (the
-- pins frame). Glyphs and links are the frame's job; the template
-- stays dumb. No cap — the queue is the whole of what is owed.
WITH q AS (
  SELECT
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'subject') AS subject,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'aspect') AS aspect,
    g.written_at AS agenda_written
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 63) AS i(i)
  WHERE g.aspect = 'pin_questions' AND i.i < json_length(g.body, 'questions')
),
open_q AS (
  SELECT DISTINCT q.subject, q.aspect
  FROM q
  WHERE q.subject IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM glossary h
      WHERE h.subject = q.subject AND h.aspect = q.aspect
        AND h.actor_kind = 'human' AND h.written_at >= q.agenda_written
    )
),
owed AS (
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
    AND NOT EXISTS (SELECT 1 FROM open_q oq
                    WHERE oq.subject = g.subject AND oq.aspect = g.aspect)
)
SELECT * FROM (SELECT * FROM owed UNION ALL SELECT * FROM loose)
ORDER BY conf ASC NULLS FIRST, subj, asp
