-- The pin queue: definitional questions the flow left open, glossed by
-- the agent under the pin_questions convention — flat, one entry per
-- option, each carrying the full body its approval would write. One
-- row here is one gesture: the pin door lands the body as the HUMAN
-- slot. Answered questions leave by derivation, never by mutation — a
-- human writing on the question's (subject, aspect) at or after the
-- agenda was glossed is the answer. The timestamp bound carries the
-- rounds: several questions on one aspect retire together on the
-- first pin (whole-body supersession), and the agent's next agenda —
-- glossed after that pin, its remaining questions re-composed on top
-- of the human's map — serves again.
WITH q AS (
  SELECT
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'question') AS question,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'subject') AS subject,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'aspect') AS aspect,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'option') AS opt,
    arrow_cast(json_as_text(json_get(json_get(g.body, 'questions'), i.i), 'body'), 'Utf8') AS body,
    coalesce(json_get_bool(json_get(json_get(g.body, 'questions'), i.i), 'chosen'), false) AS chosen,
    coalesce(json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'grounds'), '') AS grounds,
    coalesce(json_get_float(json_get(json_get(g.body, 'questions'), i.i), 'confidence'), 0.5) AS conf,
    i.i AS ord,
    g.written_at AS agenda_written
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 63) AS i(i)
  WHERE g.aspect = 'pin_questions'
    AND i.i < json_length(g.body, 'questions')
)
SELECT * FROM (
SELECT q.question, q.subject, q.aspect, q.body,
       arrow_cast(q.opt || CASE WHEN q.grounds <> '' THEN ' — ' || q.grounds ELSE '' END,
                  'Utf8') AS meta,
       CASE WHEN q.chosen THEN 'proposed' ELSE 'alternative' END AS stance,
       CASE WHEN q.chosen THEN 'g-jud' ELSE 'g-una' END AS scls,
       q.conf,
       q.ord
FROM q
WHERE q.subject IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM glossary h
    WHERE h.subject = q.subject AND h.aspect = q.aspect
      AND h.actor_kind = 'human' AND h.written_at >= q.agenda_written
  )
UNION ALL
-- Owed claims whose aspect admits an enumerable value: the schema is
-- the composition — one card per admitted value, the human's word the
-- basis (the overrule half of the surface, the lead's requirement).
-- The card retires the way any unassessed row does: any writing on
-- the slot answers it.
SELECT arrow_cast(c.subject || ': ' || c.aspect || '?', 'Utf8') AS question,
       c.subject, c.aspect,
       arrow_cast('{"value": "'
         || json_get_str(json_get(json_get(json_get(a.schema, 'properties'), 'value'), 'enum'), v.i)
         || '"}', 'Utf8') AS body,
       arrow_cast(json_get_str(json_get(json_get(json_get(a.schema, 'properties'), 'value'), 'enum'), v.i)
         || ' — the aspect admits it; your word is the basis', 'Utf8') AS meta,
       'admitted' AS stance,
       'g-una' AS scls,
       CAST(NULL AS DOUBLE) AS conf,
       v.i AS ord
FROM GLOSSARY() c
JOIN (SELECT subject FROM GLOSSARY(all => true)
      WHERE aspect = 'role' AND json_get_str(body, 'value') = 'measure') r
  ON r.subject = c.subject
JOIN aspects a ON a.name = c.aspect
CROSS JOIN generate_series(0, 7) AS v(i)
WHERE c.state = 'unassessed' AND c.aspect IN ('behavior', 'unit')
  AND v.i < json_length(json_get(json_get(a.schema, 'properties'), 'value'), 'enum')
)
ORDER BY conf ASC NULLS LAST, question, ord
