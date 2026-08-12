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
SELECT q.question, q.subject, q.aspect, q.body,
       arrow_cast(q.opt || CASE WHEN q.grounds <> '' THEN ' — ' || q.grounds ELSE '' END,
                  'Utf8') AS meta,
       CASE WHEN q.chosen THEN 'proposed' ELSE 'alternative' END AS stance,
       CASE WHEN q.chosen THEN 'g-jud' ELSE 'g-una' END AS scls,
       q.conf
FROM q
WHERE q.subject IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM glossary h
    WHERE h.subject = q.subject AND h.aspect = q.aspect
      AND h.actor_kind = 'human' AND h.written_at >= q.agenda_written
  )
ORDER BY q.conf, q.question, q.ord
