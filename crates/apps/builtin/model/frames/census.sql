-- The standing counts, one row. `needs` is the one queue's honest
-- total: open agenda questions (pinnable) plus judged assumptions
-- below full confidence not covered by one, plus fact aspects the
-- model owes and nobody wrote (state = 'unassessed', grain-bounded
-- by the read itself).
WITH raw AS (SELECT * FROM GLOSSARY(all => true)),
q AS (
  SELECT
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'subject') AS subject,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'aspect') AS aspect,
    json_get_str(json_get(json_get(g.body, 'questions'), i.i), 'question') AS question,
    g.written_at AS agenda_written
  FROM raw g
  CROSS JOIN generate_series(0, 63) AS i(i)
  WHERE g.aspect = 'pin_questions' AND i.i < json_length(g.body, 'questions')
),
open_q AS (
  SELECT DISTINCT q.subject, q.aspect, q.question
  FROM q
  WHERE q.subject IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM glossary h
      WHERE h.subject = q.subject AND h.aspect = q.aspect
        AND h.actor_kind = 'human' AND h.written_at >= q.agenda_written
    )
),
loose AS (
  SELECT count(*) AS n
  FROM raw g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    AND NOT EXISTS (SELECT 1 FROM open_q oq
                    WHERE oq.subject = g.subject AND oq.aspect = g.aspect)
),
owed AS (
  SELECT count(*) AS n
  FROM GLOSSARY() c
  JOIN (SELECT subject FROM raw
        WHERE aspect = 'role' AND json_get_str(body, 'value') = 'measure') r
    ON r.subject = c.subject
  WHERE c.state = 'unassessed' AND c.aspect IN ('behavior', 'unit')
)
SELECT
  (SELECT count(*) FROM raw WHERE kind = 'fact') AS facts,
  (SELECT count(DISTINCT subject) FROM raw) AS subjects,
  (SELECT count(*) FROM aspects) AS aspects,
  (SELECT count(*) FROM aspects WHERE kind = 'query') AS metrics,
  (SELECT count(*) FROM witnesses) AS witnesses,
  (SELECT count(*) FROM raw WHERE kind = 'measurement') AS measurements,
  (SELECT count(*) FROM open_q) + (SELECT n FROM loose) + (SELECT n FROM owed) AS needs
