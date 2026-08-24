-- The standing counts, one row. `needs` is the one queue's honest
-- total, and it is literally the queue: `open_questions`, the same
-- rows the docket lists and the door asks. Judgment only
-- — unassessed witnessed claims (behavior, unit) count
-- under `waiting` instead, because a claim a measurement can settle
-- waits on the agent's functions, never on a human answer.
--
-- `waiting` is the shipped `owed` read plus those unassessed measure
-- claims: human writings that owe an agent act, and agent work that
-- owes a function run.
WITH raw AS (SELECT * FROM GLOSSARY(all => true)),
unassessed_measures AS (
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
  (SELECT count(*) FROM open_questions q
   JOIN current_dataset d ON d.dataset = q.dataset) AS needs,
  (SELECT count(*) FROM owed) + (SELECT n FROM unassessed_measures) AS waiting
