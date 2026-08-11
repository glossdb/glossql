-- The standing counts, one row. `needs` is the queue's honest total:
-- judged assumptions below full confidence plus fact aspects the model
-- owes and nobody wrote (state = 'unassessed', grain-bounded by the
-- read itself).
WITH raw AS (SELECT * FROM GLOSSARY(all => true)),
loose AS (
  SELECT count(*) AS n
  FROM raw g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
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
  (SELECT n FROM loose) + (SELECT n FROM owed) AS needs
