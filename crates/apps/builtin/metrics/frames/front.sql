-- The front counts: declared surfaces, open questions across all of
-- them (the same winning-slot derivation the queue runs), and the
-- corridor verdict — dataset-wide, from the bands witness. 'not run'
-- is honest absence, not green.
WITH loose AS (
  SELECT count(*) AS n
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
)
SELECT
  (SELECT count(*) FROM aspects WHERE kind = 'query') AS surfaces,
  (SELECT n FROM loose) AS open,
  arrow_cast(coalesce(
    (SELECT max(band) FROM ATTEST() WHERE aspect = 'metric_bands'),
    'not run'), 'Utf8') AS corridor
