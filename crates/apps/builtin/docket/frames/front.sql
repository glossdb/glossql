-- The front counts: declared surfaces, open questions across all of
-- them — the queue itself, `open_questions`, so this number and the
-- docket's cannot drift apart — and the corridor verdict, dataset-wide
-- from the bands witness. 'not run' is honest absence, not green.
SELECT
  (SELECT count(*) FROM aspects WHERE kind = 'query') AS surfaces,
  (SELECT count(*) FROM open_questions
   WHERE dataset = CAST($dataset AS VARCHAR)) AS open,
  arrow_cast(coalesce(
    (SELECT max(band) FROM ATTEST() WHERE aspect = 'metric_bands'),
    'not run'), 'Utf8') AS corridor
