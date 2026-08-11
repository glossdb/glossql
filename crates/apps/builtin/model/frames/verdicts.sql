-- The collapsed read on one subject: per witnessed aspect, the band
-- and score the detector holds, and the state — including what is
-- owed and unwritten.
SELECT aspect, state,
  coalesce(band, '') AS band,
  arrow_cast(coalesce(CAST(score AS VARCHAR), ''), 'Utf8') AS score,
  CASE WHEN band = 'green' THEN 'ok'
       WHEN band IS NULL AND state = 'current' THEN ''
       WHEN state = 'unassessed' THEN 'warn'
       ELSE 'warn' END AS bcls
FROM GLOSSARY()
WHERE subject = $subject
ORDER BY state DESC, aspect
