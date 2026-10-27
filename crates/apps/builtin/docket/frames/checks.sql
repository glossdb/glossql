-- The standing checks: the adjudication plane, read whole and folded
-- per witness. Each witness with a detector is one row — what it
-- checks, its detector and threshold, its verdicts counted by band,
-- and its worst band as the chip — and every verdict of its that is
-- not green follows as a row of its own, so a red is never buried
-- under sixty green gates. The expectation is the witness's own
-- declaration (detector @ threshold); measured and band come from
-- ATTEST(), never from glossary lifecycle states (current/stale/
-- contested belong to Coverage). A witness without a detector is a
-- speaker gate, not a check — it never appears here. New imports
-- invalidate the measurements, so the next read recomputes — nothing
-- here is maintained by hand.
WITH v AS (
  SELECT a.witness, a.aspect, a.subject, a.band, a.score, w.detector, w.threshold,
         CASE a.band WHEN 'red' THEN 4 WHEN 'orange' THEN 3 WHEN 'yellow' THEN 2 WHEN 'error' THEN 1 ELSE 0 END AS severity
  FROM ATTEST() a
  JOIN witnesses w ON w.name = a.witness
  WHERE w.detector IS NOT NULL
),
folded AS (
  SELECT witness, aspect, detector, threshold,
         count(*) AS n,
         count(*) FILTER (WHERE band = 'green') AS green,
         count(*) FILTER (WHERE band = 'yellow') AS yellow,
         count(*) FILTER (WHERE band = 'orange') AS orange,
         count(*) FILTER (WHERE band = 'red') AS red,
         count(*) FILTER (WHERE band = 'error') AS errors,
         max(severity) AS worst
  FROM v GROUP BY witness, aspect, detector, threshold
),
rows_ AS (
  SELECT f.witness, f.worst, 0 AS kind, '' AS subject,
         f.witness AS checked,
         CAST(f.n AS VARCHAR) || CASE WHEN f.n = 1 THEN ' verdict on ' ELSE ' verdicts on ' END || f.aspect AS what,
         f.detector || coalesce(' @ ' || f.threshold, '') AS expectation,
         concat_ws(' · ',
           CASE WHEN f.red > 0 THEN CAST(f.red AS VARCHAR) || ' red' END,
           CASE WHEN f.orange > 0 THEN CAST(f.orange AS VARCHAR) || ' orange' END,
           CASE WHEN f.yellow > 0 THEN CAST(f.yellow AS VARCHAR) || ' yellow' END,
           CASE WHEN f.errors > 0 THEN CAST(f.errors AS VARCHAR) || ' error' END,
           CASE WHEN f.green > 0 THEN CAST(f.green AS VARCHAR) || ' green' END) AS measured,
         CASE f.worst WHEN 4 THEN 'red' WHEN 3 THEN 'orange' WHEN 2 THEN 'yellow' WHEN 1 THEN 'error' ELSE 'green' END AS band
  FROM folded f
  UNION ALL
  SELECT v.witness, f.worst, 1, v.subject,
         '' AS checked,
         v.subject || ' · ' || v.aspect AS what,
         '' AS expectation,
         CAST(v.score AS VARCHAR) AS measured,
         v.band
  FROM v JOIN folded f ON f.witness = v.witness
  WHERE v.band <> 'green'
)
SELECT arrow_cast(checked, 'Utf8') AS checked,
       arrow_cast(what, 'Utf8') AS what,
       arrow_cast(expectation, 'Utf8') AS expectation,
       arrow_cast(measured, 'Utf8') AS measured,
       arrow_cast(band, 'Utf8') AS band,
       CASE WHEN band = 'green' THEN 'ok' ELSE 'warn' END AS bcls,
       CASE WHEN kind = 0 THEN 'r-subj' ELSE 'r-meta' END AS ccls
FROM rows_
ORDER BY worst DESC, witness, kind, subject
