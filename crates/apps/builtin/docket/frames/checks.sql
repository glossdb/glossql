-- The standing checks: the adjudication plane, read whole and folded
-- per witness. Each witness with a detector is one row — what it
-- asks, in words; its detector and threshold; its verdicts counted by
-- band, as numbers for the bar and as a phrase; its worst band as the
-- chip — linked to the check's own page (`?check=<witness>`), where
-- the evidence behind the verdicts is listed by the detector's kind.
-- Band and score come from ATTEST(), never from glossary lifecycle
-- states (current/stale/contested belong to Coverage). A witness
-- without a detector is a speaker gate, not a check — it never
-- appears here; a witness whose detector has no verdict yet appears
-- with none, since absence is visible. New imports invalidate the
-- measurements, so the next read recomputes — nothing here is
-- maintained by hand.
WITH v AS (
  SELECT a.witness, a.band,
         CASE a.band WHEN 'red' THEN 4 WHEN 'orange' THEN 3 WHEN 'yellow' THEN 2 WHEN 'error' THEN 1 ELSE 0 END AS severity
  FROM ATTEST() a
),
folded AS (
  SELECT w.name AS witness, w.aspect, w.detector, w.threshold,
         count(v.band) AS n,
         count(*) FILTER (WHERE v.band = 'green') AS green,
         count(*) FILTER (WHERE v.band = 'yellow') AS yellow,
         count(*) FILTER (WHERE v.band = 'orange') AS orange,
         count(*) FILTER (WHERE v.band = 'red') AS red,
         count(*) FILTER (WHERE v.band = 'error') AS errors,
         coalesce(max(v.severity), -1) AS worst
  FROM witnesses w LEFT JOIN v ON v.witness = w.name
  WHERE w.detector IS NOT NULL
  GROUP BY w.name, w.aspect, w.detector, w.threshold
)
SELECT arrow_cast(witness, 'Utf8') AS witness,
       arrow_cast(CASE detector
         WHEN 'band_breach' THEN 'Did every metric''s newest complete month land inside the corridor its own history predicted?'
         WHEN 'slot_entropy' THEN 'Do the voices on ' || aspect || ' agree, column by column?'
         WHEN 'rate_tolerance' THEN 'Does the measured breach rate on ' || aspect || ' stay within the tolerance written for it?'
         ELSE 'Does ' || detector || ' pass on ' || aspect || '?' END, 'Utf8') AS asks,
       arrow_cast(detector || coalesce(' @ ' || threshold, '') || ' on ' || aspect, 'Utf8') AS expectation,
       n, green, yellow, orange, red, errors,
       arrow_cast(CASE WHEN n = 0 THEN 'no verdict yet' ELSE concat_ws(' · ',
           CASE WHEN red > 0 THEN CAST(red AS VARCHAR) || ' red' END,
           CASE WHEN orange > 0 THEN CAST(orange AS VARCHAR) || ' orange' END,
           CASE WHEN yellow > 0 THEN CAST(yellow AS VARCHAR) || ' yellow' END,
           CASE WHEN errors > 0 THEN CAST(errors AS VARCHAR) || ' error' END,
           CASE WHEN green > 0 THEN CAST(green AS VARCHAR) || ' green' END) END, 'Utf8') AS measured,
       arrow_cast(CASE worst WHEN 4 THEN 'red' WHEN 3 THEN 'orange' WHEN 2 THEN 'yellow' WHEN 1 THEN 'error' WHEN 0 THEN 'green' ELSE 'none' END, 'Utf8') AS band,
       CASE WHEN worst = 0 THEN 'ok' WHEN worst < 0 THEN '' ELSE 'warn' END AS bcls,
       arrow_cast('?check=' || witness || '&kind=' || CASE detector
         WHEN 'band_breach' THEN 'corridor'
         WHEN 'slot_entropy' THEN 'voices'
         WHEN 'rate_tolerance' THEN 'rate'
         ELSE 'verdicts' END, 'Utf8') AS link
FROM folded
ORDER BY worst DESC, witness
