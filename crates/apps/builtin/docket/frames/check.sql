-- One check, both faces: the witness's own declaration — the aspect
-- it adjudicates, who may speak on it, its detector and threshold —
-- and its verdicts as ATTEST() holds them now: how many, by band, the
-- worst as the chip, the highest score, when the newest was measured
-- and whether every one stands at this pin. The words for what the
-- detector asks are the shipped detectors'; another detector is
-- named as it is. One row, or none for a name no witness carries.
WITH v AS (
  SELECT a.band, a.score, a.computed_at, a.current,
         CASE a.band WHEN 'red' THEN 4 WHEN 'orange' THEN 3 WHEN 'yellow' THEN 2 WHEN 'error' THEN 1 ELSE 0 END AS severity
  FROM ATTEST() a WHERE a.witness = $check
),
folded AS (
  SELECT count(v.band) AS n,
         count(*) FILTER (WHERE v.band = 'green') AS green,
         count(*) FILTER (WHERE v.band = 'yellow') AS yellow,
         count(*) FILTER (WHERE v.band = 'orange') AS orange,
         count(*) FILTER (WHERE v.band = 'red') AS red,
         count(*) FILTER (WHERE v.band = 'error') AS errors,
         coalesce(max(v.severity), -1) AS worst,
         max(v.score) AS score,
         max(v.computed_at) AS at,
         coalesce(bool_and(v.current), true) AS standing
  FROM v
)
SELECT arrow_cast(w.name, 'Utf8') AS witness,
       arrow_cast(w.aspect, 'Utf8') AS aspect,
       arrow_cast(coalesce(nullif(replace(replace(replace(replace(w.speakers, '"', ''), '[', ''), ']', ''), ',', ', '), ''),
                           'function voices alone'), 'Utf8') AS speakers,
       arrow_cast(w.detector, 'Utf8') AS detector,
       arrow_cast(coalesce(w.threshold, 'none'), 'Utf8') AS threshold,
       arrow_cast(CASE w.detector
         WHEN 'band_breach' THEN 'Did every metric''s newest complete month land inside the corridor its own history predicted? The score is the worst displacement across the metrics, raised to their number, so the edges keep their base rate at any count.'
         WHEN 'slot_entropy' THEN 'Do the voices on ' || w.aspect || ' agree, column by column? The score is the share of extra distinct values across a column''s slots: 0 when every voice agrees.'
         WHEN 'rate_tolerance' THEN 'Does the measured breach rate on ' || w.aspect || ' stay within the tolerance written for it? Green at or under the tolerance, red above, yellow while no check has spoken.'
         ELSE 'Does ' || w.detector || ' pass on ' || w.aspect || '?' END, 'Utf8') AS asks,
       arrow_cast(CASE WHEN f.n = 0 THEN 'no verdict yet' ELSE
         CAST(f.n AS VARCHAR) || CASE WHEN f.n = 1 THEN ' verdict: ' ELSE ' verdicts: ' END || concat_ws(' · ',
           CASE WHEN f.red > 0 THEN CAST(f.red AS VARCHAR) || ' red' END,
           CASE WHEN f.orange > 0 THEN CAST(f.orange AS VARCHAR) || ' orange' END,
           CASE WHEN f.yellow > 0 THEN CAST(f.yellow AS VARCHAR) || ' yellow' END,
           CASE WHEN f.errors > 0 THEN CAST(f.errors AS VARCHAR) || ' error' END,
           CASE WHEN f.green > 0 THEN CAST(f.green AS VARCHAR) || ' green' END) END, 'Utf8') AS measured,
       arrow_cast(CASE f.worst WHEN 4 THEN 'red' WHEN 3 THEN 'orange' WHEN 2 THEN 'yellow' WHEN 1 THEN 'error' WHEN 0 THEN 'green' ELSE 'none' END, 'Utf8') AS band,
       CASE WHEN f.worst = 0 THEN 'ok' WHEN f.worst < 0 THEN '' ELSE 'warn' END AS bcls,
       arrow_cast(coalesce('score ' || CAST(round(f.score * 1000) / 1000 AS VARCHAR), ''), 'Utf8') AS score,
       arrow_cast(coalesce('measured ' || replace(substr(CAST(f.at AS VARCHAR), 1, 16), 'T', ' '), ''), 'Utf8') AS at,
       arrow_cast(CASE WHEN f.standing THEN '' ELSE ' · from before the last change — re-measure' END, 'Utf8') AS stale
FROM witnesses w CROSS JOIN folded f
WHERE w.name = $check
