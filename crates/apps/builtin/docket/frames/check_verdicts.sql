-- A check's verdicts, every one: subject, band and score as ATTEST()
-- holds them, the worst first — the evidence a detector of the
-- workspace's own leaves, since its slots have no shape the page
-- knows. A verdict from before the last change says so.
SELECT arrow_cast(a.subject, 'Utf8') AS subject,
       arrow_cast(a.aspect, 'Utf8') AS aspect,
       arrow_cast(coalesce(CAST(round(a.score * 1000) / 1000 AS VARCHAR), ''), 'Utf8') AS score,
       arrow_cast(coalesce(a.error, CASE WHEN a.current THEN '' ELSE 'from before the last change' END), 'Utf8') AS note,
       arrow_cast(a.band, 'Utf8') AS band,
       CASE WHEN a.band = 'green' THEN 'ok' ELSE 'warn' END AS bcls
FROM ATTEST() a
WHERE a.witness = $check
ORDER BY CASE band WHEN 'red' THEN 0 WHEN 'orange' THEN 1 WHEN 'yellow' THEN 2 WHEN 'error' THEN 3 ELSE 4 END, subject
