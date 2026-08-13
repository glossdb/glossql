-- The standing checks, workspace-wide (the sketch's table, 2026-08-13):
-- every declared witness beside what its detector currently holds per
-- subject. Expectation is the witness's own declaration; measured and
-- band are the live verdict; new imports invalidate the measurement,
-- so the next run recomputes — nothing here is maintained.
SELECT
  arrow_cast(w.name, 'Utf8') AS checked,
  arrow_cast(c.subject || ' · ' || w.aspect, 'Utf8') AS what,
  arrow_cast(coalesce(w.detector, '-') ||
    CASE WHEN w.threshold IS NOT NULL
         THEN ' @ ' || CAST(w.threshold AS VARCHAR) ELSE '' END, 'Utf8') AS expectation,
  arrow_cast(coalesce(CAST(c.score AS VARCHAR), c.state), 'Utf8') AS measured,
  arrow_cast(coalesce(c.band, c.state), 'Utf8') AS band,
  CASE WHEN c.band = 'green' THEN 'ok'
       WHEN c.state = 'unassessed' THEN 'warn'
       WHEN c.band IS NULL THEN ''
       ELSE 'warn' END AS bcls
FROM witnesses w
JOIN GLOSSARY() c ON c.aspect = w.aspect
ORDER BY CASE WHEN band = 'green' THEN 1 ELSE 0 END, checked, what
