-- The standing checks: the adjudication plane, read whole. Each row
-- is a witness's live verdict — the expectation is the witness's own
-- declaration (detector @ threshold), measured and band come from
-- ATTEST(), never from glossary lifecycle states (current/stale/
-- contested belong to Coverage). A witness without a detector is a
-- speaker gate, not a check — it never appears here; witnessed slots
-- nobody spoke to are the agent's backlog, owed in Coverage. New
-- imports invalidate the measurements, so the
-- next read recomputes — nothing here is maintained by hand.
SELECT
  arrow_cast(a.witness, 'Utf8') AS checked,
  arrow_cast(a.subject || ' · ' || a.aspect, 'Utf8') AS what,
  arrow_cast(w.detector || coalesce(' @ ' || w.threshold, ''), 'Utf8') AS expectation,
  arrow_cast(CAST(a.score AS VARCHAR), 'Utf8') AS measured,
  arrow_cast(a.band, 'Utf8') AS band,
  CASE WHEN a.band = 'green' THEN 'ok' ELSE 'warn' END AS bcls
FROM ATTEST() a
JOIN witnesses w ON w.name = a.witness
ORDER BY CASE WHEN band = 'green' THEN 1 ELSE 0 END, checked, what
