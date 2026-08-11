-- The declared benches: who may speak to each aspect, and which
-- detector adjudicates at what threshold. No detector means the slots
-- coexist uncontested.
SELECT w.name, w.aspect,
  arrow_cast(replace(replace(replace(w.speakers, '["', ''), '"]', ''), '","', ' + '), 'Utf8') AS speakers,
  coalesce(w.detector, '—') AS detector,
  arrow_cast(coalesce(CAST(w.threshold AS VARCHAR), ''), 'Utf8') AS threshold
FROM witnesses w
ORDER BY w.aspect, w.name
