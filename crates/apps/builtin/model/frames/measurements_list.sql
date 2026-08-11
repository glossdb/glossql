-- The measurement plane's live slots: what was measured, on what, by
-- which function. The dossier behind each link holds the payload.
SELECT g.subject, g.aspect, g.actor,
  arrow_cast(substr(g.written_at, 1, 10), 'Utf8') AS written,
  arrow_cast('?subject=' || g.subject, 'Utf8') AS link
FROM GLOSSARY(all => true) g
WHERE g.kind = 'measurement'
ORDER BY g.subject, g.aspect
