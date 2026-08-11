-- Every live fact claim, an index: subject, aspect, a preview of the
-- value. The preview is navigation, not the record — the dossier
-- behind the link holds the full text.
SELECT g.subject, g.aspect,
  coalesce(json_get_str(g.body, 'value'), arrow_cast(substr(g.body, 1, 120), 'Utf8')) AS what,
  arrow_cast('?subject=' || g.subject, 'Utf8') AS link
FROM GLOSSARY(all => true) g
WHERE g.kind = 'fact'
ORDER BY g.subject, g.aspect
