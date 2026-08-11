-- What is inside a table subject: its column subjects, each with the
-- claimed meaning where one is written. Empty on a column dossier —
-- stated, not hidden. Relationship subjects carry '->' and are the
-- joins frame's business, excluded here.
SELECT
  arrow_cast(substr(c.subject, length($subject) + 2), 'Utf8') AS name,
  coalesce(json_get_str(m.body, 'value'), '') AS meaning,
  arrow_cast('?subject=' || c.subject, 'Utf8') AS link
FROM (SELECT DISTINCT subject FROM GLOSSARY(all => true)
      WHERE subject LIKE $subject || '.%' AND strpos(subject, '->') = 0) c
LEFT JOIN GLOSSARY(all => true) m
  ON m.subject = c.subject AND m.aspect = 'meaning' AND m.kind = 'fact'
ORDER BY name
