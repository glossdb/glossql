-- The subject's fact claims with provenance, plus the claims the
-- model's rules owe on this subject and nobody wrote. Every row
-- carries the exact statement a writing runs, and the values the
-- aspect admits — read from its declared schema, never hardcoded.
-- The read exposes the actor id but not its kind yet, so every
-- written fact renders judged (◐); a human chain would look the same
-- until the read says who spoke. Nothing truncated.
WITH admit AS (
  SELECT name, kind,
    coalesce(
      replace(replace(replace(replace(
        json_as_text(json_get(json_get(json_get(schema, 'properties'), 'value'), 'enum')),
        '["', ''), '"]', ''), '", "', ' | '), '","', ' | '),
      'free text') AS admits
  FROM aspects
)
SELECT g.aspect,
  coalesce(json_get_str(g.body, 'value'), arrow_cast(g.body, 'Utf8')) AS text,
  arrow_cast(g.actor || ' · ' || substr(g.written_at, 1, 10) ||
    CASE WHEN json_get_str(g.body, 'grounds') IS NULL THEN ''
         ELSE ' · ' || json_get_str(g.body, 'grounds') END, 'Utf8') AS meta,
  '◐' AS glyph, 'g-jud' AS gcls, 'contest' AS act,
  coalesce(a.admits, 'free text') AS admits,
  'GLOSS ' || g.aspect || ' ON ' || g.subject || ' AS $$' || g.body || '$$;' AS statement
FROM GLOSSARY(all => true) g
LEFT JOIN admit a ON a.name = g.aspect
WHERE g.subject = $subject AND g.kind = 'fact'
UNION ALL
SELECT c.aspect,
  'owed and unwritten — the model''s rules want this claim' AS text,
  'unwritten' AS meta,
  '–' AS glyph, 'g-una' AS gcls, 'write' AS act,
  a.admits,
  'GLOSS ' || c.aspect || ' ON ' || c.subject || ' AS $${"value": "' ||
    CASE WHEN a.admits = 'free text' THEN '…' ELSE a.admits END || '"}$$;' AS statement
FROM GLOSSARY() c
JOIN admit a ON a.name = c.aspect AND a.kind = 'fact'
WHERE c.subject = $subject AND c.state = 'unassessed'
ORDER BY CASE act WHEN 'write' THEN 0 ELSE 1 END,
  CASE aspect
    WHEN 'meaning' THEN 0 WHEN 'entity' THEN 1 WHEN 'role' THEN 2
    WHEN 'behavior' THEN 3 WHEN 'unit' THEN 4 WHEN 'dimension' THEN 5
    ELSE 6 END, aspect
