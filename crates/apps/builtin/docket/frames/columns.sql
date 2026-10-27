-- The Explore tab's left column: every landed table with what one row
-- is, then its columns folded as lines — the column, its judged role
-- (the engine's type where none is judged), its behaviour and unit
-- where it has them, and what it means. Roles and meanings are the
-- collapsed record: a human's entry outranks the agent's, the newest
-- of each wins. Landed tables alone: the served reads are views in
-- the schema surface too, and the Metrics tab is theirs; the format's
-- own metadata tables beside each landed one stay out as well. One
-- row per table, so the page reads as a dictionary.
WITH g AS (
  SELECT g.subject, g.aspect, g.body,
         row_number() OVER (PARTITION BY g.subject, g.aspect
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect IN ('role', 'meaning', 'unit', 'behavior', 'entity')
),
cur AS (SELECT subject, aspect, json_get_str(body, 'value') AS value FROM g WHERE rn = 1),
cols AS (
  SELECT c.table_name, c.column_name, c.ordinal_position,
         CASE
           WHEN c.data_type LIKE 'Utf8%' THEN 'text'
           WHEN c.data_type LIKE 'Int%' OR c.data_type LIKE 'UInt%' OR c.data_type LIKE 'Float%' OR c.data_type LIKE 'Decimal%' THEN 'number'
           WHEN c.data_type LIKE 'Date%' THEN 'date'
           WHEN c.data_type LIKE 'Timestamp%' THEN 'timestamp'
           WHEN c.data_type = 'Boolean' THEN 'flag'
           ELSE lower(c.data_type) END AS typed,
         r.value AS role, m.value AS meaning, u.value AS unit, b.value AS behavior
  FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  LEFT JOIN cur r ON r.aspect = 'role' AND r.subject = c.table_name || '.' || c.column_name
  LEFT JOIN cur m ON m.aspect = 'meaning' AND m.subject = c.table_name || '.' || c.column_name
  LEFT JOIN cur u ON u.aspect = 'unit' AND u.subject = c.table_name || '.' || c.column_name
  LEFT JOIN cur b ON b.aspect = 'behavior' AND b.subject = c.table_name || '.' || c.column_name
  WHERE c.table_name NOT LIKE '%$%'
    AND c.table_name IN (SELECT i.table_name FROM imports i JOIN current_dataset d2 ON d2.dataset = i.dataset)
),
folded AS (
  SELECT table_name, count(*) AS n,
         array_to_string(array_agg(
           column_name || '  ·  ' || coalesce(role, typed)
             || coalesce('  ·  ' || behavior, '') || coalesce('  ·  ' || unit, '')
             || coalesce('  —  ' || meaning, '')
           ORDER BY ordinal_position), chr(10)) AS lines
  FROM cols GROUP BY table_name
),
latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset GROUP BY i.table_name
)
SELECT arrow_cast(f.table_name, 'Utf8') AS name,
       f.n AS columns,
       coalesce(CAST(i.landed_rows AS BIGINT), 0) AS rows,
       arrow_cast(coalesce(e.value, ''), 'Utf8') AS entity,
       arrow_cast(f.lines, 'Utf8') AS lines
FROM folded f
LEFT JOIN cur e ON e.aspect = 'entity' AND e.subject = f.table_name
LEFT JOIN latest l ON l.table_name = f.table_name
LEFT JOIN imports i ON i.table_name = l.table_name AND i.imported_at = l.imported_at
ORDER BY rows DESC, name
