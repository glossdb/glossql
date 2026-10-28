-- What a view serves: every column of the page's grounding — the
-- view the catalog holds under the metric's name — in its own order,
-- with the judged role of the column it descends from and that
-- column, by the provenance walk the cube runs (metric_sources()). A
-- field computed from several columns names them all; a field no one
-- column reaches — a constant, an expression over several — says so
-- and shows the engine's type in place of a role. The collapsed role
-- gloss, a human's over the agent's.
WITH roles AS (
  SELECT subject, arrow_cast(json_get_str(value, 'value'), 'Utf8') AS role
  FROM GLOSSARY() WHERE aspect = 'role'
),
prov AS (
  SELECT DISTINCT s.field, s.source
  FROM metric_sources() s
  WHERE s.metric = $metric AND s.field IS NOT NULL
),
folded AS (
  SELECT p.field,
         array_to_string(array_agg(p.source ORDER BY p.source), ', ') AS sources,
         array_to_string(array_agg(DISTINCT r.role), ', ') AS roles
  FROM prov p LEFT JOIN roles r ON r.subject = p.source
  GROUP BY p.field
),
cols AS (
  SELECT c.column_name, c.ordinal_position,
         CASE
           WHEN c.data_type LIKE 'Utf8%' THEN 'text'
           WHEN c.data_type LIKE 'Int%' OR c.data_type LIKE 'UInt%' OR c.data_type LIKE 'Float%' OR c.data_type LIKE 'Decimal%' THEN 'number'
           WHEN c.data_type LIKE 'Date%' THEN 'date'
           WHEN c.data_type LIKE 'Timestamp%' THEN 'timestamp'
           WHEN c.data_type = 'Boolean' THEN 'flag'
           ELSE lower(c.data_type) END AS typed
  FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  WHERE c.table_name = $metric
)
SELECT arrow_cast(c.column_name, 'Utf8') AS field,
       arrow_cast(coalesce(nullif(f.roles, ''), c.typed), 'Utf8') AS role,
       arrow_cast(coalesce(f.sources, 'computed, no one column'), 'Utf8') AS sources
FROM cols c
LEFT JOIN folded f ON f.field = c.column_name
ORDER BY c.ordinal_position
