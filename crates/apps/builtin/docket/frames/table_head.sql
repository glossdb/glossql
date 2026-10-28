-- One table's head: what one row is, its grain and time axis from
-- the entity entry (a person's over the agent's), whether it is a
-- fact or a dimension table, how many rows and columns it holds,
-- what its newest landing read and when, the source it came from,
-- and the metrics that read it. One row, or none for a name no
-- landed table carries.
WITH e AS (
  SELECT g.body, g.actor_kind, g.written_at,
         row_number() OVER (ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect = 'entity' AND g.subject = CAST($table AS VARCHAR)),
latest AS (
  SELECT i.table_name, i.landed_rows, i.source_scans, i.imported_at,
         row_number() OVER (ORDER BY i.imported_at DESC) AS rn
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  WHERE i.table_name = CAST($table AS VARCHAR)),
scans AS (
  SELECT array_to_string(array_agg(
           json_get_str(json_get(n.source_scans, s.i), 'relation') || ' ('
             || CAST(json_get_int(json_get(n.source_scans, s.i), 'rows') AS VARCHAR) || ' rows)'
           ORDER BY s.i), ' joined to ') AS read
  FROM latest n CROSS JOIN generate_series(0, 19) AS s(i)
  WHERE n.rn = 1 AND s.i < json_length(n.source_scans)),
cols AS (
  SELECT count(*) AS n FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  WHERE c.table_name = CAST($table AS VARCHAR)),
reads AS (
  SELECT array_to_string(array_agg(DISTINCT coalesce(json_get_str(a.schema, 'title'), ms.metric)), ', ') AS metrics
  FROM metric_sources() ms LEFT JOIN aspects a ON a.name = ms.metric
  WHERE ms.table_name = CAST($table AS VARCHAR)),
src AS (
  SELECT max(r.source) AS source FROM recipes r JOIN current_dataset d ON d.dataset = r.dataset
  WHERE r.table_name = CAST($table AS VARCHAR))
SELECT arrow_cast(l.table_name, 'Utf8') AS name,
       arrow_cast(coalesce(json_get_str(e.body, 'value'), 'no entity entry yet: what one row is has not been said'), 'Utf8') AS entity,
       arrow_cast(concat_ws(' · ',
         CASE json_get_str(e.body, 'role') WHEN 'fact' THEN 'a fact table' WHEN 'dimension' THEN 'a dimension table' END,
         'one row per ' || regexp_replace(replace(replace(replace(CAST(json_get(e.body, 'grain') AS VARCHAR), '"', ''), '[', ''), ']', ''), ',[[:space:]]*', ', ', 'g'),
         'time axis ' || json_get_str(e.body, 'time_axis')), 'Utf8') AS shape,
       coalesce(CAST(l.landed_rows AS BIGINT), 0) AS rows,
       c.n AS columns,
       arrow_cast(coalesce(s.read, ''), 'Utf8') AS read,
       arrow_cast(to_char(CAST(l.imported_at AS TIMESTAMP), '%d %b %Y %H:%M'), 'Utf8') AS at,
       arrow_cast(coalesce(src.source, ''), 'Utf8') AS source,
       arrow_cast(coalesce('read by ' || r.metrics, ''), 'Utf8') AS read_by,
       arrow_cast(CASE WHEN e.actor_kind = 'human' THEN 'ruled' ELSE '' END, 'Utf8') AS mark,
       arrow_cast(CASE WHEN e.actor_kind = 'human' THEN 'a person''s word, ' || to_char(CAST(e.written_at AS TIMESTAMP), '%d %b %Y %H:%M') ELSE '' END, 'Utf8') AS mark_title
FROM latest l
CROSS JOIN cols c
CROSS JOIN scans s
CROSS JOIN reads r
CROSS JOIN src
LEFT JOIN e ON e.rn = 1
WHERE l.rn = 1
