-- One column's head: its table, its engine type, its judged role and
-- meaning (a person's over the agent's), the metrics that read it,
-- and the cells its cast refused at the table's newest landing. One
-- row, or none for a name no landed column carries.
WITH slots AS (
  SELECT g.aspect, g.actor_kind, g.body,
         row_number() OVER (PARTITION BY g.aspect
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.subject = CAST($column AS VARCHAR) AND g.aspect IN ('role', 'meaning')),
cur AS (SELECT aspect, json_get_str(body, 'value') AS value FROM slots WHERE rn = 1),
col AS (
  SELECT c.table_name, c.column_name, c.data_type
  FROM information_schema.columns c JOIN current_dataset d ON d.dataset = c.table_schema
  WHERE c.table_name || '.' || c.column_name = CAST($column AS VARCHAR)),
reads AS (
  SELECT array_to_string(array_agg(DISTINCT coalesce(json_get_str(a.schema, 'title'), ms.metric)), ', ') AS metrics
  FROM metric_sources() ms LEFT JOIN aspects a ON a.name = ms.metric
  WHERE ms.source = CAST($column AS VARCHAR)),
newest AS (
  SELECT i.cast_failures, row_number() OVER (ORDER BY i.imported_at DESC) AS rn
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset JOIN col ON col.table_name = i.table_name),
refused AS (
  SELECT json_get_int(chk, 'failed') AS failed,
         array_to_string(array_agg(json_get_str(json_get(json_get(chk, 'tokens'), t.j), 0) || ' ×'
           || CAST(json_get_int(json_get(json_get(chk, 'tokens'), t.j), 1) AS VARCHAR) ORDER BY t.j), ', ') AS top
  FROM (SELECT json_get(json_get(n.cast_failures, 'checked'), c.i) AS chk
        FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
        WHERE n.rn = 1 AND c.i < json_length(n.cast_failures, 'checked')) x
  CROSS JOIN generate_series(0, 7) AS t(j)
  CROSS JOIN col
  WHERE json_get_str(chk, 'column') = col.column_name AND json_get_int(chk, 'failed') > 0
    AND t.j < json_length(chk, 'tokens')
  GROUP BY json_get_int(chk, 'failed'))
SELECT arrow_cast(col.table_name, 'Utf8') AS table_name,
       arrow_cast(col.column_name, 'Utf8') AS name,
       arrow_cast(lower(col.data_type), 'Utf8') AS typed,
       arrow_cast(coalesce(r.value, 'no role judged'), 'Utf8') AS role,
       arrow_cast(coalesce(m.value, 'not yet said'), 'Utf8') AS meaning,
       arrow_cast(coalesce('read by ' || rd.metrics, ''), 'Utf8') AS read_by,
       arrow_cast(coalesce(CAST(f.failed AS VARCHAR) || ' cells refused at the newest landing: ' || f.top, ''), 'Utf8') AS refused,
       arrow_cast('?table=' || col.table_name, 'Utf8') AS link_table
FROM col
CROSS JOIN reads rd
LEFT JOIN cur r ON r.aspect = 'role'
LEFT JOIN cur m ON m.aspect = 'meaning'
LEFT JOIN refused f ON true
