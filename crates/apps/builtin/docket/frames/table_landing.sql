-- One table's landing, as lines: what the recipe read, what landed
-- and when, the rows it dropped where its shape can count them, the
-- cells its casts refused with the values they could not read, what
-- the source's conventions say, and the recipe's SQL. One row per
-- line that has something to say.
WITH latest AS (
  SELECT i.table_name, i.landed_rows, i.source_scans, i.dropped_rows_count, i.cast_failures, i.imported_at,
         row_number() OVER (ORDER BY i.imported_at DESC) AS rn
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  WHERE i.table_name = CAST($table AS VARCHAR)),
n AS (SELECT * FROM latest WHERE rn = 1),
scans AS (
  SELECT array_to_string(array_agg(
           json_get_str(json_get(n.source_scans, s.i), 'relation') || ', '
             || CAST(json_get_int(json_get(n.source_scans, s.i), 'rows') AS VARCHAR) || ' rows'
           ORDER BY s.i), ', joined to ') AS read
  FROM n CROSS JOIN generate_series(0, 19) AS s(i)
  WHERE s.i < json_length(n.source_scans)),
checked AS (
  SELECT json_get(json_get(n.cast_failures, 'checked'), c.i) AS chk
  FROM n CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(n.cast_failures, 'checked')),
failed AS (
  SELECT json_get_str(chk, 'column') AS col, json_get_int(chk, 'failed') AS failed, json_get(chk, 'tokens') AS tokens
  FROM checked WHERE json_get_int(chk, 'failed') > 0),
tops AS (
  SELECT f.col, f.failed,
         array_to_string(array_agg(
           json_get_str(json_get(f.tokens, t.j), 0) || ' ×' || CAST(json_get_int(json_get(f.tokens, t.j), 1) AS VARCHAR)
           ORDER BY t.j), ', ') AS top
  FROM failed f CROSS JOIN generate_series(0, 7) AS t(j)
  WHERE t.j < json_length(f.tokens)
  GROUP BY f.col, f.failed),
refused AS (
  SELECT array_to_string(array_agg(
           CAST(failed AS VARCHAR) || CASE WHEN failed = 1 THEN ' cell of ' ELSE ' cells of ' END || col
             || ': ' || coalesce(top, 'no values kept')
           ORDER BY failed DESC, col), chr(10)) AS lines
  FROM tops),
rec AS (
  SELECT max(r.source) AS source, max(CAST(r.sql AS VARCHAR)) AS sql
  FROM recipes r JOIN current_dataset d ON d.dataset = r.dataset
  WHERE r.table_name = CAST($table AS VARCHAR)),
conv AS (
  SELECT g.body,
         row_number() OVER (ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g CROSS JOIN rec
  WHERE g.aspect = 'conventions' AND g.subject = rec.source),
keyed AS (SELECT body, unnest(json_object_keys(body)) AS key FROM conv WHERE rn = 1),
said AS (
  SELECT array_to_string(array_agg(key || ': ' || coalesce(json_get_str(body, key), CAST(json_get(body, key) AS VARCHAR)) ORDER BY key), ' · ') AS lines
  FROM keyed WHERE key <> 'system'),
lines AS (
  SELECT 0 AS ord, 'read' AS label, coalesce(s.read, '') || coalesce(', from the source ' || rec.source, '') AS text FROM scans s CROSS JOIN rec
  UNION ALL
  SELECT 1, 'landed', CAST(coalesce(n.landed_rows, 0) AS VARCHAR) || ' rows on ' || to_char(CAST(n.imported_at AS TIMESTAMP), '%d %b %Y at %H:%M') FROM n
  UNION ALL
  SELECT 2, 'dropped', CAST(n.dropped_rows_count AS VARCHAR) || ' rows by the recipe' FROM n WHERE CAST(n.dropped_rows_count AS BIGINT) > 0
  UNION ALL
  SELECT 3, 'refused', r.lines FROM refused r WHERE r.lines IS NOT NULL
  UNION ALL
  SELECT 4, 'the source says', s.lines FROM said s WHERE s.lines IS NOT NULL
  UNION ALL
  SELECT 5, 'the recipe', rec.sql FROM rec WHERE rec.sql IS NOT NULL)
SELECT arrow_cast(label, 'Utf8') AS label, arrow_cast(text, 'Utf8') AS text
FROM lines
WHERE text IS NOT NULL AND text <> ''
ORDER BY ord
