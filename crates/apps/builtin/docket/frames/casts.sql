-- The cast failures of the newest landing of each table, one row per
-- column whose cast nulled a cell: how many, and the values it could
-- not read, most frequent first — the landing keeps eight. A landing
-- whose casts were not accounted has no row here; the landing account
-- says so beside the table.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
checked AS (
  SELECT i.table_name, i.imported_at,
         json_get(json_get(i.cast_failures, 'checked'), c.i) AS chk
  FROM imports i
  JOIN latest l ON l.table_name = i.table_name AND l.imported_at = i.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset
  CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(i.cast_failures, 'checked')),
failed AS (
  SELECT table_name, imported_at,
         json_get_str(chk, 'column') AS col,
         json_get_int(chk, 'failed') AS failed,
         json_get(chk, 'tokens') AS tokens
  FROM checked
  WHERE json_get_int(chk, 'failed') > 0),
tops AS (
  SELECT f.table_name, f.col,
         array_to_string(array_agg(
           json_get_str(json_get(f.tokens, t.j), 0) || ' ×'
             || CAST(json_get_int(json_get(f.tokens, t.j), 1) AS VARCHAR)
           ORDER BY t.j), ', ') AS top
  FROM failed f CROSS JOIN generate_series(0, 7) AS t(j)
  WHERE t.j < json_length(f.tokens)
  GROUP BY f.table_name, f.col)
SELECT arrow_cast(f.table_name || '.' || f.col, 'Utf8') AS subject,
       f.failed,
       arrow_cast(coalesce(t.top, ''), 'Utf8') AS top,
       arrow_cast(replace(substr(CAST(f.imported_at AS VARCHAR), 1, 16), 'T', ' '), 'Utf8') AS landed_at
FROM failed f
LEFT JOIN tops t ON t.table_name = f.table_name AND t.col = f.col
ORDER BY f.failed DESC, subject
