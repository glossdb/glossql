-- The loads, one line per table: the newest landing of each — what
-- the recipe read (each source scan with its rows, a join's scans
-- joined, never summed), what landed and when, the rows the recipe
-- dropped where its shape can count them (a join's cannot, and says
-- nothing rather than a dash), and the cells its casts refused —
-- per column, how many and the values it could not read, most
-- frequent first (the landing keeps eight). A landing that could not
-- account for its casts says so. The table opens its page on the
-- Glossary tab. Numbers stay numbers; the row tile groups their
-- digits.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
newest AS (
  SELECT i.table_name, i.source_scans, i.landed_rows, i.dropped_rows_count,
         i.cast_failures, i.imported_at
  FROM imports i
  JOIN latest l ON l.table_name = i.table_name AND l.imported_at = i.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset),
scans AS (
  SELECT n.table_name,
         array_to_string(array_agg(
           json_get_str(json_get(n.source_scans, s.i), 'relation') || ' ('
             || CAST(json_get_int(json_get(n.source_scans, s.i), 'rows') AS VARCHAR) || ' rows)'
           ORDER BY s.i), ' joined to ') AS read
  FROM newest n CROSS JOIN generate_series(0, 19) AS s(i)
  WHERE s.i < json_length(n.source_scans)
  GROUP BY n.table_name),
checked AS (
  SELECT n.table_name, json_get(json_get(n.cast_failures, 'checked'), c.i) AS chk
  FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(n.cast_failures, 'checked')),
failed AS (
  SELECT table_name,
         json_get_str(chk, 'column') AS col,
         json_get_int(chk, 'failed') AS failed,
         json_get(chk, 'tokens') AS tokens
  FROM checked
  WHERE json_get_int(chk, 'failed') > 0),
tops AS (
  SELECT f.table_name, f.col, f.failed,
         array_to_string(array_agg(
           json_get_str(json_get(f.tokens, t.j), 0) || ' ×'
             || CAST(json_get_int(json_get(f.tokens, t.j), 1) AS VARCHAR)
           ORDER BY t.j), ', ') AS top
  FROM failed f CROSS JOIN generate_series(0, 7) AS t(j)
  WHERE t.j < json_length(f.tokens)
  GROUP BY f.table_name, f.col, f.failed),
lines AS (
  SELECT table_name,
         array_to_string(array_agg(
           CAST(failed AS VARCHAR) || CASE WHEN failed = 1 THEN ' cell of ' ELSE ' cells of ' END
             || col || ' refused: ' || coalesce(top, 'no values kept')
           ORDER BY failed DESC, col), chr(10)) AS refused
  FROM tops GROUP BY table_name)
SELECT arrow_cast(n.table_name, 'Utf8') AS name,
       arrow_cast(coalesce(s.read, ''), 'Utf8') AS read,
       coalesce(CAST(n.landed_rows AS BIGINT), 0) AS landed,
       arrow_cast(to_char(CAST(n.imported_at AS TIMESTAMP), '%d %b %Y %H:%M'), 'Utf8') AS at,
       arrow_cast(CASE WHEN CAST(n.dropped_rows_count AS BIGINT) > 0
                       THEN CAST(n.dropped_rows_count AS VARCHAR) || ' rows dropped by the recipe'
                       ELSE '' END, 'Utf8') AS dropped,
       arrow_cast(coalesce(li.refused, ''), 'Utf8') AS refused,
       arrow_cast(coalesce('casts unchecked: ' || json_get_str(n.cast_failures, 'unchecked'), ''), 'Utf8') AS casts,
       arrow_cast('/' || d.dataset || '/app/docket/p/glossary?table=' || n.table_name, 'Utf8') AS link
FROM newest n
CROSS JOIN current_dataset d
LEFT JOIN scans s ON s.table_name = n.table_name
LEFT JOIN lines li ON li.table_name = n.table_name
ORDER BY landed DESC, name
