-- The landing account, per table: the newest landing of each — what
-- each source scan held, what landed, what the recipe dropped, the
-- cells its casts nulled, and when. `source_scans` is per scan, and
-- listed as such: a join's scans summed would read as "what was read"
-- and is not. `dropped_rows_count` is the landing's own count, NULL
-- where the recipe's shape could not say — shown as not counted,
-- never as 0. A landing whose casts could not be accounted carries
-- the note beside its name. Numbers stay numbers; the row tile groups
-- their digits.
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
           json_get_str(json_get(n.source_scans, s.i), 'relation') || ' '
             || CAST(json_get_int(json_get(n.source_scans, s.i), 'rows') AS VARCHAR)
           ORDER BY s.i), ', ') AS scanned
  FROM newest n CROSS JOIN generate_series(0, 19) AS s(i)
  WHERE s.i < json_length(n.source_scans)
  GROUP BY n.table_name),
nulled AS (
  SELECT n.table_name,
         sum(json_get_int(json_get(json_get(n.cast_failures, 'checked'), c.i), 'failed')) AS nulled
  FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(n.cast_failures, 'checked')
  GROUP BY n.table_name)
SELECT arrow_cast(n.table_name, 'Utf8') AS name,
       arrow_cast(coalesce('scanned ' || s.scanned, ''), 'Utf8') AS scanned,
       coalesce(CAST(n.landed_rows AS BIGINT), 0) AS landed,
       CAST(n.dropped_rows_count AS BIGINT) AS dropped,
       arrow_cast(CASE WHEN n.dropped_rows_count IS NULL THEN '–' ELSE '' END, 'Utf8') AS dropped_dash,
       arrow_cast(CASE WHEN n.dropped_rows_count IS NULL THEN 'not counted' ELSE 'dropped' END, 'Utf8') AS dropped_label,
       nullif(coalesce(u.nulled, 0), 0) AS nulled,
       arrow_cast(CASE WHEN coalesce(u.nulled, 0) > 0 THEN '' ELSE '–' END, 'Utf8') AS dash,
       arrow_cast(CASE WHEN coalesce(u.nulled, 0) > 0 THEN 'nulled' ELSE '' END, 'Utf8') AS nulled_label,
       arrow_cast(CASE WHEN coalesce(u.nulled, 0) > 0 THEN 'r-gap' ELSE '' END, 'Utf8') AS ncls,
       arrow_cast(coalesce('casts unchecked: ' || json_get_str(n.cast_failures, 'unchecked'), ''), 'Utf8') AS casts,
       arrow_cast(replace(substr(CAST(n.imported_at AS VARCHAR), 1, 16), 'T', ' '), 'Utf8') AS landed_at
FROM newest n
LEFT JOIN scans s ON s.table_name = n.table_name
LEFT JOIN nulled u ON u.table_name = n.table_name
ORDER BY landed DESC, name
