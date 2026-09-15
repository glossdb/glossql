-- The landed tables of the bound dataset: the newest landing per
-- table is the table — a re-landing drops the old one first — with
-- its rows, the cells its casts nulled, and when. Column counts come
-- from the engine's own schema surface, joined by the landed name so
-- the format's metadata tables beside each (`<table>$history` and
-- kin) never count. The file links are the download door's paths;
-- the rest is wording. Numbers stay numbers — the row tile groups
-- their digits.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
nulled AS (
  SELECT i.table_name, i.imported_at,
         sum(json_get_int(json_get(json_get(i.cast_failures, 'checked'), c.i), 'failed')) AS nulled
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(i.cast_failures, 'checked')
  GROUP BY i.table_name, i.imported_at),
cols AS (
  SELECT c.table_name, count(*) AS columns
  FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  GROUP BY c.table_name)
SELECT arrow_cast(l.table_name, 'Utf8') AS name,
       coalesce(c.columns, 0) AS columns,
       arrow_cast(CASE WHEN coalesce(c.columns, 0) = 1 THEN 'column' ELSE 'columns' END, 'Utf8') AS columns_label,
       coalesce(CAST(i.landed_rows AS BIGINT), 0) AS rows,
       nullif(coalesce(n.nulled, 0), 0) AS nulled,
       arrow_cast(CASE WHEN coalesce(n.nulled, 0) > 0 THEN '' ELSE '–' END, 'Utf8') AS dash,
       arrow_cast(CASE WHEN coalesce(n.nulled, 0) > 0 THEN 'nulled' ELSE '' END, 'Utf8') AS nulled_label,
       arrow_cast(CASE WHEN coalesce(n.nulled, 0) > 0 THEN 'r-gap' ELSE '' END, 'Utf8') AS ncls,
       arrow_cast(replace(substr(CAST(l.imported_at AS VARCHAR), 1, 16), 'T', ' '), 'Utf8') AS landed,
       arrow_cast('/' || d.dataset || '/app/export/' || l.table_name || '.csv', 'Utf8') AS csv,
       arrow_cast('/' || d.dataset || '/app/export/' || l.table_name || '.parquet', 'Utf8') AS parquet
FROM latest l
JOIN imports i ON i.table_name = l.table_name AND i.imported_at = l.imported_at
JOIN current_dataset d ON d.dataset = i.dataset
LEFT JOIN nulled n ON n.table_name = l.table_name AND n.imported_at = l.imported_at
LEFT JOIN cols c ON c.table_name = l.table_name
ORDER BY rows DESC, name
