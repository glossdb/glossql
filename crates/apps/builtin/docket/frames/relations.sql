-- The Query tab's relations: every landed table (the newest landing
-- is the table) and every served read of the dataset, each with its
-- rows where the record counts them, and a read that puts the first
-- fifty rows in the pane. The SQL rides the page URL, so the pick is
-- a link and a read is shareable. A stopped read serves nothing and
-- is not offered; a plan stands on its metric's page.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
tables AS (
  SELECT l.table_name AS name, 'table' AS kind,
         CAST(i.landed_rows AS BIGINT) AS n, '' AS what
  FROM latest l
  JOIN imports i ON i.table_name = l.table_name AND i.imported_at = l.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset),
reads AS (
  SELECT s.name,
         CASE WHEN s.kind IN ('', 'measure', 'metric') THEN 'metric'
              WHEN s.kind = 'relation' THEN 'view'
              ELSE s.kind END AS kind,
         CAST(NULL AS BIGINT) AS n, s.meaning AS what
  FROM metric_surfaces s
  WHERE s.grounded AND s.stopped = '' AND s.kind <> 'plan'),
all_ AS (SELECT * FROM tables UNION ALL SELECT * FROM reads)
SELECT arrow_cast(name, 'Utf8') AS name,
       arrow_cast(kind, 'Utf8') AS kind,
       arrow_cast(coalesce(CAST(n AS VARCHAR), ''), 'Utf8') AS rows,
       arrow_cast(coalesce(what, ''), 'Utf8') AS what,
       arrow_cast('?sql=SELECT * FROM ' || name || ' LIMIT 50', 'Utf8') AS link
FROM all_
ORDER BY CASE kind WHEN 'table' THEN 0 WHEN 'view' THEN 1 WHEN 'metric' THEN 2 ELSE 3 END, name
