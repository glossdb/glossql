-- The quality counts, one row: the tables standing (the newest
-- landing of each), the rows their recipes dropped at landing, the
-- checks whose band is red, and how many of the dataset's columns
-- carry any claim — a column of the dataset, so an app's parts,
-- dotted the same way, never count. `dropped_rows_count` is the
-- landing's own count, NULL where the recipe's shape could not say —
-- a NULL adds nothing here and the landing account says so. The
-- format's metadata tables beside each table (`<table>$history` and
-- kin) never count as columns.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
newest AS (
  SELECT i.dropped_rows_count
  FROM imports i
  JOIN latest l ON l.table_name = i.table_name AND l.imported_at = i.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset),
cols AS (
  SELECT count(*) AS n
  FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  WHERE strpos(c.table_name, '$') = 0),
judged AS (
  SELECT count(DISTINCT g.subject) AS n
  FROM GLOSSARY(all => true) g
  JOIN current_dataset d ON true
  JOIN information_schema.columns c
    ON c.table_schema = d.dataset AND c.table_name || '.' || c.column_name = g.subject
  WHERE g.kind = 'fact')
SELECT
  (SELECT count(*) FROM latest) AS tables,
  (SELECT coalesce(sum(CAST(dropped_rows_count AS BIGINT)), 0) FROM newest) AS dropped,
  (SELECT count(*) FROM ATTEST() WHERE band = 'red') AS red,
  arrow_cast((SELECT CAST(n AS VARCHAR) FROM judged) || ' / ' || (SELECT CAST(n AS VARCHAR) FROM cols), 'Utf8') AS judged
