-- The lineage graph's nodes: one row per column of every landed table
-- of the bound dataset, in ordinal order, with the column's judged
-- role (the collapsed `role` gloss, human over agent) — and one row
-- per served field of every current grounding, the metric as
-- `read.<name>()` and the field's role taken from the column it
-- descends from. The element folds columns nothing touches; the
-- rule is the element's, the rows are all of them.
--
-- The tables are the record's (`imports`), and the engine's schema
-- surface supplies their columns: information_schema alone also lists
-- the format's own metadata tables beside each landed one
-- (`<table>$history`, `$manifests`, `$snapshots`), which are nobody's
-- lineage.
WITH roles AS (
  SELECT subject, arrow_cast(json_get_str(value, 'value'), 'Utf8') AS role
  FROM GLOSSARY() WHERE aspect = 'role'
),
landed AS (
  SELECT DISTINCT i.table_name
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
),
cols AS (
  SELECT arrow_cast(c.table_name, 'Utf8') AS node,
         'table' AS kind,
         arrow_cast(c.column_name, 'Utf8') AS col,
         CAST(c.ordinal_position AS BIGINT) AS ord,
         coalesce(r.role, '') AS role
  FROM information_schema.columns c
  JOIN current_dataset d ON d.dataset = c.table_schema
  JOIN landed l ON l.table_name = c.table_name
  LEFT JOIN roles r ON r.subject = c.table_name || '.' || c.column_name
),
fields AS (
  SELECT arrow_cast('read.' || s.metric || '()', 'Utf8') AS node,
         'metric' AS kind,
         arrow_cast(s.field, 'Utf8') AS col,
         min(CAST(s.ord AS BIGINT)) AS ord,
         coalesce(max(r.role), '') AS role
  FROM (SELECT metric, field, source, row_number() OVER (PARTITION BY metric ORDER BY field) AS ord
        FROM metric_sources() WHERE field IS NOT NULL) s
  LEFT JOIN roles r ON r.subject = s.source
  GROUP BY s.metric, s.field
)
SELECT node, kind, col, ord, role FROM cols
UNION ALL
SELECT node, kind, col, ord, role FROM fields
ORDER BY kind, node, ord
