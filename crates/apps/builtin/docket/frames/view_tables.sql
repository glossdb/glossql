-- The tables a view reads, as one line: every landed table the
-- grounding's provenance walk reaches (metric_sources()), in name
-- order. One row, or none for a grounding that reads nothing landed.
WITH t AS (
  SELECT DISTINCT table_name FROM metric_sources()
  WHERE metric = $metric AND table_name IS NOT NULL
)
SELECT arrow_cast(array_to_string(array_agg(table_name ORDER BY table_name), ', '), 'Utf8') AS tables
FROM t
HAVING count(*) > 0
