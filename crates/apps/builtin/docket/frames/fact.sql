-- A declared fact's value (`x-kind: fact`), served whole by
-- `fact_values()` — one row when the frame is one row with a `value`,
-- none for a series, a relation, or a fact that served no number, so
-- the tile that reads this renders only where there is a number to
-- show. Beside the number: the unit and meaning the definition
-- carries, and the as-of — the newest landing of the tables the
-- grounding scans. The number stays a number; the tile groups its
-- digits.
WITH reads AS (
  SELECT DISTINCT table_name AS t
  FROM metric_sources()
  WHERE metric = CAST($metric AS VARCHAR) AND table_name IS NOT NULL
),
asof AS (
  SELECT max(CAST(i.imported_at AS VARCHAR)) AS at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  JOIN reads r ON r.t = i.table_name
)
SELECT f.metric,
       f.value,
       arrow_cast(coalesce(s.unit, ''), 'Utf8') AS unit,
       arrow_cast(coalesce(s.meaning, ''), 'Utf8') AS meaning,
       arrow_cast(coalesce('as of ' || replace(substr(a.at, 1, 16), 'T', ' '), ''), 'Utf8') AS asof
FROM fact_values() f
LEFT JOIN metric_surfaces s ON s.name = f.metric
CROSS JOIN asof a
WHERE f.metric = CAST($metric AS VARCHAR) AND f.value IS NOT NULL
