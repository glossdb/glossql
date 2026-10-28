-- The quality counts, one row: the corridor verdict, dataset-wide
-- from the bands witness ('not run' is honest absence, not green);
-- the standing checks — how many are not green, of how many declared;
-- the cells the newest landings' casts nulled; and how many of the
-- dataset's columns carry any claim — a column of the dataset, so an
-- app's parts, dotted the same way, never count. The tables standing
-- and the rows dropped ride along for a page that wants them. The
-- format's metadata tables beside each table (`<table>$history` and
-- kin) never count as columns.
WITH latest AS (
  SELECT i.table_name, max(i.imported_at) AS imported_at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name),
newest AS (
  SELECT i.dropped_rows_count, i.cast_failures
  FROM imports i
  JOIN latest l ON l.table_name = i.table_name AND l.imported_at = i.imported_at
  JOIN current_dataset d ON d.dataset = i.dataset),
nulled AS (
  SELECT sum(json_get_int(json_get(json_get(n.cast_failures, 'checked'), c.i), 'failed')) AS n
  FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
  WHERE c.i < json_length(n.cast_failures, 'checked')),
checks AS (
  SELECT count(*) AS total, count(*) FILTER (WHERE worst > 0) AS bad
  FROM (SELECT w.name,
               coalesce(max(CASE a.band WHEN 'green' THEN 0 WHEN 'error' THEN 1 WHEN 'yellow' THEN 2
                                        WHEN 'orange' THEN 3 WHEN 'red' THEN 4 END), -1) AS worst
        FROM witnesses w LEFT JOIN ATTEST() a ON a.witness = w.name
        WHERE w.detector IS NOT NULL
        GROUP BY w.name)),
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
  arrow_cast(coalesce(
    (SELECT max(band) FROM ATTEST() WHERE aspect = 'metric_bands'),
    'not run'), 'Utf8') AS corridor,
  arrow_cast((SELECT CASE WHEN total = 0 THEN 'none declared'
                          ELSE CAST(bad AS VARCHAR) || ' of ' || CAST(total AS VARCHAR) || ' not green' END
              FROM checks), 'Utf8') AS checks,
  (SELECT coalesce(CAST(n AS BIGINT), 0) FROM nulled) AS nulled,
  arrow_cast((SELECT CAST(n AS VARCHAR) FROM judged) || ' / ' || (SELECT CAST(n AS VARCHAR) FROM cols), 'Utf8') AS judged,
  (SELECT count(*) FROM latest) AS tables,
  (SELECT coalesce(sum(CAST(dropped_rows_count AS BIGINT)), 0) FROM newest) AS dropped
