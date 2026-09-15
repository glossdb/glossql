-- The current facts: every QUERY aspect declared `x-kind: fact` —
-- `metric_surfaces`, the record — with the one number its grounding
-- serves (`fact_values()`) and an as-of: the newest landing of the
-- tables the grounding scans, from `metric_sources()` and `imports`.
-- A fact has no period and no move; it stands as of the data it was
-- read from, and that is the date to show. Where the fact served no
-- number, the read's reason takes its place. Status and colour follow
-- the pulse's rule, so the two lists cannot disagree. The link
-- carries the kind: the metric page draws a fact without the series
-- tiles.
WITH asked AS (
  SELECT q.aspect, count(*) AS n FROM open_questions q
  JOIN current_dataset d ON d.dataset = q.dataset GROUP BY q.aspect
),
ruled AS (
  SELECT r.aspect, max(r.written_at) AS at FROM ruling_entries r
  JOIN current_dataset d ON d.dataset = r.dataset GROUP BY r.aspect
),
reads AS (
  SELECT DISTINCT metric, table_name AS t
  FROM metric_sources() WHERE table_name IS NOT NULL
),
landed AS (
  SELECT i.table_name, max(CAST(i.imported_at AS VARCHAR)) AS at
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  GROUP BY i.table_name
),
asof AS (
  SELECT r.metric, max(l.at) AS at
  FROM reads r JOIN landed l ON l.table_name = r.t
  GROUP BY r.metric
)
SELECT s.name,
       s.title,
       s.unit,
       s.meaning,
       f.value,
       arrow_cast(coalesce(f.reason, ''), 'Utf8') AS note,
       arrow_cast(coalesce('as of ' || replace(substr(a.at, 1, 16), 'T', ' '), ''), 'Utf8') AS asof,
       arrow_cast(CASE
         WHEN s.stopped <> '' THEN 'stopped'
         WHEN coalesce(q.n, 0) > 0 THEN CAST(q.n AS VARCHAR) || ' open'
         WHEN r.at IS NOT NULL THEN 'human-ruled ' || substr(r.at, 1, 10)
         WHEN NOT s.grounded THEN 'nothing recorded'
         ELSE 'grounded' END, 'Utf8') AS status,
       CASE
         WHEN s.stopped <> '' THEN 'warn'
         WHEN coalesce(q.n, 0) > 0 THEN 'warn'
         WHEN r.at IS NOT NULL THEN 'ok'
         WHEN NOT s.grounded THEN 'warn'
         ELSE 'ok' END AS scls,
       arrow_cast('?metric=' || s.name || '&kind=fact', 'Utf8') AS link
FROM metric_surfaces s
LEFT JOIN fact_values() f ON f.metric = s.name
LEFT JOIN asof a ON a.metric = s.name
LEFT JOIN asked q ON q.aspect = s.name
LEFT JOIN ruled r ON r.aspect = s.name
WHERE s.kind = 'fact'
ORDER BY s.name
