-- The pulse: every metric surface with its latest cube month, the
-- month-over-month move, the axes, and a validation chip. The rows are
-- `metric_surfaces`; what is left here is formatting — rounding, the
-- percentage, the em dash for a metric the cube has not reached, and
-- the chip's wording and colour. The open count is the docket's own,
-- so the chip and the queue cannot disagree. Both counts scope to the
-- bound dataset: the glossary relation is workspace-wide.
WITH asked AS (
  SELECT aspect, count(*) AS n FROM open_questions
  WHERE dataset = CAST($dataset AS VARCHAR) GROUP BY aspect
),
ruled AS (
  SELECT aspect, max(written_at) AS at FROM ruling_entries
  WHERE dataset = CAST($dataset AS VARCHAR) GROUP BY aspect
)
SELECT s.title,
       s.metric AS name,
       s.kind AS mkind,
       s.unit,
       arrow_cast(coalesce(s.period, ''), 'Utf8') AS period,
       arrow_cast(coalesce(CAST(round(s.value, 1) AS VARCHAR), '—'), 'Utf8') AS latest,
       arrow_cast(CASE
         WHEN s.delta IS NULL OR s.value IS NULL THEN ''
         WHEN s.delta >= 0 THEN '+' || CAST(round(100.0 * s.delta / nullif(s.value - s.delta, 0), 1) AS VARCHAR) || '%'
         ELSE CAST(round(100.0 * s.delta / nullif(s.value - s.delta, 0), 1) AS VARCHAR) || '%'
       END, 'Utf8') AS delta,
       arrow_cast(coalesce(s.axes, 'no axes admitted'), 'Utf8') AS axes,
       arrow_cast(CASE
         WHEN coalesce(q.n, 0) > 0 THEN CAST(q.n AS VARCHAR) || ' open'
         WHEN r.at IS NOT NULL THEN 'human-ruled ' || substr(r.at, 1, 10)
         WHEN NOT s.grounded THEN 'nothing recorded'
         ELSE 'grounded' END, 'Utf8') AS status,
       CASE
         WHEN coalesce(q.n, 0) > 0 THEN 'warn'
         WHEN r.at IS NOT NULL THEN 'ok'
         WHEN NOT s.grounded THEN 'warn'
         ELSE 'ok' END AS scls,
       arrow_cast('?metric=' || s.metric, 'Utf8') AS link
FROM metric_surfaces s
LEFT JOIN asked q ON q.aspect = s.metric
LEFT JOIN ruled r ON r.aspect = s.metric
ORDER BY CASE WHEN s.kind = 'metric' THEN 0 ELSE 1 END, name
