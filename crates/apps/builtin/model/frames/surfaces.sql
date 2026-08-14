-- The declared metric surfaces with their standing formula and the
-- validation chip (2026-08-13): open questions first, then human-ruled
-- with its date, then merely grounded. The rows and the formula are
-- `metric_surfaces`; the two counts scope to the bound dataset here,
-- because the glossary relation is workspace-wide and only this frame
-- knows which dataset it serves. The list is the navigation — each row
-- opens the metric's dossier.
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
       s.unit,
       s.kind AS mkind,
       s.formula,
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
       arrow_cast('/app/metrics?metric=' || s.metric, 'Utf8') AS link
FROM metric_surfaces s
LEFT JOIN asked q ON q.aspect = s.metric
LEFT JOIN ruled r ON r.aspect = s.metric
ORDER BY CASE WHEN s.kind = 'metric' THEN 0 ELSE 1 END, name
