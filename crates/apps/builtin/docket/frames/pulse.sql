-- The pulse: every metric surface with its validation chip. The rows
-- are `metric_surfaces` — the record; the numbers beside them (latest
-- period, the move into it, the admitted axes) are `frames/latest`, a
-- data frame the page joins to these rows by `name`, the metric's
-- key. The defaults below stand until it arrives, so the two frames
-- keep their classes: a ruling refreshes this one and leaves the cube
-- alone. What is left here is formatting — the chip's wording and
-- colour. The open count is the docket's own, so the chip and the
-- queue cannot disagree. Both counts scope to the bound dataset: the
-- glossary relation is workspace-wide, and `current_dataset` is what
-- names the one this session is on.
WITH asked AS (
  SELECT q.aspect, count(*) AS n FROM open_questions q
  JOIN current_dataset d ON d.dataset = q.dataset GROUP BY q.aspect
),
ruled AS (
  SELECT r.aspect, max(r.written_at) AS at FROM ruling_entries r
  JOIN current_dataset d ON d.dataset = r.dataset GROUP BY r.aspect
)
SELECT s.title,
       s.metric AS name,
       s.kind AS mkind,
       s.unit,
       arrow_cast('', 'Utf8') AS period,
       arrow_cast('—', 'Utf8') AS latest,
       arrow_cast('', 'Utf8') AS delta,
       arrow_cast('no axes admitted', 'Utf8') AS axes,
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
