-- The views: every derived relation of the dataset — a QUERY aspect
-- declared `x-kind: relation`, governed SQL the metrics compose from
-- — `metric_surfaces`, the record. A view serves rows, not a series:
-- it has no number here and the cube abstains on it by design, so
-- this list says what each is (the definitions entry's meaning) and
-- where its judgment stands, by the pulse's chip rule so the two
-- lists cannot disagree. The link carries the kind: the metric page
-- draws a relation without the series tiles, with its SQL and its
-- assumptions. The export tab serves every view as a file. `$stopped`
-- picks the served views (0) or the stopped ones (1), which carry
-- the author's reason in place of a meaning and fold under the list.
WITH asked AS (
  SELECT q.aspect, count(*) AS n FROM open_questions q
  JOIN current_dataset d ON d.dataset = q.dataset GROUP BY q.aspect
),
ruled AS (
  SELECT r.aspect, max(r.written_at) AS at FROM ruling_entries r
  JOIN current_dataset d ON d.dataset = r.dataset GROUP BY r.aspect
)
SELECT s.title,
       s.name,
       arrow_cast(CASE WHEN s.stopped <> '' THEN 'stopped: ' || s.stopped
                       ELSE coalesce(nullif(s.meaning, ''), 'no definitions entry yet') END, 'Utf8') AS meaning,
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
       arrow_cast('?metric=' || s.name || '&kind=relation', 'Utf8') AS link
FROM metric_surfaces s
LEFT JOIN asked q ON q.aspect = s.name
LEFT JOIN ruled r ON r.aspect = s.name
WHERE s.kind = 'relation'
  AND (s.stopped <> '') = (coalesce($stopped, '0') = '1')
ORDER BY name
