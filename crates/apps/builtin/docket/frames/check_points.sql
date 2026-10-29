-- A corridor check's evidence: every walked metric's newest complete
-- point — the one the bands detector scores — with its actual, its
-- corridor, how far out it sits and on which side, the farthest out
-- first. The band beside each point is the detector's own rule on
-- that one displacement (green to 0.8, yellow to 0.9, orange to the
-- threshold, red past it); the verdict's score is the worst of them
-- raised to their number. A point whose PIT is withheld carries the
-- walk's reason; a metric the walk found inapplicable carries its
-- reason and no numbers. `band_points()` is the recorded walk, no
-- re-run. Rows only for a witness whose detector is band_breach, so
-- another check's page draws nothing here.
WITH w AS (
  SELECT name, CAST(coalesce(threshold, '0.98') AS DOUBLE) AS threshold
  FROM witnesses WHERE name = $check AND detector = 'band_breach'
),
newest AS (
  SELECT metric, applicable, reason, period, actual, p10, p90, pit, displacement AS far, withheld,
         row_number() OVER (PARTITION BY metric ORDER BY point_seq DESC) AS recency
  FROM band_points()
  WHERE NOT applicable OR (partial = false AND period IS NOT NULL)
),
-- The axis that moved most between the previous and the newest
-- complete month, per metric, from the cube's cells: a point's link
-- opens the metric sliced by it, so the page beyond shows where the
-- move is rather than the bare total.
cells AS (
  SELECT s.metric AS name, s.dimension AS axis, s.member, to_char(s.period, '%Y-%m') AS ym, s.value
  FROM metric_series(grain => 'month') s
  WHERE s.world = 'actual' AND s.dimension NOT IN ('', 'alternative')
),
moves AS (
  SELECT c.name, c.axis, c.member,
         coalesce(max(CASE WHEN c.ym = n.period THEN c.value END), 0)
           - coalesce(max(CASE WHEN c.ym = p.period THEN c.value END), 0) AS delta
  FROM cells c
  JOIN newest n ON n.metric = c.name AND n.recency = 1
  LEFT JOIN newest p ON p.metric = c.name AND p.recency = 2
  WHERE c.ym = n.period OR c.ym = p.period
  GROUP BY c.name, c.axis, c.member
),
top AS (
  SELECT name, axis, row_number() OVER (PARTITION BY name ORDER BY abs(delta) DESC, axis, member) AS rn
  FROM moves
)
SELECT arrow_cast(coalesce(json_get_str(a.schema, 'title'), n.metric), 'Utf8') AS title,
       arrow_cast(n.metric, 'Utf8') AS metric,
       arrow_cast(coalesce(n.period, ''), 'Utf8') AS period,
       arrow_cast(CASE WHEN n.actual IS NULL THEN ''
                       WHEN abs(n.actual) < 10 THEN CAST(round(n.actual * 1000) / 1000 AS VARCHAR)
                       ELSE CAST(CAST(round(n.actual) AS BIGINT) AS VARCHAR) END, 'Utf8') AS actual,
       arrow_cast(CASE WHEN n.p10 IS NULL THEN ''
                       WHEN abs(n.p90) < 10 THEN CAST(round(n.p10 * 1000) / 1000 AS VARCHAR) || ' to ' || CAST(round(n.p90 * 1000) / 1000 AS VARCHAR)
                       ELSE CAST(CAST(round(n.p10) AS BIGINT) AS VARCHAR) || ' to ' || CAST(CAST(round(n.p90) AS BIGINT) AS VARCHAR) END, 'Utf8') AS corridor,
       arrow_cast(coalesce(CAST(round(n.far * 1000) / 1000 AS VARCHAR), ''), 'Utf8') AS displacement,
       arrow_cast(CASE
         WHEN NOT n.applicable THEN 'not walked'
         WHEN n.far IS NULL THEN 'withheld'
         WHEN n.far >= 0.95 AND n.pit >= 0.5 THEN 'above'
         WHEN n.far >= 0.95 THEN 'below'
         ELSE 'inside' END, 'Utf8') AS side,
       arrow_cast(CASE
         WHEN NOT n.applicable OR n.far IS NULL THEN 'none'
         WHEN n.far <= 0.8 THEN 'green'
         WHEN n.far <= 0.9 THEN 'yellow'
         WHEN n.far <= w.threshold THEN 'orange'
         ELSE 'red' END, 'Utf8') AS band,
       CASE
         WHEN NOT n.applicable OR n.far IS NULL THEN ''
         WHEN n.far <= 0.8 THEN 'ok'
         ELSE 'warn' END AS bcls,
       arrow_cast(coalesce(CASE WHEN NOT n.applicable THEN n.reason ELSE n.withheld END, ''), 'Utf8') AS note,
       arrow_cast('/' || d.dataset || '/app/docket/p/metrics?metric=' || n.metric || coalesce('&dim=' || t.axis, ''), 'Utf8') AS link
FROM newest n
CROSS JOIN w
CROSS JOIN current_dataset d
LEFT JOIN aspects a ON a.name = n.metric
LEFT JOIN top t ON t.name = n.metric AND t.rn = 1
WHERE n.recency = 1
ORDER BY n.far DESC NULLS LAST, metric
