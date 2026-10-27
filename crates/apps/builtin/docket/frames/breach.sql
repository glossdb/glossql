-- Behind the corridor tile: every metric whose newest complete walked
-- point landed outside its own history's corridor — the point the
-- bands detector scores — with the actual, the corridor and which
-- side, linked to the metric's page. `band_points()` is the recorded
-- walk, no re-run; a point is outside when its PIT sits in the outer
-- 2.5% on either side (displacement 0.95 and over). A green corridor
-- serves no row, and the tile stands alone.
WITH newest AS (
  SELECT metric, period, actual, p10, p90, pit, displacement,
         row_number() OVER (PARTITION BY metric ORDER BY point_seq DESC) AS recency
  FROM band_points()
  WHERE applicable AND partial = false AND period IS NOT NULL AND pit IS NOT NULL
)
SELECT arrow_cast(coalesce(json_get_str(a.schema, 'title'), n.metric), 'Utf8') AS title,
       arrow_cast(n.metric, 'Utf8') AS metric,
       arrow_cast(n.period, 'Utf8') AS period,
       arrow_cast(CASE WHEN abs(n.actual) < 10 THEN CAST(round(n.actual * 1000) / 1000 AS VARCHAR)
                       ELSE CAST(CAST(round(n.actual) AS BIGINT) AS VARCHAR) END, 'Utf8') AS actual,
       arrow_cast(CASE WHEN abs(n.p90) < 10 THEN CAST(round(n.p10 * 1000) / 1000 AS VARCHAR) || ' to ' || CAST(round(n.p90 * 1000) / 1000 AS VARCHAR)
                       ELSE CAST(CAST(round(n.p10) AS BIGINT) AS VARCHAR) || ' to ' || CAST(CAST(round(n.p90) AS BIGINT) AS VARCHAR) END, 'Utf8') AS corridor,
       arrow_cast(CASE WHEN n.pit >= 0.5 THEN 'above' ELSE 'below' END, 'Utf8') AS side,
       arrow_cast('/app/docket/p/metrics?metric=' || n.metric, 'Utf8') AS link
FROM newest n
LEFT JOIN aspects a ON a.name = n.metric
WHERE n.recency = 1 AND n.displacement >= 0.95
ORDER BY n.displacement DESC, metric
