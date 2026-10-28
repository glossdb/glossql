-- A metric's corridor as a picture: every walked point with its
-- actual and the 10th-to-90th corridor the walk predicted for the
-- month, and whether the point landed outside it (displacement 0.95
-- and over). The spec draws the corridor as a band, the actuals as a
-- line, an outside point in the breach hue. `band_points()` is the
-- recorded walk, no re-run; the metric is a page param the finding's
-- frame fills in.
SELECT * FROM (
  SELECT to_timestamp(p.period || '-01') AS period,
         arrow_cast(coalesce(json_get_str(a.schema, 'title'), p.metric), 'Utf8') AS series,
         p.actual AS value,
         p.p10 AS low,
         p.p90 AS high,
         coalesce(p.displacement >= 0.95, false) AS outside
  FROM band_points() p
  LEFT JOIN aspects a ON a.name = p.metric
  WHERE p.metric = CAST($metric AS VARCHAR) AND p.applicable AND p.period IS NOT NULL
)
ORDER BY period
