-- The numbers beside each metric surface: the newest period of the
-- cube's total series at the metric's own resolution, the move into
-- it, and the axes the cube admitted. Data-class — the cells at a
-- grain — joined to the record rows of `frames/pulse` in the browser
-- by `name`, the metric's own key, so a ruling refreshes the record
-- without touching the cube. Formatting is this frame's: the rounded
-- value, the percentage, the em dash the pulse shows until this
-- arrives. A declared fact has no period and no move: its one value
-- joins the same rows from `fact_values()`, so the list shows the
-- number beside the surface instead of the dash — or, where the fact
-- served no number, the read's reason.
WITH totals AS (
  SELECT metric, period, value,
         value - lag(value) OVER (PARTITION BY metric ORDER BY period) AS delta
  FROM metric_series() WHERE dimension = ''
  QUALIFY row_number() OVER (PARTITION BY metric ORDER BY period DESC) = 1
),
axes AS (
  SELECT metric, string_agg(dimension, ' · ') AS axes
  FROM (SELECT DISTINCT metric, dimension FROM metric_series()
        WHERE dimension NOT IN ('', 'alternative'))
  GROUP BY metric
)
SELECT t.metric AS name,
       t.period,
       arrow_cast(coalesce(CAST(round(t.value, 1) AS VARCHAR), '—'), 'Utf8') AS latest,
       arrow_cast(CASE
         WHEN t.delta IS NULL THEN ''
         WHEN t.delta >= 0 THEN '+' || CAST(round(100.0 * t.delta / nullif(t.value - t.delta, 0), 1) AS VARCHAR) || '%'
         ELSE CAST(round(100.0 * t.delta / nullif(t.value - t.delta, 0), 1) AS VARCHAR) || '%'
       END, 'Utf8') AS delta,
       arrow_cast(coalesce(x.axes, 'no axes admitted'), 'Utf8') AS axes
FROM totals t
LEFT JOIN axes x ON x.metric = t.metric
UNION ALL
SELECT f.metric AS name,
       arrow_cast(NULL, 'Timestamp(Nanosecond, None)') AS period,
       arrow_cast(coalesce(CAST(round(f.value, 1) AS VARCHAR), '—'), 'Utf8') AS latest,
       arrow_cast('', 'Utf8') AS delta,
       arrow_cast(coalesce(f.reason, 'a current fact — no series'), 'Utf8') AS axes
FROM fact_values() f
