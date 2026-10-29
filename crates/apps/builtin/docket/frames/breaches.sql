-- The findings' first rows: every walked metric whose newest complete
-- point landed outside the corridor its own history predicted — the
-- point the bands detector scores — as one finding: a sentence from
-- the record's fields, the numbers behind it, the bands witness's
-- verdict as the chip, and the axis that moved most for the link to
-- the metric sliced by it. The corridor picture and the movers under
-- the finding are the metric's own frames, keyed by its name.
-- `band_points()` is the recorded walk, no re-run; a point is outside
-- at displacement 0.95 and over. A green corridor serves no row. The
-- axis that moved most is the cube's own cells at month grain, the
-- newest complete walked month against the one before it.
WITH verdict AS (
  SELECT a.witness, a.band, a.computed_at, a.current, w.detector, w.threshold,
         row_number() OVER (ORDER BY CASE a.band WHEN 'red' THEN 0 WHEN 'orange' THEN 1 WHEN 'yellow' THEN 2 ELSE 3 END, a.witness) AS rn
  FROM ATTEST() a JOIN witnesses w ON w.name = a.witness
  WHERE w.detector = 'band_breach'
),
walked AS (
  SELECT metric AS name, period, actual, p10, p90, pit, displacement AS far,
         row_number() OVER (PARTITION BY metric ORDER BY point_seq DESC) AS recency
  FROM band_points()
  WHERE applicable AND partial = false AND period IS NOT NULL
),
newest AS (SELECT * FROM walked WHERE recency = 1),
previous AS (SELECT name, period FROM walked WHERE recency = 2),
cells AS (
  SELECT s.metric AS name, s.dimension AS axis, s.member, to_char(s.period, '%Y-%m') AS ym, s.value
  FROM metric_series(grain => 'month') s
  JOIN newest n ON n.name = s.metric
  WHERE s.world = 'actual' AND s.dimension NOT IN ('', 'alternative')
),
moves AS (
  SELECT c.name, c.axis, c.member,
         coalesce(max(CASE WHEN c.ym = n.period THEN c.value END), 0)
           - coalesce(max(CASE WHEN c.ym = p.period THEN c.value END), 0) AS delta
  FROM cells c
  JOIN newest n ON n.name = c.name
  LEFT JOIN previous p ON p.name = c.name
  WHERE c.ym = n.period OR c.ym = p.period
  GROUP BY c.name, c.axis, c.member
),
top AS (
  SELECT name, axis, row_number() OVER (PARTITION BY name ORDER BY abs(delta) DESC, axis, member) AS rn
  FROM moves
),
counted AS (SELECT count(*) AS n FROM newest WHERE pit IS NOT NULL),
outside AS (SELECT * FROM newest WHERE pit IS NOT NULL AND far >= 0.95),
outs AS (SELECT count(*) AS k FROM outside)
SELECT arrow_cast(coalesce(json_get_str(a.schema, 'title'), o.name), 'Utf8') AS title,
       arrow_cast(o.name, 'Utf8') AS metric,
       arrow_cast(coalesce(json_get_str(a.schema, 'title'), o.name)
         || CASE WHEN o.pit >= 0.5 THEN ' rose out of its corridor in ' ELSE ' fell out of its corridor in ' END
         || to_char(to_timestamp(o.period || '-01'), '%B %Y') || '.', 'Utf8') AS headline,
       CASE WHEN abs(o.actual) < 10 THEN round(o.actual * 1000) / 1000 ELSE round(o.actual) END AS actual,
       CASE WHEN abs(o.p90) < 10 THEN round(o.p10 * 1000) / 1000 ELSE round(o.p10) END AS lo,
       CASE WHEN abs(o.p90) < 10 THEN round(o.p90 * 1000) / 1000 ELSE round(o.p90) END AS hi,
       arrow_cast(CASE WHEN o.pit >= 0.5 THEN 'above' ELSE 'below' END, 'Utf8') AS side,
       arrow_cast(CAST(round(o.far * 1000) / 1000 AS VARCHAR), 'Utf8') AS displacement,
       arrow_cast(CASE WHEN c.n - k.k <= 0 THEN ''
                       WHEN c.n - k.k = 1 THEN 'The one other walked metric sits inside its corridor.'
                       ELSE 'The other ' || CAST(c.n - k.k AS VARCHAR) || ' walked metrics sit inside their corridors.' END, 'Utf8') AS others,
       arrow_cast(coalesce(v.band, 'outside'), 'Utf8') AS band,
       arrow_cast(CASE WHEN v.band IS NULL OR v.band = 'green' THEN '' ELSE 'warn' END, 'Utf8') AS bcls,
       arrow_cast(coalesce(v.witness || ' · ' || v.detector || coalesce(' @ ' || v.threshold, '') || ' on metric_bands',
                           'no witness judges the corridor'), 'Utf8') AS meta,
       arrow_cast(coalesce('measured ' || to_char(CAST(v.computed_at AS TIMESTAMP), '%d %b %Y %H:%M'), ''), 'Utf8') AS at,
       arrow_cast(CASE WHEN coalesce(v.current, true) THEN '' ELSE ' · from before the last change' END, 'Utf8') AS stale,
       arrow_cast(coalesce('?check=' || v.witness || '&kind=corridor', ''), 'Utf8') AS link_check,
       arrow_cast('/' || d.dataset || '/app/docket/p/metrics?metric=' || o.name || coalesce('&dim=' || t.axis, ''), 'Utf8') AS link_metric,
       arrow_cast(coalesce(t.axis, 'nothing'), 'Utf8') AS axis
FROM outside o
CROSS JOIN counted c
CROSS JOIN outs k
CROSS JOIN current_dataset d
LEFT JOIN aspects a ON a.name = o.name
LEFT JOIN verdict v ON v.rn = 1
LEFT JOIN top t ON t.name = o.name AND t.rn = 1
ORDER BY o.far DESC, o.name
