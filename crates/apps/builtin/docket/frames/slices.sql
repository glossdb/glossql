-- The picked slice: one admitted dimension's members over the cube's
-- months, at the grounding's own verb (the measurement already
-- aggregated — nothing recomputes here). The page renders this tile
-- only when the URL carries a dim.
SELECT period, member AS series, value, num, den, behavior
FROM metric_series()
WHERE metric = CAST($metric AS VARCHAR) AND dimension = CAST($dim AS VARCHAR)
ORDER BY period, member
