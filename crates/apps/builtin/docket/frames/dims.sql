-- The slice picker: the axes the cube admitted for this metric (served
-- dimension columns with 2..24 members — the caps are the cube's,
-- stated in its body). Links are boosted navigation: the dim rides the
-- URL, the page re-renders with the slice tile.
SELECT dimension,
  count(DISTINCT member) AS members,
  arrow_cast('?metric=' || CAST($metric AS VARCHAR) || '&dim=' || dimension, 'Utf8') AS link
FROM metric_series()
WHERE metric = CAST($metric AS VARCHAR) AND dimension NOT IN ('', 'alternative')
GROUP BY dimension
ORDER BY dimension
