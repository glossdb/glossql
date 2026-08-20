-- The slice picker: the axes the cube admitted for this metric from
-- judged verdicts (applicable dimension_relevance, relevance first —
-- the rule is the cube's). Links are boosted navigation: the dim
-- rides the URL beside the viewer's window, and the page re-renders
-- with the slice tile.
SELECT dimension,
  count(DISTINCT member) AS members,
  arrow_cast('?metric=' || CAST($metric AS VARCHAR) || '&dim=' || dimension
    || '&grain=' || CAST($grain AS VARCHAR) || '&span=' || CAST($span AS VARCHAR),
    'Utf8') AS link
FROM metric_series()
WHERE metric = CAST($metric AS VARCHAR) AND dimension NOT IN ('', 'alternative')
GROUP BY dimension
ORDER BY dimension
