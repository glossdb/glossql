-- Why the metric moved: every admitted dimension's member cells at the
-- viewer's grain, each member ranked by its own move between the
-- newest two periods, largest first, the top eight. A member's move
-- is at the metric's verb — a ratio member's move is its own ratio
-- shifting, never a share of the total (mix effects are a reading,
-- not a cell). The cells are the cube's, re-bucketed to the grain on
-- the server; the ranking is this SQL, nothing is computed in the
-- browser.
WITH cells AS (
  SELECT dimension, member, period, value,
         dense_rank() OVER (ORDER BY period DESC) AS recency
  FROM metric_series(grain => $grain)
  WHERE metric = CAST($metric AS VARCHAR) AND dimension NOT IN ('', 'alternative')
),
moves AS (
  SELECT dimension, member,
         max(CASE WHEN recency = 2 THEN value END) AS from_value,
         max(CASE WHEN recency = 1 THEN value END) AS to_value,
         max(CASE WHEN recency = 2 THEN period END) AS prev_period,
         max(CASE WHEN recency = 1 THEN period END) AS last_period
  FROM cells WHERE recency <= 2
  GROUP BY dimension, member
)
SELECT dimension, member,
       arrow_cast(CASE WHEN to_value - from_value >= 0 THEN '+' ELSE '' END
         || CAST(round(to_value - from_value, 2) AS VARCHAR), 'Utf8') AS delta,
       from_value, to_value, prev_period, last_period
FROM moves
WHERE from_value IS NOT NULL AND to_value IS NOT NULL
ORDER BY abs(to_value - from_value) DESC, dimension, member
LIMIT 8
