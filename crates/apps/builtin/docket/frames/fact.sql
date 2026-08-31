-- A current fact's value: the grounding the cube does not chart,
-- served whole by `fact_values()` — one row when this metric is a
-- fact (a frame of one row with a `value` column and no date), none
-- when it is a series or a relation, so the tile that reads this
-- renders only where there is a number to show. Data-class, like the
-- cube's reads. Formatting is this frame's.
SELECT metric,
       value,
       arrow_cast(CAST(round(value, 2) AS VARCHAR), 'Utf8') AS shown
FROM fact_values()
WHERE metric = CAST($metric AS VARCHAR) AND value IS NOT NULL
