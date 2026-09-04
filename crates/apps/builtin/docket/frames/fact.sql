-- A declared fact's value (`x-kind: fact`), served whole by
-- `fact_values()` — one row when the frame is one row with a `value`,
-- none for a series, a relation, or a fact that served no number, so
-- the tile that reads this renders only where there is a number to
-- show. Data-class, like the cube's reads. Formatting is this frame's.
SELECT metric,
       value,
       arrow_cast(CAST(round(value, 2) AS VARCHAR), 'Utf8') AS shown
FROM fact_values()
WHERE metric = CAST($metric AS VARCHAR) AND value IS NOT NULL
