# A function's body

A function that checks a validation is written in SQL. The example
declares two checks with their bodies in the statement (`AS $$…$$`) and
a test on the first.

```glossql
USE fin;

DECLARE FUNCTION inventory_rollforward_check FOR fin AS $$
  SELECT 'prior level plus the month''s movements equals this month''s level' AS outcome,
         coalesce(sum(abs(gap)) / nullif(count(*), 0), 0.0) AS breach_rate
  FROM (
    SELECT s.units_on_hand
             - lag(s.units_on_hand) OVER (PARTITION BY s.product_id, s.location ORDER BY s.period)
             - coalesce(f.moved, 0) AS gap
    FROM stock_levels s
    LEFT JOIN monthly_moves f
      ON f.product_id = s.product_id AND f.location = s.location AND f.period = s.period)
$$ RETURNS inventory_rollforward;

DECLARE FUNCTION ar_settles_in_full_check FOR fin AS $$
  SELECT 'a receipt settles its invoice in full; a short receipt is the exception' AS outcome,
         coalesce(CAST(count(*) FILTER (WHERE settled < billed) AS DOUBLE)
                    / nullif(count(*), 0), 0.0) AS breach_rate
  FROM ar_settlement
$$ RETURNS ar_settles_in_full;

DECLARE TEST inventory_rollforward_test ON inventory_rollforward
  BY (AGENT, HUMAN) DETECTOR rate_tolerance THRESHOLD 0.0;
```

A function's body is read back from the catalog:

```glossql
SELECT script FROM functions WHERE name = 'rate_tolerance'
```

The language does not take a function's body from a file path. This is
by design: a client of the MCP interface sends statements and has no
filesystem.

```glossql-gap
DECLARE FUNCTION outliers FOR GLOBAL FROM 'functions/outliers.rhai'
  ACCEPTS (column_profile) RETURNS outlier_profile;
```
