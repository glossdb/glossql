# What a stored result depends on

A function's result is stored and served again while what it read is
unchanged. For data this is tracked per table. For the catalog it is
tracked per relation: a result that read the glossary is computed
again after any write to the glossary.

That is too coarse for a result that is expensive. The example is a
forecast. Its body reads the metric's definition and its time grain
from the glossary, and calls a model service. A meaning written on an
unrelated column should not make it run again.

The function says what it depends on, in a query of its own:

```glossql
DECLARE FUNCTION trains_forecast FOR subway AS $$
  SELECT forecast(list(value ORDER BY month), 12) AS points
  FROM trains_run
$$ RETURNS trains_forecast
DEPENDS ON $$
  SELECT md5(string_agg(body::VARCHAR, '|' ORDER BY seq))
  FROM glossary
  WHERE dataset = 'subway' AND aspect IN ('trains_run', 'time_grains')
$$;
```

The query returns one value. It is stored with the result. The result
is current while the query returns the same value and the tables the
body read are unchanged.

The clause belongs to a function that stores a result. A test's
scoring function stores none and computes at each read, so the clause
is refused there:

```glossql-gap
DECLARE FUNCTION forecast_exists FOR subway AS $$
  SELECT subject, 'green' AS state, 0.0 AS score FROM annotations
$$ DEPENDS ON $$SELECT 1$$;
```
