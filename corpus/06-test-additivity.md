# Test: a column's additivity

A column is additive when its values sum over time (a flow) and
semi-additive when they do not (a stock). The example declares the
property, a function that derives it from the data, and a test that
compares the function's value with the agent's and the person's
annotations and reports a state above a threshold.

```glossql
DECLARE PROPERTY additivity WITH $${
  "type": "object",
  "properties": {"value": {"enum": ["semi-additive", "additive"]}}
}$$ AS FACT ON COLUMN;

DECLARE FUNCTION temporal_additivity FOR GLOBAL AS $$/* stock/flow from a column's period shape */$$
  RETURNS additivity;

DECLARE FUNCTION additivity_entropy FOR GLOBAL AS $$/* detector: spread across the additivity values */$$;

DECLARE TEST additivity_test ON additivity BY (AGENT, HUMAN)
  DETECTOR additivity_entropy THRESHOLD 0.7;

SELECT * FROM TESTS(orders.amount::additivity);
```
