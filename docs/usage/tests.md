# Tests

A test (`DECLARE TEST`) is declared on one property. It says
who may record a value for it, and which detector scores how far the
values agree: the computed values, the agent's and the person's.
`TESTS()` returns the results. Tests run when they are read and never
write into the data.

## Declare a test

```sql
DECLARE TEST behavior_test ON behavior
  BY (AGENT, HUMAN)
  DETECTOR behavior_entropy
  THRESHOLD 0.7;

DECLARE TEST reconciliation_test ON reconciliation
  DETECTOR reconcile_bands THRESHOLD 0.5;
```

- `BY` lists who may record the property: `AGENT`, `HUMAN`. Functions
  are not listed here; a function contributes by `RETURNS`. `BY` is
  refused on a `MEASUREMENT` property, which nobody writes by hand.
- `DETECTOR` names a function without `RETURNS` that scores the values.
- Name `BY`, `DETECTOR`, or both. On a `MEASUREMENT` property only
  `DETECTOR` applies: it judges the measurement's output.
- `THRESHOLD` (0 to 1): above it, and where the values differ, the
  glossary withholds the value and marks the asset `conflict`
  ([reading](reading.md)).

## Write a detector

A detector is one query over `slots`: every value on the property, one
row per asset and source, with `speaker`, `current` and `body`. The
threshold binds as `$threshold`. It returns one row per asset:

```sql
DECLARE FUNCTION reconcile_bands FOR fin AS $$
  SELECT subject,
         CASE WHEN max(body['delta']) <= $threshold THEN 'green' ELSE 'red' END AS band,
         coalesce(max(body['delta']), 0.0) AS score
  FROM slots GROUP BY subject
$$;
```

`band` is `green`, `yellow`, `orange` or `red`; `score` is the
disagreement from 0 to 1.

## Read the results

```sql
SELECT * FROM TESTS(orders.amount::behavior);
SELECT subject, band FROM TESTS(fin.trial_balance) WHERE band = 'red';
```

Every row has the same columns:
`(subject, aspect, test, band, score, computed_at, current, error)`.

- `current` is true when every computed value the result read is
  current.
- A detector that fails does not fail the read: its rows come back with
  band `error`, score 0, and the failure in `error`. An error never
  marks an asset `conflict`, and the other tests still answer.
- One detector can serve several tests; each test gets its own results,
  from its own values and threshold.
- With no argument, `TESTS()` covers the dataset in use;
  `subject::aspect` narrows it to one property. Filtering is a `WHERE`.

Normative text: [SPEC.md §7](../../SPEC.md#7-tests).
