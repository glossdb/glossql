# Tests

A test (`DECLARE TEST`) is declared on one property. It says who may
annotate the property, and which function scores how far the values
agree: the computed values, the agent's and the person's. `TESTS()`
returns the results. A test runs when it is read; its results are not
stored.

## Declare a test

```sql
DECLARE TEST unit_test ON unit
  BY (AGENT, HUMAN)
  DETECTOR unit_agreement
  THRESHOLD 0.7;

DECLARE TEST gap_test ON gap_rate
  DETECTOR gap_states THRESHOLD 0.05;
```

- `BY` lists who may annotate the property: `AGENT`, `HUMAN`. An
  annotation by another kind of actor is refused. `BY` is refused on a
  `MEASUREMENT` property.
- `DETECTOR` names a function without `RETURNS` that scores the
  values.
- Name `BY`, `DETECTOR`, or both.
- `THRESHOLD` is a number from 0 to 1. Where the score is above it and
  an asset has more than one value, `GLOSSARY()` withholds the value
  and reports `conflict` ([reading](reading.md)).
- A property has one test. Declaring another test on it replaces the
  first.

## Write a detector

A detector is one query over the relation `annotations`: every value
for the property, with the columns `subject`, `aspect`, `actor_kind`
(`agent`, `human` or `function`), `body` and `current`. `$threshold`
is the test's threshold. The query returns one row per asset:
`(subject, state, score)`.

```sql
DECLARE FUNCTION gap_states FOR plant AS $$
  SELECT subject,
         CASE WHEN max(body::DOUBLE) <= $threshold THEN 'green' ELSE 'red' END AS state,
         least(max(body::DOUBLE), 1.0) AS score
  FROM annotations GROUP BY subject
$$;
```

`state` is `green`, `yellow`, `orange` or `red`. `score` is a number
from 0 to 1. The `kpi` kit ships `value_entropy`, a detector that
scores disagreement between the values of a property.

## Read the results

```sql
SELECT * FROM TESTS(readings.temperature::unit);
SELECT subject, state FROM TESTS(plant.readings) WHERE state = 'red';
```

Columns: `(subject, aspect, test, state, score, computed_at, current,
error)`.

- With no argument, `TESTS()` covers the dataset in use.
  `subject::aspect` narrows it to one property. Filter with `WHERE`.
- `current` is false when a computed value the result read is stale.
- A detector that fails does not fail the read. Its rows have the
  state `error`, score 0, and the failure text in `error`. An `error`
  does not make `GLOSSARY()` report `conflict`.
- One detector can serve several tests. Each test gets its own
  results, from its own values and threshold.

Normative text: [SPEC.md §7](../../SPEC.md#7-tests).
