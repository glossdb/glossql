# Tests

A test (`DECLARE TEST`) is declared on one property. It says who may
annotate the property, and which function scores how far the values
agree: the computed values, the agent's and the person's. A test's
result is stored per asset, like a measurement's. `TESTS()` returns
the stored results.

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
- A detector that compares assets with each other is refused with
  `cross_subject`: a window that does not partition by `subject`, an
  aggregate over the values that does not group by `subject`, or a
  join of two reads of the values that does not match them on
  `subject`. A result is stored per asset and reads that asset's
  values alone. To compare the columns of a table, declare the
  property on the table; to compare the tables of a dataset, on the
  dataset.

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

## Run a test

```sql
SELECT unit_test() FROM plant;
SELECT unit_test() FROM readings.temperature;
```

The statement runs the detector on each asset at or under the one
named that holds a value of the property. It computes an asset only
where no result is stored or the stored one is out of date, and
stores every result it computes in one write. It returns one row per
asset. A stored result on an asset that holds no value any more is
removed. The server runs these statements after each write, as it
runs measurements ([measurements](measurements.md)).

A result records what it read: the asset's values of the property,
the asset's definition, and the declarations of the test and the
detector. It is out of date once one of them changed after it was
computed. `glossql_outdated()` lists it with the change.

- A test or a detector declared again puts its results out of date in
  every dataset. Each dataset computes them when its test runs next.
- Data written by another tool changes no result until a statement
  computes it: `TESTS()` serves the stored result, marked not
  current.
- A column retyped and typed back reads as changed: the results that
  read its definition are out of date until they are computed again.

## Read the results

```sql
SELECT * FROM TESTS(readings.temperature::unit);
SELECT subject, state FROM TESTS(plant.readings) WHERE state = 'red';
```

Columns: `(subject, aspect, test, state, score, computed_at, current,
error)`.

- With no argument, `TESTS()` covers the dataset in use.
  `subject::aspect` narrows it to one property. Filter with `WHERE`.
- `current` is false when the stored result is out of date, or read a
  computed value that is. The state is served as it was stored.
- A detector that fails does not fail the statement. The result of
  each asset it ran on has the state `error`, score 0, and the failure
  text in `error`. An `error` does not make `GLOSSARY()` report
  `conflict`.
- One detector can serve several tests. Each test gets its own
  results, from its own values and threshold.

Normative text: [SPEC.md §7](../../SPEC.md#7-tests).
