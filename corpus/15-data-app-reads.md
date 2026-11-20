# Reads behind a data app

A data app or an answering agent needs the context of a dataset, the
checks before a drill-down, the confidence of an answer, the metric
lineage and a status summary. The examples show that each is a read
over the glossary, the test results and the catalog's tables.

## Context for a question

The loads, the current annotations, the relationships, the count of
missing annotations and the tests that are not passing.

```glossql
USE fin;
SELECT * FROM imports;
SELECT subject, property, value FROM GLOSSARY(fin) WHERE state = 'current';
SELECT left_path, right_path FROM relationships;
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'missing';
SELECT subject, state, score FROM TESTS(fin) WHERE state != 'green';
```

## Drill-down

Before a measure is summed over a dimension, the reader takes the
measure's additivity and unit and the dimension's annotation, and
checks that the join keeps the row count.

```glossql
SELECT value FROM GLOSSARY(results.points::additivity) WHERE state = 'current';
SELECT value FROM GLOSSARY(results.points::unit) WHERE state = 'current';
SELECT value FROM GLOSSARY(races.circuit_id::dimension) WHERE state = 'current';
SELECT count(*) FROM results r JOIN races a ON r.race_id = a.race_id;
```

## Confidence of an answer

```glossql
SELECT * FROM TESTS(fin.trial_balance);
```

## Metric lineage

The declared properties and their current definitions are the nodes and
edges of the lineage graph.

```glossql
SELECT name, kind, grains FROM properties;
SELECT subject, property, value FROM GLOSSARY(fin) WHERE state = 'current';
```

A relationship's annotation can name the conformed dimension it joins
to:

```glossql
GLOSS meaning ON results.driver_id -> drivers.driver_id AS $${
  "value": "each result row names its driver; grain-preserving (26,080 = 26,080)",
  "conformed": "driver"
}$$;
```

## Status summary

```glossql
SELECT subject, state, score FROM TESTS(fin) WHERE state = 'red';
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'missing';
SELECT * FROM imports;
```
