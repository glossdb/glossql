# Measurements

A measurement is a value a function computes from the data: a profile,
an outlier count, relationship candidates. You declare the function
once. Running it stores the result with what it read. The next run
returns the stored result while nothing it read changed.

## Declare a function

```sql
DECLARE ASPECT min_max WITH $${"type": "object", "required": ["min", "max"]}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS
  $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
```

- `FOR` names a dataset, or `GLOBAL` for every dataset.
- The body is one DuckDB query. Declaring the function again replaces
  it.
- `$subject` is the asset the function measures, as text.
  `subject_column($subject)` is that column as a relation with one
  column, `v`.
- `RETURNS` names the property the result is stored under. The result
  is validated against that property's schema.
- A function without `RETURNS` is a detector: it scores a test
  ([tests](tests.md)).

The result takes its shape from the query: one row with one column is
the value, one row with several columns is an object, several rows are
an array of objects.

A function can return a `FACT` property. Its result is then one more
value beside the agent's and the person's, and a test can compare
them.

## Run it

```sql
SELECT min_max() FROM readings.temperature;
```

The asset after `FROM` is the input. A call takes no arguments.
Several calls go in one statement: `SELECT f(), g() FROM asset`.

The statement returns `(function, subject, aspect, value, computed)`
and stores one row in
[measurements](../specification/tables/measurements.md).

- [inputs](../specification/tables/inputs.md) holds one row per node
  the result read, as an asset and what was read of it:
  `fin.orders.amount` `annotation role`, `fin.orders` `data`,
  `fin.revenue` `definition`, `min_max` `declaration`. Join it to a
  result on `begin_snapshot`.
- The lake's snapshot id is the version. A result stores the snapshot
  it read at (`read_at`). The next run returns the stored row while no
  node it read changed after that snapshot and no result it read is
  out of date, and computes again otherwise. `computed` is true when
  the run computed the value. `glossql_outdated()` names the node that
  changed. A result computed again with the same value keeps the
  snapshot its value changed at (`changed_at`), so what read it stays
  current. A body that reads the catalog past the read functions (the
  lineage kit) is refused at its declaration.
- If the value has a top-level `summary` object, the statement returns
  the summary. The whole value is stored; read it with
  `GLOSSARY(subject::aspect)`.

## Call it as SQL

A measurement function is also a table macro, in the dataset's schema
or in `main` for `GLOBAL`. Any SQL can call it with the asset as its
argument. This call stores nothing.

```sql
FROM min_max('plant.readings.temperature');
```

## Measure a whole dataset

`dataset_cells(subject)` returns every scalar cell of a dataset, or of
one table, as one relation, so a check over a dataset is plain SQL. See
[functions](../functions.md#dataset_cells).

Normative text: [SPEC.md §6](../../SPEC.md#6-the-function-library).
