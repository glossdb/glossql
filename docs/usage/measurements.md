# Measurements

A measurement is a value a function computes from the data: a profile,
an outlier count, relationship candidates. You declare the function
once. Running it stores the result with the versions of the tables it
read. The next run returns the stored result while those are unchanged.

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

- `reads` lists the tables the body's plan scans: lake tables as
  `dataset.table`, catalog tables as `main.<name>`, and `**` for a scan
  that has no table name (a file, a table function, another database).
- `data_version` holds the versions at the run, as `name:version`: for
  a lake table, the last snapshot that changed it; for a catalog table,
  its highest `seq`.
- The next run returns the stored row while those versions are
  unchanged, and computes again once one changed. `computed` is true
  when the run computed the value. With `**` in `reads`, any change in
  the dataset or the catalog computes again, so a function should read
  tables.
- If the value has a top-level `summary` object, the statement returns
  the summary. The whole value is stored; read it with
  `GLOSSARY(subject::aspect)`.

## Say what a result depends on

A result that read the catalog is computed again after any write to
the catalog table it read. A function can narrow this with a query of
its own:

```sql
DECLARE ASPECT unit_count WITH $${"type": "integer"}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION unit_count FOR GLOBAL AS
  $$ SELECT count(*) AS n FROM main.glossary g WHERE g.subject = $subject AND g.aspect = 'unit' $$
  RETURNS unit_count
  DEPENDS ON
  $$ SELECT md5(string_agg(g.body::VARCHAR, '|' ORDER BY g.seq)) FROM main.glossary g
     WHERE g.subject = $subject AND g.aspect = 'unit' $$;
```

- `DEPENDS ON` carries one read-only query that returns one row with
  one column. It reads `$subject` as the body does. It is allowed only
  with `RETURNS`.
- A run stores the query's value with the result, in `depends_on`.
- The stored result is current while the query returns the same value
  and the lake tables and views the body read are unchanged. A write
  to a catalog table does not make it stale by itself.
- A query that fails or returns no row at a read marks the result
  stale. The read still answers. At a run, such a query is an error of
  the run.

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
