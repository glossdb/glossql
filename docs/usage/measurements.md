# Measurements

A measurement is a value computed from the data by a function: a
profile, an outlier count, relationship candidates. You declare the
function once; running it stores the result with the versions of
everything it read, and the next run serves that result while those
are unchanged.

## Declare a function

```sql
DECLARE ASPECT min_max WITH $${"type": "object", "required": ["min", "max"]}$$ AS MEASUREMENT ON COLUMN;
DECLARE FUNCTION min_max FOR GLOBAL AS
  $$ SELECT min(v) AS min, max(v) AS max FROM subject_column($subject) $$ RETURNS min_max;
```

- `FOR` scopes the function to a dataset, or `GLOBAL`.
- The body is one read-only DuckDB query, written inline. Re-declaring
  replaces it.
- `$subject` is the asset it measures, as a string;
  `subject_column($subject)` is that column as a relation with one
  column, `v`.
- `RETURNS` names the property the result fills. The result is
  validated against that property's schema.
- A function without `RETURNS` is a detector: it scores a test
  ([tests](tests.md)).

The result takes its shape from the query: one row and one column is
the value, one row is an object of its columns, several rows are an
array of row objects.

A function that returns a `FACT` property adds a computed value beside
the agent's and the person's, and a test compares them.

## Run it

```sql
SELECT min_max() FROM orders.amount;
SELECT outliers() FROM orders.amount;
```

A run always looks like `f()` with no arguments: the asset after
`FROM` is the input, and settings are read from the catalog inside the
body. Several runs go in one statement or in several; the grammar has
no ordering.

The run stores one row in `measurements`: dataset, function, asset,
property, `pin`, value, time and `reads`.

- `reads` names what the body's plan scanned: lake tables as
  `dataset.table`, catalog tables as `glossql.relation`, `**` for a
  scan with no name (a file, a catalog function, another database), and
  nothing for a body that read nothing.
- `pin` is every input at its version: a table's last snapshot that
  changed it, a catalog table's highest `seq`.
- The next run serves the stored row while the inputs it names are
  unchanged, and computes again once one moved. The row's `computed`
  column says which happened. A body with `**` reads is served only
  while everything is unchanged, so a function should read tables.
- A result with a top-level `summary` object returns the summary; the
  whole value reads back through `GLOSSARY(subject::aspect)`.

## Call it as SQL

A measurement function is a table macro in the dataset's schema
(`glossql` for `GLOBAL`), callable from any SQL with the asset as its
argument:

```sql
FROM min_max('fin.orders.amount');
```

## Measurements over a whole dataset

`dataset_cells(subject)` serves every scalar cell of a dataset, or of
one table, as one relation, so a screen over a dataset is plain SQL.
A `cells => n` bound reads larger tables as a repeatable sample of
their rows; the result is then an estimate and says how many rows it
read ([functions](../functions.md#dataset_cells)).

Normative text: [SPEC.md §6](../../SPEC.md#6-the-function-library).
