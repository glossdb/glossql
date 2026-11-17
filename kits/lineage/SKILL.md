---
name: glossql-lineage
description: The lineage kit for a glossql workspace — a dataset's lineage as one relation, and the three read functions a function's body reads annotations, results and view definitions through. Use when writing a function (DECLARE FUNCTION) or reading what a function depends on.
---

# The lineage kit

The kit declares no property. It serves the lineage of a dataset and
the read functions a function's body uses to read the catalog. The
extension carries it: `PRAGMA glossql` loads it where the workspace
does not hold its text, so the other kits' functions can read through
it.

## The lineage

`lineage(dataset)` returns one row per edge: `from`, `to`, `kind`.
An asset is named by its path: `fin`, `fin.orders`,
`fin.orders.amount`, and a relationship by its pair path,
`fin.orders.customer_id -> fin.customers.customer_id`.

| Kind | From | To |
|---|---|---|
| `contains` | a dataset; a table or view | its tables, views and relationships; its columns |
| `reads` | a view | the tables and columns it scans and joins on, and each view it reads |
| `relates` | a relationship | each of its columns |
| `loaded from` | a loaded table | the source its recipe loads it from |
| a kit's own | an asset of a kind a kit declares | what its `lineage_edges_<kind>` macro names, such as the plan kit's `plans` |

```sql
SELECT "to" FROM lineage('fin') WHERE "from" = 'fin.revenue' AND kind = 'reads';
```

A view's lineage is a stored result: the result of `lineage` on the
view, stored when the view is defined, with the nodes its planning read
(the view's definition, and that of each table, view and macro its SQL
names, through the views it reads) and its `reads` edges as rows of
`edges`. Once a table it reads is altered or a view it reads is defined
again, it is out of date like any result, and computed again by
`SELECT lineage() FROM <view>`; the server does it after each write,
before the checks that read it. Computed again with the same value, it
keeps the snapshot it changed at, so what read it stays current.

Four reads serve the stored lineage of a dataset's views, one row per
view and part; NULL reads every dataset's:

| Read | Columns | A row is |
|---|---|---|
| `views_lineage(dataset)` | `dataset`, `view`, `column`, `derivation`, `complete`, `shape` | one output column of a view; a view that does not plan has one row, no column, `complete` false |
| `views_scans(dataset)` | `dataset`, `view`, `table_name`, `columns` | one table a view scans, joins and filters included, with the columns it reads |
| `views_joins(dataset)` | `dataset`, `view`, `left_column`, `right_column`, `left_unique`, `right_unique`, `join_number` | one pair of columns a join of a view equates, as `schema.table.column`, the left side as the predicate's left input; `left_unique` where that side is joined one row per value, `join_number` shared by the pairs of one join |
| `views_mapping(dataset)` | `dataset`, `view`, `column`, `branch`, `derivation`, `table_name`, `table_column`, `alias`, `condition`, `lookup`, `literals`, `complete` | one output column of a view per branch of its union and per lake column it is computed from: what the view's joins and filters require of that scan (`condition`), the equalities that look it up (`lookup`), the literals the derivation tests or returns |

```sql
SELECT "column", derivation FROM views_lineage('fin') WHERE view = 'revenue';
```

## The read functions

A function's body reads annotations, results and view definitions
through three read functions. The first argument says what, the last
says where. Both are constants.

| Read | Returns, one row per asset of the place |
|---|---|
| `annotations(property, where)` | `asset`, `property`, `value`, `state`, `threshold`; state `missing` where nobody annotated the asset; `threshold` the THRESHOLD of the test on the property, NULL where it has none. A property declared on tables returns no row for a column. `'x-kind <kind>'` as the property reads every property declared with that `x-kind`, a scenario's among them: a person's newest annotation, else an agent's |
| `results(function, where)` | `asset`, `function`, `value`, `error`; both empty where no result is stored |
| `definition(where)` | one row per view: `view`, `kind`, `schema` (the property's, where a kit reads its own keys), `sql`, `stopped`, `columns`, `reads`, `joins`, `conditions`. The view's SQL and what it reads, each loaded table with the `source` it is loaded from, and per read of a table (`branch`, `alias`) the `condition` the view's joins and filters require of it alone; not the rest of the annotation that defines it |

The places, walked from the function's subject:

| Where | The assets |
|---|---|
| `itself` | the subject |
| `its columns` | what the subject contains |
| `its sources` | the tables, columns and views a view reads |
| `the columns it joins on` | both columns of each join in a view |
| `its relationships` | a view's: those between the tables it reads and those that touch the view; a table's or a column's: those that touch it; a dataset's: all; a relationship's: itself |
| `its source systems` | a table's: the source its recipe loads it from; a view's: those of the tables it reads. `annotations()` of a property declared `ON SOURCE`, such as `conventions`, returns each source's annotation |
| `its dataset` | the subject's dataset itself |
| `the dataset` | the dataset and every asset in it |
| `the dataset's metrics` | the views of the dataset that are metrics, not plans, and their columns |
| a kit's own | what its `lineage_places_<kind>` macro names, such as the plan kit's `its metric` and the outlook kit's `its levers` |

```sql
DECLARE FUNCTION unlabelled_columns FOR GLOBAL AS $$
  SELECT count(*) FILTER (WHERE state = 'missing') AS missing
  FROM annotations('role', 'its columns')
$$ RETURNS unlabelled_columns;
```

The engine sets the subject while it computes a result. Read by
hand, the subject is the dataset in use; to read from another
subject, set it:

```sql
SET VARIABLE glossql_subject = 'fin.revenue';
FROM annotations('role', 'its columns');
```

## What a function reads

At `DECLARE FUNCTION` the engine takes the reads from the body as
written, follows the macros it calls, and stores them in the
`reads` column of `functions`: one object per read with `kind`
(`read`, `data`, `catalog`, `other`), `read`, `what`, `where`,
`constant` and `via`. A read whose what or where is not a constant
has `constant` false.

```sql
SELECT name, reads FROM functions WHERE name = 'additivity_check';
```

When the engine stores a result, it stores with it what each of
those reads returned for the subject, as a digest, one row of
`inputs` per read. The result is current while each read returns the
same and no result it read is out of date. `glossql_outdated()` names
the read that differs:

```sql
SELECT function, subject, input, reason FROM glossql_outdated();
```
