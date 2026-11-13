---
name: glossql-lineage
description: The lineage kit for a glossql workspace — a dataset's lineage as one relation, and the three read functions a function's body reads annotations, results and view definitions through. Use when writing a function (DECLARE FUNCTION) or reading what a function depends on.
---

# The lineage kit

The kit declares no property. It serves the lineage of a dataset and
the read functions a function's body uses to read the catalog. Load
it before the other kits: their functions read through it.

```sql
FROM glossql_kit('kits/lineage/kit.sql');
```

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
| `plans` | a plan | the metric it plans |

```sql
SELECT "to" FROM lineage('fin') WHERE "from" = 'fin.revenue' AND kind = 'reads';
```

## The read functions

A function's body reads annotations, results and view definitions
through three read functions. The first argument says what, the last
says where. Both are constants.

| Read | Returns, one row per asset of the place |
|---|---|
| `annotations(property, where)` | `asset`, `property`, `value`, `state`; state `missing` where nobody annotated the asset. A property declared on tables returns no row for a column. `'x-kind <kind>'` as the property reads every property declared with that `x-kind`, a scenario's among them: a person's newest annotation, else an agent's |
| `results(function, where)` | `asset`, `function`, `value`, `error`; both empty where no result is stored |
| `definition(where)` | one row per view: `view`, `kind`, `plan_of`, `covers`, `body`, `sql`, `stopped`, `columns`, `reads`, `joins` |

The places, walked from the function's subject:

| Where | The assets |
|---|---|
| `itself` | the subject |
| `its columns` | what the subject contains |
| `its sources` | the tables, columns and views a view reads |
| `the columns it joins on` | both columns of each join in a view |
| `its relationships` | a view's: those between the tables it reads; a table's or a column's: those that touch it; a dataset's: all |
| `its metric` | the metric a plan plans |
| `the dataset` | the dataset and every asset in it |

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
