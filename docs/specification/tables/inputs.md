# inputs

A row is one node a stored result read: an asset and what was read of
it. `SELECT f() FROM asset` writes the rows with the result, in the
same transaction. Key: `begin_snapshot`, `asset` and `what`. A row
carries its result's `begin_snapshot` and ends when its result does.

The lake's snapshot id is the only version. A result stores the
snapshot it read at (`measurements.read_at`) and the snapshot its
value last changed at (`measurements.changed_at`). A node changed at
the latest snapshot one of its rows begins or ends at. A result is out
of date when a node it read changed after the snapshot the result read
at. No digest of what a read returned is stored or compared.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The snapshot of the write that replaced the result; NULL while the result is current. The view leaves this column out. |
| `asset` | VARCHAR | The asset read, by its path; the name of a property or a function; a macro's `schema.name`; an x-kind for the set of its properties. |
| `what` | VARCHAR | What was read of it, one of the nodes below. |

## Nodes

| `what` | Asset | Changed at the latest begin or end of |
|---|---|---|
| `annotation <property>` | any | the `glossary` rows of the property on the asset, every actor's |
| `value <property>` | any | the `measurements` rows functions other than the reader stored under the property on the asset, in the reader's dataset: the current row's `changed_at`, else the last row's end |
| `result <function>` | any | the function's `measurements` rows on the asset, in the reader's dataset, the same way |
| `definition` | a table | its table and column rows in the lake (created, altered, dropped), and each load whose recipe derived its columns otherwise than the load before (`imports.lineage`) |
| `definition` | a view | the annotation that defines it, its view row in the lake, and its property's `aspects` row |
| `data` | a table | its table rows in the lake (created, dropped), its loads (`imports.snapshot_id`) and the lake's change log for it |
| `recipe` | a table | its `recipes` rows |
| `property` | a property | its `aspects` and `tests` rows |
| `function` | a function | its `functions` rows |
| `macro` | a macro | its macro rows in the lake |
| `relationships` | a table | the `relationships` rows with a side on the table |
| `relationships` | a dataset | the `relationships` rows of the dataset |
| `assets` | a dataset | the table, column and view rows of its schema in the lake |
| `views <kind>` | a dataset | the view rows of its schema in the lake whose property has that x-kind (`metric` where it names none): a view added or removed; a view defined again stays a member |
| `properties` | an x-kind | the `aspects` rows of that x-kind |

The last five are sets: a member added or removed is a row begun or
ended, so the set changed at that snapshot. A member's own definition
is a node of its own.

## What a read records

A body reads the catalog through the read functions (SPEC §6). Each
read records the nodes it returned rows from, and the nodes its place
was walked over.

| Read | Nodes |
|---|---|
| `annotations(p, where)` | per asset of the place that `p`'s grains admit, `annotation p`, `value p` and the `definition` of the asset's table or view (an annotation is stale once its asset's definition moved); `property` `p`, and `function` of each detector its tests name |
| `annotations('x-kind k', where)` | the same for each property of the kind, and `properties` of `k` |
| `results(f, where)` | per asset of the place `result f` |
| `definition(where)` | per view of the place its `definition`, the `definition` of each table and view it reads, through views, and the `recipe` of each table |
| the subject's data, or a table the body names | `data` of each table read, through views, and the `definition` of each view on the way |
| the lineage, read by a primitive (`glossql_additivity_cells`) | the nodes its place's walk passes |
| the function itself | `function` of the function, `macro` of each macro its body follows, and `property` of the property it fills |

| Place | Nodes its walk passes |
|---|---|
| `itself`, `its dataset` | none |
| `its columns` | a table's `definition`; a view's and those of what it reads; a dataset's `assets` and `relationships` |
| `its sources`, `the columns it joins on` | the view's `definition` and those of what it reads |
| `its relationships` | a view's `definition`, those of what it reads, and `relationships` of the view and of each table and view it reads; a table's or a column's: `relationships` of the table; a dataset's or a relationship's: the dataset's `relationships` |
| `its metric` | the plan's `definition`; the metric's and those of what it reads |
| `its source systems` | a table's `recipe`; a view's `definition`, those of what it reads, and each table's `recipe` |
| `the dataset` | the dataset's `assets` and `relationships` |
| `the dataset's metrics` | the dataset's `views metric`, and each metric's `definition` |

## The check

`glossql_outdated()` serves each stored result that is out of date
with the node that changed:

```sql
SELECT dataset, function, subject, asset, what, reason FROM glossql_outdated();
```

`reason` is `changed` (the node changed after the result read it: the
result is out of date) or `possibly` (a result it read is out of date or
possibly so and is not computed again yet). Once that result is
computed again, the same value leaves its reader current and another
value is a `result <function>` or `value <property>` node that changed,
so its reader is out of date. A result computed again
with the value and error it had keeps its `changed_at`, so what read
it stays current. A value changed and changed back is two snapshots:
the results that read it are computed once more. A node's name is
matched as the lake matches names, whatever its case: a table a body
read before the lake held it is the table created later.
