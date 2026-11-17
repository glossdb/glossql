# edges

A row is one lineage edge a stored result holds. A view's lineage, the
result of `lineage` on the view (SPEC §6), holds one edge from the view
to each table, column and view it reads; the lineage kit's
`lineage(dataset)` reads them with the edges the catalog holds itself.
Defining the view writes its rows with its lineage, in the same
transaction, and `SELECT lineage() FROM <view>` writes them again once
a node the lineage read changed. Key: `begin_snapshot`, `from`, `to`
and `kind`. A row carries its result's `begin_snapshot` and ends when
its result does: a view ended takes its edges with it.

| Column | Type | Description |
|---|---|---|
| `begin_snapshot` | BIGINT | The lake snapshot the result took: `measurements.begin_snapshot`. |
| `end_snapshot` | BIGINT | The snapshot of the write that replaced or retired the result; NULL while the result is stored. The view leaves this column out. |
| `from` | VARCHAR | The asset the edge leaves, by its path: the view. |
| `to` | VARCHAR | The asset it reaches, by its path: a table, a column or a view the view reads. |
| `kind` | VARCHAR | What the edge says: `reads`. |
