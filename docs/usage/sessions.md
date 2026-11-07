# Sessions and datasets

A session is a DuckDB connection with the glossql grammar on and a
DuckLake attached as its workspace. A dataset is a schema of that
lake: the working set for one topic.

## Open a session

```sql
LOAD glossql;
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
PRAGMA glossql;
```

`PRAGMA glossql` turns the grammar on for this connection and creates
the catalog in the lake: the `glossql_*` tables beside the `ducklake_*`
tables, read as views in `ws.main`. Run it after the `ATTACH`.

A Postgres-backed lake works the same way and lets several clients
share one workspace.

## Declare a dataset and use it

```sql
DECLARE DATASET fin SET (purpose: 'working-capital analysis');
USE fin;
```

`USE` is DuckDB's own. Under it, unprefixed paths resolve against the
dataset: `orders` is `fin.orders`, `orders.amount` is
`fin.orders.amount`. The full `dataset.table.column` path always works.
`USE ws.fin` names the lake and the dataset at once.

A two-part path whose head names both a dataset and a table of the
dataset in use is `table.column`: the dataset in use is the nearer
scope.

## Say who is speaking

The actor rides the connection, as two session variables:

```sql
SET VARIABLE glossql_actor = 'human';
SET VARIABLE glossql_actor_id = 'ada@example.com';
```

- `glossql_actor` is the kind, `agent` or `human`. A value is kept per
  asset, property and kind, so a person's value and an agent's stand
  side by side, and the person's wins on read.
- `glossql_actor_id` is the writer, stored with every value.

Nothing in a statement names the actor. A wrapper that authenticates
its caller sets both on the caller's connection; that is the whole
identity model.

## More than one lake

A connection reads one workspace. With several DuckLakes attached,
name the one glossql writes to:

```sql
SET VARIABLE glossql_workspace = 'ws';
```

Normative text: [SPEC.md §3](../../SPEC.md#3-sources-and-datasets).
