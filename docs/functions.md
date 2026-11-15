# Functions

The functions the extension adds to DuckDB. `GLOSSARY()` and `TESTS()`
are part of the grammar. The rest are DuckDB functions that any SQL
can call.

## GLOSSARY

```sql
SELECT * FROM GLOSSARY(orders.amount);
SELECT * FROM GLOSSARY(orders.amount, all => true);
SELECT * FROM GLOSSARY(shop::revenue);
```

What is known about an asset and the assets under it. Without an
argument, the dataset in use. `subject::aspect` narrows to one
property.

| Form | Columns |
|---|---|
| default | `subject VARCHAR`, `aspect VARCHAR`, `value JSON`, `test_state VARCHAR`, `score DOUBLE`, `state VARCHAR` |
| `all => true` | `subject`, `aspect`, `kind`, `test`, `actor`, `body`, `written_at`, `current` |

`state` is `current`, `stale`, `conflict` or `missing`. See
[reading](usage/reading.md).

## TESTS

```sql
SELECT * FROM TESTS(orders.amount::unit);
```

Test results: `subject VARCHAR`, `aspect VARCHAR`, `test VARCHAR`,
`state VARCHAR`, `score DOUBLE`, `computed_at TIMESTAMP`, `current
BOOLEAN`, `error VARCHAR`. `state` is `green`, `yellow`, `orange`,
`red` or `error`. Without an argument, the dataset in use. See
[tests](usage/tests.md).

## glossql_kit

```sql
FROM glossql_kit('kits/kpi/kit.sql');
```

`glossql_kit(path VARCHAR)` → `(statement VARCHAR, outcome VARCHAR)`.
Runs a kit's statements in order and adds a row to the `kits` table.
See [kits](kits.md).

## glossql_lineage

```sql
FROM glossql_lineage($$SELECT id, amount * 2 AS twice FROM shop.orders$$);
```

`glossql_lineage(sql VARCHAR)` → `(column VARCHAR, derivation VARCHAR,
complete BOOLEAN, shape VARCHAR)`. What computes each output column of
a `SELECT`, read from its plan. A leaf is `dataset.table.column:TYPE`.
A function, an operator and a `CASE` read as their name over their
operands, a literal as written: `coalesce(a, b)`, `=(a, 'D')`,
`and(a, b)`, `in(a, 'D', 'A')`, `is_null(a)`,
`case(when a then b, else c)`, `row_number() over(partition by a order by b)`.
`complete` is false where a part of the plan could not be resolved; that
part reads `<?>`, or `<?class>` with the expression's class.
`shape` says how the column was computed where that decides how it
adds up: `ratio` (a division by an aggregate), `average`, `distinct` (a
distinct count), else NULL. A sum over one of them keeps the shape.

Four functions return the same for the views of a dataset. Pass NULL
for every dataset.

| Function | Columns | A row is |
|---|---|---|
| `glossql_views_lineage(dataset VARCHAR)` | `dataset`, `view`, `column`, `derivation`, `complete`, `shape` | one output column of a view |
| `glossql_views_scans(dataset VARCHAR)` | `dataset`, `view`, `table_name`, `columns` | one table a view scans, joins and filters included, with the columns it reads |
| `glossql_views_joins(dataset VARCHAR)` | `dataset`, `view`, `left_column`, `right_column`, `left_unique`, `right_unique` | one pair of columns a view's join equates, as `schema.table.column` |
| `glossql_views_mapping(dataset VARCHAR)` | `dataset`, `view`, `column`, `branch`, `derivation`, `table_name`, `table_column`, `alias`, `condition`, `literals`, `complete` | one output column of a view per branch of the union its rows come from (1 where it has none) and per lake column it is computed from: the branch's derivation, where a table read more than once carries each scan's alias as `scan[alias].column`; what the view's joins and filters require of that scan alone; the literals the derivation tests or returns, as written. `complete` is false where the walk could not follow a step or two scans of one table read alike |

`left_unique` and `right_unique` are true where the view joins that
side with one row per value of the join columns: a subquery grouped by
them, `DISTINCT` on them, or filtered to `row_number() = 1` partitioned
by them.

## glossql_columns

```sql
FROM glossql_columns('shop');
FROM glossql_columns('shop.orders', false);
```

`glossql_columns(subject VARCHAR [, views BOOLEAN])` → `(table_name,
column_name, column_type, ordinal_position, table_type)`. Every column
of a dataset's tables, or of one table, and of its views unless `views`
is false.

## dataset_cells

```sql
FROM dataset_cells('shop');
FROM dataset_cells('shop.orders');
FROM dataset_cells('shop', 'views');
FROM dataset_cells('shop', cells => 1000000);
```

`dataset_cells(subject VARCHAR [, mode VARCHAR | views BOOLEAN] [, cells
=> BIGINT])` → `("table", "column", ci, column_type, rn, val)`. Every
scalar cell of a dataset's tables, or of one table, as one relation.

- `val` is the value as text. `column_type` is the stored type. `ci`
  numbers the columns, `rn` the rows of each table. Nested columns are
  left out.
- The second argument is `'tables'` (the default), `'views'`,
  `'all'`, or a place of the lineage kit (`'itself'`, `'its metric'`,
  `'the dataset''s metrics'`): the tables and views of that place,
  walked from the running function's subject; `true` means `'all'`.
- `cells => n` bounds what is read. A table with more scalar cells
  than `n` is read as a sample of its rows. The sample is chosen by a
  hash of each row, so the same rows come back on every run. A result
  over a sample is an estimate.

## subject_column

```sql
SELECT min(v), max(v) FROM subject_column('shop.orders.amount');
```

`subject_column(subject VARCHAR)` → `(v)`. One column as a relation
with the column `v`. Takes `dataset.table.column`. A measurement's body
reads its asset with `subject_column($subject)`.

## json_schema_check

```sql
SELECT json_schema_check('{"type": "object", "required": ["value"]}', '{"value": "EUR"}');
```

`json_schema_check(schema JSON, document JSON)` → `VARCHAR`: NULL when
the document matches the schema, else the first failure as text. The
extension validates every annotation and every measurement with it.

## glossql_service (preview)

```sql
SELECT glossql_service('forecast', '{"series": []}');
SELECT glossql_service('forecast', '{"series": []}', true);
```

`glossql_service(route VARCHAR, body JSON [, soft BOOLEAN])` → `JSON`.
Posts `body` to a model service at `route` and returns its answer. A
call that fails raises an error. With `soft` true it returns
`{"error": …}` instead, so a query can go on.

| Setting | Default | Meaning |
|---|---|---|
| `glossql_service_url` | empty | the service's base URL; while it is empty, every call is refused |
| `glossql_service_token` | empty | the bearer token sent with every call; empty sends none |
| `glossql_service_timeout` | 600 | seconds one call may take |

The call needs `httpfs` loaded.

## Session variables

| Variable | Meaning |
|---|---|
| `glossql_actor` | the kind of actor writing: `agent` or `human` |
| `glossql_actor_id` | the writer's id |
| `glossql_workspace` | the DuckLake to use when several are attached |

See [installation](installation.md#set-the-actor).
