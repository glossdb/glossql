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

`state` is `current`, `stale`, `possibly`, `conflict` or `missing`. See
[reading](usage/reading.md).

## TESTS

```sql
SELECT * FROM TESTS(orders.amount::unit);
```

Test results: `subject VARCHAR`, `aspect VARCHAR`, `test VARCHAR`,
`state VARCHAR`, `score DOUBLE`, `computed_at TIMESTAMP`, `current
BOOLEAN`, `error VARCHAR`. `state` is `green`, `yellow`, `orange`,
`red` or `error`. Without an argument, the dataset in use. It reads
the stored results; `SELECT <test>() FROM <asset>` computes them. See
[tests](usage/tests.md).

## glossql_kit

```sql
FROM glossql_kit('kits/kpi/kit.sql');
```

`glossql_kit(path VARCHAR)` → `(statement VARCHAR, outcome VARCHAR)`.
Runs a kit's statements in order and adds a row to the `kits` table.
See [kits](kits.md).

## lineage

A view's lineage is a stored result: the result of `lineage` on the
view (`SELECT lineage() FROM <view>`), stored when the view is defined
and computed again once a node it read changed. It holds what
computes each of the view's columns (`derivation`, `complete`,
`shape`), read from its plan. A leaf is `dataset.table.column:TYPE`.
A function, an operator and a `CASE` read as their name over their
operands, a literal as written: `coalesce(a, b)`, `=(a, 'D')`,
`and(a, b)`, `in(a, 'D', 'A')`, `is_null(a)`,
`case(when a then b, else c)`, `row_number() over(partition by a order by b)`.
`complete` is false where a part of the plan could not be resolved; that
part reads `<?>`, or `<?class>` with the expression's class.
`shape` says how the column was computed where that decides how it
adds up: `ratio` (a division by an aggregate), `average`, `distinct` (a
distinct count), else NULL. A sum over one of them keeps the shape.

The lineage kit reads it per dataset (`views_lineage`, `views_scans`, `views_joins`,
`views_mapping`; its SKILL.md).

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
- With no second argument it reads the tables. The second argument is
  `true` (the tables and the views), `'views'`, or a place (`'itself'`,
  `'its columns'`, `'its sources'`): the tables and views
  of that place, walked from the running function's subject.
- `cells => n` bounds what is read. A table with more scalar cells
  than `n` is read as a sample of its rows. The sample is chosen by a
  hash of each row, so the same rows come back on every run. A result
  over a sample is an estimate.

## series_aggregate

```sql
FROM series_aggregate('shop', 'the dataset''s metrics', 'series_axes', 'month');
FROM series_aggregate('shop.revenue', 'the dataset''s metrics', 'series_axes', NULL);
```

`series_aggregate(subject VARCHAR, where VARCHAR, axes VARCHAR, grain
VARCHAR)` → `(view, dimension, member, period, instant, value, num, den,
weight, rows)`. A metric's series, summed over the view's own typed
columns as the rows are read. No row of the view is kept.

- `subject` is a dataset, or one view of it. `where` is a place
  (`'the dataset''s metrics'`). The views read are the views of the
  place, or the one view the subject names.
- `axes` names a table macro that takes the grain and returns one row
  per view: `view`, `period_column`, `value_column`, `numerator`,
  `denominator`, `dims` (the dimension columns, in order), `at_grain`
  and `by_instant`. The kit decides which column plays which part.
  A view whose `at_grain` is NULL is not read.
- One row per period, and per member of each dimension within it.
  `dimension` is `''` for the total. `member` is the value as text,
  `'NULL'` where it has none. `period` is the period column truncated
  to `at_grain`. `instant` is the period column's own value where
  `by_instant` is true, else NULL.
- `value`, `num` and `den` are the sums of the value column and of a
  ratio's two halves. `weight` is the sum of the value's magnitude per
  member. `rows` counts the rows summed.
- A number is read as itself where its type converts exactly, else
  through its text. A column the view does not have reads as NULL. A
  view that does not bind is skipped. A nested column has no member.

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
