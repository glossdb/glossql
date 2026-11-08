# Functions

The functions the extension adds to DuckDB. `GLOSSARY()` and `TESTS()`
are grammar; the rest are ordinary DuckDB functions any SQL can call.

## GLOSSARY

```sql
SELECT * FROM GLOSSARY(orders.amount);
SELECT * FROM GLOSSARY(orders.amount, all => true);
SELECT * FROM GLOSSARY(fin::dso);
```

What is known about an asset and what lies under it. Without an
argument, the dataset in use.

- Default: `(subject, aspect, value, band, score, state)`, one row per
  asset and property; `state` is `current`, `stale`, `contested` or
  `unassessed`.
- `all => true`: `(subject, aspect, kind, test, actor, body,
  written_at, current)`, one row per source of a value.
- `subject::aspect` narrows to one property.

See [reading](usage/reading.md).

## TESTS

```sql
SELECT * FROM TESTS(orders.amount::behavior);
```

Test results: `(subject, aspect, test, band, score, computed_at,
current, error)`. Without an argument, the dataset in use. See
[tests](usage/tests.md).

## glossql_kit

```sql
FROM glossql_kit('kits/kpi/kit.sql');
```

`glossql_kit(path VARCHAR)` → `(statement VARCHAR, outcome VARCHAR)`.
Runs a kit statement by statement and records it in `kits`. See
[kits](kits.md).

## glossql_lineage

```sql
FROM glossql_lineage($$SELECT id, amount * 2 AS twice FROM fin.orders$$);
```

`glossql_lineage(sql VARCHAR)` → `(column VARCHAR, derivation VARCHAR,
complete BOOLEAN)`. What computes each output column of a `SELECT`, read
off its bound plan; a leaf is `scan.column:type`.

`glossql_views_lineage(dataset VARCHAR)` → `(view, column, derivation,
complete)`: the same for every view of a dataset. See
[lineage](usage/lineage.md).

## glossql_columns

```sql
FROM glossql_columns('fin');
FROM glossql_columns('fin.orders', false);
```

`glossql_columns(subject VARCHAR [, views BOOLEAN])` → `(table_name,
column_name, column_type, ordinal_position, table_type)`. Every column
of a dataset's tables, or of one table, and of its views unless `views`
is false. It walks the dataset's own relations by name, where
`information_schema` binds every view of every attached catalog.

## dataset_cells

```sql
FROM dataset_cells('fin');
FROM dataset_cells('fin.orders');
FROM dataset_cells('fin', 'views');
FROM dataset_cells('fin', cells => 1000000);
```

`dataset_cells(subject VARCHAR [, mode VARCHAR | views BOOLEAN] [, cells
=> BIGINT])` → `("table", "column", ci, column_type, rn, val)`. Every
scalar cell of a dataset's tables, or of one table, as one relation.

- `val` is the value as text; `column_type` is the stored type, so a
  screen compares values within one type. `ci` numbers the columns in
  table order, `rn` the rows of each table. Nested columns are left out.
- The second argument is `'tables'` (the default), `'views'` or
  `'all'`; `true` means `'all'`.
- `cells => n` bounds what is read. A table whose scalar columns times
  rows exceed the bound is read as a sample of its rows, chosen by a
  hash of each row, so the same rows come back every run on any number
  of threads. The result is then an estimate; `rn` says how many rows
  were read.

## subject_column

```sql
SELECT min(v), max(v) FROM subject_column('fin.orders.amount');
```

`subject_column(subject VARCHAR)` → `(v)`. One column as a relation
named `v`. Takes `dataset.table.column`. A measurement's body reads its
asset with `subject_column($subject)`.

## json_schema_check

```sql
SELECT json_schema_check('{"type": "object", "required": ["value"]}', '{"value": "EUR"}');
```

`json_schema_check(schema JSON, document JSON)` → `VARCHAR`: NULL when
the document matches, else the first failure, such as `at $: missing
required property 'value'`. Every value and every measurement passes
through it, over the yyjson library DuckDB ships.

## glossql_service

```sql
SELECT glossql_service('forecast', '{"series": []}');
SELECT glossql_service('forecast', '{"series": []}', true);
```

`glossql_service(route VARCHAR, body JSON [, soft BOOLEAN])` → `JSON`.
Posts `body` to a model service at `route` and returns its answer. A
call that fails refuses; with `soft` true it returns `{"error": …}`
instead, so a kit's query can keep going. DuckDB's `TRY()` refuses
volatile functions, which is why the soft form exists.

| Setting | Default | Meaning |
|---|---|---|
| `glossql_service_url` | empty | the service's base URL; empty refuses every call by name |
| `glossql_service_token` | empty | the bearer sent on every call; empty sends none |
| `glossql_service_timeout` | 600 | seconds one call may take, in all |

POST and TLS need `httpfs` loaded.
