# Storage

A workspace is a catalog and a warehouse. The catalog is one SQL
database — the workspace directory's SQLite file on a laptop, the
Postgres server `GLOSSQL_CATALOG_SQL` names in a deployment — and it
holds two things side by side: the record, every relation the
language declares as a table of rows ([store](store.md)), and the
data plane's own tables — every landed table, its versions, and each
version's files. The warehouse is where the files are: a directory,
or the object-store location `GLOSSQL_WAREHOUSE` names.

## The data plane's tables

The data plane's tables have the names and columns the DuckLake 1.0
specification gives them (`ducklake_snapshot`, `ducklake_schema`,
`ducklake_table`, `ducklake_column`, `ducklake_data_file`, …), so any
DuckLake reader attaches to the same database and reads the same
files. All twenty-eight exist; the server writes the ones a landing,
a replace, an append and a drop touch.

- **A dataset is a schema row.** `DECLARE DATASET` writes it; its
  settings are a row of the record's `datasets` relation.
- **A landed table is a table row, its column rows and its file
  rows.** A column row spells its type in the specification's
  vocabulary, and the type set a landing holds is what
  `glossql-import` folds every source type into, so the two agree by
  construction. A file row names a parquet file under the table's
  directory in the warehouse, with its size, its row count and the
  size of its footer.
- **A version is a snapshot.** Every write is one transaction that
  appends a snapshot row; a file row carries the snapshot it began at
  and, once replaced or dropped, the snapshot it ended at. A table's
  version is the snapshot that last changed it, and that is the
  number a gloss row stores as `snapshot_id` and the pin carries.
- **A column is versioned.** A replace keeps the row of a column
  whose name, type and recipe expression are unchanged, begins a new
  version under the same `column_id` when its type or expression
  changed, and ends one that is gone; a new column takes a new id.
  The expression is the column's `glossql.expr` tag, in the
  specification's column-tag table. That history is what a gloss
  ages against: a column's live version, or the snapshot a column of
  the table was last re-versioned or dropped at, later than the
  gloss's snapshot.
- **A replace is one commit.** The new rows are written beside the
  old files first; the commit ends the old file rows and begins the
  new ones. A reader that pinned the table before the commit reads
  the old files, one after it the new; there is no moment without a
  table. The ended files are scheduled for deletion and deleted by a
  later commit once a grace has passed, longer than any statement
  runs. The data keeps no history.
- **The cube's head is a table.** Each metric's cells at the floor
  grain, over the longest window of the ladder, land in the catalog's
  `main` schema as `cube__<dataset>__<metric>`, sorted by period and
  replaced in one commit; the key the cube was built at — the data
  legs and a digest over what else the build read — and its fact row
  are the table's tags, `glossql.cube.key` and `glossql.cube.fact`.
  Every grain a read serves is a plan over the head, windowed to the
  ladder's rung for that grain and cached beside it in memory; it is
  never landed. Every instance and every restart read the same head,
  and the engine's cache is the read-through level over it: a landing
  rebuilds the heads over the tables it moved before it answers, and
  every grain follows. A cube that abstains, or whose frame reads the
  record itself, stays in memory.
- **A landing's facts are the record's.** What it read, what it
  dropped, the casts, the files and the version it made are one row
  of the `imports` relation, written beside the commit and outliving
  the files. The recipe behind a table is a row of `recipes`.

## Reads

A statement pins its dataset with one query over the tables, columns
and live files, whatever the table count, and each table becomes a
provider over its file list at that version — nothing is listed or
fetched from the warehouse at plan time. The scan is the engine's own
parquet scan, so the engine's pruning and its dynamic filters apply to
every landed table.

The mounted catalog — what `information_schema` and a
dataset-qualified name resolve through — is built from the same
query, once, and shared by every session until a commit rebuilds it.

## The warehouse

Files are written by the engine's parquet writer under
`<warehouse>/<dataset>/<table>/`, one file per landing. The object
store behind a remote warehouse is the crate the engine already runs
on, one client per bucket or container, configured by the
environment: on Azure with nothing set, the managed identity; on
Google Cloud, the attached service account. The local filesystem stays
the laptop's shape.
