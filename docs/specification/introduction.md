# The catalog

glossql stores what is declared and known about the data in eleven
tables. They are in the DuckLake's metadata catalog, beside the lake's
own `ducklake_*` tables, as `glossql_<name>`.

`PRAGMA glossql` creates the tables where they are missing. It also
creates one view per table in the lake's `main` schema,
`ws.main.<name>`. The views return JSON columns as JSON and list
columns as lists. Read the views; the statements write the tables.

## How rows are written and read

- **A write takes a lake snapshot.** Each row carries `begin_snapshot`,
  the snapshot its write took, and `end_snapshot`, the snapshot of the
  write that replaced or removed it. No row is deleted, so the data and
  the catalog have one order.
- **The views serve the current rows**, those with no end. Each table
  also reads whole as `<name>_history`, every row with its
  `end_snapshot`.
- **A key says what a new row replaces.** A new row ends the current
  row with the same key. `DELETE FROM glossary` ends the rows it
  selects.
- **An unchanged declaration adds no row.**

## Tables

| Table | A row is | Key |
|---|---|---|
| [datasets](tables/datasets.md) | a declared dataset | `name` |
| [sources](tables/sources.md) | a declared source | `name` |
| [recipes](tables/recipes.md) | a declared load query | `dataset`, `table_name` |
| [imports](tables/imports.md) | one load of a table | none |
| [properties](tables/properties.md) | a declared property | `name` |
| [glossary](tables/glossary.md) | one annotation | none |
| [relationships](tables/relationships.md) | a declared key between columns | none |
| [functions](tables/functions.md) | a declared function or detector | `name`, `scope` |
| [results](tables/results.md) | one result of a function | none |
| [tests](tables/tests.md) | a declared test on a property | `property` |
| [kits](tables/kits.md) | one load of a kit | none |
