# The catalog

glossql stores what is declared and known about the data in eleven
tables. They are in the DuckLake's metadata catalog, beside the lake's
own `ducklake_*` tables, as `glossql_<name>`.

`PRAGMA glossql` creates the tables where they are missing. It also
creates one view per table in the lake's `main` schema,
`ws.main.<name>`. The views return JSON columns as JSON and list
columns as lists. Read the views; the statements write the tables.

## How rows are written and read

- **Statements add rows.** A statement does not update a row. `DELETE
  FROM glossary` is the one statement that removes rows.
- **`seq` orders the rows.** Each table numbers its rows from 1.
- **A key says what a new row replaces.** In a table with a key, the
  newest row per key is the one in force. A table without a key is
  history; `GLOSSARY()` and `TESTS()` decide which of its rows count.
- **An unchanged declaration adds no row.**

## Tables

| Table | A row is | Key |
|---|---|---|
| [datasets](tables/datasets.md) | a declared dataset | `name` |
| [sources](tables/sources.md) | a declared source | `name` |
| [recipes](tables/recipes.md) | a declared load query | `dataset`, `table_name` |
| [imports](tables/imports.md) | one load of a table | none |
| [aspects](tables/aspects.md) | a declared property | `name` |
| [glossary](tables/glossary.md) | one annotation | none |
| [relationships](tables/relationships.md) | a declared key between columns | none |
| [functions](tables/functions.md) | a declared measurement or detector | `name`, `scope` |
| [measurements](tables/measurements.md) | one result of a measurement | none |
| [tests](tables/tests.md) | a declared test on a property | `aspect` |
| [kits](tables/kits.md) | one load of a kit | none |
