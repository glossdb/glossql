# The catalog

glossql keeps what is declared and known about the data in eleven
tables. They live in the DuckLake's metadata catalog, beside the lake's
own `ducklake_*` tables, as `glossql_<name>`. So what is known about the
data is versioned with the data, and one transaction-safe database holds
both.

`PRAGMA glossql` creates the tables where they are missing. It also
creates one view per table in the lake's `main` schema, `ws.main.<name>`.
The views serve JSON columns as JSON and list columns as lists. Read the
views; the statements write the tables.

## How rows are written and read

- **Append only.** A statement adds a row. It never updates one. The
  one exception is `DELETE FROM glossary`, which removes rows.
- **`seq` orders the rows.** Each table numbers its rows from 1, the
  newest highest.
- **A key says what a new row replaces.** A declaration table has a key:
  a new row with the same key supersedes the old one, and the reads use
  the newest row per key. A table without a key is history; its reads
  decide which rows count (the glossary, for example, keeps the newest
  value per asset, aspect and kind of actor).
- **An unchanged declaration adds nothing.** Declaring the same thing
  again with the same content is a no-op.

The language that writes these rows is defined in
[SPEC.md](../../SPEC.md). What this extension implements of it, section
by section: [CONFORMANCE.md](../../CONFORMANCE.md).

## Tables

| Table | A row is | Key |
|---|---|---|
| [datasets](tables/datasets.md) | a declared dataset | `name` |
| [sources](tables/sources.md) | a declared source | `name` |
| [recipes](tables/recipes.md) | a declared load query | `dataset`, `table_name` |
| [imports](tables/imports.md) | one load of a table | none |
| [aspects](tables/aspects.md) | a declared property of the harness schema | `name` |
| [glossary](tables/glossary.md) | one annotation | none |
| [relationships](tables/relationships.md) | a declared key between columns | none |
| [functions](tables/functions.md) | a declared measurement or detector | `name`, `scope` |
| [measurements](tables/measurements.md) | one result of a measurement | none |
| [witnesses](tables/witnesses.md) | a declared test on an aspect | `aspect` |
| [kits](tables/kits.md) | one load of a kit | none |
