# Documentation

glossql is the language for writing data harnesses, built as a DuckDB
extension. This documentation has two parts: the extension, and the
specification of its catalog. The language itself is
[SPEC.md](../SPEC.md), with the grammar in
[grammar.ebnf](../grammar.ebnf); what this extension implements of it
is [CONFORMANCE.md](../CONFORMANCE.md).

## The extension

- [Introduction](introduction.md)
- [Installation](installation.md)
- [Terms](terms.md): every word, beside the industry's.

Usage:

- [Sessions and workspaces](usage/sessions.md)
- [Loading data](usage/loading-data.md)
- [Describing data](usage/describing-data.md)
- [Measurements](usage/measurements.md)
- [Tests](usage/tests.md)
- [Relationships](usage/relationships.md)
- [Reading what is known](usage/reading.md)
- [Lineage](usage/lineage.md)

Reference:

- [Functions](functions.md)
- [Kits](kits.md)
- [Interfaces](interfaces.md): the CLI and the MCP server.

## Specification

- [Introduction](specification/introduction.md): the catalog and its
  tables.
- Tables: [datasets](specification/tables/datasets.md),
  [kits](specification/tables/kits.md),
  [sources](specification/tables/sources.md),
  [recipes](specification/tables/recipes.md),
  [imports](specification/tables/imports.md),
  [aspects](specification/tables/aspects.md),
  [glossary](specification/tables/glossary.md),
  [relationships](specification/tables/relationships.md),
  [functions](specification/tables/functions.md),
  [measurements](specification/tables/measurements.md),
  [witnesses](specification/tables/witnesses.md).

## Internals

- [How glossql maps onto DuckDB](internals/duckdb.md)
- [Updating DuckDB](internals/updating-duckdb.md)
