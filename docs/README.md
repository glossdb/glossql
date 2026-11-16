# Documentation

glossql is a DuckDB extension. These pages say how to use it and list
what it provides. The language itself is defined in
[SPEC.md](../SPEC.md); the syntax is [grammar.ebnf](../grammar.ebnf).

Start here:

- [Installation](installation.md): build, load, open a workspace, the
  CLI and the example MCP server.
- [The data harness](harness.md): the idea, and how the parts work
  together.
- [Terms](terms.md): each keyword beside the industry's word.

Usage:

- [Loading data](usage/loading-data.md)
- [Describing data](usage/describing-data.md): properties,
  annotations, views, relationships.
- [Measurements](usage/measurements.md)
- [Tests](usage/tests.md)
- [Reading what is known](usage/reading.md): the glossary, the catalog
  tables, lineage.

Reference:

- [Functions](functions.md)
- [Kits](kits.md)
- [The catalog](specification/introduction.md), one page per table:
  [datasets](specification/tables/datasets.md),
  [sources](specification/tables/sources.md),
  [recipes](specification/tables/recipes.md),
  [imports](specification/tables/imports.md),
  [aspects](specification/tables/aspects.md),
  [glossary](specification/tables/glossary.md),
  [relationships](specification/tables/relationships.md),
  [functions](specification/tables/functions.md),
  [measurements](specification/tables/measurements.md),
  [tests](specification/tables/tests.md),
  [kits](specification/tables/kits.md).

For maintainers: [updating DuckDB](internals/updating-duckdb.md).
