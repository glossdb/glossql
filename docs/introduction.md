# Introduction

glossql is a DuckDB extension. It adds a handful of statements and two
table functions to DuckDB SQL, and keeps its catalog in a DuckLake
beside the lake's own catalog tables, so what is known about the data
is versioned with the data.

## What it adds to DuckDB

- **Statements** to declare what must be known (`DECLARE ASPECT`), how
  it is measured (`DECLARE FUNCTION`) and tested (`DECLARE WITNESS`),
  where data comes from (`DECLARE SOURCE`, `DECLARE RECIPE`, `IMPORT`),
  and to record values (`GLOSS`).
- **Two table functions** to read it back: `GLOSSARY()`, what is known
  per asset and property, and `ATTEST()`, the test results.
- **A catalog** in the lake: eleven tables beside the `ducklake_*`
  tables, readable as plain views.
- **Kits**: scripts of declarations for a domain, with a skill that
  teaches an agent the words.

Everything else is DuckDB: datasets are the lake's schemas, loads are
its snapshots, a metric is a view.

## Where to go next

- [Installation](installation.md): build, load, attach a workspace.
- Usage: [sessions](usage/sessions.md),
  [loading data](usage/loading-data.md),
  [describing data](usage/describing-data.md),
  [measurements](usage/measurements.md), [tests](usage/tests.md),
  [relationships](usage/relationships.md), [reading](usage/reading.md),
  [lineage](usage/lineage.md).
- [Kits](kits.md) and [functions](functions.md).
- [Terms](terms.md): every word beside the industry's.
- The normative text: [SPEC.md](../SPEC.md); what this extension
  implements of it: [CONFORMANCE.md](../CONFORMANCE.md).
