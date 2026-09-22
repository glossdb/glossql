# Storage

The lake is the store. A workspace is a warehouse directory plus a
catalog file; every relation the language declares crosses the lake as
an Iceberg table, and the tables recipes land live beside them under
the same catalog.

## Layout

- **Datasets are namespaces.** Each dataset the workspace lands is an
  Iceberg namespace holding its tables. A dataset's settings ride its
  namespace as a property.
- **The store's own relations live in one namespace** — `glossql`, one
  table per relation (glosses, functions, aspects, witnesses,
  measurements, relationships). A workspace holds many datasets, so a
  dataset-scoped relation carries a `dataset` column declared as its
  identity-partition key: separate files per dataset and pruning on a
  dataset filter are the format's own feature, not a namespace layout.
- **Two relations are the lake's own record, composed at read** —
  `datasets` from the namespace list, `imports` from the append
  snapshots. No table of the store's carries them.
- **Facts ride what they describe.** A dataset's settings on its
  namespace; a recipe's source and SQL on its table; a landing's
  source-side facts — scans, dropped rows, cast failures — on the
  snapshot that rode it.
- **Writes are appends.** Supersession stays a read rule; replacement
  is a later row, never an update, and a scan of an unwritten relation
  is empty, never an act — tables are created by the first append
  alone. The appended rows are themselves the event record: who said
  what, as which kind, when.
- **The one in-memory hold is the mounted catalog provider**, shared
  by every session and rebuilt when a namespace is created or a table
  is created or dropped — it freezes the namespace list and each
  namespace's table map at build; a table lookup inside a namespace
  reads that map, never the catalog, so a recipe's new table is seen
  by the rebuild its create causes. Nothing held in memory is ever the
  record.

## The catalog

The catalog sits behind the `Catalog` trait, built at one site:
iceberg-rust's SqlCatalog, in process, on the workspace's own SQLite
file or on the Postgres server `GLOSSQL_CATALOG_SQL` names — one
implementation, the bind style following the URI's scheme
([install](../start/install.md)). The same database holds the record
([store](store.md)), opened on the same URI. The catalog tier is
relied on, never copied: table names, schemas, and snapshot ids are answered
by the provider chain, not mirrored into a structure of the server's
own.

The bytes, whichever catalog answers, move through one seam: iceberg's
`Storage` trait, implemented once over the `object_store` crate the
engine already runs on, for the S3 family and the Azure family. A REST
catalog's table loads deliver the store's properties and, vending, its
credentials; a SQL catalog's warehouse in a bucket (`GLOSSQL_WAREHOUSE`)
delivers none, and the store's own environment conventions configure
the client — on Azure with nothing set, the managed identity. The
local filesystem stays the dev shape.

Landings read back from the format's own record: one entry per append
snapshot, its facts taken from the snapshot summary it rode.
