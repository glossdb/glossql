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
  by every session and rebuilt when a namespace or a table is created
  — it freezes the namespace list and each namespace's table map at
  build; a table lookup inside a namespace reads that map, never the
  catalog, and a recipe's table registered by a session enters it
  live. Nothing held in memory is ever the record.

## The catalog

The catalog sits behind the `Catalog` trait — iceberg-rust's
SqlCatalog on a SQLite file, in process, built at one site; a
different catalog is a different builder there, and nothing above the
trait changes. The catalog tier is relied on, never copied: table
names, schemas, and snapshot ids are answered by the provider chain,
not mirrored into a structure of the server's own.

Landings read back from the format's own record: one entry per append
snapshot, its facts taken from the snapshot summary it rode.
