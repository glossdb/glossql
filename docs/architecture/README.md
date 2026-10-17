# Architecture

How the server is built.

- [`kernel.md`](kernel.md) — the business information kernel: the
  core, the kits, drivers and services, and what is planned in which
  order.
- [`store.md`](store.md) — one store, one pin, one plan; how reads and
  writes behave.
- [`substrate.md`](substrate.md) — building on DataFusion: the seams
  used and the rules they impose.
- [`storage.md`](storage.md) — the lake's physical layout and the
  catalog.

## The crates

The server is a Cargo workspace. `parser` reads the language;
`glossary` is the record and its reads; `catalog` is the lake, its
snapshots and the pin; `import` lands sources through recipes;
`scripts` carries the shipped functions and the kits; `session` runs
statements; `apps` serves the pages; `serverd` is the binary with its
doors. Inside `session`, `shared.rs` is the leaf every other module
reads: the refusal, the outcome, the runtime seam, the state a session
holds and the resolution a statement carries into planning, and it
imports no module of its own. `reads.rs` plans the record —
`GLOSSARY()`, `ATTEST()`, the store's relations, a served grounding —
and `doors.rs` answers the doors by name, with the registry serverd
lists. The conformance suite holds the crate's mutual imports at a
ceiling that only comes down.
