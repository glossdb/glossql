# The store

Every relation the language declares — glosses, functions, aspects,
witnesses, sources, relationships, measurements — is an Iceberg table
in the workspace's lake. The record keeps no database of its own —
SQLite remains only as the catalog's backend: the tables the workspace
lands and the record the language keeps live in one storage system,
under one transaction model.

## One store

Rows are ordered by the format's own row lineage — the commit sequence
number and the position in file — so the store never mints its own
ordering and writers never coordinate. Writes are appends; one
statement is one commit. Nothing updates in place.

**Supersession is a read, never an update.** The current slot for a
subject is the latest row per (subject, aspect, actor kind), computed
at read time. History is therefore never destroyed by a correction: the
superseded row remains, carrying its actor and timestamp, and the read
rule decides what "current" means.

## One pin

A statement's reads are pinned to a snapshot, so two scans inside one
query cannot straddle a landing. A pin stays addressable after later
commits, which makes it a durable key.

**Measurements are not a cache.** Measurement rows are keyed by the pin
digest of the data they measured. Old rows are the drift record, not
garbage; reads never write; a duplicate measurement row is harmless.
Re-measuring the same pin answers the same fact.

## One head

Reading the record means one Iceberg scan per relation, and each scan
opens one small file per row ever written there — so the cost tracks a
workspace's write history, not its data. The store therefore holds what
it resolved, keyed by its own version: every relation at its snapshot,
enumerated from the catalog rather than curated, so a relation added
later joins the key on its own.

**A commit moves the version; nothing else does.** Appending is the one
place a store relation changes, so it is the one place the head is
dropped — after the commit lands, never before, or a concurrent reader
would cache the snapshots from before the write and nothing would be
left to displace them. There is no freshness check on the read path and
no invalidation call at the write: a moved version simply misses.

What the *lake* knows is never held this way. The subjects that exist
and each table's current snapshot are read from the catalog on every
statement, so a landing is visible the moment it commits.

Holding the head in memory is correct while one process owns the
workspace. A second writer's commit would leave it stale with nothing
to say so; two processes need shared state, which is a different
design.

## One plan

A statement plans once, against schemas resolved before planning
begins: every door it names — a grounding, a scenario, a subject
column — is fetched and built into a plan in the async pre-pass, so
planning itself is synchronous with nothing left to fetch.

## Streaming

Reads stream end to end, one batch in memory at a time. The Arrow
door never caps — the client drains the stream, and hanging up
cancels the work upstream. The MCP door's row cap pulls batches until
the cap is met, then drops the stream: what the reader won't see, the
engine stops computing.

## Invalidation and disclosure

Definition changes are coarse, explicit, and refused rather than
migrated; dependencies are declared, never sniffed; data staleness is
marked at read. Machinery never suppresses judgment: `stale` is served
and marked, `contested` is withheld with its band and score,
`unassessed` is a visible row.
