# The store

Every relation the language declares — glosses, functions, aspects,
witnesses, sources, relationships, measurements — is a table in the
catalog's own database: the workspace directory's SQLite file on a
laptop, the Postgres server a deployment names. The tables the
workspace lands are Iceberg tables in the lake; the record the
language keeps sits beside the catalog that names them.

## One store

Rows are ordered by the database's own identity column, assigned at
insert, so the store never mints its own ordering. Writes are inserts;
each statement's rows land as it runs. Nothing updates in place.

**Supersession is a read, never an update.** The current slot for a
subject is the latest row per (subject, aspect, actor kind), computed
at read time. A correction therefore never destroys history: the
superseded row remains, carrying its actor and timestamp, and the read
rule decides what "current" means.

## One pin

A statement's reads are pinned to a snapshot, so two scans inside one
query cannot straddle a landing. A pin stays addressable after later
commits, which makes it a durable key.

**Measurements are not a cache.** Measurement rows carry the pin of
the data they measured and, in `reads`, the names of the inputs the
computation read; a row stands, and serves, while those inputs are
unchanged — scope the effect to its cause, so ordinary glossing work
never churns the record. Old rows are the drift record, not garbage;
reads never write; a duplicate measurement row is harmless.
Re-measuring over unchanged inputs answers the same fact.

The `reads` vocabulary, visible wherever the `measurements` relation
is read as a table: `<dataset>.<table>` and `glossql.<relation>` name
individual inputs, `*` is every table of the row's dataset, `**` means
the reads could not be enumerated (the row then stands only at its
exact pin — the conservative floor), and empty means the body read
nothing at all, so its answer cannot change and always stands.

## One version

The store's version is every relation at its own — the highest
identity it holds — and any write moves it. A read context is built
from it on every statement: six reads of the record, and the pin over
the data snapshots and the relation versions. A cube built at a
version serves while the version stands.

What the *lake* knows is read the same way. The subjects that exist
and each table's current snapshot come from the catalog on every
statement, so a landing is visible the moment it commits.

## One plan

A statement plans once, against schemas resolved before planning
begins: every door it names — a grounding, a scenario, a subject
column — is fetched and built into a plan in the async pre-pass, so
planning itself is synchronous with nothing left to fetch.

## Streaming

Reads stream end to end, one batch in memory at a time. The Arrow
door never caps — the client drains the stream, and hanging up
cancels the work upstream. The MCP door's row cap is a limit on the
read's plan: what the reader won't see, the engine is never asked
for.

## Invalidation and disclosure

Definition changes are coarse, explicit, and refused rather than
migrated; dependencies are declared, never sniffed; data staleness is
marked at read. Machinery never suppresses judgment: `stale` is served
and marked, `contested` is withheld with its band and score,
`unassessed` is a visible row.
