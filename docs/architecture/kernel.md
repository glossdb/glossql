# The business information kernel

glossdb is a kernel. A small core is hard-wired: what it takes to land
a correct cube for a dataset and to keep the record about it. Around
the core sits a fixed set of questions the core asks of the outside.
Each question is an extension point. Something outside answers it. The
core owns the question, the shape of the answer, and who reads the
answer. The outside party owns the answer and nothing else.

Two kinds of party answer. A **driver** turns what is not rows into
rows. A **service** turns rows into rows. Both attach on the server.
The agent at the door learns nothing new when a point is filled: it
sees a declaration in the kit, an arm in the next read, and a page.

## The core

- **The language and the record.** The parser over its corpus
  (`crates/parser`) and the glossary store (`crates/glossary`):
  declarations, glosses, measurements, supersession as a read
  ([store](store.md)).
- **The engine and the data plane.** DataFusion, the catalog as rows
  of the record's own database in the DuckLake shapes, parquet under
  the warehouse, pins and versions ([substrate](substrate.md),
  [storage](storage.md)).
- **Sources and recipes.** File and relational sources, probe, import,
  keyed merge (`crates/import`).
- **The cube.** One head per metric landed as a table, every grain a
  plan over it, the three verbs, the judged axes, and the four reads
  `metric_series()`, `metric_axes()`, `fact_values()` and
  `metric_sources()` ([reads](../reference/reads.md)).
- **Checks and rulings.** Witnesses, detectors, `ATTEST()`, the
  question round, the ruling at the app door.
- **The doors and the docket.** The two agent doors, the Arrow door,
  the app door with the built-in docket and the glossed apps
  ([doors](../reference/doors.md)).
- **The next read.** Seven goals, one hand-written arm per step
  (`crates/session/reads/next.sql`), and the two lines on every result.
- **The kits.** Below.

The core is whole without any service. It answers from the customer's
data alone.

## The kits

The shipped system is three files of declarations, declared into
every workspace at every boot (`crates/serverd/src/bootstrap.rs`):

- **The KPI kit** (`crates/scripts/functions/kpi_kit.glossql`): the
  vocabulary. What a column means, its role, how a measure behaves,
  its unit, which columns slice, what a table is, the dataset
  registries, the source conventions, the `cube` settings.
- **The measurement library** (`bootstrap.glossql` and the fourteen
  bodies beside it): the profilers, the detectors of relationships,
  hierarchies and derivations, the coherence check, the bands walk and
  its detector.
- **The witness plane** (`witnesses.glossql`): who may speak on each
  aspect, and which detector adjudicates.

The kits are core content, not an extension. The cube admits a time
axis by `temporal_profile` and a dimension by `dimension_relevance`.
The library conditions on the kit's `role`. The next read and the
docket name the kit's aspects and functions. There is one pack, and it
is this. A workspace's own vocabulary stays the agent's work: metrics,
validations, scenarios, declared at the door.

## Drivers

A driver turns something that is not rows into rows. The rows are the
interface: a recipe lands them, typed by the recipe, versioned on
every import ([SPEC.md §3](../../SPEC.md)).

Today:

- **Files.** Parquet, CSV and JSON under a source's location, read by
  the engine's own readers, in process.
- **Relational databases.** The recipe runs at the source over ADBC.
  The driver is a shared library the source's `driver` setting names,
  installed by the operator (`crates/import/src/adbc.rs`).

Planned:

- **Workbooks**, read in process: a sheet as a table, and the
  workbook's definitions as rows, one per formula, defined name,
  macro module or query.
- **Documents**, read by a processor service: a PDF, a Word file or a
  slide deck as passages, one row each, keyed by document and
  reference.

A new kind of source is a new `type` in SPEC §3. The corpus comes
first: one real workbook and one real handbook, transcribed, the forms
reviewed. A document lands by a recipe and updates by `IMPORT`. A
gloss that learned from it cites its rows by key.

## Services

A service turns rows into rows. The pattern of a point:

1. **Declared in the kit.** An aspect holds the shape of the answer. A
   function or a door asks the question.
2. **Filled by a service the deployment names.** The core sends rows.
   The service answers rows.
3. **Refused by name when none is named.** The read says what to set.
   Start-up logs it. Nothing stands in for a missing service.

This is how the bands work today. `metric_bands` and `band_breach`
are declarations in the kit. Behind `metric_band_walk` the runtime
calls the model service, which the code names the kernel service
(`crates/scripts/src/remote.rs`), reached at `GLOSSQL_TABICL_URL`. It
refuses by name when none is set (`crates/session/src/session.rs`,
`no_model`). `misfit.<frame>()` and `whatif.<scenario>()` take the
same road.

Two rules hold for every service:

- **A service never writes the record.** It returns rows. The core
  lands them as a measurement, like any function's result.
- **Semantics stay in the core.** The verb, the grain, the axes and
  the pin are decided here. The service decides nothing about what the
  numbers mean.

Today the runtime has one typed method per read: the walk's points,
the what-if's grid, the misfit's scores. Numbers travel as JSON lists,
column names and types lost. One URL serves the three reads. The
walk's rows are built in Rust and sent as training rows.

Planned: one general call in two shapes.

- A **scalar service** answers row by row: a text in, a vector out. It
  rides the engine's async scalar function seam, so a recipe or a
  query names it like any function.
- A **table service** answers a table with a table: a series in, bands
  or a forecast out; a frame in, a score per row out. It stays a
  compute door, as the bands are today.

Rows travel as Arrow IPC, column names and types with them. The
deployment names a service per point. Every answer carries the
service's name and version, and the measurement records it. The core
sends the series or the frame as it stands. The service builds its
own rows from it. A service wraps a standard model behind a standard
interface, so a deployment may run another service in its place.

## The door

Any agent that speaks MCP drives the kernel through one tool, the
skills and the pages ([doors](../reference/doors.md)). Nothing
extends here. A filled point reaches the agent as a declaration it
reads in the record, an arm in the next read, and a page the door
serves.

## Planned, in order

**Phase 1, in.** Workbooks. Documents. Search: `embed(text)` as a
scalar service called from a recipe, vectors stored at unit length, a
query ordered by `array_distance` over a full scan. Search ranks rows
for a reader. A similarity is never a join key, an identity or what
closes a question. A ranking service is a candidate point; its
experiment runs in the evaluation rig first.

**Phase 1, out.** The typed methods become the general call. The row
design and the replay grid leave the engine; the service builds its
rows. The what-if of today moves to the service side as a project of
its own. The model service takes its own name: it is the forecast
services, not the kernel.

**After phase 1.** The forecast as a declared analysis: an aspect, a
door, a backtest measurement, its arms in the next read. The vector
index in the parquet footer, when the full scan is too slow. The
cube: every served column, the whole history, and the two
measurements order the columns and warn instead of deciding. Whether
the cube's limits fit the forecast's use cases is analysed once phase
1 is done.

Nothing in phase 1 needs a second pack, a change to how the next read
is built, or the cube change. An item that needs one of them is not
phase 1 work.
