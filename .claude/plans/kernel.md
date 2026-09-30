# The kernel plan

The evaluation behind `docs/architecture/kernel.md`. The docs page
states what the system is and what is planned. This file holds what
the page cannot: how hard each element is, what it touches, why it
sits where it sits, and what is not done and why not now.

Read it when picking the next piece of kernel work. It links its
issues and it shrinks: a row that becomes an issue links the issue;
when the issue closes the row leaves, and the docs page says what the
system now is. No standing markers. The code and the issues override
this file wherever they differ.

## The analysis

The project lead's position, in short.

- Kernel means the business information kernel. glossdb is it.
- glosskernels is misnamed. It is the forecast services and takes
  that name, in its own repo.
- Define the core first, including the kits. Then evolve that idea.
  That is the meta layer, and the docs page is its statement.
- The pattern of a point: declared in the kit, filled by a service the
  deployment names, refused by name when none is. The bands are the
  instance to generalize.
- Extension happens on the server, over services and drivers. Never
  on the client side.
- The order: in first, the experiments included. Out once the forecast
  services refactoring, already running, is finished. Then the three
  after items: forecast, vector index, cube. The cube's current limits
  may limit some forecasting use cases; analyse that after in and out.

## The tests behind the columns

- **Easy**: the element attaches at a seam that exists. A source kind
  in the import crate, a method on the runtime, a scalar function, a
  compute door.
- **Hard**: the core changes shape first. Bootstrap takes more than
  one pack; the read, page and docket lists open; the next read takes
  arms from outside; somebody other than the core declares a shape.
- **The phase 1 gate**: nothing in phase 1 needs a second pack, a
  change to how the next read is built, or the cube change. An item
  that needs one of them is not phase 1 work.
- **The kits are core content.** The cube admits a time axis by
  `temporal_profile` and a dimension by `dimension_relevance`; the
  library conditions on the kit's `role`; the next read and the docket
  name the kit's aspects and functions. There is one pack and it is
  this. Opening that is the hard side of the table.

## The table

| Element | Difficulty | What it touches | When | Issue |
|---|---|---|---|---|
| One general service call; the service builds its own rows | Easy, mostly removal | scripts, the runtime trait, the band walk, what-if, misfit | Phase 1, out | |
| The what-if door retires | Easy | one module and its tests; SPEC §9's note, fixture 19 | Phase 1, out | |
| Spreadsheets as a source, in process | Easy | import crate, SPEC §3 | Phase 1, in | |
| Documents as a source, a processor service | Easy | import crate, SPEC §3, one service | Phase 1, in | |
| Search: embed as a remote scalar function, distance in plain SQL | Easy | scripts, one service; the async scalar seam exists at our pin | Phase 1, in | |
| Ranking and CLM experiments | Easy, outside the tree | nothing here; glossval | Phase 1, beside it | |
| Forecast as a declared analysis in glossql | Medium | a point on the general call, the kit, next arms | After phase 1 | |
| Vector index in parquet | Medium | catalog, an optimizer rule | When the full scan is too slow | |
| Cube: every column, whole history, measurements warn instead of gate | Medium, core | cube, docket, issue #41 | Its own decision, after in and out | |
| Reports at a data version, forms as a second write | Medium | catalog retention, app door | Not yet | |
| Packs with functions, more than one pack | Hard, core | bootstrap, the read, page and docket lists, next arms | Not yet | |
| Plugins, plugins in the next graph | Hard, core | everything above, plus who declares shapes | Not yet | |
| Agent on own hardware | Not glossql work | cockpit, platform | Not yet | |
| Iceberg as a read-only source, calibration for customers | Parked | | Not yet | #101 |

## What stands, per phase 1 row

The anchors an issue's "what stands" cites.

**The service call today.**

- The runtime trait: `crates/session/src/session.rs:263`
  (`FunctionRuntime`). One typed method per read: `band_grid` (287),
  `misfit_scores` (303), `band_points` (340). `reconcile` (319) is
  native and stays. `no_model` (254) is the refusal by name.
- The client: `crates/scripts/src/remote.rs`. Two routes, `/bands`
  and `/misfit`, JSON bodies, matrices as nested lists (`rows`, 420).
  Column names and types do not cross.
- The runtime: `crates/scripts/src/lib.rs` (`KernelRuntime`). One URL
  for every read: `crates/serverd/src/config.rs:123` (`Kernel`,
  `GLOSSQL_TABICL_URL`). Start-up logs the absence
  (`crates/serverd/src/main.rs`, "no kernel service").
- The walk's row design in Rust: `crates/session/src/search/bands.rs`
  (`metric_band_walk`, 241). The same design exists in Python in the
  forecast services repo, and a test there holds the two equal.
- The what-if door: `crates/session/src/whatif.rs`, 743 lines, the
  replay grid and the plan rewrite; registered in
  `crates/session/src/reads.rs:1006`. Its language surface: SPEC.md
  §9's note (line 627) and fixture 19
  (`crates/parser/tests/corpus/19-whatif-scenario.md`);
  `docs/methods/whatif.md`.
- The misfit door: `crates/session/src/misfit.rs` (`ROW_CAP` 2000,
  `COL_CAP` 16); registered in `reads.rs:1019`.
- The words "kernel service" and `GLOSSQL_TABICL` appear in 15 files
  across `docs/`, `skills/`, `crates/` and `.claude/`. The rename
  touches every one.
- The async scalar function seam:
  `datafusion-expr-55.1.0/src/async_udf.rs` (`AsyncScalarUDFImpl`).
  Arrow IPC is in the build (`arrow-ipc` 59.2, the engine's arrow).

**Sources today.**

- The kinds: `crates/import/src/lib.rs:93` (`SourceKind`: parquet,
  csv, json, relational). The spec: SPEC.md §3, `type: relational_db
  | parquet | csv | json`; grammar.ebnf line 67 lists the same keys
  in a comment, so a new kind is a spec change, not a production
  change.
- A recipe opens as a stream: `open_recipe` (292). The reader
  functions a recipe may name: `reader_functions` (762), registered
  by `registry` (736). A source's files: `list_source` (1027).
- Relational: `crates/import/src/adbc.rs`, the driver a shared library
  the source's `driver` setting names.

**Search today.**

- `array_distance` is registered at our pin
  (`datafusion-functions-nested-55.1.0/src/lib.rs:195`; the
  `nested_expressions` feature is on in the workspace `Cargo.toml`).
- No remote scalar function exists. No vector column has been landed.
- The quality rule that bounds search: identity is an explicit key,
  never text matching (`docs/quality/README.md`). Search ranks rows
  for a reader; a similarity is never a join key, an identity or what
  closes a question.

## Phase 1 issues, drafted

In the repo's form: what it buys, what stands, done-when. Facts only.
When one is opened, its number goes into the table and the draft
leaves this file.

### Workbooks as a source

- **What it buys.** A company's workbook lands as tables, and its
  formulas, defined names, macro modules and queries land as rows.
  An agent reads a formula and writes the metric's grounding; the
  basis names workbook, sheet and cell. A migration is checked
  against the workbook's own stored values. No calculation engine.
- **What stands.** `SourceKind` (`crates/import/src/lib.rs:93`) and
  SPEC.md §3 name four kinds. A recipe lands rows typed by the
  recipe; the rows are the interface. A reader crate that reads
  values, formulas, macro source, defined names and tables from
  xlsx, xlsm, xlsb, xls and ods exists (calamine). Pivot tables and
  Power Query are read by none of the candidates.
- **Done when.** The corpus holds one real workbook, and SPEC.md §3
  names the kind. A fixture workbook lands as tables and as
  definition rows keyed by path, sheet, and cell or name. A
  grounding's basis names a cell, and the grounding's result equals
  the workbook's stored value on the fixture. The suite holds it.

### Documents as a source, through a processor service

- **What it buys.** A handbook, a standard or a schema description
  lands as passages. An agent finds the passage it needs, writes a
  convention or a definition, and cites the passage by key. The
  critical data stays in the systems of record; documents give
  context.
- **What stands.** Same seams as the workbook: `SourceKind`, SPEC.md
  §3, the recipe as the landing. A processor that reads PDF, Word
  and slides into structure, tables and reading order exists as an
  open server (docling) and runs per deployment, since text is
  customer content. Nothing in the tree calls a service from a
  recipe.
- **Done when.** The corpus holds one real handbook, and SPEC.md §3
  names the kind. A fixture document lands as passage rows: document,
  reference, ordinal, headings, kind, text, page; the key is document
  and reference. The same document processed twice, the second time
  with one paragraph added, keeps the untouched passages' references,
  or the issue records that a key of our own is needed. A gloss cites
  a passage by key and the suite holds it.

### Search: embed as a remote scalar function

- **What it buys.** An agent finds the passage it needs by meaning,
  not by string, over the passages a document landed.
- **What stands.** `array_distance` is registered
  (`datafusion-functions-nested-55.1.0/src/lib.rs:195`). The async
  scalar function seam exists (`datafusion-expr-55.1.0/src/async_udf.rs`).
  No remote scalar function exists in `crates/scripts`. The quality
  rule: identity never rides similarity (`docs/quality/README.md`).
- **Done when.** `embed(text)` is a scalar function the recipe names,
  answered by an embedder service the deployment names, refused by
  name when none is. The recipe lands the vector at unit length and
  the column records the model's name and revision. A query ordered
  by `array_distance` over a full scan returns the fixture passage in
  the top rows. 10,000 passages land with the embedder stopped
  half-way, and no half table results. The time per passage is
  recorded on the issue.

### One general service call

- **What it buys.** A point is filled by a service without Rust of
  its own in the engine. glossdb sends a series or a frame as it
  stands, with column names and types; the service builds its own
  rows and answers rows. Every answer names the service and its
  version, and the measurement records it. A deployment names a
  service per point.
- **What stands.** `FunctionRuntime`
  (`crates/session/src/session.rs:263`) has one typed method per
  read. `crates/scripts/src/remote.rs` sends JSON nested lists (420)
  to one URL (`crates/serverd/src/config.rs:123`). The walk's row
  design is built in `crates/session/src/search/bands.rs` and again
  in the forecast services' Python. The forecast services own row
  design, calibration and the model, and serve `/forecast`, `/bands`
  and `/misfit`.
- **Done when.** The trait carries one call in two shapes: a scalar
  service and a table service. Rows travel as Arrow IPC. The band
  walk sends the monthly series and the PIT history and receives the
  bands; `bands.rs` holds no feature recipe. The misfit door sends
  the frame and receives the scores. The runtime's typed methods are
  gone. The same band points as today on the fixtures, and the
  scripts and session suites hold it. The deployment names a service
  per point; `GLOSSQL_TABICL_URL` is gone.

### The what-if door retires

- **What it buys.** 743 lines of replay and plan rewrite leave the
  engine. The what-if becomes a project of the forecast services,
  driven by its evaluation there.
- **What stands.** `crates/session/src/whatif.rs`; the registration in
  `crates/session/src/reads.rs:1006`; `docs/methods/whatif.md`; the
  door's skill references; SPEC.md §9's note (line 627) and fixture
  19, which are the project lead's.
- **Done when.** The module, its door and its docs page are gone; the
  three functions other modules use from it have a home; the SPEC.md
  note and the fixture are changed by the project lead or the fixture
  is retagged; the suite holds.

### For the other repos

- **Forecast services**: the rename; the row design as the one copy;
  the what-if as a project; the two wire shapes and Arrow IPC on the
  service side; the version on every answer is there already.
- **glossval**: the ranking experiment. Question 1, steps: where the
  door offered several, does a picker rank first the one the
  high-scoring sessions took, against today's order. Question 2,
  candidates: on the relationship truth, does it rank the true edges
  above the coincidences, against today's ranking. Untuned first. If
  it does not clearly beat the baselines, it gets no place.

## Not now, and why

- **Packs with functions, more than one pack.** Bootstrap declares
  one sequence (`crates/serverd/src/bootstrap.rs`); the reads
  (`crates/session/src/library.rs`), the pages
  (`crates/serverd/src/skills.rs`) and the docket
  (`crates/apps/src/builtin.rs`) are fixed lists; the next read is
  one file. Opening them is the core changing shape. No customer has
  asked to change the shipped system.
- **Plugins, and plugins in the next graph.** Everything above, plus
  somebody other than the core declaring shapes, plus tools and goals
  matched by needs and gives in place of the hand-written arms. The
  arms were tuned over eval rounds and work. Not planned.
- **A second build, a closed side.** Services are replaceable behind a
  standard interface. Not planned.
- **Reports at a data version, forms.** The catalog deletes replaced
  files after a grace of one hour (`crates/catalog/src/lib.rs:186`),
  so a release cannot read an old version. Forms are a second kind of
  write at the app door. Both wait for a need.
- **Agent on own hardware.** The server is the same in every stage.
  The work is the cockpit's and the platform's.
- **Iceberg read-only, calibration for customers.** Parked (#101; the
  services' own docs).

## Decisions that are the project lead's

- **Where a point is declared.** Recommended: the kit and the
  deployment's configuration, no grammar. SPEC.md §9 already rules
  analyses as operation-named doors whose machinery is never syntax.
- **New source types.** Each is a SPEC.md §3 change. Corpus first: one
  real workbook and one real handbook, transcribed, the forms
  presented.
- **The what-if's language surface.** SPEC.md §9's note and fixture
  19 change or the fixture is retagged.
- **The wire.** Recommended: Arrow IPC, column names and types with
  the rows.
