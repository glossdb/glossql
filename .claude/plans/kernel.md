# The kernel plan

The evaluation behind `docs/architecture/kernel.md`: the docs page
states what the system is and what is planned; the plans hold how
hard each element is, what it touches, why it sits where it sits,
and what is not done and why not now. This file is the shared frame;
the work is in five plans beside it:

- `sources.md` — what comes in: workbooks, documents, search, and
  the ranking experiments. It touches the import crate and one scalar
  function on the general service call's seam, which stands.
- `kits.md` — kits as build-time overlays, a harmonisation kit over
  the KPI kit, opening the hard side of the table at build time only;
  the demo that drove them lives in the demo repo beside this one.
- `clients.md` — the trial's road: a workspace reachable at its own
  name from Claude and ChatGPT, the registration, the file in. Build
  order is risk order. It touches serverd and the apps crate and
  names what glossdeploy builds.
- `chat.md` — how a result reaches a person inside the chat and how
  a ruling gets back: two roads, one measure. It touches the door's
  result and the docket.
- `grounding.md` — one SPEC.md §5.2 change: the grounding contract
  the engine reads and the grounding schema a kit declares. It touches
  the glossary crate and the KPI kit's script; the corpus decides first.

Read a plan when picking the next piece of kernel work. A plan links
its issues and shrinks: a row that becomes an issue links the issue;
when the issue closes the row leaves, and the docs page says what the
system now is. No standing markers. The code and the issues override
these files wherever they differ.

## Terms

- **World.** Which set of numbers a cell belongs to: `actual`, what
  landed; `plan`, what the business set ahead; `forecast`, what the
  model says for the same periods, from an origin; `scenario`, a plan
  with a lever moved. The wire carries it as a row's `status`.
- **Origin.** The last period a forecast was made from.
- **Lever.** A column someone sets: a price, a headcount, a spend, a
  promotion flag. The what-if page calls it a well-defined lever and
  names the assumptions under which its effect can be read.
- **Span.** An interval between two quantiles, never a point. A
  forecast's 80% span is the values between its 0.1 and 0.9 quantiles.
- **Coverage.** How often the actual fell inside a span of a stated
  level, on past forecasts. A span that states 80% and holds 0.78 is
  calibrated; one that holds 0.71 is too narrow.
- **PIT.** The quantile at which the actual landed in the forecast's
  distribution, 0 to 1. Uniform PITs mean the spans are right.
- **Propagation.** Carrying a lever's effect through the definitions:
  revenue moves because price moved and revenue is price times units.
- **Ruling.** A human's answer to a question the agent raised as a
  keyed assumption. Stored by key in the human's slot, it outranks
  the agent's at every read. Picking a scenario is a ruling.

## The analysis

The project lead's position, in short.

- Kernel means the business information kernel. glossdb is it.
- The model service is glossthesi, in its own repo. glossql names it
  per point: `GLOSSQL_FORECAST_URL`, `GLOSSQL_MISFIT_URL`.
- Define the core first, including the kits. Then evolve that idea.
  That is the meta layer, and the docs page is its statement.
- The pattern of a point: declared in the kit, filled by a service the
  deployment names, refused by name when none is. The bands are the
  instance to generalize.
- Extension happens on the server, over services and drivers. Never
  on the client side.
- **The plans here steer every repo.** Work the kernel needs in
  glossthesi, glosstheval, glossval or glosstest is a row here under
  "For the other repos", an issue here when it is a unit of work;
  those repos carry notes, experiments and records, never a kernel plan.
- **The futures are core.** The kernel holds four worlds: the
  actuals, the plan, the forecast and the scenario
  (`docs/methods/forecast.md`, `scenario.md`, `plan.md`). The
  decision tree is the comparison of scenarios by their forecasts.
  The search is the engine's own arithmetic through the groundings;
  the estimate of what a lever does where no definition reaches is
  the service's. Both halves stay visible on every cell.
- **A number is served with its measured coverage beside it.** A
  span is served at every step with how often a span of its level
  held at that grain and step: this dataset's own record of forecasts
  and actuals where it has one, the service's held-out table
  otherwise. Where the service gives no span, or a lever has no
  history of being moved, the read gives a direction and says why.
  The number is refused by name, as a missing service is.
- **One pack grows.** Planning over sales, hiring, stock and
  logistics is the KPI kit with futures, not a second pack. The next
  read gains a goal; how it is built does not change.
- **No abstraction before it works.** Routes as data and plugins wait
  until the planning goal runs on the eval.
- The order is ad hoc within the phase: the lead picks the next row
  when an issue closes. The recommendations read waits on glossval's
  planning task (#174).

## The tests behind the columns

- **Easy**: the element attaches at a seam that exists. A source kind
  in the import crate, a method on the runtime, a scalar function, a
  compute door, an arm in the next read.
- **Hard**: the core changes shape first. Bootstrap takes more than
  one pack; the read, page and docket lists open; the next read takes
  arms from outside; somebody other than the core declares a shape.
- **The phase 1 gate**, for the sources plan: nothing needs a second
  pack, a change to how the next read is built, or the cube change.
  An item that needs one of them is not phase 1 work.
- **The phase 2 gate**, for work over the worlds (#174): nothing
  needs a second pack, routes as data, or a change to how the next
  read is built. An item that needs more is not phase 2 work.
- **The kits are core content.** The cube admits a time axis by
  `temporal_profile` and a dimension by `dimension_relevance`; the
  library conditions on the kit's `role`; the next read and the docket
  name the kit's aspects and functions. There is one pack and it is
  this. Opening that is the hard side of the table.

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
  arms were tuned over eval rounds and work. Not before the planning
  goal runs on the eval.
- **Reports at a data version, forms.** The catalog deletes replaced
  files after a grace of one hour (`crates/catalog/src/lib.rs:186`),
  so a release cannot read an old version. Forms are a second kind of
  write at the app door. Both wait for a need.
- **A second build with a closed side, the agent on own hardware,**
  **Iceberg read-only, calibration for customers.** Services are
  replaceable behind a standard interface; the server is the same in
  every stage, so that work is the cockpit's and the platform's;
  Iceberg and calibration are parked (#101; the services' own docs).

## Decisions that are the project lead's

- **Where a point is declared.** Recommended: the kit and the
  deployment's configuration, no grammar. SPEC.md §9 already rules
  analyses as operation-named doors whose machinery is never syntax.
- **New source types.** Each is a SPEC.md §3 change. Corpus first: a
  real workbook and a real handbook, transcribed, the forms presented.
- **The wire.** Recommended: Arrow IPC, column names and types with
  the rows.
