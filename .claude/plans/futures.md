# The futures plan

The service integration first, then the futures, then the cube's
widening. The shared frame, the terms, the gates and the decisions
are in `kernel.md`. This plan touches the session, scripts and
serverd crates, the kit, the next read and the docket.

## The order

1. The general service call lands and `/bands` leaves the client; the
   walk runs on `/forecast` with origins (#103). The what-if door's
   grid, support worlds and band read go; the plan rewrite stays
   (#104). These two are one step: everything else in this plan rides
   the general call.
2. The rename (#102): the service's name, the dev rig, the sibling
   notes. glossthesi's `main` is at 0.2.0, tagged `v0.2.0`.
3. The futures, in the order of the table below. The forecast first,
   because the world row serves it and the span rule reads its record.
4. The cube's widening, its own decision once the world is in.

## The table

| Element | Difficulty | What it touches | When | Issue |
|---|---|---|---|---|
| One general service call; `/bands` leaves the client; the walk on `/forecast` with origins | Easy, mostly removal | scripts, the runtime trait, the band walk, misfit | First | #103 |
| The what-if door: the grid, the support worlds and the band read go; the plan rewrite stays as the propagation's seed | Easy | one module and its tests; SPEC §9's note, fixture 19 | First | #104 |
| The rename: the service's name in ten files, the dev rig, the sibling notes | Easy | docs, skills, crates, .claude, dev; glossval's working notes | Next | #102 |
| The forecast as a measurement at an origin, its read, the PIT at read | Medium | the library, a point on the general call, the kit | Phase 2, first | |
| A world on the cube reads: plan and scenario as heads, the forecast from the measurement, variance as a join | Medium, core | cube, the two cube reads, the docket | Phase 2 | |
| Levers and the scenario in the kit | Easy | kpi_kit, the scenario's reader | Phase 2 | |
| The scenario read in two passes: replay through the groundings, the estimate from the service | Medium | session, scripts, a point on the general call | Phase 2 | |
| The plan grounding's form | Language | SPEC §5.2 or §9; one corpus fixture, a real budget beside its actuals | Phase 2, the lead's | |
| The planning goal in the next read | Easy, additive | next.sql, window.rs, the skills, the docket | Phase 2, last | |
| Cube widening: every column, the whole history, measurements warn instead of gate | Medium, core | cube, docket, issue #41 | After the world; its own decision | |

## What stands

**The futures today.**

- One world. `metric_series()` serves `metric`, `dimension`,
  `member`, `period`, `value`, `num`, `den`, `behavior` and no world
  (`docs/reference/reads.md`). The cube's head is keyed by dataset,
  metric, the data legs and a digest that folds the grounding and the
  frame as planned (`crates/session/src/cube.rs`, module header;
  `CubeKey`, 540). A head over another frame is another digest
  already; the cube itself is a query result under the cache, never
  recorded.
- The walk. `band_points()` (`crates/session/src/reads.rs:523`), one
  step ahead, `pit` per point. `metric_bands` is declared in
  `crates/scripts/functions/bootstrap.glossql:141`, its function at
  196, `band_breach` at 199, the witness `bands_w` in
  `witnesses.glossql:18`. The bands goal's arms in
  `crates/session/reads/next.sql` read `band_points()` and `owed`.
- The wire's worlds. A row's `status` is `actual`, `plan`, `forecast`
  or `scenario` (`../glossthesi/docs/contract.md`). `/forecast` takes
  covariates known ahead at any horizon, `origins` for a backtest,
  `totals`, `grain` and `width`. A member with two rows of one period
  is refused, so a plan and a scenario go as two requests or as two
  members. `/whatif` returns per column a sign, the model's rows, the
  history's estimate, `basis` and the held-back error.
- The plan rewrite. Every scan of an overridden table gains a
  projection over the overridden column from the scenario's start
  month, and the groundings replay over it
  (`crates/session/src/whatif.rs`, module header; `whatif_batch`, 63;
  `build_plan`, 553). The `replay` column is the arithmetic half. It
  takes one factor from a start month, not a value per period.
- Levers. None is declared. The `role` enum is key, measure,
  dimension, timestamp, attribute
  (`crates/scripts/functions/kpi_kit.glossql:37`). What a lever is,
  and the four assumptions under which its effect reads, is on the
  what-if page (`../glossthesi/docs/whatif.md`).
- The plan. None. The standard grounding schema is `sql` or
  `stopped`, with `behavior` and `axes`
  (`crates/glossary/src/schemas.rs:10`). The supersession key is
  subject, aspect and actor kind (`grammar.ebnf`, `gloss`), so a
  second QUERY gloss on the same aspect supersedes the actual.
- Propagation. Rides the groundings, not the `formulas` registry: a
  metric over another metric nests as `read.<metric>()` behind a
  cycle guard (SPEC.md §9, PoC notes). The registry
  (`kpi_kit.glossql:98`) is for drills, and its operands are not
  checked (`docs/reference/kpi-kit.md`).
- The span evidence. The forecast's 80% span holds 0.78 to 0.80 one
  period ahead and 0.71 a year ahead on held-out monthly data
  (`../glossthesi/docs/forecast.md`, GK-28, GK-42). The what-if's size
  was right in one region: a set lever that explains a fifth or more
  of the column, pinned by history to within about a third, standing
  apart from the other inputs; 98% of 597 instances
  (`../glossthesi/docs/whatif.md`). The under-reaction finding is
  TabICL's forecaster; the forecast route's response to a covariate
  change is not measured.
- Counterfactual truth. glosstest ships levers as generator
  parameters: a same-seed pair is an exact counterfactual, recorded
  in `intervention.yaml` with `counterfactual_corpus_id`. Rate levers
  on price, frequency and collection lag; a mix lever; scopes by
  segment, region, customer, product group, product; interaction
  pairs (`../glosstest/docs/operating-model.md`, S5).

## Issues, drafted

In the repo's form: what it buys, what stands, done-when. Facts only.
When one is opened, its number goes into the table and the draft
leaves this file.

### The forecast as a measurement

- **What it buys.** Every applicable metric carries the model's
  numbers for the periods ahead, from the current origin, with
  spans, and the record keeps them. When the actuals land, the PIT
  per point is read, so the dataset learns its own coverage by
  horizon. The walk is the same call with origins.
- **What stands.** `metric_bands` and `band_points()` (above) are the
  one-step instance. `/forecast` takes `horizon`, `levels`, `totals`,
  `grain`, `width` and `origins`. The cube's resolution and rung per
  metric are on the fact row (`metric_axes()`). The contract's walk
  sends `metric_series()` at month grain.
- **Done when.** A library function lands per metric at an origin:
  horizon from the cube's rung, fixed levels, the totals of the
  quarter and the year, the grain from the metric's resolution. A
  read serves the points, and the PIT is a join to the actual cells
  at read, never written. The walk is this function with origins and
  `metric_bands` reads from it. The coverage by horizon per dataset is
  a read. The session suite holds it on the fixtures.

### A world on the cube reads

- **What it buys.** The same metric, member and period in four
  worlds. Variance against plan is a join. A scenario is a world the
  comparison reads. The docket shows actual, plan and forecast on one
  chart.
- **What stands.** The cube's head key already distinguishes a frame
  by digest. `metric_series()` has no world column. The forecast is
  a measurement, not a query result, and is served from the record.
- **Done when.** `metric_series()` and `metric_axes()` carry `world`.
  The actual world is today's rows unchanged. The plan world is a head
  over the plan grounding. A scenario world is a head over the
  rewritten frame. The forecast world reads the forecast measurement
  at the newest origin. The docket's metric page shows the worlds it
  has. The session suite holds a metric in all four.

### Levers and the scenario in the kit

- **What it buys.** The engine knows which columns are moves. A
  scenario is declared as values per period on named levers, and the
  kit refuses a lever no one declared.
- **What stands.** `role` (`kpi_kit.glossql:37`) has no lever value.
  Fixture 19's scenario carries overrides as a factor from a start
  month. The what-if page names the lever's assumptions.
- **Done when.** `role` admits `lever`. A `scenario` FACT aspect
  carries, per lever, values per period, or a factor per period. A
  gloss on it validates against the declared levers. SPEC.md §9's
  note and fixture 19 say the new form, as the project lead rules.

### The scenario read in two passes

- **What it buys.** Which way the watched metrics move under a
  scenario, with a span where one is honest and a direction where it
  is not, every cell saying which half produced it.
- **What stands.** The plan rewrite (above). `/forecast` with the
  lever as a covariate known ahead; `/whatif` with the plan and
  scenario rows. The what-if's "no declared path" refusal.
- **Done when.** Pass one: where a declared lever reaches a metric
  through its groundings, the engine replays with the scenario's
  values and marks the cell `replayed`. Pass two: where no path
  reaches it, the service estimates and the cell is `estimated`, with
  its span and the coverage the dataset measured at that horizon, or
  a direction and the service's `basis`. A span past the gate is not
  served and the row says why. The read serves the scenario world.
  The session and scripts suites hold it on the fixtures, with the
  service stubbed.

### The planning goal

- **What it buys.** The next read tells an agent where the dataset
  stands toward a plan: the plan landed, the forecast at the current
  origin, a variance beyond the band, a scenario declared, its levers
  checked against history, the comparison read, the decision as a
  ruling.
- **What stands.** Seven goals, each a chain of arms in
  `crates/session/reads/next.sql`; `window.rs` carries the line;
  the skills name the seven.
- **Done when.** An eighth goal's arms serve from the reads above.
  The door's line and the skills name it. The eval's planning
  scenario runs through it. The suite holds every statement the arms
  hand out.

## For the other repos

- **glossthesi**: `main` at 0.2.0, tagged `v0.2.0`. No work before
  phase 2. The forecast route takes the lever as a
  covariate, and the what-if route takes the plan and scenario rows.
  Two futures of one member in one request is a wire convenience for
  later, if the engine wants one call per comparison. The what-if's
  estimator may move to the forecast route if the experiment below
  says so.
- **glosstheval**: two experiments before the span rule is decided,
  on its evaluation page under Open. The size of a covariate's effect
  on `/forecast`, by the what-if's own procedure. The coverage by
  horizon on the held-out sets, the table the span rule reads.
- **glossval**: the futures lane, `notes/futures-lane.md` there. The
  counterfactual truth from the generator's lever pairs, the forecast
  scored end to end through an import, the plan world, and a planning
  task for the agent lane.
- **glosstest**: nothing. The levers are shipped.
