# The futures plan

The service integration first, then the futures, then the cube's
widening. The shared frame, the terms, the gates and the decisions
are in `kernel.md`. This plan touches the session, scripts and
serverd crates, the kit, the next read and the docket.

## The order

The general service call stands: one table in, one table out per
point; the walk runs on `/forecast` with origins; the what-if door is
gone and the plan rewrite stays. The forecast measurement stands on
it: `metric_forecast` lands the model's quantiles per step and span
at the origin, and two reads serve them. The world stands on the
measurement: `metric_series()` carries `world`, and the forecast
world is served from the newest run. Everything below rides the
three.

1. The futures, in the order of the table below. The plan and the
   scenario worlds come with their forms.
2. The cube's widening, its own decision.

## The table

| Element | Difficulty | What it touches | When | Issue |
|---|---|---|---|---|
| The range with how often it held: coverage per grain and step beside the quantiles, the service's held-out coverage as the default | Easy | the forecast reads, the docket, the wire | Phase 2, before the scenario read | |
| Levers and the scenario in the kit | Easy | kpi_kit, the scenario's reader | Phase 2 | |
| The scenario read in two passes: replay through the groundings, the estimate from the what-if call | Medium | session, scripts, the what-if point on the general call | Phase 2 | |
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
- The walk. `metric_band_walk` (`crates/session/src/search/bands.rs:238`)
  reads each grounding at month grain by its verb and sends one table
  to `/forecast` with `origins: 6`, `horizon: 1` and five levels (242);
  it records bands, actual and PIT per walked month and `served_by`.
  `band_points()` (680) flattens the latest measurement. `metric_bands`
  is declared in `crates/scripts/functions/bootstrap.glossql:141`, its
  function at 196, `band_breach` at 199, the witness `bands_w` in
  `witnesses.glossql:18`. The bands goal's arms in
  `crates/session/reads/next.sql:431` read `band_points()` and `owed`.
  The walk reads the groundings, not the cube.
- The forecast measurement (#105). `metric_forecast` lands, per
  metric as a whole at its own grain, the model's quantiles per step
  and span from the origin (`crates/session/src/search/forecast.rs`:
  `metric_forecast_ahead`); `forecast_points()` joins the actual at
  read and says where it landed, `forecast_coverage()` counts the
  shares inside the 80% and 90% bands over every run. The horizon per
  grain is month 12, week 13, day 31, quarter 4; a flow asks the
  quarter and the year to go as spans where they fit
  (`docs/methods/forecast.md`). The kit declares the aspect, the
  function and a witness without a detector; the owed rule admits
  such a witness (`cube.rs`, `wanted`; `owed.sql`).
- The world on the series read (#106). `metric_series()` carries
  `world` beside `metric`; `world => 'forecast'` serves the newest
  `metric_forecast` run's medians as cells at the metric's own
  resolution, the default read is the actual world alone, and `plan`
  and `scenario` refuse by name until their forms exist
  (`crates/session/src/cube.rs`: `World`, `series_args`,
  `metric_series_batch`; `crates/session/src/search/forecast.rs`:
  `world_cells`). The head's cells carry the column; `HEAD_SHAPE`
  names it. The docket's trend frame unions the forecast world as a
  dashed line named `forecast`. `metric_axes()` is unchanged until the
  plan grounding.
- The wire's worlds. A row's `status` is `actual`, `plan`, `forecast`
  or `scenario` (`../glossthesi/docs/contract.md`). `/forecast` takes
  covariates known ahead at any horizon, `origins` for a backtest,
  `totals`, `grain` and `width`. A member with two rows of one period
  is refused, so a plan and a scenario go as two requests or as two
  members. `/whatif` returns per column a sign, the model's rows, the
  history's estimate, `basis` and the held-back error.
- The plan rewrite. Every scan of an overridden table gains a
  projection over the overridden column from a start date, and a
  grounding replays over it (`crates/session/src/replay.rs`:
  `Override`, 21; `apply_overrides`, 33; `crates/session/src/plan.rs`:
  `build_plan`, 18). It takes one factor from a start date, not a
  value per period. No door calls it yet.
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
- The span evidence. The forecast's 80% span, measured per step on
  held-out data (`../glossthesi/docs/forecast.md`, GK-46): weeks hold
  0.79 to 0.84 at every step and 0.85 for the quarter's total;
  quarters 0.79 to 0.82 at every step and 0.84 for the year's total;
  months 0.75 one ahead down to 0.67 at twelve, 0.72 and 0.67 for the
  quarter's and the year's total, 0.80 at no step; days 0.79 one
  ahead, 0.70 to 0.80 out to thirty-one, the calendar month's total
  0.94 at the monthly dependence. Pooled over the steps the table
  reproduces GK-28. The what-if's size was right in one region: a set
  lever that explains a fifth or more of the column, pinned by history
  to within about a third, standing apart from the other inputs; 98%
  of 597 instances (`../glossthesi/docs/whatif.md`). A covariate moved
  ahead on the forecast route (GK-47): the direction holds on 88% and
  95% of the series whose history shows an effect, against the
  what-if's 93% and 96%; the size is about half the history's where
  the what-if had it right, 0.51 and 0.24 of the history's shift
  against 0.99 and 0.42; the column moves where the history shows no
  effect in a third to a half of the cases, still on 69% and 49%
  against 90% and 86%. The plan row is level. The evaluation's
  recommendation: the scenario's estimate rides `/whatif`, the
  forecast route carries the plan's world. The decision is this
  plan's.
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

### The range with how often it held

- **What it buys.** Every forecast range is served with how often a
  range of its level held at that grain and step, so a reader knows
  what an 80% range is worth a year out. Months hold 0.80 at no step
  (the span evidence); the number stays and says so.
- **What stands.** `forecast_points()` serves five quantiles per step
  and span (`crates/session/src/search/forecast.rs:520`);
  `forecast_coverage()` serves the shares inside the 80% and 90%
  bands per metric, grain, step and span over the dataset's own runs
  (753). The service's held-out coverage per grain and step is prose
  on the forecast page (`docs/methods/forecast.md`), not on the wire
  and not in the engine. The docket's trend draws the forecast
  world's medians, no band.
- **Done when.** The service's answer carries its held-out coverage
  per level at each step and span it serves (a wire addition on
  glossthesi), and `metric_forecast` records it with the quantiles.
  `forecast_points()` carries, per point and level, the share such a
  span held: the dataset's own from `forecast_coverage()` where its
  count at that grain, step and span reaches a floor the issue sets,
  the service's otherwise, and a column saying which. The docket's
  trend draws the 80% band ahead with the share in its tooltip. The
  reference and the forecast page say the rule. The session suite
  holds both sources on the fixture, with the service stubbed.

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
  scenario, with a span and how often such a span held, or a
  direction where the service gives none, every cell saying which
  half produced it.
- **What stands.** The plan rewrite (above). The engine has two
  points on the service call, `FORECAST` and `MISFIT`, and no what-if
  point. `/whatif` takes the plan and scenario rows; the what-if's
  "no declared path" refusal. GK-47 (the span evidence): the
  scenario's estimate rides `/whatif`, the forecast route carries the
  plan's world.
- **Done when.** Pass one: where a declared lever reaches a metric
  through its groundings, the engine replays with the scenario's
  values and marks the cell `replayed`. Pass two: where no path
  reaches it, the what-if call estimates, a third point on the
  service call, `WHATIF`, one table in and one out like the other
  two, and the cell is `estimated`, with its span and the coverage
  measured at that grain and step beside it, the dataset's own where
  it has one, the service's otherwise; where the service gives no
  span, a direction and its `basis`. The read serves the scenario
  world. The session and scripts suites hold it on the fixtures, with
  the service stubbed.

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

- **glossthesi**: `main` is past `v0.2.0` by the forecast page's
  GK-46 and GK-47 rows and one wire fix: an error on one member's
  column, in a table with timestamp periods, failed the whole request
  because its origin was a date the answer could not say; it is said
  as text now. No tag yet. The forecast route takes the lever as a
  covariate, and the what-if route takes the plan and scenario rows.
  Two futures of one member in one request is a wire convenience for
  later, if the engine wants one call per comparison. GK-47 says the
  what-if's estimator stays on `/whatif`; the what-if call returns to
  the engine as a third point, `WHATIF`. The forecast answer gains the
  service's held-out coverage per step (the range draft). Once the
  three points are wired, the model behind each can be tuned per
  point.
- **glosstheval**: the two experiments before the span rule are
  done, M9 and M10 on its evaluation page: the coverage by horizon
  per grain and step, the table the span rule reads, and the
  covariate's effect on `/forecast`; both in the span evidence above.
  Open there: the dependence for days, the constant under a total's
  band on daily data.
- **glossval**: the futures lane, `notes/futures-lane.md` there. The
  forecast lane runs (`evals/tools/forecast_lane.py`; its record
  `evals/notes/forecast-lane.md`: two scenarios, the 80% band's
  coverage within the binomial tolerance at steps 1 and 3 and the
  quarter to go, every key right, no refusal). Still to come: the
  counterfactual truth from the generator's lever pairs, the plan
  world, and a planning task for the agent lane.
- **glosstest**: nothing. The levers are shipped.
