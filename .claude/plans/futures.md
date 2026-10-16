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
| Cube widening: every column, the whole history, measurements warn instead of gate | Medium, core | cube, docket, issue #41 | After the world; its own decision | |

## What stands

**The futures today.**

- Four worlds. `metric_series()` serves `metric`, `world`,
  `dimension`, `member`, `period`, `value`, `num`, `den`, `behavior`;
  the actual world by default, the forecast world (#106), the
  scenario world (#113) and the plan world (#115) by name
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
  (`docs/methods/forecast.md`). The range with how often it held
  (#110): the service's answer carries `coverage`, `metric_forecast`
  records `service_80` and `service_90` per point, and
  `forecast_points()` serves `held_80`, `held_90` and `held_from`:
  the metric's own record once 30 of its points landed at that grain,
  step and span, else the dataset's, else the service's (`forecast.rs`:
  `RECORD_FLOOR`, `held`). The docket's trend draws the 80% band
  ahead. The kit declares the aspect, the
  function and a witness without a detector; the owed rule admits
  such a witness (`cube.rs`, `wanted`; `owed.sql`).
- The world on the series read (#106). `metric_series()` carries
  `world` beside `metric`; `world => 'forecast'` serves the newest
  `metric_forecast` run's medians as cells at the metric's own
  resolution, the default read is the actual world alone, and the
  scenario (#113) and plan (#115) worlds are read by name
  (`crates/session/src/cube.rs`: `World`, `series_args`,
  `metric_series_batch`; `crates/session/src/search/forecast.rs`:
  `world_cells`). The head's cells carry the column; `HEAD_SHAPE`
  names it. The docket's trend frame unions the forecast world as a
  dashed line named `forecast`. `metric_axes()` carries `plan_of` as
  its last column (#115).
- The wire's worlds. A row's `status` is `actual`, `plan`, `forecast`
  or `scenario` (`../glossthesi/docs/contract.md`). `/forecast` takes
  covariates known ahead at any horizon, `origins` for a backtest,
  `totals`, `grain` and `width`. A member with two rows of one period
  is refused, so a plan and a scenario go as two requests or as two
  members. `/whatif` returns per column a sign, the model's rows, the
  history's estimate, `basis` and the held-back error; it reads one
  or several changed inputs per request, with `parts` per input.
- The plan rewrite. Every scan of an overridden table gains a
  projection over the overridden column, a factor per period, and a
  grounding replays over it (`crates/session/src/replay.rs`:
  `Override`, `Factor`, `apply_overrides`). The scenario read calls it
  (#113).
- Levers and the scenario (#112). `lever` is a FACT aspect on a column
  in the kit (`crates/scripts/functions/kpi_kit.glossql`), `value` and
  `grounds`, witness-free. A scenario is a FACT aspect per scenario,
  `x-kind: scenario`, glossed with `levers`: per lever the column, a
  factor per period and the basis; a column without a current `lever`
  gloss refuses the gloss (`crates/session/src/scenario.rs`: `check`).
  Fixture 19 and SPEC.md §9 carry the form. What a lever is, and the
  four assumptions under which its effect reads, is on the what-if
  page (`../glossthesi/docs/whatif.md`).
- The scenario world (#113). `metric_series(world => 'scenario',
  scenario => '<name>')` and `scenario_points(scenario => '<name>')`
  (`crates/session/src/scenario.rs`: `points`). Pass one replays the
  groundings that scan a lever's table over the data; pass two sends
  every metric's series with the lever's to the what-if point,
  `WHATIF`, the third point on the service call
  (`crates/session/src/session.rs`: `POINTS`), as planned and under
  the scenario, and serves the median, the 80% band, `held_80` by the
  forecast record's rule, the sign and the basis. Several levers
  estimate as one change, every lever an input column, and
  `scenario_levers()` serves each lever's part (#121). Nothing
  recorded, nothing cached (`docs/methods/scenario.md`).
- The plan world (#115). A plan is its own QUERY aspect, `x-kind:
  plan` with `x-plan-of` naming its metric, its body the standard
  grounding schema unchanged (fixture 27,
  `crates/parser/tests/corpus/27-plan-budget.md`; SPEC.md §9;
  `docs/methods/plan.md`). The slot carries the tie
  (`crates/session/src/search/collisions.rs`, `QuerySlot::plan_of`);
  the cube keeps the plans apart from the metrics' heads
  (`crates/session/src/cube.rs`, `Surface::plans`) and serves a plan's
  cells relabelled under its metric, `metric_series(world => 'plan',
  plan => '<name>')`, at the plan's resolution or coarser; the walk,
  the forecast and the collision pass skip plan slots; `metric_axes()`
  carries `plan_of`; the declaration is checked in
  `crates/session/src/plan_world.rs`. An edition is an aspect of its
  own. The real artifact: the Baton Rouge expense budget, public
  domain, the plan two columns of the fact table at year grain beside
  a monthly actual, 106 months, 8 complete years, no lever.
- The planning goal (#117). `plan_points()`
  (`crates/session/src/plan_world.rs`) serves every plan against the
  actual and the forecast at the plan's grain and at the year, from
  the cube (`crates/session/src/cube.rs`, `plan_series`) and the
  forecast record; the next read's eighth goal, `plan`
  (`crates/session/reads/next.sql`), routes the plan to a judged year:
  declare, ground, forecast, then per gap a scenario or the gap as the
  plan's assumption under `plan-<year>`, blocked while the question
  stands, done on the ruling. `workspace_next` carries `plans`; the
  docket's metric page carries the plan tile (`frames/plan.sql`) and
  the plan world on the trend. The data's edge is the latest period
  the metrics' frames serve, measured on the frames (#118, #120,
  `Surface::edges`, `Surface::data_edge`): a plan's table and a
  calendar dimension running ahead move nothing; a plan's head takes
  its own frame's edge.
- Propagation. Rides the groundings, not the `formulas` registry: a
  metric over another metric nests as `read.<metric>()` behind a
  cycle guard (SPEC.md §9, PoC notes). The registry
  (`kpi_kit.glossql:98`) is for drills, and its operands are not
  checked (`docs/reference/kpi-kit.md`).
- The span evidence. The forecast's 80% span, measured per step on
  held-out data (`../glossthesi/docs/forecast.md`, GK-46): weeks hold
  0.79 to 0.84 at every step and 0.85 for the quarter's total;
  quarters 0.79 to 0.82 at every step and 0.80 for the year's total;
  months 0.75 one ahead down to 0.67 at twelve, 0.72 and 0.67 for the
  quarter's and the year's total, 0.80 at no step; days 0.79 one
  ahead, 0.70 to 0.80 out to thirty-one, the calendar month's total
  0.79. The day's and the quarter's totals stand at a dependence
  fitted on the same datasets, 0.12 and 0.25 (GK-48): fits on three
  and four datasets, not held-out readings. Pooled over the steps the
  table reproduces GK-28. The what-if's size was right in one region: a set
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
leaves this file. None drafted.

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
  the engine as a third point, `WHATIF` (#113). Every forecast answer
  carries `coverage`, the share per step, span and band on held-out
  data (#110). The three points are wired; the model behind each can
  be tuned per point. Several inputs changed at once are read as one change on its
  `timestral-candidate` branch (GK-50, `parts` per input, the
  combination checked against what the history held together); #121
  reads them, and lands with the branch.
- **glosstheval**: the two experiments before the span rule are
  done, M9 and M10 on its evaluation page: the coverage by horizon
  per grain and step, the table the span rule reads, and the
  covariate's effect on `/forecast`; both in the span evidence above.
  M11 (GK-48) measured the dependence for days and quarters on the
  same held-out backtests, there being no other daily or quarterly
  data, and read the table again against it. Nothing open.
- **glossval**: the futures lane, `notes/futures-lane.md` there. The
  forecast lane runs (`evals/tools/forecast_lane.py`; its record
  `evals/notes/forecast-lane.md`: two scenarios, the 80% band's
  coverage within the binomial tolerance at steps 1 and 3 and the
  quarter to go, every key right, no refusal). Still to come: the
  scenario read graded against the generator's lever pairs, the
  counterfactual truth (#113 serves the world; the lane grades it),
  the plan world (#115 serves it; the lane grades it against the generator's budget, with the Baton Rouge file as the ungraded real-data check), and a planning task for the agent lane.
- **glosstest**: a budget table per corpus from its own levers, a
  planned value per metric and month in at least two editions, with
  a truth file; the plan lane reads it through one plan aspect per
  metric and edition. The levers are shipped.
