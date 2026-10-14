# Forecast — the periods ahead from a forecast service

`metric_forecast` asks the forecast service, for every applicable
metric, what the periods ahead should be from the last period with an
actual. It runs at dataset grain (`SELECT metric_forecast() FROM fin`)
and records per metric the bands per step and, for a flow, the totals
of the quarter and the year to go. The record keeps every run at its
origin. `forecast_points()` serves the latest run with the actuals
joined as they land; `forecast_coverage()` counts how often they landed
inside the bands.

## Terms

The origin is the last period with an actual. A step is one period
past it. The horizon is how many steps are asked. A span is the total
of the first so many steps. A level is a quantile the service answers;
the band between two levels is the range the actual should land in
that often. Coverage is the share of actuals that landed inside a
band.

## Mechanism

The door, `metric_forecast_ahead`, reads each applicable metric as a
whole from the cube: the `''` dimension at the metric's own resolution
over its rung ([reads](../reference/reads.md), `metric_series()`). A
forecast period is therefore a cube bucket. It sends one table per
grain and totals: `member` the metric, `period` the bucket's start,
`value` and `status`. The history goes as `actual` rows, then the
horizon's periods ahead as `forecast` rows with no value. The specification
names the target, the walk's five levels (0.05, 0.1, 0.5, 0.9, 0.95),
the grain and the horizon, and for a flow the totals as spans.

The horizon is fixed per grain where the service measured its
coverage:

| grain | horizon | total asked |
|---|---|---|
| day | 31 | the month to go |
| week | 13 | the quarter to go |
| month | 12 | the quarter and the year to go |
| quarter | 4 | the year to go |

A total runs from the first period ahead to the end of the quarter,
year or month that period falls in. It is asked only where it fits in
the horizon. A stock or a ratio has no total. A minute or hour metric
is inapplicable: the service forecasts day and coarser. A year metric
is inapplicable: too few periods. A metric with fewer than six periods
is inapplicable before any call. One the service cannot forecast is
inapplicable in the service's words.

What lands per metric: the origin, the grain, the service and its
version as `served_by`, and per step and span the five quantiles and
the period. No actual and no PIT land: neither is known at the origin.

## The reads

`forecast_points()` flattens the latest run, one row per metric and
point. For every point whose actual the cube now holds, it joins the
actual and says where it landed: `below` p05, `low` between p05 and
p10, `inside` p10 to p90, `high` between p90 and p95, `above` p95. A
span's actual is the sum of its periods' actuals, where every one is
known.

`forecast_coverage()` counts over every run the record holds, one
forecast per metric and origin: per grain, step and span, the points
whose actual has landed, the share inside the 80% band, the share
inside the 90% band, and how many forecasts they come from. The `''`
metric row is the dataset as a whole. This is the table a span rule
reads: a number handed out as a range comes with the coverage this
dataset measured at that step.

`metric_series(world => 'forecast')` serves the newest run as cells
beside the actuals: per metric, one row per step with the median as
`value`, at the metric's own resolution. The actual world is the
default, so a read that names no world sees no forecast rows. A
variance of forecast against actual is a join of the two worlds on
metric and period ([reads](../reference/reads.md), `metric_series()`).

## Why no PIT

The walk ([bands](bands.md)) asks about months that have already
happened. The service holds the whole distribution at that moment and
places the actual in it exactly. A forecast is written before the
actual exists, and the record keeps five quantiles. Months later,
those five numbers say which band the actual fell in, not its exact
position. Coverage needs only the band.

## Serving the model

The call goes to the service the forecast point names,
`GLOSSQL_FORECAST_URL` ([install](../start/install.md)), as one Arrow
IPC stream each way. Without it the measurement refuses by name.

The service measured its 80% band on held-out data at every step the
horizon asks and for the totals it asks:

| Grain | One ahead | At the horizon | The totals |
|---|---|---|---|
| month | 0.75 | 0.67 at twelve | quarter 0.72, year 0.67 |
| week | 0.81 | 0.80 at thirteen | quarter 0.85 |
| day | 0.79 | 0.74 at thirty-one | the calendar month 0.94 |
| quarter | 0.81 | 0.80 at four | year 0.84 |

Weeks and quarters hold at every step. Months hold at none. Days hold
at the nearest three steps and run narrow beyond.

## Limits

- The bands are the model's own. The record learns the dataset's
  coverage over time; until it has, the service's held-out numbers are
  the only measure.
- The metric is forecast as a whole. Its members per dimension are
  not.
- A total's band on day grain takes the monthly dependence, so a
  calendar month of days covers 0.94: too wide. The day's own
  dependence is not measured.
- A re-run at the same origin counts once in coverage: the newest run
  at that origin.
