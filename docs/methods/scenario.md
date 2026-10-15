# The scenario world

A scenario asks what the metrics do if a lever moves. The engine
answers in two passes and says, cell by cell, which pass answered.
`metric_series(world => 'scenario', scenario => '<name>')` serves the
cells beside the actual and the forecast worlds;
`scenario_points(scenario => '<name>')` serves the detail.

## What a scenario is

A lever is a column someone sets: a list price, a discount rate, a
promotion flag. It is declared on the column, witness-free, with who
sets it:

```glossql
GLOSS lever ON sales_order_lines.unit_price AS $${"value": true, "grounds": "the list price, set by sales operations"}$$;
```

A scenario is one FACT aspect per scenario with `x-kind: scenario`,
the way a metric is one QUERY aspect per concept. Its body names
declared levers, a factor per period on each, and the basis:

```glossql
DECLARE ASPECT price_hike WITH $${
  "title": "Price +15% from January 2027", "x-kind": "scenario",
  "type": "object", "required": ["levers"],
  "properties": {"levers": {"type": "array"}}
}$$ AS FACT ON DATASET;

GLOSS price_hike ON fin AS $${"levers": [
  {"column": "sales_order_lines.unit_price",
   "factor": {"2027-01": 1.15, "2027-02": 1.15, "2027-03": 1.15},
   "basis": "the price list the board approved"}
]}$$;
```

A period is `YYYY-MM`, a calendar month, or `YYYY-MM-DD`, a day. A
period the body does not name keeps a factor of one. A gloss naming a
column without a current `lever` gloss is refused before it lands,
and so is a body of another shape. Supersession is the usual key: a
re-gloss updates the scenario, and a human's gloss retires an
agent's.

## Pass one: the arithmetic

Where a metric's grounding scans a lever's table, the engine replays
the grounding with the factors. Every scan of the table gains a
projection that multiplies the column over the periods named, and the
grounding's SQL runs over it unchanged (`crates/session/src/replay.rs`).
The result is exact, what the definitions say, and reaches as far as
the rows do. The cells are `replayed`: from the first period the
scenario names to the last row the data holds.

## Pass two: the estimate

Ahead of a metric's last actual there are no rows to replay. For the
periods ahead, up to the last period the scenario names, the engine
asks the what-if point, `WHATIF`, the third point on the service call,
for every metric. It sends one table per grain: each metric's series
with the lever's series beside it, the lever as the mean of its column
per period, and each period ahead twice, as planned, the lever's last
value held, and under the scenario, that value times the period's
factor. The service answers the model's quantiles under the scenario,
a sign and a basis.

The cell is `estimated`. Its value is the median, `low` and `high` the
model's 80% band, and `held_80` and `held_from` say how often such a
band held at that grain and step and whose count that is: the
metric's own record, the dataset's or the service's, by the rule
`forecast_points()` applies ([forecast](forecast.md)). Where the
service gives no band, the sign and its basis stand alone.

The what-if reads one changed input. A scenario of several levers is
replayed and not estimated, and each estimated cell carries the
reason. Without a what-if service the arithmetic is served and each
estimated cell says what to set.

## The measured verdict

The service's what-if route promises a direction, not a size. On
3,737 tested series where the history showed an input moving a
column, the model agreed with the history's direction on 94%. The
size was within half to one and a half times the history's on 47%
and under half of it on another 47%. The service's what-if page
carries the tests and the four assumptions under which a lever's
effect reads.

## Limits

- The lever's series is the mean of the column per period. A lever
  whose meaning is a sum reads as a mean; the factor scales both the
  same way.
- The arithmetic reaches a metric through a scan of the lever's
  table, in the grounding or in a view it nests.
- Ahead of the data every metric is estimated, the ones the
  arithmetic reaches too. The estimate is the model's reading of the
  metric's history against the lever's; the definition pass one
  applies does not ride ahead.
- Nothing is recorded and nothing is cached. The service is called at
  each read.
