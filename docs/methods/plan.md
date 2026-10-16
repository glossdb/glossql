# The plan world

A plan is a set of numbers someone decided for a metric: a budget, a
reforecast, a target. Its numbers arrive as a landed table, thousands
of rows and an edition or two, and the glossary holds what they mean:
which table, which column, which edition counts as the plan, and which
metric it belongs to. `metric_series(world => 'plan', plan =>
'<name>')` serves the plan's cells beside the actual, the forecast and
the scenario worlds, under the metric it plans.

## What a plan is

A plan is its own QUERY aspect, one per plan, the way a metric is one
QUERY aspect per concept. Its declaration carries `x-kind: plan` and
`x-plan-of`, the metric it plans; its gloss is a grounding like a
metric's, SQL over the landed table, validated by the standard
grounding schema:

```glossql
DECLARE ASPECT revised_budget WITH $${
  "title": "Revised budget",
  "description": "The adopted budget with the council's amendments, as booked",
  "x-kind": "plan", "x-plan-of": "expense", "x-unit": "USD"
}$$ AS QUERY ON DATASET;

GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth, revisedbudget AS value, fund, segment2 FROM expense_budget",
  "behavior": "flow",
  "axes": ["fund", "segment2"]
}$$;
```

The tie is checked when the plan is declared: `x-plan-of` must name a
declared QUERY aspect that is not itself a plan, a plan is a QUERY
aspect, and `x-plan-of` rides `x-kind: plan` and nothing else. The
declaration is refused otherwise, with the road out.

An edition of a plan is an aspect of its own: the adopted budget and
the amended budget both stand, each with its own grounding, its own
`behavior`, `axes` and assumptions, its own slot under the
supersession key and its own witness if wanted. A re-gloss corrects
one edition's SQL; a human's gloss retires an agent's.

## The read

The plan's grounding plans as every grounding does: a view in the
catalog under the plan's name, `read.<plan>()` serving its rows, a
frame with a judged time axis and a `value` column, a verb. The cube
builds its head as it builds a metric's and serves it in the plan
world alone: `metric_series(world => 'plan', plan => 'revised_budget')`
serves the cells with `metric` the metric the plan names and `world`
`plan`, at the plan's own resolution or at any coarser grain asked,
over the plan's own axes. A finer grain serves no rows. The actual
world never carries a plan's cells, and the walk, the forecast and the
collision pass do not read it. `metric_axes()` lists the plan with its
verdict, `plan_of` naming the metric.

A variance against the plan is a join of two worlds on metric and
period: the actual at `grain => 'year'` against the plan at the same
grain, `a.value - p.value`. A forecast against the plan is the same
join with `world => 'forecast'` on one side. Original against revised
is a join of two plan reads. `world => 'plan'` without a name, a name
that is no plan aspect, and `plan => …` in another world are refused
with the spelling.

## Grain

A budget often comes at a coarser grain than the actual: a year's
number beside monthly spend. The plan's grounding serves the judged
date column as it stands, and the cube buckets it as it buckets any
flow. Where a budget is booked in the month it was authorized, the
adoption in January and an amendment in May, the plan's month cells
are what the ledger booked that month, and the year is where the plan
means something; the read asks the year. A phased budget, a twelfth a
month or a seasonal curve, is a planner's own table, read at month
grain. The engine spreads nothing.

## Limits

- One plan aspect serves one metric's numbers. A budget file of many
  lines is read by one plan aspect per line and edition, each
  grounding a filter on its line.
- The plan's verb and axes are its own. A plan declared `flow` beside
  a `stock` metric is served as declared; the join is the reader's.
- A metric whose grounding is stopped leaves its plans standing. The
  stop is the metric's own `metric_axes()` row.
- Nothing is recorded: the plan's cells are a cube head under the
  cache, like a metric's, and move when the plan's table or its
  grounding does.
