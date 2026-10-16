# 27 · Plan: a budget beside its actuals — RULED (a plan is its own QUERY aspect, `x-kind: plan`, tied to its metric by name; the plan world on `metric_series`, read by name)

Source: the City of Baton Rouge / Parish of East Baton Rouge expense
budget, Socrata dataset `5emz-4788` on data.brla.gov — public domain,
attributed to the Finance Department, refreshed daily. One row per
account and fiscal month: the account coded on eight segments (fund,
department, function, division, program, subprogram, project, object)
with their names, `fiscalyear`, `fiscalmonth`, `originalbudget`,
`revisedbudget`, `actual`, `accountstatus`. Read 2026-10-15: 274,379
rows, 6,328 accounts, 82 funds, 58 departments, fiscal years 2018 to
2026 (January to October); the fiscal year is the calendar year. The
field names below are the dataset's own (the API's); the CSV download
spells them as headers with spaces and the month as text.
Ruled: a plan is its own QUERY aspect, one per plan, tied to its
metric by name in the declaration (Fork B; the second SQL inside the
metric's grounding died on the slot and the schema, the tie as a
`metric` column in the plan's rows on the ledger's precedent, §5).

What the rows hold, checked over the whole file:

- `actual` is the month's expenditure, not the year to date the
  export's header (`YTD ACTUAL`) says: the twelve months of a 2024
  salary account sum to 20.6M against its 21.0M budget, and December
  alone is 1.6M. 1,819 rows are negative — reversals.
- `originalbudget` is the year's adopted budget, booked on the
  account's January row; every other month of the year is zero.
- `revisedbudget` is the January row's figure plus one row per council
  amendment, booked in the month it passed (an account's May row
  `-15,000`, an April row `10,920`); the year's sum is the revised
  budget the page describes. 2024 as a whole: original 947.8M, revised
  1,042.9M, actual 940.1M.
- A row is one account's month; fund, department and character code
  slice it.

So the plan arrives inside the fact table as two more columns: a year's
number, booked as a flow in the month it was authorized, beside the
actual at month grain. A budget is a landed table read by SQL, never
numbers in a gloss body — this one is 32,000 account-years in two
editions. The other shape a budget takes (San Diego's: one annual file
per budget cycle, the actuals in separate yearly files, joined through
reference tables) is also a table read by SQL; the form holds for
both.

Terms: a *world* is one set of numbers over the metric's cells —
actual, forecast, scenario, and now plan; the *supersession key* is
(subject, aspect, actor kind): one current body per key, a re-gloss
retires the last. An *edition* of a plan is one complete set of planned
numbers for the same metric and periods — here the adopted budget and
the amended one.

## 1. The floor — the actual

The metric declares and grounds as every metric does (fixture 16 §1,
§2). The glosses on the columns say what the header does not, and the
conventions deposit says how the year and the budget are booked.

```glossql
USE brla;

GLOSS meaning ON expense_budget.actual AS $${
  "value": "the month's expenditure on the account — the export's header reads YTD ACTUAL; a year's twelve months sum to the year's spend"
}$$;
GLOSS meaning ON expense_budget.revisedbudget AS $${
  "value": "budget authority booked in the month: the adopted budget on the January row, each council amendment on the row of its month; the year's sum is the revised budget"
}$$;
GLOSS conventions ON brla_open_data AS $${
  "fiscal_year": "the calendar year, January to December",
  "budget_booking": "the adopted budget is booked in January; an amendment in the month the Metropolitan Council approved it, as a delta",
  "sign": "expenditures positive; a negative actual is a reversal"
}$$;

DECLARE ASPECT expense WITH $${
  "title": "Operating expense",
  "description": "What the City-Parish spent, per account and month",
  "x-kind": "measure", "x-unit": "USD"
}$$ AS QUERY ON DATASET;

GLOSS expense ON brla AS $${
  "sql": "SELECT fiscalmonth, actual AS value, fund, segment2, charactercodedescription FROM expense_budget",
  "behavior": "flow",
  "axes": ["fund", "segment2", "charactercodedescription"],
  "assumptions": [
    {"dimension": "grain", "assumption": "actual is the month's amount, not the year to date",
     "basis": "a year's twelve months sum to its budget; December is one twelfth", "confidence": 0.95},
    {"dimension": "scope", "assumption": "closed and inactive accounts keep their rows",
     "basis": "accountstatus", "confidence": 1.0}
  ]
}$$;
```

Where the shape is the recipe's and where the meaning is the gloss's:
a recipe lands the table typed (SPEC §3 — the CSV download's month
as text becomes a date there, the amounts numbers), and that is all
it does; the grounding reads the landed rows at their own grain and
aggregates nothing (fixture 16 §2); the `meaning` gloss says in words
what a row is; the sum over the year is the cube's verb, `flow`, at
the grain the reader asks. "The revised budget is the January row
plus the amendments" is therefore said three times and computed
once, and none of it is a recipe.

## 2. The plan declares like a metric — one QUERY aspect per plan

The placement a scenario has (fixture 19 §2: one FACT aspect per
scenario), for a QUERY: one aspect per plan, `x-kind: plan`, tied to
its metric by `x-plan-of` in the declaration — the tie is the plan's
identity, checked at `DECLARE` against the declared QUERY aspects,
never re-tied. The body is the standard grounding schema (SPEC §5.2),
unchanged: a plan's numbers are a landed table, read by SQL as the
actual's are. The plan has its own `behavior`, `axes` and
assumptions, its own slot under the supersession key, its own witness
if wanted. An edition is an aspect: the adopted budget and the amended
budget both stand, and a re-gloss of either corrects that edition's
SQL. A budget file of many lines is read as the ledger is: one plan
aspect per line and edition, each grounding a filter on its line.

```glossql
DECLARE ASPECT revised_budget WITH $${
  "title": "Revised budget",
  "description": "The adopted budget with the Metropolitan Council's amendments, as booked",
  "x-kind": "plan", "x-plan-of": "expense", "x-unit": "USD"
}$$ AS QUERY ON DATASET;

GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth, revisedbudget AS value, fund, segment2, charactercodedescription FROM expense_budget",
  "behavior": "flow",
  "axes": ["fund", "segment2", "charactercodedescription"],
  "assumptions": [
    {"dimension": "grain", "assumption": "a year's budget: the adopted figure in January, each amendment in its month; read at year grain",
     "basis": "the rows", "confidence": 0.95}
  ]
}$$;

DECLARE ASPECT original_budget WITH $${
  "title": "Original budget",
  "description": "The budget the Metropolitan Council adopted",
  "x-kind": "plan", "x-plan-of": "expense", "x-unit": "USD"
}$$ AS QUERY ON DATASET;

GLOSS original_budget ON brla AS $${
  "sql": "SELECT fiscalmonth, originalbudget AS value, fund, segment2, charactercodedescription FROM expense_budget",
  "behavior": "flow",
  "axes": ["fund", "segment2", "charactercodedescription"]
}$$;
```

Plan authorship as a policy is one witness, as the scenario's (fixture
19 §5):

```glossql
DECLARE WITNESS plan_gate ON revised_budget BY (HUMAN);
```

## 3. The read — a world beside the actual, a variance a join

The plan is a set of numbers over the metric's cells and reads as the
forecast and the scenario do: `metric_series(world => 'plan', plan =>
'<name>')`, the cells under the metric the plan names, at the plan's
own resolution. A variance is a join of two worlds on metric and
period, at the plan's grain or coarser (fixture 26: the grain is the
reader's). Original against revised is a join of two plan reads.

```glossql
SELECT metric, period, value FROM metric_series(world => 'plan', plan => 'revised_budget', grain => 'year');

SELECT a.period, a.value AS actual, p.value AS plan, a.value - p.value AS variance
FROM metric_series(grain => 'year') a
JOIN metric_series(world => 'plan', plan => 'revised_budget', grain => 'year') p
  ON a.metric = p.metric AND a.period = p.period
WHERE a.metric = 'expense' AND a.dimension = '' AND p.dimension = '';
```

## 4. Grain — what this artifact says

A budget at year grain beside an actual at month grain is the
ordinary case, and nothing in the form speaks to it: the plan's
grounding serves the judged date column, the cube buckets it as it
buckets any flow, and the plan's cells mean something at year grain.
At month grain they are what the ledger booked that month — the
adoption in January, an amendment in May — and a variance against
them would be a variance against bookkeeping. The reads in §3 ask the
year. A phased budget (a twelfth per month, a seasonal curve) is a
planner's own table — the generator's budget is monthly and reads at
month grain — never the engine's spread. `metric_series(world =>
'plan')` serves the plan at its own resolution and at any coarser
grain asked, as the actual does; it spreads nothing.

## 5. The forks that died

- **A second SQL inside the metric's grounding** — one body carrying
  `sql` and `plan`, superseding as one. Dies on the slot and the
  schema. The standard grounding schema would grow a key, in SPEC
  §5.2 and `crates/glossary/src/schemas.rs`, and every QUERY body —
  fixture 20's sample frames included — could carry a plan where it
  means nothing. One actor kind would carry both: an agent's actual
  beside a human's plan is not a state one slot holds, and whoever
  corrects the plan re-glosses the actual's SQL with it. One plan per
  metric: the adopted budget would be the superseded body, in the
  glossary's history and in no world, and original against revised
  no cube read. The plan would have no name of its own — the catalog's
  view under the metric's name is the actual's, `read.<metric>()`
  serves the actual — so the plan's rows would need a second spelling
  and a second view name. And `behavior`, `axes` and `grain` would be
  shared, right here and wrong where a budget comes by department
  and the actual by account.
- **One SQL with a `world` column** (`'actual' AS world … UNION ALL
  'plan'`) — dies on the grain, here concretely: the January row
  carries the month's actual and the year's budget on one row, and
  one frame cannot carry two cadences under one judged time axis. The
  actual world would carry plan rows unless the cube learned the
  column.
- **The plan as numbers in a gloss body** (a FACT aspect per plan,
  the scenario's shape) — dies on size: a budget is thousands of
  account-years in two editions; the scenario is a factor per period
  on a handful of levers.
- **The plan as a second QUERY gloss on the metric's own aspect** —
  dies on the supersession key: the second gloss retires the actual.
- **The tie as a `metric` column in the plan's rows** — one aspect
  per budget file and edition, its SQL serving `metric, period,
  value`, the column's values the names of declared metrics. Dies on
  the ledger's precedent: one table of journal lines carries every
  account type, and the language declares one metric aspect per
  concept, each grounding a filter on its rows — never one aspect for
  the ledger with a column mapping account types to concepts. A budget
  file of many lines is the same shape: a ten-metric scorecard with a
  budget and a reforecast declares twenty plan aspects, the price
  fixture 19 pays per scenario. The column would also have been the
  first gloss whose output names other aspects by value — a tie
  carried in data, found at the read instead of at `DECLARE` — and one
  body's `behavior` and `axes` would speak for every line at once.
  Here the two placements coincide, one metric; the generator's
  budget, many metrics in one table, is where they part.

## 6. Machinery, not language

None of this appears in a statement; recorded so the fixture is honest
about what the read does:

- The plan's SQL plans as a grounding plans — a view at the pin, a
  frame with a judged time axis and a `value` column, a verb — and its
  head is built as a metric's (`crates/session/src/cube.rs`,
  `Surface::plans`); its cells are served under `world = 'plan'` and
  the metric the plan names, at the plan's own resolution or any
  coarser grain asked, `dimension` and `member` as its own axes give
  them (`metric_series_batch`, `relabel`).
- A plan aspect is no head of the actual world: the slot carries the
  tie (`crates/session/src/search/collisions.rs`, `QuerySlot::plan_of`),
  the cube keeps the plans apart, and the walk, the forecast and the
  collision pass pass them over. The catalog holds the plan's view
  under its own name, `read.<plan>()` serves its rows, and
  `metric_axes()` lists it with `plan_of`.
- The tie is checked at `DECLARE` (`crates/session/src/plan_world.rs`,
  `check_declaration`): `x-kind: plan` on a FACT, no `x-plan-of`, a
  metric nobody declared, a FACT or MEASUREMENT aspect, or another
  plan is refused before the declaration lands. A metric stopped
  later leaves its plan standing; the stop is the metric's own
  `metric_axes()` row.
- `plan_points()` (`crates/session/src/plan_world.rs`) serves every
  plan against the actual and the forecast, one row per plan and
  period at the plan's own resolution and at the year — the plan's
  cell, the actual where the period has begun, the variance where it
  is complete, the forecast's band where it covers the rest, and where
  the plan stands against it. The next read's eighth goal, `plan`
  (`crates/session/reads/next.sql`), routes the plan: declared,
  grounded, the forecast run, every year the band does not hold judged
  — a scenario on a lever, or the gap recorded under `plan-<year>` in
  the plan's body for the human to rule. A plan's table is no edge of
  the actuals: the edge of a metric's head is measured over the tables
  the metrics' groundings scan (`Surface::edge_subjects`), a plan's
  over its own.
- The evaluation side: the generator writes a budget table per corpus
  from its own levers, a planned value per metric and month, with a
  truth file; the eval runs a plan lane beside the forecast lane. The
  lane reads that table through plan aspects, one per metric and
  edition.
