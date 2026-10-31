---
name: gloss-metrics
description: The metrics kit for a gloss workspace — a metric is a view whose columns carry the vocabulary; the fact row per metric, the series over periods and slices, the no-claim refusal, the ladder on a time column, facts and sources. Use after gloss_kit('kits/metrics/kit.sql') to ground metrics and read their numbers.
---

# The metrics kit: what it teaches

For an actor working in a DuckDB that has the glossql extension loaded,
the KPI kit and the quality kit declared and this kit after them. The
extension serves every grounding as a view under the metric's name in
the dataset's schema; this kit reads the view's columns as subjects and
serves the numbers from what the record says about them. Nothing is
marked at read: a claim older than its data reads stale, and the reads
serve it as the record does.

## Load and check

```sql
FROM gloss_kit('kits/kpi/kit.sql');
FROM gloss_kit('kits/quality/kit.sql');
FROM gloss_kit('kits/metrics/kit.sql');    -- the same three lines in the opener
SELECT * FROM glossql.metric_axes();       -- one row per metric: what its columns say
```

## A metric is a view, and its columns are subjects

A grounding is a QUERY gloss on the dataset, SQL under the contract:
`sql` or `stopped`, with `assumptions` disclosed. The view it makes is a
table of the dataset, so its columns take the KPI kit's words, like a
landed table's columns do. What a reader of the numbers needs is said
there, never as a key in the grounding body:

| On the view's column | Gloss | What it decides |
|---|---|---|
| the period | `role` = `timestamp` | which column the series walks; serve it as `period` where two columns are timestamps |
| the value | `role` = `measure` | which column is the number; serve it as `value` where two columns are measures |
| the value | `behavior` = `flow`, `stock` or `ratio` (`numerator`, `denominator` naming the halves the view serves) | how the number combines over periods and members: a flow sums, a stock keeps the period's last level, a ratio re-divides its summed halves |
| a slice | `role` = `dimension`, `dimension` = `primary`, `supporting` or `none` | which columns slice the series, primary first; `none` closes one |
| the period | `ladder` — `floor`, `windows` | the grain the series is served at (by default the measured cadence) and how many buckets each grain serves (by default the kit's rungs) |
| the period | `temporal_profile`, a measurement: `SELECT temporal() FROM <dataset>.<metric>.period` | the measured cadence, window, completeness and gaps |

The round for a new metric: ground it; read `metric_axes()`; gloss what
`owed` lists; run what `wanted` lists; gloss `behavior` on the value;
read the series.

```sql
DECLARE ASPECT spend WITH $${"title": "Spend"}$$ AS QUERY;
GLOSS spend ON fin AS $${"sql": "SELECT date_trunc('month', booked)::DATE AS period, sum(amount) AS value, fund FROM fin.ledger GROUP BY ALL"}$$;
GLOSS role ON spend.period AS $${"value": "timestamp"}$$;
GLOSS role ON spend.value AS $${"value": "measure"}$$;
GLOSS behavior ON spend.value AS $${"value": "flow", "grounds": "a row is the amount booked in its month"}$$;
GLOSS role ON spend.fund AS $${"value": "dimension"}$$;
SELECT temporal() FROM fin.spend.period;
SELECT * FROM glossql.metric_series() WHERE metric = 'spend';
```

## The reads

| Read | Serves |
|---|---|
| `metric_axes()` | one row per metric of the bound dataset: `kind` (the aspect's `x-kind`), `stopped`, `applicable` and `reason` (the road out), `period_column`, `value_column`, `numerator`, `denominator`, `cadence`, `floor`, `rung`, `behavior` and `behavior_state`, `dims`, `unadmitted` (served columns with no role) and `owed` (the gloss each wants), `wanted` (the measurement nobody ran), `judged_current` |
| `metric_series(grain := …)` | the cells: `metric`, `world`, `dimension` (`''` the total), `member`, `period` (the bucket's start), `value`, `num`, `den`, `behavior`; without a grain each metric at its floor, with one each metric at or below it at that grain, a coarser metric serving no rows; a rung of buckets measured back from the edge; a flow's empty periods zero in the total; a dimension wider than 24 members as its top 23 and `other` |
| `fact_values()` | one row per metric whose aspect carries `x-kind: fact`: `value` where the view is one row with a `value` column, else `reason` |
| `metric_sources(metric)` | what feeds the metric, through `gloss_lineage` over the view: `field`, `source` (`table.column`), `table_name`; one row per table read beside |

## The no-claim rule

Where rows have to be combined — a grain coarser than the floor, or
several rows in one period — and no `behavior` stands on the value
column, `metric_series()` refuses and names the gloss:

```
no behaviour stands on fin.spend.value, and spend serves several rows in one
period: gloss it — GLOSS behavior ON fin.spend.value AS $${"value": "<flow|stock|ratio>"}$$
```

It defaults nothing. A contested behaviour — the witness withholding the
slot — refuses the same way, naming the contest. At the floor, with one
row per period, nothing is combined and the series serves as the view
does.

## What is not here

A plan, a forecast and a scenario are worlds on the same cells; the
futures kit brings them. The behaviour evidence a detector measures on a
value column is a measurement of its own; where none was run, the
refusal names the gloss alone.
