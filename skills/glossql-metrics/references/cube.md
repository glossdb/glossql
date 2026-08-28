# The cube and the bands walk — read when a metric's axes, resolution or window are not what you expected

## The bands walk and the cube

`metric_bands` asks, per metric and recent month: knowing only what
came before, what would this month have had to be for nobody to be
surprised? PIT says where the actual landed in that corridor — 0.5 is
on the trajectory, past 0.95 or under 0.05 is outside what the
metric's own history can explain. `band_breach` is the detector on
top. Every point is honest: its fit saw only the months before it.

- **The read is recall, you are the judge.** A business shift and a
  data defect breach identically.
- The corridor knows only the history it is shown; under about five
  months the walk says nothing.
- It follows the grounding's authored behavior — mark your stocks.

The cube is its sibling and the app's fuel: every grounded metric's
cells — the total, the slices along its judged dimensions, the
disclosed rival — at the metric's resolution, computed at read and
cached, never recorded. Nothing to run and nothing to land:
`metric_series()` builds what is not built. A dimension the cube
should slice must be a served column of the extract.

The axes come from judged verdicts, not from the data's shape: a
served column enters as a dimension when a verdict admits it: its own
`dimension_relevance`, or — for a label in a dimension table, a
near-key there by construction — the verdict on the key column that
reaches it through a declared relationship from a table the grounding
scans; `metric_axes().basis` names that key. Relevance orders the
admitted, up to four. The `dimension` gloss on the column is the read
policy over that, human over agent: `none` closes an axis whatever was
measured, `primary` admits it and puts it first, `supporting` admits
it; `admitted_by` says whose word it is. The time axis is the served
date column whose `temporal_profile` is applicable — a named cadence
before none, highest completeness first; an `irregular` verdict
anchors at the declared floor. A column nobody judged and nobody
glossed stays out, and a frame with no judged date column abstains;
`metric_axes()` says so per metric, with the road out. The verdicts
the cube admits on are the newest landed, whatever pin they were
judged at — served and marked, as every function voice is, and every
write moves the pin — so after a gloss or an import the cube still
builds on them and `metric_axes().judged_current` turns false. Re-run
`temporal()` and `dimension_relevance()` over the served columns
after a data change; the docket's re-measure re-runs every
measurement standing from before the last change.

Resolution and window are the `cube` aspect's, declared by the kit on
the dataset: a metric's cells stand at its judged cadence and never
finer than the floor (`day` by default), over the ladder's rung for
that resolution — minutes for a day, hours for a month, days for 18
months, weeks for 3 years, months for 48, quarters for 10 years,
years for 20 — measured back from the data's own edge. A gloss
overrides the floor or any rung, and supersedes like any gloss:

```glossql
GLOSS cube ON ops AS $${"resolution": "hour", "windows": {"hour": "7 days"}}$$;
```

Every coarser grain derives from the cells by the verb on the server
— a flow sums, a stock takes the bucket's last period, a ratio
divides its summed halves — so mark your stocks and serve a ratio's
halves; both hold at every grain and for the rival too.

Wide axes bucket instead of dropping out: up to 24 members every one
is named; above that the axis serves its top 23 by weight — summed
value, a ratio by its denominator — and the rest fold into an
`'other'` member. Each metric's `bucketed` field names the axes this
happened to: never read `'other'` as a business member. If a bucketed
axis is too coarse for the question at hand, that is a grounding
decision, not a cap to fight — serve a narrower or derived column (a
region for the country, a group for the org) from the metric's own
SQL.

Where every metric stands, in one read — the record:

```sql
SELECT metric, title, unit, meaning, formula
FROM metric_surfaces ORDER BY metric
```

What the cube admitted, per metric, and why not:

```sql
SELECT metric, applicable, reason, behavior, resolution, dims, bucketed, alternative
FROM metric_axes() ORDER BY metric
```

And the cells through `metric_series(grain => …)` — one row per
metric, dimension, member and period at the asked grain (absent, each
metric's own resolution; a grain finer than a metric's resolution
serves no rows for it). `dimension = ''` is the metric's own total,
`'alternative'` the disclosed rival, anything else a served
dimension; a ratio row carries `num` / `den`, and `behavior` is the
verb that made the row. This is what an app frame charts:

```sql
SELECT metric, period, value FROM metric_series(grain => 'month')
WHERE dimension = '' ORDER BY metric, period
```

**Fold in every standing ruling, then re-measure.** The cube computes
at the next read from the newest verdicts — so one batch of fold-ins,
then the profilers over the served columns, and `judged_current` is
true on the docket's next load.
