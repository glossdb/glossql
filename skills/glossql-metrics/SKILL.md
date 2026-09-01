---
name: glossql-metrics
description: Take a glossql workspace from raw exports to metrics someone can trust — land what the topic needs, judge the structure, gloss the vocabulary, ground the cohort, stand up validations, and close with the question round. Use for any substantive work in a workspace: onboarding a source, declaring relationships, grounding a metric, authoring a check or a surface.
---

# From files to numbers someone trusts

The deliverable is metrics the business trusts and the validations
that say why. The `glossql` skill teaches the language and the reads;
this one is judgment — what to decide, what to measure, what to ask.
It is one page and nine references; each reference is named below
with the moment to open it, and none is needed before that moment.

**There is no fixed order.** This is not a pipeline and the work is
not a batch job: a person is talking to you while it happens. Ask the
workspace what it affords and where it stands, then do the next thing
that matters:

```sql
SELECT surface, how, stands, open FROM workspace_next ORDER BY open DESC;
```

Every surface the system affords, what each is extended through, what
stands and what is open on it. It reports state, never an order — the
judgment is yours.

## What you are making

Four things look alike here and are not. Decide which one you mean
before you write it, because the workspace accepts all four under the
same `GLOSS` statement and reads each differently afterwards:

- **A metric** — a QUERY aspect whose grounding serves a row-grain
  relation with a `value` column and a date column. The cube turns it
  into a series at its judged cadence, the docket charts it, the bands
  walk watches it. It is a series only when the date column is one the
  machine can trace to a table column that `temporal()` has profiled:
  the cadence and the window come from that verdict, and a date
  computed inside the SQL has no verdict behind it. Serve the table's
  own date column; the cube buckets it at every grain. **A level with
  an as-of definition is a stock metric, not a fact**: payables
  outstanding, inventory on hand, headcount — anything you could state
  as of any date. Collapse the events to the frame's grain first — a
  stock frame serves **one row per entity per period** — then run the
  level over the collapse; a GROUP BY keeps the date column's verdict,
  because group keys trace. Mark `"behavior": "stock"` (a window sum
  gives the verb no descent, and an unmarked stock sums as a flow) and
  declare the grain — the served columns that identify a row; the cube
  validates the frame against it and refuses one that breaks it:

  ```glossql
  GLOSS payables_outstanding ON fin AS $${
    "sql": "WITH daily AS (SELECT entry_date, sum(amount) AS delta FROM journal_lines GROUP BY entry_date) SELECT entry_date AS date, sum(delta) OVER (ORDER BY entry_date) AS value FROM daily",
    "behavior": "stock",
    "grain": ["date"]
  }$$;
  ```

  A date spine you generate has no verdict behind it and abstains.
- **A current fact** — a value with no as-of definition: a balance the
  source hands over already summed, a count from a snapshot table.
  Ground it as a QUERY aspect that serves `value` and no date, with
  `"x-kind": "fact"` in the aspect's blob so `metric_surfaces` says
  what it is. `read.<name>()` serves it, `fact_values()` serves every
  fact's number in one read, and the docket shows it as a value tile
  and in the list beside its name. The cube abstains on it — "no
  judged time column" — and that is the right answer, not a defect to
  work around. A fact given a date becomes a one-point series: a fact
  in costume, and a chart of nothing.
- **A derived relation** — governed SQL other groundings build on: a
  snapshot boundary, a cleaned join, a scoped subset. Ground it as a
  QUERY aspect with `"x-kind": "relation"`; every other grounding
  composes `FROM read.<name>()`, so a ruled change propagates through
  everything built on it. It serves whatever columns it serves, needs
  no `value`, and the cube abstains on it by design. There is no
  `CREATE VIEW` here; this is the view.
- **A validation** — an authored expectation (a FACT gloss) with a
  function voice that measures it and a detector that bands the two;
  `ATTEST()` is where it shows. Never a metric.

"Shows up in the app" is not the goal. The right kind is; the app
follows.

**The write answers — read it.** A `GLOSS` on a QUERY aspect returns
the metric's fact row, the `metric_axes()` shape at the pin the write
moved to: `applicable` and `reason` (does the SQL plan; is a served
date column judged), `behavior` and `behavior_basis` (`ratio`,
`marked`, `glossed`, `evidence`, or `default` — summed as a flow
because nothing said otherwise), `grain` (the declared row identity
as served; empty is undeclared), `dims` (the axes admitted), and `unadmitted` with
`unadmitted_why` (every served column the cube will not slice on, and
the act that admits it). For a metric, what the workspace accepts and
reads wrong later — a ratio summed, a stock summed, a series nobody
can slice — shows there first: read the row, run what it names,
re-record. For a fact or a relation the row abstains, and you move on.

## The pages

| reference | open it |
|---|---|
| `references/land.md` | before the first `PROBE`: the topic, the cohort, landing what the topic needs and nothing more |
| `references/structure.md` | once tables stand: what each table is, the join structure, the slice axes |
| `references/vocabulary.md` | before glossing a column: role first, behavior by evidence, unit |
| `references/ground.md` | before writing a grounding: the two registries, the row-grain shape, ratios and stocks, keys on assumptions, the rival |
| `references/read-sql.md` | before any read over a metric: flows, stocks and ratios, and what this engine has that postgres lacks |
| `references/validate.md` | when a number needs a check that says why it holds, and at the close — a reconciliation run by hand becomes a standing check |
| `references/cube.md` | when a metric's axes, resolution or window are not what you expected, and after every ruling |
| `references/doors.md` | for a what-if, a which-rows question, a bespoke function or an app |
| `references/close.md` | before the read-back: the three lists and the question round |

Each is served beside this page as `skill://glossql-metrics/<reference>`.
The measurements that settle a statistic before anyone is asked, the
brief, and the reads are the `glossql` skill's.
