---
name: glossql-metrics
description: Take a glossql workspace from raw exports to a workspace someone can use — the seven goals the door's next: line tracks: structure, metrics, slices, bands, checks, a page, rulings folded in; done when every goal is done or blocked. Use for any substantive work in a workspace: onboarding a source, declaring relationships, grounding a metric, authoring a check or a page.
---

# From files to numbers someone trusts

The deliverable is a workspace someone can use, and it has seven
goals: the join structure, the metrics the business trusts, the axes
they slice on, the bands that watch them, the checks that say why a
number holds, a page to look at them, and every ruling the human made
folded in. The `glossql` skill teaches the language and the reads;
this one teaches the judgment inside each goal — what to decide, what
to measure, what to ask.

**The work is done when every goal is done or blocked.** The door
says where you stand on each after every call, on the `next:` line:

```text
next: structure: done · metrics → ground churn (next://ops/metrics) · slices → re-record churn serving one of region, channel (next://ops/slices) · bands → blocked: no applicable metric stands · checks → run metric_bands (next://ops/checks) · app → write the first page over 3 metrics (next://ops/app) · rulings: done
```

A goal that is neither done nor blocked is yours to move. Read its
link — `next://<dataset>/<goal>` as a resource, or `SELECT * FROM
next(surface => '<goal>')` through the tool — and send the statement
it hands you, edited to your judgment. A metric list is one goal of
seven, not the deliverable: the metrics the human names are the ones
to ground first, and the other six goals stand whether or not anyone
named them. The human's ask sets the order and can close a goal ("no
page this round"); nothing else does. Beyond that there is no fixed
order, and the line is never one.

| goal | done when | the judgment |
|---|---|---|
| `structure` | the edges are declared and the coherence check ran | `references/structure.md` |
| `metrics` | every metric the dataset claims is grounded | `references/ground.md`, and below |
| `slices` | every applicable metric admits an axis, or its grounding names its own (`"axes": []` for none) | `references/cube.md` |
| `bands` | the walk ran and no band is red, or the red carries your question or a ruling | `references/cube.md` |
| `checks` | a check of your own stands, every check ran, none is red | `references/validate.md` |
| `app` | a page stands over the grounded metrics | the `glossql-apps` skill |
| `rulings` | every ruling is folded in and nothing is owed | `skill://glossql/references/rulings.md` |

`workspace_next` is the map behind the line — every surface, what
stands, what is open — when you want the counts.

## What you are making

Four things look alike here and are not. Decide which one you mean
before you write it: the workspace accepts all four under the same
`GLOSS` statement and reads each differently afterwards:

- **A metric** — a QUERY aspect whose grounding serves a row-grain
  relation with a `value` column and a date column. The cube turns it
  into a series at its judged cadence, the docket charts it, the bands
  walk watches it once it runs (`next:` says when, `bands`). It is a
  series only when the machine can trace the
  date column to a table column that `temporal()` has profiled:
  the cadence and the window come from that verdict, and a date
  computed inside the SQL has no verdict behind it. Serve the table's
  own date column; the cube buckets it at every grain. **A level with
  an as-of definition is a stock metric, not a fact**: payables
  outstanding, inventory on hand, headcount — anything you could state
  as of any date. Collapse the events to the frame's grain first — a
  stock frame serves **one row per entity per period** — then run the
  level over the collapse; a GROUP BY keeps the date column's verdict,
  because group keys trace. Mark `"behavior": "stock"` (a window sum
  blocks the verb's trace, and an unmarked stock sums as a flow) and
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
  An interval table — a row per stint with a start and an end date —
  is the same stock with two event columns, `+1` at the start and
  `-1` at the end, unioned into one served date; the axis traces when
  both columns are judged, at the coarser of their cadences:

  ```glossql
  GLOSS headcount ON hr AS $${
    "sql": "WITH events AS (SELECT from_date AS date, 1 AS delta FROM stints UNION ALL SELECT to_date AS date, -1 AS delta FROM stints WHERE to_date IS NOT NULL), daily AS (SELECT date, sum(delta) AS delta FROM events GROUP BY date) SELECT date, CAST(sum(delta) OVER (ORDER BY date) AS DOUBLE) AS value FROM daily",
    "behavior": "stock",
    "grain": ["date"]
  }$$;
  ```
- **A current fact** — a value with no as-of definition: a balance the
  source hands over already summed, a count from a snapshot table.
  Ground it as a QUERY aspect that serves `value` and no date, with
  `"x-kind": "fact"` in the aspect's blob — the declaration is what
  makes it a fact. `read.<name>()` serves it, `fact_values()` serves
  every declared fact's number in one read, and the docket shows it as
  a value tile and in the list beside its name. The cube abstains on
  it — "no judged time column" — and that is the right answer, not a
  defect to work around. A fact given a date becomes a one-point
  series — a chart of nothing. A metric that serves no date and
  declares no kind is neither: nothing charts it and nothing shows its
  number, and the list carries the cube's reason.
- **A derived relation** — governed SQL other groundings build on: a
  snapshot boundary, a cleaned join, a scoped subset. Ground it as a
  QUERY aspect with `"x-kind": "relation"`; every other grounding
  composes `FROM read.<name>()`, so a ruled change propagates through
  everything built on it. It serves any columns, needs no `value`,
  and the cube abstains on it by design. There is no
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
as served; empty is undeclared), `dims` (the axes admitted), `unadmitted` with
`unadmitted_why` (every served column the cube will not slice on, and
the act that admits it), and `wanted` with `wanted_over` (the
measurements the row reads and nobody made — the function and the
column to run it over; `owed` lists them as never measured until they
land). For a metric, what the workspace accepts and
reads wrong later — a ratio summed, a stock summed, a series nobody
can slice — shows there first, and the `next:` line under it names
the act that answers it. For a fact or a relation the row abstains,
and you move on.
For a grounding you stopped (`stopped` in place of `sql`,
`references/ground.md`) the row abstains with your own text as the
reason.

## The pages

| reference | open it |
|---|---|
| `references/land.md` | before the first `PROBE`: the topic, the cohort, landing what the topic needs and nothing more, and what to do with the dirt you find |
| `references/structure.md` | once tables stand: what each table is, the join structure, the slice axes |
| `references/vocabulary.md` | before glossing a column: role first, behavior by evidence, unit |
| `references/ground.md` | before writing a grounding: the two registries, the row-grain shape, ratios and stocks, keys on assumptions, the rival, what a basis must be, serve or stop |
| `references/read-sql.md` | before any read over a metric: flows, stocks and ratios, what this engine has that postgres lacks, and the three refusals that cost most calls |
| `references/validate.md` | when a number needs a check that says why it holds, and at the close — a reconciliation run by hand becomes a standing check |
| `references/cube.md` | when a metric's axes, resolution or window are not what you expected, and after every ruling |
| `references/doors.md` | for a what-if, a which-rows question, a bespoke function or an app |
| `references/close.md` | before the read-back: the three lists and the question round |

Each is served beside this page as `skill://glossql-metrics/<reference>`.
The measurements that settle a statistic before anyone is asked, the
brief, and the reads are the `glossql` skill's.
