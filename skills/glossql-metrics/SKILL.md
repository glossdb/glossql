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
next WHERE surface = '<goal>'` through the tool — and send the statement
it hands you, edited to your judgment. A metric list is one goal of
seven, not the deliverable: the metrics the human names are the ones
to ground first, and the other six goals stand whether or not anyone
named them. The human's ask sets the order and can close a goal ("no
page this round"); nothing else does. Beyond that there is no fixed
order, and the line is never one.

| goal | done when | the judgment |
|---|---|---|
| `structure` | the edges are declared and the coherence check ran | `references/structure.md` |
| `metrics` | every metric the dataset claims is served by the cube, or stopped with the reason | `references/ground.md`, and below |
| `slices` | every applicable metric admits an axis, or its grounding names its own — `"axes": []` holds for a distinct count or a ratio, which no column slices whole | `references/cube.md` |
| `bands` | the walk ran and no band is red, or the red carries your question or a ruling | `references/cube.md` |
| `checks` | a check of your own stands, every check ran, none is red | `references/validate.md` |
| `app` | a page stands over the grounded metrics | the `glossql-apps` skill |
| `rulings` | every ruling is folded in and nothing is owed | `skill://glossql/references/rulings.md` |

**When every goal is done or blocked, offer one next step of each
kind, as a proposal.** The metrics done row's why names what the
record left unused: the tables no grounding reads, the judged columns
none serves. From those, say what one more metric could be, in a
sentence, with its grounds. From the served metrics the app's pages
do not show, say what one more page could be, the same way. One of
each, in prose. Write nothing until the human says so: the next
concept is theirs to name, and the proposal is there to help them
think, not to fill the workspace.

`workspace_next` is the map behind the line — every surface, what
stands, what is open — when you want the counts.

## What you are making

Four things look alike here and are not. Decide which one you mean
before you write it: the workspace accepts all four under the same
`GLOSS` statement and reads each differently afterwards. Whichever it
is, the serving grounding is a view of the dataset under the aspect's
name: `SELECT … FROM <name>` reads it, filters it and joins it as any
view, and `information_schema` lists it. `read.<name>()` names the
same view.

- **A metric** — a QUERY aspect whose grounding serves a row-grain
  relation with a `value` column and a date column. The cube turns it
  into a series at its judged cadence, the docket charts it, the
  bands walk watches it. It is a series only when the date column
  traces to a table column that `temporal()` has profiled: serve the
  table's own date column, and the cube buckets it at every grain; a
  date computed inside the SQL has no verdict behind it. **A level
  with an as-of definition is a stock, not a fact** — payables
  outstanding, inventory on hand, headcount, anything you could state
  as of any date: collapse the events to the frame's grain, one row
  per entity per period, mark `"behavior": "stock"` and declare the
  `grain`. `references/ground.md` carries both shapes written down,
  the interval table included.
- **A current fact** — a value with no as-of definition: a balance
  the source hands over already summed, a count from a snapshot
  table. Ground it as a QUERY aspect that serves `value` and no date,
  with `"x-kind": "fact"` in the aspect's blob — the declaration is
  what makes it a fact. `FROM <name>` serves it, `fact_values()`
  serves every declared fact's number, and the docket lists it under
  facts: one number, as of the newest landing of the tables it
  reads. The cube abstains on it, and that is the right answer. A fact
  given a date becomes a one-point series — a chart of nothing.
- **A derived relation** — governed SQL other groundings build on: a
  snapshot boundary, a cleaned join, a scoped subset. Ground it with
  `"x-kind": "relation"`; every other grounding composes `FROM
  <name>`, so a ruled change propagates. It serves any columns,
  needs no `value`, and the cube abstains on it by design. There is
  no `CREATE VIEW` here; a grounding is the view.
- **A validation** — an authored expectation (a FACT gloss) with a
  function voice that measures it and a detector that bands the two;
  `ATTEST()` is where it shows. Never a metric.

"Shows up in the app" is not the goal. The right kind is; the app
follows.

**The write answers — read it.** A `GLOSS` on a QUERY aspect returns
the metric's fact row, the `metric_axes()` shape at the pin the write
moved to: `applicable` and `reason`, `behavior` and `behavior_basis`,
`grain`, `dims` and `axes_basis`, `unadmitted` with `unadmitted_why`
and the act that admits each, `wanted` (what the row reads and
nobody measured; `owed` lists it until it lands), and at a re-record
`superseded_divergence` — what changed against the writing it
supersedes. A ratio summed, a stock summed, a series nobody can slice
shows there first, and the `next:` line under it names the act. For a
fact or a relation the row abstains, and you move on; for a grounding
you stopped (`stopped` in place of `sql`, `references/ground.md`) it
abstains with your own text.

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
