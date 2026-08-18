---
name: glossql-metrics
description: Take a glossql workspace from raw exports to metrics someone can trust — land what the topic needs, judge the structure, gloss the vocabulary, ground the cohort, stand up validations, and close with the question round. Use for any substantive work in a workspace: onboarding a source, declaring relationships, grounding a metric, authoring a check or a surface.
---

# From files to numbers someone trusts

The deliverable is metrics the business trusts and the validations
that say why. The `glossql` skill teaches the language and the reads;
this one is judgment — what to decide, what to measure, what to ask.

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
The sections below are the craft for each surface, not stages to march
through. Read the one you need.

## Agree the topic before anything lands

A dataset has a topic — on-time delivery, capacity, cost control —
and the topic is what makes every later choice decidable:
which tables to land, which metrics to propose, which questions
matter. Propose one in prose from what you can see, let the user shape
it, then declare it:

```glossql
DECLARE DATASET ops SET (purpose: 'service delivery — what gets done, how fast, and where it stalls');
```

**Then propose the metric cohort** — what the topic implies, including
the heavy ones (an end-to-end cycle time, real utilization), not just
what looks easy to compute. The user prunes and extends in prose. That
conversation is where scope questions surface while they are cheap:
"cycle time from which timestamp?" costs one sentence now and a wrong
dashboard later. Aim high deliberately — **a cohort metric the data
cannot ground is a finding, not a failure.** Name what is missing and
which tables would close it; surfacing that gap is the product
working.

This is conversation, not a form. **Prose shapes the work; forms rule
the record.** Anything deciding what the work *is* — the topic, the
cohort, whether to widen the import — is chat: present the facts,
propose, interpret the answer. The question round carries only
standing assumptions to confirm or correct, and it fails as a
substitute for conversation because there is nothing standing yet.

## Land what the topic needs — this is not ETL

Probe and recipe are the filter. A dataset is a curated working set
for its topic, never a mirror of the export: land the tables the
cohort needs, take only the columns the recipe's SELECT list earns,
filter wide tables in the recipe's WHERE. Leaving something out costs
one later `DECLARE RECIPE`; landing everything costs attention on
every read after — more slots to gloss, more owed claims, more noise
between you and the questions that matter, until the deep scope
questions drown in a hundred-column long tail. Width also costs
compute: the structure searches scale with a table's column pairs, so
a wide table landed whole is the one thing that makes them slow.

**Read the source's conventions before probing.** Source-grain slots
serve in every dataset, so what an earlier onboarding learned about
this system — placeholder dates, format warts, key spellings — is
already readable, and what you learn goes back the same way:

```glossql
GLOSS conventions ON erp_export AS $${
  "placeholder_date": "1900-01-01 stands for unset",
  "timestamp_format": "%b %e %Y %I:%M%p, month names mixed-language"
}$$;
```

Only what the *next* export from that system will also carry belongs
at source grain; dataset-local evidence stays in dataset glosses.

**Rehearse the schema with `LIMIT 0`, per file, before authoring any
recipe.** A zero-row probe still carries every `(name, type)`, which
is its whole point. Row probes cannot replace it — probe rows omit
null fields, so a column that is null in your sample is invisible
there, and a missed join key carries a missed relationship with it.

```glossql
PROBE erp_export AS $$SELECT order_id,
       try_cast(amount AS DOUBLE) AS amount,
       try_to_date(order_date, '%d.%m.%Y') AS order_date
FROM read_parquet('orders/*.parquet') LIMIT 0$$;
```

**Name the columns — never `SELECT *`.** A star recipe survives a
schema change in the source and fails later, somewhere downstream where
nothing points back at the source; a named SELECT list fails at the
re-import, which is where the drift is and where you can fix it.

**Typing is authored.** The recipe carries the casts and the column
choices; there is no typing machinery behind it. A failed cast lands
NULL — a kept row with a NULL cell, not a dropped row.

```glossql
DECLARE RECIPE orders ON ops FROM erp_export AS $$
  SELECT order_id,
         try_cast(amount AS DOUBLE) AS amount,
         try_to_date(order_date, '%d.%m.%Y') AS order_date
  FROM read_parquet('orders/*.parquet')$$;
```

**One date column may carry several conventions.** `try_to_date` and
`try_to_timestamp` take as many formats as you name and take the first
that parses, so a mixed column is one call rather than a coalesce
ladder over three copies of the value:

```glossql
PROBE erp_export AS $$SELECT
  try_to_date(paid, '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%b-%y') AS paid
FROM read_csv('payments.csv') LIMIT 5$$;
```

Order is your claim about the source and it decides the ambiguous
rows: `02/03/2025` is March 2nd under `%d/%m/%Y` and February 3rd under
`%m/%d/%Y`, and whichever you name first wins. Name the unambiguous
formats first. Where two readings both parse and the count matters,
measure it (`substr` the parts and count which are impossible under
each) and disclose the residual with a key — say how many rows no
evidence could decide, rather than picking quietly.

The outcome carries the **cast account** at the decision moment: for
every `try_*`, how many cells the cast nulled and the top such values.
Those tokens came from the data, not a list — judge them. A repeated
`\N` or `n/a` is a null marker: amend the recipe (`NULLIF` before the
cast) and re-declare, which supersedes and re-lands. A scattered long
tail may be genuinely bad data worth a `meaning` gloss. A re-landing
keeps the glosses (their snapshot ids show their age) — re-run the
measurements for columns the new recipe changed.

For a relational source, probe and recipe SQL run **at the source** in
its own dialect, and the wire decides what can land. SQLite has no
date type: `CAST(date(x) AS DATE)` silently lands `2010` for
`'2010-12-27'` because DATE takes NUMERIC affinity — measured, not
theorized. Land it as text and cast at read time, and put that gap in
the column's `meaning` so nobody rediscovers it. Read `DESCRIBE
<table>` the moment a table lands: a numeric that landed as text shows
up there instead of three reads later.

## Say what each table is

Before the columns. Every correct aggregate downstream depends on this
verdict, and it is judged from the data, never from the name.

- **value** — what one row is, in business words.
- **role** — `fact` (events at volume, carrying numbers) or
  `dimension` (descriptive, referenced by others), read from the
  evidence: measures, an event date, row counts, who references whom.
- **grain** — the columns identifying one row. **Verify, never
  assert**: `COUNT(*)` against `COUNT(DISTINCT (col, …))` must agree.
  A composite grain gets the composite; a table with no key gets none,
  said plainly. Watch for document-header values repeated onto every
  line — summing them at row grain multiplies by line count.
- **time_axis** — the column recording when the row's event happened.
  Attribute dates (due_date, hire_date) are not an axis; one at most.

```glossql
GLOSS entity ON work_orders AS $${"value": "one site visit on a work order", "role": "fact",
  "grain": ["order_id", "visit_no"], "time_axis": "completed_at"}$$;
```

## Judge the join structure

`detect_relationships` proposes at high recall — false positives
included, you are the precision. Per candidate, before declaring:

- **Anti-join both directions and *read* what doesn't resolve.** An
  orphan count is a question, not a verdict: orphans that are exactly
  a business population (the cancelled orders, the pre-migration
  records) confirm the edge; random misses argue against it.
- **Distrust coincidence.** Two unique integer columns overlap
  perfectly without meaning it — parallel row-number sequences are the
  classic false positive. Names, values and business objects must all
  agree.
- **Judge a composite on all its legs.** Anti-join anchor *and* scope
  together; the anchor alone fans out and over-counts, which is what
  the composite exists to collapse. Declare it as a tuple, never the
  anchor leg alone.
- **Ground the verdict, not the story.** Why the data looks this way
  is a hypothesis — verify it or label it. A correct rejection with a
  wrong causal story misleads everyone reading the grounds later.

```glossql
DECLARE RELATIONSHIP work_orders.site_id -> sites.id;
DECLARE RELATIONSHIP visits.(region_id, site_code) -> sites.(region_id, site_code);
GLOSS meaning ON work_orders.site_id -> sites.id AS
  $${"value": "each order serves one site; the orphans are the cancelled orders, never dispatched"}$$;
```

Rejected candidates stay in the measurement — visible and undeclared
is the record that they were seen and judged. Once edges are declared,
`relationship_coherence` watches them: orphan rate (exact, and it
catches shapes no column statistic can, including a single repeated
invented key) and the temporal read — a child event dated before its
parent record *exists* is the trace a wrong pairing leaves, while a
child event before a *deadline* is ordinary. Re-run after new batches.

## Gloss the columns — role first

Read the measurements before speaking. Relevance is conditional: a
column owes `behavior` and `unit` only once its `role` says `measure`,
and `dimension` only on `role = 'dimension'` — so gloss role first and
the rest of the backlog derives from it.

- **meaning** — one sentence, specific to the business, saying what
  the column contains and how it is used; `term` is the name a report
  would print. Never state summability here — that verdict has one
  home.
- **role** — `key` · `measure` · `dimension` · `timestamp` ·
  `attribute`, judged from this table alone. Never call a column a
  foreign key here; references are `DECLARE RELATIONSHIP`.
- **behavior** — measures only. `stock` is a carried point-in-time
  level that must not be summed across periods; `flow` accumulates. A
  column's own trajectory cannot decide this — a trending flow and a
  mean-reverting stock look alike — so run `behavior_evidence`, which
  reconciles the column against period movements over *declared*
  edges. Each anchor is served raw and year-scoped: a cumulative that
  resets abstains at raw grain and reconciles as a stock on the year
  anchor; read the pair together. Names lie either way — a column
  called "total" can carry a per-period movement.
- **unit** — where a magnitude has one; `source_column` names the
  column carrying it when it rides beside the value.

**When `behavior_evidence` starves** — every anchor abstains, no
entity persists across periods — climb the ladder: land the missing
dimension (a fact whose counterparty has no table starves only for
lack of a declared edge; `SELECT DISTINCT site_id FROM …` is a
legitimate recipe); then your own data test, cited as the basis; and last, on an
installation where a whole family of columns needs it, author a
workspace-scoped function that decides behavior the way *this* dataset
demands. That function is the installation's recorded thinking —
versioned, re-runnable, honest about its method in a way a one-off
judgment never is. Unwilling to climb? Don't gloss: absence shows as
an honest `unassessed` row, a guess does not.

"Does not apply" *within* relevance is still a judgment: a ratio is a
measure with no stock/flow nature, and that lands as
`{"value": "none", "grounds": "…"}`, never as a permanent unassessed
row.

## Score the slice axes

`dimension_relevance` scores `coverage × evenness` — zero free
parameters, one scale for every axis. The number answers "is this axis
usable, how much does it resolve"; **interest is yours**. A
near-uniform sequence column scores high and is still `none`.
Abstentions are gates, not defects: near-keys, null-dominated columns,
constants.

`detect_hierarchies` screens within-table dependencies at high recall.
Judge each:

- **λ < 0.5 is the vacuous-skew signature.** A ≥98%-dominant dependent
  passes the screen vacuously; the floor kills those in bulk with no
  truth lost — treat it as binding.
- **A perfect 1:1 is a relabeling or a coincidence, and only meaning
  separates them.** A code↔label bijection collapses to one axis; an
  entity key that happens to align with a timestamp must not.
- **Same-family role columns stay apart**, however cleanly they align
  — an origin and a destination, a bill-to and a pay-to. Merging them
  corrupts every aggregation that crosses them.

Record a surviving nest as a same-table relationship, finer → coarser.
Then the judged join: **run the grain check before trusting any
join** — `COUNT(*)` before and after must be equal, exactly, or the
join is not grain-preserving and multiplies every downstream
aggregate. Check each join alone in a one-hop star.

## Ground the cohort

One QUERY aspect per concept, on the dataset. The aspect blob is thin
— declarations have no supersession, so anything the company might
revise (meaning, unit, owner, source) belongs in the `definitions`
gloss where a correction supersedes with actor and timestamp. A field
lives in exactly one place, never both.

```glossql
DECLARE ASPECT throughput WITH $${"title": "Throughput", "x-kind": "measure"}$$ AS QUERY ON DATASET;
```

**Two registries ship with the kit — gloss into them, never redeclare.**
Both are FACT on the dataset, both keyed by the concept's own name.

`formulas` is a derived metric's **definition**: a window-generic
expression over sibling concepts, with the window `w` as its one free
variable. `[w]` reads the window as a flow, `[end of w]` as a stock,
`[w-1]` is the previous one. It covers every window because it names
none, and it is the ruled definition — the recorded SQL is one
evaluation of it, not a replacement for it.

```glossql
GLOSS formulas ON ops AS $${"formulas": {
  "backlog_days": "backlog[end of w] / throughput[w] * days[w]",
  "net_completions": "completions[w] - reopens[w]",
  "throughput_growth": "(throughput[w] - throughput[w-1]) / throughput[w-1]"
}}$$;
```

Operands name sibling concepts, plus window-derived constants like
`days[w]`. Nothing validates them — a misspelled sibling is silent, so
read your own operand names back against `SELECT name FROM aspects
WHERE kind = 'query'`. A base concept that grounds as an extract has no
formula and needs no entry.

`definitions` is the **handbook content** — what the company revises:
meaning, unit, owner, source. It lives here and not in the aspect blob
because declarations have no supersession: whatever sits in a `WITH`
blob cannot be corrected, contested or outranked once anything is
glossed on the aspect. The blob keeps only the `title` display label
and the `x-kind` tooling flag. A field lives in exactly one place,
never both — a unit written in both copies goes stale in one.

```glossql
GLOSS definitions ON ops AS $${"definitions": {
  "throughput": {"meaning": "work completed and accepted; counted at completion date",
                 "unit": "hours", "owner": "Operations", "source": "KPI handbook v3 §2"},
  "backlog_days": {"meaning": "open work expressed in days of current throughput",
                   "unit": "days", "owner": "Operations", "source": "KPI handbook v3 §2"}
}}$$;
```

Each registry is a single gloss, so a write replaces the whole map.
Read what stands before adding to it — a second concept glossed blind
drops the first one's entry:

```sql
SELECT aspect, body FROM glossary
WHERE aspect IN ('formulas', 'definitions') AND actor_kind = 'agent'
ORDER BY written_at DESC
```

**A grounding carries no grain** — no GROUP BY, no window. It is the
semantic core: scoping, signs, grain-preserving joins composed inline,
served as a row-grain relation with the time axis and the judged
dimensions as columns.

```glossql
GLOSS throughput ON ops AS $${
  "sql": "-- completed work: hours per closed order, at completion date, with its judged axes\nSELECT w.completed_at AS date, w.duration_min / 60.0 AS value, w.region, s.service_line FROM work_orders w JOIN sites s ON w.site_id = s.id WHERE w.status = 'closed'",
  "assumptions": [
    {"dimension": "scope", "key": "closed-only", "assumption": "closed orders only; cancelled and reopened excluded", "basis": "status values + judgment", "confidence": 0.7},
    {"dimension": "behavior", "key": "throughput-is-a-flow", "assumption": "a flow: sums valid over any partition", "basis": "behavior_evidence on work_orders.duration_min", "confidence": 1.0}
  ]
}$$;
```

**Say the mechanics inside the SQL as comments**; the assumptions
array carries judgment. A comment inside the query cannot drift from
the query the way a separate description can.

**A composed ratio serves only the axes its composition carries.** The
cube slices on the dimension columns an extract *serves*, so a ratio
that groups to `(date, value)` can never be sliced — a cohort grounded
entirely that way reports "no axes admitted" on every metric,
which is the headline numbers being the only ones nobody can cut.

A ratio is never drilled from its output rows. Drilling backlog days
by region means **re-scoping its components per the `formulas` gloss**
— each operand evaluated at the new scope, then the formula applied
per member. The recorded SQL below is that recomposition written down
for one choice of axis; the formula is what it was written down from:

```glossql
GLOSS backlog_days ON ops AS $${"sql": "-- backlog days by region as well as in total.\nWITH bl AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS bal FROM read.backlog() GROUP BY date_trunc('month', date), region), th AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS th FROM read.throughput() GROUP BY date_trunc('month', date), region) SELECT CAST(bl.m AS DATE) AS date, bl.bal / nullif(th.th, 0) * 30.0 AS value, bl.region FROM bl JOIN th ON bl.m = th.m AND bl.region = th.region"}$$;
```

Two things this is not: it is not a roll-up (each member's ratio is
computed from its own numerator and denominator, never averaged), and
it is not free — an axis you carry must exist on both sides, so pick
the axes the question actually needs rather than every one available.

**A ratio must serve `num` and `den` beside `value`.** They are how the
cube and the bands walk total it — `sum(num)/sum(den)` at every grain,
which is right for the headline and for every member. Without them a
ratio takes the flow verb and is **summed** — a dozen member ratios
added into one absurd headline, an order of magnitude off its true
value. Nothing infers this from the SQL — serve the columns.

```glossql
GLOSS backlog_days ON ops AS $${"sql": "WITH bl AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS bal FROM read.backlog() GROUP BY 1, 2), th AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS th FROM read.throughput() GROUP BY 1, 2) SELECT CAST(bl.m AS DATE) AS date, bl.bal / nullif(th.th, 0) * date_part('day', bl.m + INTERVAL '1' MONTH - INTERVAL '1' DAY) AS value, bl.bal AS num, th.th * (1.0 / date_part('day', bl.m + INTERVAL '1' MONTH - INTERVAL '1' DAY)) AS den, bl.region FROM bl JOIN th ON th.m = bl.m AND th.region = bl.region"}$$;
```

`value` stays each row's own ratio — that is what `read.backlog_days()`
serves and what a drill shows. `num` and `den` are the same division's
halves, scaled so their quotient is the metric in its own unit: here
the day factor rides the denominator, so `sum(num)/sum(den)` is days.

**`behavior`, `sign` and `grain` assumptions carry 1.0, always.** The
round never serves them to a human — statistics are your work — so a
measurable assumption below 1.0 is a question nobody will ever be
asked. Settle it before recording. Below-1.0 confidence is for
judgment dimensions only: definition, scope, convention.

Mark a stock with `"behavior": "stock"` as a top-level key in the
body. The library's readers follow the marker; an unmarked grounding
reads as a flow and its walk sums levels and lies.

**After grounding, run `detect_grounding_collisions`.** Two concepts
grounding to the same extract make every ratio between them compute
1.0, silently. A reported pair is either a deliberate synonym — say so
— or a definition error.

**Name the strongest rival runnably.** Definitional risk is invisible
from inside your own judgment; it shows only against an alternative.
An assumption may carry one:

```json
{"dimension": "definition", "key": "cycle-time-basis",
 "assumption": "cycle_time runs from dispatch to completion",
 "alternative": "from order creation to completion",
 "alternative_sql": "SELECT w.completed_at AS date, ... FROM ...",
 "basis": "operations convention", "confidence": 0.7}
```

The cube runs the rival monthly and the docket draws both lines, so
the gap is a chart the human reads before ruling instead of an
argument from prose. Start where the families diverge hardest.

## Read at the reader's grain

Grain is the reader's: the app asks by month, another reader by day,
the same definitions answer both. Evaluate through `read.<aspect>()` —
the current grounding as an ordinary relation, human slot outranking
agent, so a human answer is what runs.

- **Flows sum** over any partition.
- **Stocks take the last period per window** —
  `last_value(value ORDER BY ts)`, the aggregate form, ORDER BY
  mandatory. A running sum of a stock is arithmetic nonsense; a
  period-over-period delta is legal, it is the derived flow. ROLLUP
  across time is illegal for stocks.
- **Ratios don't roll up**: compose per the formula at the window
  asked — numerator and denominator each at the new scope, never an
  average of finer ratios. Never regroup a ratio's output rows.
- **Gap-filling**: a flow coalesces a missing period to 0 — no
  transactions is zero flow. A stock never does — a missing level is
  unknown, not zero.
- **`lag` and ROWS frames count rows, not periods.** On an axis with a
  missing month, `lag(value, 12)` is quietly thirteen months ago.
  Densify the axis, or use a RANGE frame with an interval.

Percentiles and medians are level statistics — safe for both
behaviors, which makes p50/p95 the honest summary when behavior is
unglossed.

### This is columnar, not postgres

You know how to write windows; what you cannot know from memory is
which ones exist *here*. The engine is DataFusion at a pinned version,
and three of its strongest tools have no postgres equivalent, so
nobody reaches for them by reflex. The habit to break is the
postgres shape: a self-join per comparison, a subquery per rank.
`glossql`'s **what will bite** section lists what is absent; this is
what is present.

**One scan, many answers.** Declare the window once and reuse it;
`FILTER (WHERE …)` pivots a driver into a column without a self-join
(it is not WHERE — WHERE removes rows from every aggregate, FILTER
from one; an empty filter gives COUNT 0 but SUM/AVG NULL, so coalesce
before dividing). Window *inheritance* does not parse at this pin —
flat definitions only.

```sql
SELECT period, driver, value,
  value - lag(value) OVER w                     AS delta,
  sum(value) OVER (PARTITION BY period)         AS period_total,
  value / sum(value) OVER (PARTITION BY period) AS share,
  avg(value) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3,
  sum(value) FILTER (WHERE driver = 'EMEA') OVER (PARTITION BY period) AS emea
FROM series
WINDOW w AS (PARTITION BY driver ORDER BY period)
```

**`QUALIFY` filters on a window result** — top-N per period with no
subquery. It runs after windows, before ORDER BY and LIMIT. Give the
ORDER BY a full tiebreaker or `row_number` reshuffles on every
refresh. `ntile` splits row *count*, not value mass — a
top-decile-by-value question is cumulative share,
`sum(value) OVER (ORDER BY value DESC) / sum(value) OVER ()`.

```sql
SELECT period, site, value FROM completions
QUALIFY rank() OVER (PARTITION BY period ORDER BY value DESC, site) <= 10
```

**Densify before you lag.** `generate_series` works as a table
function in FROM (not in the SELECT list) and includes its upper
bound; `range` excludes it. The coalesce is the flow rule — a stock
keeps its NULLs.

```sql
SELECT months.m AS period, coalesce(b.value, 0) AS value
FROM generate_series(DATE '2025-01-01', DATE '2025-12-01', INTERVAL '1' MONTH) AS months(m)
LEFT JOIN monthly b ON b.period = months.m
```

Or make the frame value-based — **RANGE accepts intervals here**,
which is "the last three calendar months" regardless of holes:

```sql
SELECT period, avg(value) OVER (ORDER BY period
  RANGE BETWEEN INTERVAL '2' MONTH PRECEDING AND CURRENT ROW) AS smoothed
FROM monthly
```

Two frame defaults that bite: with an ORDER BY and no frame clause the
frame is `UNBOUNDED PRECEDING AND CURRENT ROW` **with peers**, so tied
ORDER BY values share a running total — order by the grain-unique
column. And window `last_value` under that default returns the current
row, not the partition's last; use the aggregate form from the stock
rule above and skip the trap. For year-over-year on a sparse axis the
self-join earns its place: `ON prev.period = cur.period - INTERVAL '1
year'` cannot be fooled by a hole.

**Never guess a GROUP BY — the judged reads hand you the axes.**
Partition on `dimension` glosses, primary first, ranked by
`dimension_relevance`: low *coverage* means a top-N over that axis
speaks for a minority, low *evenness* means ranks and ntiles
degenerate onto one dominant value. ROLLUP order comes from the
declared hierarchy, coarse to fine — `ROLLUP(region, country, city)`;
reversed, the subtotals are junk. Subtotal rows carry NULL in the
rolled-up columns and a real NULL driver looks identical, so branch on
`grouping(col)`, never on `IS NULL`. `GROUP BY ALL` is for probing;
a recorded grounding spells its GROUP BY, because ALL silently
re-groups when someone edits the select list.

**Time mechanics.** `date_trunc` down to the metric's grain and no
finer — truncating below grain fabricates resolution; `'week'` is ISO
Monday. `date_bin(stride, d, origin)` for everything calendar months
cannot say (fiscal periods, offset weeks) — the default origin is the
epoch and 1970-01-01 was a Thursday, so weekly bins without an origin
start on Thursday; the declared fiscal calendar goes in that argument.
`date_part('month', d)` folds years together — the seasonality idiom;
trends stay with `date_trunc`. `dow` is Sunday=0, `isodow` Monday=1.
Shifts are `± INTERVAL` arithmetic.

**Statistics that pay.** `percentile_cont(0.95) WITHIN GROUP (ORDER BY
v)` is exact, `approx_percentile_cont` scales (t-digest — never diff
approximate values across refreshes and call it a trend).
`regr_slope(value, date_part('epoch', period))` per driver is a
which-segments-are-declining detector in one aggregate: aggregate the
flow to its grain first, y comes first, and swapping the arguments
gives a wrong number rather than an error — slopes are per-second, so
scale before showing. `stddev(value) / nullif(avg(value), 0)` ranks
volatility; `approx_distinct` is active-count at scale; `corr` needs
period-aligned series, so densify or join first or the number is
noise; `array_agg(x ORDER BY period)` — unordered `array_agg` is
nondeterministic and must never feed a sparkline.

**Record what a read proves.** A composed evaluation you verified may
land as the metric's own QUERY gloss — durable executable knowledge,
served by `read.<aspect>()` from then on. Compose it `FROM
read.throughput()` where you can, so a re-ruled component propagates
through every metric built on it.

## Stand up validations

The authored expectation is a FACT gloss; the check is a function
voice on the same aspect; a detector bands across both slots;
`ATTEST` is the verdict surface.

```glossql
DECLARE ASPECT hours_reconcile WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"},
                 "breach_rate": {"type": "number"}}
}$$ AS FACT ON TABLE WHEN entity = 'work log line';
GLOSS hours_reconcile ON work_logs AS $${
  "outcome": "Logged minutes match the order's recorded duration, exactly.", "tolerance": 0.0}$$;
DECLARE WITNESS hours_w ON hours_reconcile BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

- **Scope the check with `WHEN`.** A check declared bare `ON TABLE`
  owes an unassessed row on every table in the workspace — a handful
  of unscoped checks fills the backlog with unfillable rows.
- **`breach_rate` is the violation share.** 0.0 is fully passing, and
  it is compared against `tolerance` upward. Reporting a pass rate
  under that key bands red.
- **The expectation is authored, never assumed zero.** A source with
  known dirt expects its own breach rate; a check reporting 0.0 there
  has overcleaned, itself a failure.
- **Promote confirmed reconciliations.** A `behavior_evidence`
  convention that reconciled at ~0 residual is a standing invariant —
  make it a check.

**The check half is a function, and you write it here** — the body
rides its declaration, so an expectation without a measuring voice is
a choice rather than a limit. `glossql-functions` has
the contract, the kernels and the abstention rule; `rate_tolerance` is
the detector that bands an authored expectation against a check voice:

```glossql
DECLARE FUNCTION hours_reconcile_check FOR ops AS $$
  SELECT 'measured: logged minutes against recorded durations' AS outcome,
         CASE WHEN count(*) = 0 THEN 0.0
              ELSE CAST(count(*) FILTER (WHERE abs(logged - recorded) > 0.5) AS DOUBLE) / count(*)
         END AS breach_rate
  FROM (SELECT order_id, sum(minutes) AS logged, max(duration_min) AS recorded
        FROM work_logs GROUP BY order_id)
$$ RETURNS hours_reconcile;
```

A voice speaks the aspect's own schema — `outcome` like any slot, the
measurement beside it. One schema, every speaker.

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

`metric_cube` is its sibling and the app's fuel: monthly totals,
slices along served dimension columns, and the disclosed rival series.
Run it whenever you run the walk — a grounding write or a landed
import stales both caches. A dimension the cube should slice must be a
served column of the extract.

Where every metric stands, in one read:

```sql
SELECT metric, title, unit, meaning, period, value, axes, formula
FROM metric_surfaces ORDER BY metric
```

And the cube's own numbers read back through `metric_series()` — one
row per metric, dimension, member and period. `dimension = ''` is the
metric's own total, `'alternative'` is the disclosed rival, anything
else is a served dimension; a wide dimension serves its top members
plus an `'other'` bucket, and the cube's fact row names which ones
were bucketed. This is what an app frame charts:

```sql
SELECT metric, period, value FROM metric_series()
WHERE dimension = '' ORDER BY metric, period
```

A workspace write does not blank the series — it serves the last
landed cube with `current = false` on every row, and the docket shows
the same numbers marked stale. The recompute is yours to pull, and
`workspace_next`'s `cube` surface shows `open = 1` while it is owed:

```sql
SELECT DISTINCT current FROM metric_series()
```

**Fold in every standing ruling before recomputing either.** Each
grounding write stales both caches, so one batch of fold-ins then one
recompute, never a recompute per ruling — run the cube LAST, after
your final write, or you hand the docket a stale flag on your way out.

## Asking what would happen — the scenario door

A what-if is declared and then read, never hand-edited SQL: the
declared form is versioned, witness-gated and reproducible. One FACT
aspect per scenario, exactly as one QUERY aspect is one metric.

```glossql
DECLARE ASPECT demand_surge WITH $${
  "title": "Orders +15% from Jan 2027",
  "x-kind": "scenario",
  "type": "object", "required": ["overrides"],
  "properties": {"overrides": {"type": "array", "items": {
    "type": "object", "required": ["column", "factor", "from", "basis"],
    "properties": {"column": {"type": "string"}, "factor": {"type": "number"},
                   "from": {"type": "string"}, "basis": {"type": "string"}}}}}
}$$ AS FACT ON DATASET;
```

Each override names a real column, a factor, a start month and its
**basis** — the same discipline the grounding assumptions carry. A
behavioral response no history ever saw is not guessed: declare it as
its own override and say so, or leave it out and let the read name it.

```glossql
GLOSS demand_surge ON ops AS $${
  "overrides": [
    {"column": "work_orders.order_count", "factor": 1.15, "from": "2027-01",
     "basis": "the declared lever"},
    {"column": "work_orders.duration_min", "factor": 1.05, "from": "2027-01",
     "basis": "assumed congestion response, hand-declared; not in any history"}
  ]
}$$;
```

`whatif.<scenario>()` then serves one relation over every concept the
replay reaches. Sweeps are `WHERE` clauses over it, never a special
form:

```sql
SELECT concept, month, replay, p05, p50, p95, basis
FROM whatif.demand_surge() WHERE concept = 'throughput' ORDER BY month
```

- **Read `basis` before the numbers.** A concept no declared path
  connects to the overridden columns comes back as a refusal row with
  its reason, not a silent guess — `detect_derivations` proposes the
  identities that would close the gap.
- `replay` is exact arithmetic at the declared factors; the bands are
  the model's. Both are served so neither hides behind the other.
- The server replays each grounding at a bracket of strengths around
  your factor, so the scenario's own point is always interpolation.
  Nothing about that grid is yours to write.

## Which rows — the sample door

When a signal fires and the question is *which rows*, author a sample
frame: a QUERY aspect with `x-kind: "sample"`, glossed with one SELECT
that holds known-good history and the suspects together, read through
`misfit.<frame>()` for a per-row score.

```sql
SELECT * FROM misfit.late_pairs() ORDER BY misfit DESC LIMIT 20
```

Pick the surface to match the suspicion: a relationship suspicion needs
the **join** in the frame, because a single table is structurally blind
to wrong pairings whose individual values are all legal. The more
known-good history the frame carries, the cleaner the ranking. Run it
on a signal, never as a routine sweep.

Both doors are on the affordance map (`SELECT * FROM workspace_next`)
as `scenarios` and `samples`, with `open` counting vocabulary that
stands without a body.

## Author what is missing

**A function** when a shipped measurement does not fit this dataset's
shape — and it is also the measuring half of a validation, which the
expectation gloss above owes. **`glossql-functions` teaches it**: the
declaration carries the body, so a check is writable over the door and
the shipped library reads back as worked examples
(`SELECT script FROM functions WHERE name = 'rate_tolerance'`). The
short version: a measurement's body is one SQL query the engine plans,
no `RETURNS` declares a detector (a script over slots), and a function
abstains (`applicable: false` with a reason) rather than throwing.

**An app** when someone needs to look at this — a standalone page at
`/app/<name>` whose URL is its whole state, so a filtered view is a
link somebody can send. **`glossql-apps` teaches it**: shape it with
the user in prose first, then write it as glosses (`app`, `app_page`,
`app_frame`, `app_spec`, one per part). Add an app beside the docket
rather than forking it. Frames are SQL and display logic is computed
there, never in a template; an app you author carries no write.

## Close with the question round

A definition is a choice between families the data cannot arbitrate.
Walk the ladder before asking: close it by measurement first, and if
it must keep holding, declare a witness so a standing check re-decides
it on every import instead of anyone re-asking. Only what no
measurement can arbitrate goes to the user — **a choice between
readings, never a statistic**. A question you ask that data could have
answered costs the user's attention twice: once now, and once more
when they learn to skim your questions.

The close carries three lists, kept apart:

1. **Definitional choices** — every grounding assumption whose basis
   is your judgment rather than a measurement or a ruling. The basis
   is the marker, never the number. One per definition, with its
   alternative named.
2. **Data findings** — a reconciliation gap, a column whose name lies
   about its content. Facts to explain, not choices to make.
3. **World-coverage wishes** — what resolves neither by choice nor by
   SQL, only by more world: an opening position, a prior-year extract,
   the policy document naming the convention. Name each as a specific
   ask and say which numbers shift when it arrives. The ask is a
   document, not a decision — keep it out of the questions.

And the read-back covers the whole agreed cohort, not just what
grounded: every metric that did not ground gets named with what would
close it. Size the review honestly — the load-bearing verdicts
(entity, behavior, unit, anything a wrong value silently corrupts) get
named one by one, while for a hundred `meaning` glosses exhaustive
review is theater: show the distribution and a spot-check sample, and
treat a failed spot-check as the batch's problem, not the row's.
