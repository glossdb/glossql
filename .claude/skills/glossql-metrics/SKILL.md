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

Nine surfaces, what each is extended through, what stands and what is
open on it. It reports state, never an order — the judgment is yours.
The sections below are the craft for each surface, not stages to march
through. Read the one you need.

## Agree the topic before anything lands

A dataset has a topic — working capital, sales performance, cost
control — and the topic is what makes every later choice decidable:
which tables to land, which metrics to propose, which questions
matter. Propose one in prose from what you can see, let the user shape
it, then declare it:

```glossql
DECLARE DATASET fin SET (purpose: 'working capital — where cash sits and how fast it moves');
```

**Then propose the metric cohort** — what the topic implies, including
the heavy ones (a cash conversion cycle, real margins), not just what
looks easy to compute. The user prunes and extends in prose. That
conversation is where scope questions surface while they are cheap:
"DSO over which receivables?" costs one sentence now and a wrong
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
between you and the questions that matter. The first live run measured
it: the deep scope questions drowned in a 109-column long tail.

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
there. The first validated run lost three columns that way, one of
them a join key, and the missed relationship rode the missed column.

```glossql
PROBE erp_export AS $$SELECT order_id,
       try_cast(amount AS DOUBLE) AS amount,
       try_to_date(order_date, '%d.%m.%Y') AS order_date
FROM read_parquet('orders/*.parquet') LIMIT 0$$;
```

**Typing is authored.** The recipe carries the casts and the column
choices; there is no typing machinery behind it. A failed cast lands
NULL — a kept row with a NULL cell, not a dropped row.

```glossql
DECLARE RECIPE orders ON fin FROM erp_export AS $$
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
each) and disclose the residual with a key — run 4 found 2,466 payment
dates of 14,928 that no evidence could decide, and said so rather than
picking quietly.

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
GLOSS entity ON orders AS $${"value": "sales order line", "role": "fact",
  "grain": ["order_id", "line_no"], "time_axis": "order_date"}$$;
```

## Judge the join structure

`detect_relationships` proposes at high recall — false positives
included, you are the precision. Per candidate, before declaring:

- **Anti-join both directions and *read* what doesn't resolve.** An
  orphan count is a question, not a verdict: orphans that are exactly
  a business population (the cancelled invoices, the pre-migration
  accounts) confirm the edge; random misses argue against it.
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
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
GLOSS meaning ON orders.customer_id -> customers.id AS
  $${"value": "each order belongs to one customer; 140 orphans are the cancelled orders, never posted"}$$;
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
  anchor; read the pair together. Names lie either way — a "trial
  balance" column can carry period turnover.
- **unit** — where a magnitude has one; `source_column` names the
  column carrying it when it rides beside the value.

**When `behavior_evidence` starves** — every anchor abstains, no
entity persists across periods — climb the ladder: land the missing
dimension (an AP side whose vendor has no table starves only for lack
of a declared edge; `SELECT DISTINCT vendor_id FROM …` is a legitimate
recipe); then your own data test, cited as the basis; and last, on an
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
  passes the screen vacuously. Measured: a λ floor killed 48 false
  positives with zero truth lost.
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
DECLARE ASPECT revenue WITH $${"title": "Revenue", "x-kind": "measure"}$$ AS QUERY ON DATASET;
```

**A grounding carries no grain** — no GROUP BY, no window. It is the
semantic core: scoping, signs, grain-preserving joins composed inline,
served as a row-grain relation with the time axis and the judged
dimensions as columns.

```glossql
GLOSS revenue ON fin AS $${
  "sql": "-- recognized revenue: credit minus debit on revenue-typed accounts\nSELECT e.date, l.credit - l.debit AS value, l.cost_center FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id JOIN chart_of_accounts a ON l.account_id = a.account_id WHERE a.account_type = 'revenue'",
  "assumptions": [
    {"dimension": "scope", "key": "revenue-accounts-only", "assumption": "revenue-typed accounts only, service lines included", "basis": "chart_of_accounts + judgment", "confidence": 0.7},
    {"dimension": "behavior", "key": "revenue-is-a-flow", "assumption": "a flow: sums valid over any partition", "basis": "behavior_evidence on journal_lines.credit", "confidence": 1.0}
  ]
}$$;
```

**Say the mechanics inside the SQL as comments**; the assumptions
array carries judgment. A comment inside the query cannot drift from
the query the way a separate description can.

**A composed ratio serves only the axes its composition carries.** The
cube slices on the dimension columns an extract *serves*, so a ratio
that groups to `(date, value)` can never be sliced — run 4 grounded six
composed metrics and every one of them reported "no axes admitted",
which is the headline numbers being the only ones nobody can cut. Carry
the axis through both halves and group by it, and the ratio recomposes
per member exactly as the reader's grain rule demands:

```glossql
GLOSS dso ON fin AS $${"sql": "-- DSO by customer segment as well as in total.\nWITH ar AS (SELECT date_trunc('month', date) AS m, segment, sum(value) AS bal FROM read.accounts_receivable() GROUP BY date_trunc('month', date), segment), rev AS (SELECT date_trunc('month', date) AS m, segment, sum(value) AS rev FROM read.revenue() GROUP BY date_trunc('month', date), segment) SELECT CAST(ar.m AS DATE) AS date, ar.bal / nullif(rev.rev, 0) * 30.0 AS value, ar.segment FROM ar JOIN rev ON ar.m = rev.m AND ar.segment = rev.segment"}$$;
```

Two things this is not: it is not a roll-up (each member's ratio is
computed from its own numerator and denominator, never averaged), and
it is not free — an axis you carry must exist on both sides, so pick
the axes the question actually needs rather than every one available.

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
{"dimension": "definition", "key": "gross-profit-basis",
 "assumption": "gross_profit = revenue - COGS",
 "alternative": "revenue - all expenses",
 "alternative_sql": "SELECT e.date, ... FROM ...",
 "basis": "textbook convention", "confidence": 0.7}
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

**Record what a read proves.** A composed evaluation you verified may
land as the metric's own QUERY gloss — durable executable knowledge,
served by `read.<aspect>()` from then on. Compose it `FROM
read.revenue()` where you can, so a re-ruled component propagates
through every metric built on it.

## Stand up validations

The authored expectation is a FACT gloss; the check is a function
voice on the same aspect; a detector bands across both slots;
`ATTEST` is the verdict surface.

```glossql
DECLARE ASPECT journal_balanced WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"},
                 "breach_rate": {"type": "number"}}
}$$ AS FACT ON TABLE WHEN entity = 'journal line';
GLOSS journal_balanced ON journal_lines AS $${
  "outcome": "Total debits equal total credits, exactly.", "tolerance": 0.0}$$;
DECLARE WITNESS journal_w ON journal_balanced BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

- **Scope the check with `WHEN`.** A check declared bare `ON TABLE`
  owes an unassessed row on every table — three checks on a 14-table
  workspace put 39 unfillable rows in the backlog.
- **`breach_rate` is the violation share.** 0.0 is fully passing, and
  it is compared against `tolerance` upward. Reporting a pass rate
  under that key bands red.
- **The expectation is authored, never assumed zero.** A source with
  known dirt expects its own breach rate; a check reporting 0.0 there
  has overcleaned, itself a failure.
- **Promote confirmed reconciliations.** A `behavior_evidence`
  convention that reconciled at ~0 residual is a standing invariant —
  make it a check.

The check half needs a `.rhai` file in the workspace directory. Over
the MCP door alone you cannot author it: say so in the read-back and
record the expectation gloss anyway, rather than shipping a
self-measured snapshot as if it were a standing check.

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
SELECT metric, title, period, value, axes, formula
FROM metric_surfaces ORDER BY metric
```

**Fold in every standing ruling before recomputing either.** Each
grounding write stales both caches, so one batch of fold-ins then one
recompute, never a recompute per ruling.

## When something breaks pattern

A scenario is declared, then read: a FACT aspect with
`x-kind: "scenario"`, overrides glossed, served through the `whatif.`
door. Never hand-edit SQL for a what-if — the declared form is
versioned, witness-gated and reproducible. Read `basis` before the
numbers; `replay` is exact arithmetic, the bands are the model's.

When a signal fires and the question is *which rows*, author a sample
frame — a QUERY aspect with `x-kind: "sample"`, one SELECT holding
history and suspects together — and read it through `misfit.`. Pick
the surface to match the suspicion: a relationship suspicion needs the
**join** in the frame, because a single table is structurally blind to
wrong pairings whose individual values are all legal. The more
known-good history the frame carries, the cleaner the ranking. Run it
on a signal, never as a routine sweep.

## Author what is missing

**A function** when a shipped measurement doesn't fit this dataset's
shape. A rhai script registered with a declaration; the aspect schema
is its one validated contract. `ACCEPTS` names the aspects arriving as
context and is also the invalidation edge — `imports` and
`relationships` may ride it as edges only. No `RETURNS` declares a
detector, which never sees table data. Abstain rather than throw:
`#{applicable: false}` when the subject doesn't fit,
`#{applicable: false, missing_aspects: [...]}` when a dependency is
absent — that one heals on its own when the dependency lands. Read the
closest script in `crates/scripts/functions/` before writing.

Three constants are in scope and the script's last expression is its
result: `subject` (`"table"` or `"table.column"`), `context` (one
entry per accepted aspect, `()` where that aspect has no value; a
detector gets `slots` and `threshold` instead), and `db` — the door
into the dataset. `db.query("sql")` returns a Table and
`db.query_all([sql, …])` answers a batch in order; the door overlaps
the batch below the seam, so a fan-out of small queries belongs in one
`query_all()`, never a sequential loop.

### Kernels

Zero-copy readers on query results — the compute-heavy halves of a
measurement live here, in Rust. A script nesting loops over rows or
pairs should be reaching for one instead.

Table: `num_rows()`, `columns()`, `col(name)`, `cell(name)` — the
first row's value as a string, `()` for NULL (the one-row aggregate
read).

Col: `dtype()` (a `LIMIT 0` query types a column without scanning it),
`count()`, `null_count()`, `distinct()`, `entropy()` — exact Shannon
entropy over the non-null distribution — `min()`, `max()`, `sum()`,
`mean()`, `stddev()`, `percentile(p)`, `mad()`, `top_k(k)`,
`len_stats()`, `match_rate(regex)`, `parse_rate(sql_type)`,
`value_at(i)`, `floats()`. Read numbers you will loop over with
`floats()`, never `value_at()` per cell — `value_at` renders display
strings and a hot loop through it is interpreter-bound. **A score
reads exact scalars, never `top_k` buckets**: a display cap must not
become a statistics cap.

Statistical: `key_vec()` on a Col — distinct values as sorted typed
keys, with `matched(other)` giving intersection by linear merge, so
containment is `a.matched(b) / a_distinct` and never a per-pair join ·
`pair_keys(c1, c2)` on a Table for composite domains ·
`reconcile(y_table, m_table, terms)`, the stock/flow discriminator
over two grouped results · `tabicl_bands(train_x, train_y, test_x,
alphas, actual)` — one fit and read, returning the corridor a new
value would have to land in and where the actual fell.

Two free functions handle stored text: `parse_json(s)` for a stored
body, and `canonical_sql(s)` for SQL as an identity — parse and
re-render, so whitespace and keyword case collapse while identifiers
survive.

**An app** when someone needs to look at this. Shape it with the user
in prose first — what decision the surface serves — then write it as
glosses: `app`, `app_page`, `app_frame`, `app_spec`, one gloss per
part, so a frame can be edited without rewriting the app. Frames are
SQL; display logic is computed in the frame, never in the template.
Apps carry one write and one only — the docket's ruling form, which
answers a question the workspace already derived. Anything an app you
author needs to change, change with a statement.

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
