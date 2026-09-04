# Grounding — read before writing a metric's SQL: the registries, the row-grain shape, ratios and stocks, the rival, serve or stop

## Ground the cohort

One QUERY aspect per concept, on the dataset. The aspect blob is thin
— declarations have no supersession, so anything the company might
revise belongs in the `definitions` gloss, where a correction
supersedes with actor and timestamp. A field lives in exactly one
place, never both.

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
meaning, unit, owner, source. Whatever sits in a `WITH` blob cannot
be corrected, contested or outranked once anything is glossed on the
aspect. The blob keeps only the `title` display label and the
`x-kind` tooling flag. A unit written in both copies goes stale in
one. `x-kind` lives in the `DECLARE ASPECT` blob and nowhere else —
the grounding body's schema refuses it.

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
SELECT g.aspect, g.body FROM glossary g
JOIN current_dataset d ON d.dataset = g.dataset
WHERE g.aspect IN ('formulas', 'definitions') AND g.actor_kind = 'agent'
ORDER BY g.written_at DESC
```

The join is required: `glossary` serves the whole workspace, and
reading another dataset's registry before rewriting this one drops
every entry this dataset holds.

**A flow grounding carries no grain** — no GROUP BY, no window. It is
the semantic core: scoping, signs, grain-preserving joins composed
inline, served as a row-grain relation with the time axis and the
judged dimensions as columns.

**A stock is the exception.** The running level already aggregates the
events, so the frame must collapse to its own grain — one row per
period, or one per entity and period with the entity served as a
column (partition the window by it). Served at event grain, every row
on a date carries the same correct level, and the cube — which must
sum same-period rows, because that is how a multi-account balance
totals — multiplies the level by the day's event count. The number is
exact on every row and wrong in every sum. Declare that grain in the
body — `"grain": ["date"]`, or `["date", "account_id"]` — and the
cube validates the frame against it instead of trusting it: a frame
that breaks its declared grain abstains, with the counts in the
reason. An interval table is a stock with two event columns, `+1` at
the start date and `-1` at the end date, unioned into one served
date; both columns must be judged for the axis to trace.

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

**A basis that names a computation is a claim to verify, and prose is
not the tie.** "Ties to GL 4* net" names a route you can run. An
assumption that carries it while the SQL computes something else
drifts silently: every review confirms the words, and no ruling on
the words repairs the numbers. Run the named route against the served
frame before recording the basis, and keep the claim machine-checked
after the session — disclose the route as `alternative_sql` so
divergence measures it on every read, or promote it to a standing
check (`references/validate.md`). A route that exists only in your
head is judgment: write that as the basis and hold the confidence
below 1.0.

**A composed ratio serves only the axes its composition carries.** The
cube slices on the dimension columns an extract *serves*, so a ratio
that groups to `(date, value)` can never be sliced — a cohort
grounded entirely that way reports "no axes admitted" on every
metric: headline numbers nobody can cut.

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
added into one wrong headline, an order of magnitude off its true
value. Nothing infers this from the SQL — serve the columns.

```glossql
GLOSS backlog_days ON ops AS $${"sql": "WITH bl AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS bal FROM read.backlog() GROUP BY 1, 2), th AS (SELECT date_trunc('month', date) AS m, region, sum(value) AS th FROM read.throughput() GROUP BY 1, 2) SELECT CAST(bl.m AS DATE) AS date, bl.bal / nullif(th.th, 0) * date_part('day', bl.m + INTERVAL '1' MONTH - INTERVAL '1' DAY) AS value, bl.bal AS num, th.th * (1.0 / date_part('day', bl.m + INTERVAL '1' MONTH - INTERVAL '1' DAY)) AS den, bl.region FROM bl JOIN th ON th.m = bl.m AND th.region = bl.region"}$$;
```

**A distinct count is the same class without halves.** The cube sums
same-period rows, and distinct customers per (doc type, sales org) do
not sum to distinct customers per month — a distinct count served with
dimension columns reports a wrong total, and nothing infers this from
the SQL. Serve it at the scope the metric is asked at, with no
dimension columns, and say so in a keyed assumption; per-member
distinct counts are separate metrics or a drill recomputed from the
entity rows.

`value` stays each row's own ratio — that is what `read.backlog_days()`
serves and what a drill shows. `num` and `den` are the same division's
halves, scaled so their quotient is the metric in its own unit: here
the day factor rides the denominator, so `sum(num)/sum(den)` is days.

**`behavior`, `sign` and `grain` assumptions carry 1.0, always.** The
round never serves them to a human — statistics are your work — so a
measurable assumption below 1.0 is a question nobody will ever be
asked. Settle it before recording. Below-1.0 confidence is for
judgment dimensions only: definition, scope, convention. The inverse
holds: a defensible default does not raise a judgment dimension to
1.0. What "on time" means, which accounts count, whether partly paid
rows belong — record the default you chose at the confidence it
deserves, and the round serves it for confirmation. Judgment recorded
at 1.0 silences the round on exactly what it exists to ask.

Mark a stock with `"behavior": "stock"` as a top-level key in the
body. The marker is your word and wins. Without it the cube and the
walk take the `behavior` gloss on the column the value is or sums
(the kit's vocabulary, human over agent), and where none speaks the
`behavior_evidence` verdict on that column; with none of the three,
the metric reads as a flow, which sums levels and lies.
`metric_axes().behavior_basis` says which happened — `marked`,
`glossed`, `evidence`, or `default`. An assumption in the body saying
"this is a flow" decides nothing: assumptions are disclosure, the
verb reads the marker, the gloss and the evidence.

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

The cube runs the rival monthly and the docket draws both lines, and
`metric_axes().alternative_divergence` serves the measured gap — the
breaching periods against an authored `tolerance` on the assumption,
or the maximum relative gap and its period — so the question carries
coordinates instead of two lines to eyeball. Start where the families
diverge hardest.

**Hunt the corpus for a second path.** The strongest rival is often
not a rival definition but a second route to the same number the data
already carries — a published statement beside the journals, a control
account beside its subledger, any declared relationship that offers
another aggregation path. Look for one per served metric; a found
path lands as an assumption with `alternative_sql` at the confidence
your judgment gives the discrepancy, and "no second path exists" is
itself a finding worth recording. A contradiction the corpus carries
and nobody measured is a question nobody will ever be asked. A
derived metric inherits no rival from its components: when the corpus
carries a statement line for the subtotal itself, that line is the
rival — component ties do not stand in for it.

**Four defaults where the brief is silent.** Ground on the row-level
path; a pre-aggregated or denormalized column beside it is the second
path, never the ground. A ratio of sums takes each row whole: a null in
either half drops the row from both. Metrics on one cohort share its
anchor — the same date column, the same year — unless the brief says
otherwise. The impossible goes and the implausible stays: exclude what
violates an identity, keep what is merely an outlier, and disclose
both. A null key does not unmake a row: a count takes every row
unless the brief names the entity counted, and the null share is a
disclosure beside the number, never an exclusion.

## Serve or stop

The dirt you measured at landing reaches the metric here. Two cases,
and the measure decides which.

**Serve with disclosure** when the dirt is bounded: you can say how
many rows it touches and which way it moves the number. Ground the
metric and carry the finding as a keyed assumption below 1.0 —
dimension `scope` for rows in or out, `convention` for a recode — with
the count in the `basis`. The number is served, the flaw is visible in
the docket's open count, and the round asks the human.

**Stop** when the dirt is not bounded: the evidence rows are missing
rather than wrong, a join key cannot be trusted, the column the metric
needs never landed. Do not serve a number from a proxy. Record the stop
as the grounding — `stopped` in place of `sql`, the finding as its
text: what is missing, where, how much, and what closes it (an
upstream fix, a ruling, a recipe change). The assumptions ride it as on
any grounding, so the round still asks the judgment behind it. Say the
same in chat, and write the `definitions` entry: a stopped metric is a
finding, not a failure, and the user learns something about their data
they did not know.

```glossql
GLOSS utilization ON ops AS $${
  "stopped": "capacity never landed: the roster export carries no hours column, so hours worked / capacity cannot be served; closes when the roster export adds capacity_hours, or a ruling names contract hours as capacity",
  "assumptions": [
    {"dimension": "definition", "key": "capacity-source", "assumption": "capacity is the roster's planned hours, not contract hours", "basis": "operations convention", "confidence": 0.6}
  ]
}$$;
```

The write answers with the fact row, `applicable` false and your own
text as the `reason` — that is the check that it landed. The read door
refuses with the same text, `metric_surfaces` serves it as `stopped`,
and the docket says so beside the name. A human grounding with `sql`
outranks it, which is how a stop is lifted.

A plan, a schedule, a calendar is never evidence that its events
happened. Count the record; check the plan's coverage as a validation.
