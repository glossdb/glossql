---
name: glossql-metrics
description: Define the metric and validation framework on a glossed glossql workspace — concepts ground as grain-free extracts, derived metrics as formulas, validations as expectation + check voice + ATTEST. Use when the target asks for performance monitoring, after the add-source, relationships and dimensions flows.
---

# The metric framework

The operating-model deliverable: metrics the business trusts and the
validations that say why. Everything below rides existing constructs —
QUERY aspects, glosses, functions, witnesses. The governing rule:
**nothing is evaluated before a reader asks; everything a reader
proves may be recorded.**

The framework is domain-neutral: a metric is whatever the target asks
— throughput, defect rate, utilization, revenue. The worked examples
below come from our finance test runs.

## 1. Read the floor first

Every grounding cites the judged knowledge underneath it. Before
writing any SQL: no summed term without a `behavior` gloss under it
(`behavior_evidence` first), no join without its grain-check gloss on
the relationship, any sign convention stated before signed values
split into measures, units checked before cross-unit arithmetic. A
grounding whose assumptions cannot name their bases is not ready to
write.

Where the data carries a sign convention (ledger-style bookings), the
convention has measured evidence: a behavior_evidence anchor
carries `sign` — voters re-judged against the negated convention. A
mirror-heavy count says the store carries the negation of the anchor's
named convention (ledger-signed); primary-heavy says the convention
reads as named (natural balance); a split says the entities disagree
and the P&L grounding must scope them. Cite it as the sign
assumption's basis instead of asserting from column names.

## 2. The vocabulary

One QUERY aspect per concept, on the dataset. Base concepts and
derived metrics declare uniformly — the difference is whether the SQL
half is an extract (§3) or a formula over siblings (§4):

```glossql
DECLARE ASPECT revenue WITH $${
  "title": "Revenue", "x-kind": "measure", "x-unit": "currency"
}$$ AS QUERY ON DATASET;
DECLARE ASPECT dso WITH $${
  "title": "Days Sales Outstanding", "x-kind": "metric", "x-unit": "days"
}$$ AS QUERY ON DATASET;
DECLARE ASPECT formulas WITH $${
  "type": "object", "properties": {"formulas": {"type": "object"}}
}$$ AS FACT ON DATASET;
```

## 3. Ground concepts as grain-free extracts

A grounding carries **no grain** — no GROUP BY, no window. It is the
semantic core: scoping, signs, the grain-preserving joins composed
inline, served as a row-grain relation with the time axis and the
judged dimensions as columns. Every assumption names its basis:

```glossql
GLOSS revenue ON fin AS $${
  "sql": "SELECT e.date, l.credit - l.debit AS value, l.cost_center FROM journal_lines l JOIN journal_entries e ON l.entry_id = e.entry_id JOIN chart_of_accounts a ON l.account_id = a.account_id WHERE a.account_type = 'revenue'",
  "assumptions": [
    {"dimension": "sign", "assumption": "revenue accounts carry credit balances", "basis": "conventions gloss", "confidence": 0.95},
    {"dimension": "grain", "assumption": "joins are grain-preserving", "basis": "relationship glosses", "confidence": 1.0},
    {"dimension": "behavior", "assumption": "a flow: sums valid over any partition", "basis": "behavior_evidence on journal_lines.credit", "confidence": 0.95}
  ]
}$$;
```

A stock's extract is bounded by its **source grain** (a table of
period balances speaks per period; no read can answer finer) — serve
the grain column as-is and say so in the assumptions. Mark it too:
`"behavior": "stock"` as a top-level key in the grounding body. The
library's readers follow the marker (a stock takes last-per-window,
never a sum), and an unmarked grounding reads as a flow.

The assumptions array is a contract, not commentary: every metric
writing carries `assumptions: [{dimension, assumption, basis,
confidence}]`, and confidence means it — 1.0 only for what is pinned
or proven, less for judgment however common. The world-model surface
reads exactly this shape to build its judgement queue, so an
assumption you leave out is invisible to the humans who would have
caught it, and a confidence you inflate empties their queue falsely.

**After grounding, run the collision read.** Two concepts grounding to
the same extract make every ratio between them compute 1.0, silently:

```glossql
SELECT detect_grounding_collisions() FROM fin;
SELECT value FROM GLOSSARY(fin::grounding_collisions);
```

It buckets current groundings by canonical SQL and reports shared
buckets — recall, not judgment. A reported pair is either a deliberate
synonym (say so: one concept, or a FACT gloss naming the alias) or a
definition error (re-ground one of them). Its cache stales on any
gloss write, so a re-read after new groundings recomputes.

## 4. Evaluate at read — windows are read policy

Grain is the reader's: the app defaults to month, another reader asks
by day, the same definitions answer both. Evaluate through
`read.<aspect>()` — the current grounding served as an ordinary
relation (human slot outranking agent, so a pinned definition is what
runs); windows and filters ride your SQL. The prefix is `read.` for
every QUERY gloss — metrics, suspect lists, any declared aggregation —
one serving door, whatever `x-kind` names:

```glossql
SELECT date_trunc('month', date) AS month, sum(value)
FROM read.revenue() GROUP BY 1 ORDER BY 1;
```

- **Flows sum** over any partition — time window or judged dimension.
- **Stocks take the last period per window**, never a sum across.
- **Ratios don't roll up**: compose them per the formula at the
  window asked — `dso[w] = accounts_receivable[end of w] /
  revenue[w] * days[w]`. The formula gloss is the pinned definition;
  it covers every window because it names none. Never regroup a
  ratio's output rows — re-compose at the new scope.

**Record what a read proves.** A composed evaluation you verified
(against the oracle, against the ledger) may land as the metric's own
QUERY gloss — durable executable knowledge, superseding as
definitions change, served by `read.<aspect>()` from then on.
Record it composing `FROM read.revenue()` where you can: a
re-pinned component then propagates through every metric built on it
(a self-reference is refused as a cycle). The formula gloss and the
recorded evaluation are one definition in two forms: change one,
update the other in the same act — or carry the difference as a
disclosed assumption. Recording a proven read is not pre-evaluation.

## 5. Validations — expectation beside check, ATTEST answers

The authored expectation is a FACT gloss; the check is a function
**voice** on the same aspect; a detector bands across both slots;
`ATTEST` is the verdict surface:

```glossql
DECLARE ASPECT journal_balanced WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"},
                 "severity": {"enum": ["critical", "warning", "info"]}}
}$$ AS FACT ON TABLE;
GLOSS journal_balanced ON journal_lines AS $${
  "outcome": "Total debits equal total credits, exactly.",
  "tolerance": 0.0, "severity": "critical"
}$$;
DECLARE FUNCTION journal_check FOR fin FROM 'functions/journal_check.rhai'
  ACCEPTS (imports) RETURNS journal_balanced;
DECLARE FUNCTION framework_bands FOR fin FROM 'functions/framework_bands.rhai';
DECLARE WITNESS journal_w ON journal_balanced BY (AGENT, HUMAN)
  DETECTOR framework_bands THRESHOLD 0.5;
SELECT journal_check() FROM journal_lines;
```

- **The expectation is authored, never assumed zero.** A source with
  known dirt expects its own rate (`"expected_rate": 0.895`) — a
  check reporting 1.0 there has overcleaned, itself a failure.
- **The check speaks the aspect's schema**: its output carries
  `outcome` like any slot, with the measurement beside it. One
  schema, every speaker.
- `ACCEPTS (imports)` keeps it honest: a new import invalidates the
  voice, and the next read recomputes.
- **Promote confirmed reconciliations.** A behavior_evidence
  convention that reconciled at ~0 residual (a balance equal to the
  sum of its movement rows) is a standing invariant — turn it into a
  check.
- Checks and detectors are workspace-authored (`FOR` the dataset,
  not GLOBAL) — write them per the glossql-functions skill.

### Expected ranges — the band walk

The library ships a trajectory read (still finding out how well this
works in practice). `metric_bands` answers, for each grounded metric
and each of its recent months: **knowing only what came before, what
would this month's number have had to be for nobody to be
surprised?** Each walked point records that corridor (p05–p95, p50
the single most expected value), the actual, and the PIT — where the
actual landed in the corridor, 0..1. Read PIT in plain terms: 0.5,
the month landed where the trajectory pointed; 0.9, it beat nine in
ten plausible outcomes — high but explainable; past 0.95 or under
0.05, the month is outside what the metric's own history can explain
— something changed, the business or the data. Seasonality the walk
has seen is inside the corridor: a strong December does not flag, a
flat one might. Every point is honest — its fit saw only the months
before it, so the corridor was drawn before looking at the answer.

`band_breach` is the detector on top, the pager line: does any
monitored metric currently have a month its history cannot explain,
and how decisively? Green — every metric's recent months continue
their story. Red — one broke pattern; the score says how far outside
(0.98 is beyond the 99th percentile of expectation), and the
measurement's cached body names the metric and the month. The
vertical wiring is yours:

```glossql
DECLARE WITNESS bands_w ON metric_bands DETECTOR band_breach THRESHOLD 0.98;
SELECT metric_bands() FROM fin;
SELECT subject, band, score FROM ATTEST(fin::metric_bands);
```

- The model weights ride the build: `build.rs` stages safetensors,
  config, and the pinned `DIGESTS` beside the binaries from the
  tabicl-candle checkout, digest-verified at load. A workspace
  `weights/` directory overrides the staged set; a build made without
  the checkout's weights fails the extraction with every path it
  tried.
- **The read is recall, you are the judge**: a business shift and a
  data defect breach identically — telling them apart is your work,
  not the detector's. Read the body for which metric and month, look
  at the rows underneath, and rule.
- The corridor knows only the history it is shown: a short history
  gives wide corridors and weak claims, and under about five months
  the walk says nothing. The read sharpens as the workspace ages.
- The read follows the grounding's authored behavior: flows sum per
  month; a grounding whose body carries `"behavior": "stock"` takes
  the last value per month instead. No marker reads as flow — mark
  your stocks (§3), or their walk sums levels and lies.
- The model app's metric dossier renders the walk (the trajectory
  tile); the score is ordinal (band displacement), never a probability.

## 6. What-if — a scenario is declared, then read

A scenario is its own FACT aspect, exactly as a metric is its own
QUERY aspect: declare it with `x-kind: "scenario"`, gloss the
overrides, read it through the `whatif.` door. Never compute a
scenario by hand-editing SQL — the declared form is versioned,
witness-gated, and reproducible; an ad-hoc calculation is none of
those.

```glossql
DECLARE ASPECT price_hike WITH $${
  "title": "Price +15% from July", "x-kind": "scenario",
  "type": "object", "required": ["overrides"]
}$$ AS FACT ON DATASET;

GLOSS price_hike ON fin AS $${"overrides": [
  {"column": "sales_data.unit_price", "factor": 1.15, "from": "2026-07",
   "basis": "the declared lever"}]}$$;

SELECT month, replay, p50, p90, basis FROM whatif.price_hike()
WHERE concept = 'revenue';
```

Each override names a **raw column on a landed table** (the table
must carry a date column — the start month anchors there), a factor,
and its basis. A behavioral response the history never saw — demand
dropping when prices rise — is a second override with its basis
saying it is assumed; undeclared, the read proceeds as if behavior
holds, and says so.

What comes back, per concept and month: `replay` is the exact
recomputation of the grounding at the declared factors (the
arithmetic half — trust it as arithmetic), `p05..p95` are the model's
bands read across replayed support worlds around the declared point,
and `basis` is the judgment. Read `basis` before the numbers. Three
refusals matter:

- **"unmoved by the overrides"** — the grounding reads a stored total
  (`line_amount`) the overridden parts never reach. Run
  `detect_derivations` on the table and gloss the identity, or ground
  the concept on the parts.
- **"starts after the recorded history ends"** — replay works over
  recorded months; a scenario about a future with no rows needs its
  start inside the books.
- **"the grounding is contested/stale"** — the concept's own state,
  not the scenario's; fix the grounding first.

Superseding the scenario gloss recomputes the read; `DELETE FROM
cache` forces it. One scenario = one factor set — a different
strength is a new gloss (re-gloss to revise) or a sibling aspect.
In a workspace with apps, a scenario ships with its authored chart
tile (glossql-apps: "A scenario ships with its tile"); the built-in
model app lists scenarios without it.

## 7. Misfit — rank a frame when a signal fires

When something breaks pattern — `band_breach` flags a month, a
validation goes red, a user says these numbers look wrong — the next
question is *which rows*. Author a sample frame: a QUERY aspect,
`x-kind: "sample"`, glossed with one SELECT that holds history and
suspects together. The `misfit.` door serves the frame's rows back
with a `misfit` score — how badly each row fits the rest of the same
frame — and the top of the ranking is where to look. This is the
investigation step of the judge loop: the ranking optimizes recall,
you remove the false positives, and what you conclude lands as a
gloss. Run it on a signal, never as a routine sweep.

```glossql
DECLARE ASPECT payment_pairs WITH $${
  "title": "Payments against their invoice, six months and March",
  "x-kind": "sample"}$$ AS QUERY ON DATASET;

GLOSS payment_pairs ON fin AS $${
  "sql": "SELECT p.amount, i.amount AS invoiced, p.paid_date - i.invoice_date AS day_delta, i.terms_days FROM payments p JOIN invoices i ON p.invoice_id = i.invoice_id WHERE p.paid_date >= DATE '2025-09-01'",
  "assumptions": [{"assumption": "joined surface: the suspicion is the pairing"}]}$$;

SELECT * FROM misfit.payment_pairs() ORDER BY misfit DESC LIMIT 20;
```

Pick the surface to match the suspicion: a relationship suspicion
needs the **join** in the frame — a single table is structurally
blind to wrong pairings whose individual values are all legal. A
value suspicion can frame the metric's own extract. `basis` names
the columns ranked and every exclusion (text, constants, id-named
columns); read it before trusting the ranking. The read refuses by
name rather than serve noise: a frame past the row cap (narrow with
WHERE), or too little numeric surface — some frames cannot carry a
density read, and the abstention is the honest answer. Nothing is
cached; the durable record is your verdict, glossed.

## 8. Read back

```glossql
SELECT subject, band, score FROM ATTEST(fin) WHERE band = 'red';
SELECT count(*) FROM GLOSSARY(fin) WHERE state = 'unassessed';
```

Red bands are where a human closes what you could not; unassessed
rows are the vocabulary nobody has spoken to yet.

## 9. The pinning agenda — end every framework with it

A definition is a choice between formula families the data cannot
arbitrate: both DSO variants compute, both gross-profit subtrahends
compute, interest income sums in or out. Where evidence *can* decide
(a stock never sums across periods), the measurements already did;
what remains is convention, and convention is the user's to pin.
Definitional risk is invisible from inside your own judgment — it
shows only against an alternative — so the alternative must be
named, every time.

Close the flow by presenting **every definitional choice you made**
as a question to the user — one per definition, multiple choice,
through the cockpit's question tool when it has one, numbered prose
otherwise:

> **gross_profit** — I used revenue − COGS (textbook). The other
> family is revenue − all expenses (closer to operating profit).
> Which does this business mean? My grounds: … · confidence 0.7.

What counts as definitional: any grounding assumption with
`dimension: "definition"` whose basis is your judgment rather than a
measurement or an engineer pin — the **basis is the pin marker**,
never the number. Calibrate `confidence` soberly: 1.0 is reserved
for a measured fact; 0.9 is already very high — a well-argued
convention choice tops out there. Keep data findings (a bank gap, a
turnover-not-balance table) in a separate list: those are facts to
explain, not choices to make.

And a third list beside those two — **world-coverage wishes**. Some
assumptions resolve neither by the user's choice nor by SQL, only by
*more world*: an opening balance sheet that would anchor every
cumulative level, a prior-year extract, the subledger a role gloss
implies but no table carries, the fx policy document. A single
confidence number flattens these — "no opening entries exist" is a
fact at 1.0, "therefore the levels are the true levels" is a world
claim the data cannot settle (a negative opening month is evidence
against it on a real book). Name each wish as a specific ask: which
document or source, and which numbers shift when it arrives ("every
stock level moves by its opening values"). The ask is a document,
not a decision — keep it out of the pinning questions.

The user's answer lands as your re-gloss: supersede your own slot
with the pinned choice and set the assumption's basis to
`engineer-pinned <date>`. (You cannot write the human slot — actor
rides the connection.) Until then, your report says which
definitions stand on your judgment alone.
