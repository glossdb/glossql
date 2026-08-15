# Onboarding run 4: the shrunk system, MCP-only

2026-08-15, a freshly bootstrapped workspace (0 datasets, only the shipped
system), the finance generator's **medium** export, driven through the MCP
door alone. First run against the shrink: **two skills instead of nine**,
`workspace_next` instead of a staged arc, **one app instead of two**, and
the read library behind the door.

The generator's `ground_truth.yaml`, `metadata_truth.yaml`,
`entropy_map.yaml` and `manifest.yaml` stayed sealed, and no CSV was
opened directly — everything below was learned through the door.

**This is not a blind accuracy run, and it must not be read as one.** The
runner had already seen the findings of the 2026-08-14 run (they are on the
published docket design prototype), so the topic and part of the cohort are
contaminated by that. What it does measure honestly is the *system*: whether
two skills carry the work, whether order derives from the record, where the
substrate bites, and what the surfaces look like at the end. The friction
list is the point of this report.

## What was built

**Topic** (agent's proposal, no human shaping — see the caveat): *working
capital — how long cash is tied up between selling, collecting from
customers, paying suppliers and holding stock.* Proposed from the LIMIT 0
schemas of all 17 CSVs before anything landed.

**Landed: 11 tables of 17 files.** The GL (journal entries/lines, chart of
accounts, trial balance, balance sheet), bank transactions and FX rates were
deliberately left out — single currency, and the topic does not reach them.
This is the import-as-filter rule doing its job.

The eleventh table is not in the export. `behavior_evidence` abstained on
every anchor for both AP amount columns — "fewer than two entities carry 4+
periods" — because a supplier invoice is its own entity and happens once,
and the export ships no vendor table. That is exactly the starvation the
practice skill names, and its prescribed cure worked verbatim: land the id
domain as a dimension (`SELECT DISTINCT vid`, 120 vendors), declare the edge,
re-run. `supplier_payments.amount` then measured **flow** at 0.54 support on
a vendor-month anchor that did not exist before. The invoice side still
abstains — payments can hop to a vendor through their invoice, but invoices
cannot hop to payments through a vendor, since payments carry no vendor
column.

**The record at close**: 223 glosses over 88 subjects · 11 declared
relationships · 13 metric surfaces, all evaluating · 106 standing checks,
105 green and 1 red · 23 open questions · nothing waiting on an act.

## Findings in the data

1. **`payments.date` has no ISO rows and three interleaved formats.** 5,030
   are `%d-%b-%y`; 9,898 are slashed and *mutually ambiguous* — 2,958 can
   only be day-first, 2,958 can only be month-first, 3,982 could be either.
   The symmetry is decisive: neither convention is "the" one, so any single
   format mis-dates thousands of rows. The recipe carries a repair ladder —
   shape, then unambiguous digits, then *the invoice date*, which settles
   1,516 more because a payment never precedes its bill — and lands a
   `date_basis` column recording how each row was decided. **2,466 payments
   (16.5%) remain undecidable** and were taken day-first; roughly half of
   those are wrong by a month or more, and DPO's month attribution rests on
   it. That column is glossed as a supporting axis so DPO can be sliced by
   date certainty.

2. **746 supplier payments (USD 6.74M) cite `ORPHAN-9xxxxx` invoice ids**
   that exist in no invoice row. Not late-arriving invoices — the ids follow
   no invoice series.

3. **The AP amount mismatch is systematic, not noise.** Of 14,182 payments
   matched to a bill, 11,548 are exact, 2,011 short, 623 over. The ratio
   histogram is the tell: **1,265 sit at exactly 1/1.1** (payment net of a
   10% tax), 117 at 1/1.3, 92 at 1.2. This is a tax-treatment inconsistency
   between invoice and payment, and it is why `accounts_payable` treats any
   payment as settling in full — the remainder reading would leave a 9% stub
   open on 1,265 bills. Recorded at confidence 0.55; it is the lowest-
   confidence definitional claim in the workspace.

4. **`stock_movements.source_document` points at two different tables.**
   Every one of the 23,613 issues cites a sales order line; every one of the
   7,817 receipts cites a supplier invoice; the 329 adjustments cite neither.
   A perfect split, and the reason the column is *not* declared as a
   relationship — a polymorphic reference is not one join edge, and
   declaring either leg would leave `relationship_coherence` reporting the
   other population as a 74% orphan rate forever. It is recorded in the
   column's `meaning` instead.

5. **The inventory roll-forward closes exactly.** Prior month's
   `units_on_hand` plus the month's summed movements equals this month's
   level on **5,280 of 5,280** product-location-months, worst gap 0.000 —
   on units and on value. This settled `stock_movements.units` as a flow
   after `behavior_evidence` abstained on every anchor, and it was promoted
   to a standing expectation (`inventory_rollforward`, tolerance 0.0).

6. **DSO and DPO ramp, and the ramp is mostly structural.** DSO 22.5 → 72.4
   days, DPO 15.3 → 65.8 over the year. No invoice on either side pre-dates
   2025-01-01, so January's book can only hold January — the series is a
   book filling from empty, not a trend, and even December has not reached
   steady state. Both are disclosed at 0.6 with the rival reading named.

7. **Collection is bimodal.** Invoices that get paid are paid in a mean of
   **24.7 days**; 3,410 invoices worth **USD 40.4M** carry no receipt at all,
   and 65.9% of the December book is already past due. DSO's level is set by
   the non-paying tail, not by slow paying — a materially different business
   story from "collections are deteriorating", and the two cannot be
   separated without a 2026 extract.

8. **An October regime change** shows up independently in revenue (14 → 18.9M),
   COGS (9.6 → 13.3M) and purchases (9.7 → 13.5M), all three at PIT 0.999 in
   the bands walk. Three series agreeing is a business event, not a defect.

## Friction — the point of the run

**F1. Sweeping the round stalled every read, and the server did not
survive it.** The brief instructs every agent to sweep the round. Doing so
produced `question-round: no round-trip: request timeout after PT120S` —
twice, each stalling the tool call for two minutes — and on the third
record read the MCP transport dropped mid-call and **the serverd process
was gone**. The workspace survived (SQLite + warehouse on disk) and a
restart recovered everything.

**Correction, on the lead's word after the run: nobody was at the
keyboard.** So this is NOT evidence that the client cannot round-trip an
elicitation — the timeout is exactly what an unanswered form looks like,
and the wire monitor shows Claude Code negotiating `2025-11-25` **with a
session**, which is the lifecycle that carries the answer back. Whether
the form rendered is untested and needs a run with a human present.

What the run does prove stands on its own, and is worse for being
ordinary: a person being away is the normal case, and the round charged
the agent two minutes for it on every record read, forever, because the
next read asked the same question again. The failure mode was never "the
client is broken" — it was "the design waits on a human inside a tool
call".

**Fixed** (2026-08-15). The ask still travels on the call's own stream,
because that stream closes when the call returns and firing it into a
closed sink would simply lose the question — the door's own session test
proves the delivery path and it stays. What changed is the price of
silence: the wait is 25 s, and a silence is treated as a decline, so the
question rests exactly like a "not now" and the round stays quiet until a
writing call moves the workspace. One pause per question per move, never a
pause per read. The poisoned-mutex path that could have followed a panic
in the round is gone too. Whether the answer *arrives* is the open half,
and only a run with a human present will say.

**F2. A measurement body costs an agent 60KB of context to learn one word.**
**Fixed** — `behavior_evidence` now carries a `summary` (winning anchor's
verdict, support, voters, convention, alignment, residuals, sign; the
abstention reason when every anchor abstains), so extraction serves ~200
bytes and every anchor still reads back via `GLOSSARY(subject::aspect)`.
`SELECT behavior_evidence() FROM sales_order_lines.line_cost` returned 102
anchors — 59.6KB, large enough that the harness spilled it to a file — to
deliver the word "flow". Seventeen measure columns would be roughly a
megabyte of JSON to learn seventeen words. `profile` and `metric_cube` show
the cure already exists and is cheap: both carry a `summary` object and come
back in ~200 bytes, with the full body readable via
`GLOSSARY(subject::aspect)`. **`behavior_evidence` needs the same summary** —
the winning anchor's verdict, support, voters, `r_flow`/`r_stock`, and the
abstention reason when every anchor abstains.

**F3. A measurement's output cannot be projected or filtered.**
`SELECT json_get_str(...) FROM (SELECT behavior_evidence() FROM t.c)` fails
with `table 'datafusion.t.c' not found` — the `FROM table.column` form is
special routing and does not survive a subquery. Combined with F2, the agent
has no way to ask for less than everything.

**F4. `workspace_next` gave two different answers for the same relation.**
**Fixed, and the mechanism was worth the hunt.** `EXPLAIN` showed the
predicate pushed down into the union's branches, constant-folded to false
against each branch's literal `surface`, and answered by replacing the
table scans *inside that branch's scalar subqueries* with an empty
relation — while keeping the branch's row. Hence nine rows and zeroed
counts. The read no longer offers that shape: the counts are taken once
in a single row and the nine surfaces are a literal `VALUES` relation
joined to it, so a predicate on `surface` lands on plain rows and cannot
reach a subquery. Regression-tested against filtered and unfiltered reads
of the same session.
Filtered — `... WHERE surface IN ('tables','sources','relationships')` — it
returned **all nine rows** (the predicate was not applied) and reported
`aspects` and `functions` as **0 standing**, while `SELECT count(*) FROM
aspects` in the same session returned 27 and the unfiltered read later
returned the correct 42/15. A filter that is silently dropped *and* corrupts
counts is worse than an error. Needs a targeted reproduction in the session
suite; the mechanism is unresolved (the read is a nine-branch UNION of
literal projections with scalar subqueries, and the earlier stack-overflow
fix put those counts in CTEs).

**F5. `workspace_next` reports every landed table as open work.**
**Fixed** — `tables.open` now counts landings whose casts nulled cells,
read out of the accounting JSON properly; the dropped-row term is gone,
since which rows a WHERE excluded is the author's question. The relation
also stops reporting a dropped count for a recipe whose shape is not
row-preserving (DISTINCT, aggregates, relational sources), which is the
`vendors` case. **Still open:** a row-preserving recipe that scans two
relations reports the difference against the sum of both scans;
`source_rows` is `NOT NULL` in the schema, so making that honest is a
schema change and a wipe — a ruling, not a patch. The `open`
count for `tables` is `dropped_rows_count > 0 OR cast_failures > 0`, and
`cast_failures` is a JSON *text* column — comparing it to 0 is true for every
row, so 11 of 11 tables read as open with clean casts throughout. The
`dropped_rows_count` half is also wrong for any recipe that scans two
sources or uses DISTINCT: `supplier_payments` reports 16,817 dropped rows
(the joined invoice scan) and `vendors` reports 16,697 (the DISTINCT
collapse), neither of which dropped anything. The recipe outcome says so
honestly in prose — "casts unaccounted — recipe shape (DISTINCT/HAVING/TOP)"
— but the counter it writes does not.

**F6. `try_to_date` takes exactly one format.** `try_to_date(x, f1, f2, f3)`
was refused with a coercion error; multi-format parsing needed a `coalesce`
ladder over three copies of the value. **Fixed** — both `try_to_date` and
`try_to_timestamp` are variadic now and take the first format that parses,
so a mixed column is one call. Order is the author's claim about the
source and decides the ambiguous rows, which the skill now says plainly.
Tested on run 4's own shape: day-first-only, month-first-only, a named
month, an unparseable cell, and one value both slashed formats accept.

**F7. A PROBE that fails reports itself as a recipe.** The refusal read
`recipe failed: type_coercion` for a statement that was a `PROBE`, which
sends the author looking at a recipe they have not written yet.
**Fixed** — a probe's engine failures are named as probe failures.

**F8. One decision shared across metrics is asked once per metric.**
**Ruled the other way, deliberately.** Fanning one answer across every
aspect that discloses the key is the obvious cure, and it is wrong: run
2's human confirmed `goods-only` on `purchases` in the same session where
they corrected it on `dpo`, on purpose, and `ruling_conflicts` exists to
surface exactly that. A fan-out would silently deny a human the right to
differ. What was actually costly is the RE-READING, so the second ask now
offers the first ruling back as a third stance — "same as before
(corrected on dio)" — which replays that stance and the human's own
words onto this aspect. Agreeing is one click; differing is untouched. The
practice skill says to use one key for one decision, and it was followed —
but `open_questions` is keyed `(aspect, key)`, so `days-in-period` stands
open three times (dso, dpo, dio), `goods-only` twice, `period-balance-not-
average` twice, `overdue-from-dates` twice. Of 23 open questions, **7 are
duplicates of another question's decision.** Either the round dedupes by key
within a dataset and one ruling fans out, or the skill's "compose from a
shared concept" is the only cure and needs to be much louder.

**F9. A composed ratio loses every axis.** `revenue` serves segment, region
and payment_terms; `dso` composes from it through a `GROUP BY` and serves
none — the Metrics page reads "no axes admitted" for all six composed
surfaces. Carrying an axis through a ratio means grouping by it in both
CTEs, which the author must do by hand for every axis they want. Nothing is
wrong, but the cohort's headline numbers are exactly the ones that cannot be
sliced.

**F10. `gl-value` had no `text` format**, so the docket's Corridor tile
rendered `NaN` for the band word `red`. **Fixed.**

**F9 is the one left standing**, along with F5's multi-scan half. Neither
is a defect to patch: carrying an axis through a composed ratio means
grouping by it in both halves of the composition, and whether that should
be the author's work or the library's is a design question.

## What worked

- **`workspace_next` replaced the staged arc without a gap.** No point in
  the run needed an order that the record could not supply. The nine
  surfaces with `how`/`stands`/`open` were enough to know what could be
  extended next (F4/F5 are defects in its counts, not in the idea).
- **Two skills carried the whole run.** Nothing sent me looking for a
  deleted flow skill.
- **The round's boundary held.** 23 open questions, and not one of them is
  a `behavior`, `sign` or `grain` claim — every statistical verdict was
  settled by a function or a cited data test at 1.0, as the 2026-08-14
  ruling requires.
- **The brief rides tool results.** The open count went 4 → 9 → 20 → 23 as
  groundings landed, without reconnecting.
- **The judge pattern.** `detect_relationships` proposed 34 candidates;
  11 survived. The rejects are the instructive ones — `inventory_positions.
  unit_cost -> products.standard_cost` overlaps perfectly and is a derivation,
  not an edge, and five amount↔amount pairs overlap on value alone.
- **Orphans as populations, exactly as the skill says.** Every declared
  edge's orphan set turned out to be a named business population: 3,410
  unreceipted invoices are the open book, and the 8,146 movements that miss
  the order-line join are precisely the receipts and adjustments.
- **`read.<name>()` composition.** Six ratios are composed from the measured
  surfaces, so a ruling on `purchases.goods-only` reaches DPO without an
  edit — which is the whole argument for composing rather than copying SQL.
- **The docket.** Three pages, all served, all reading the same counts the
  door reports. The open band, the metric surfaces and the coverage
  denominators (dashes where a table has no measures) all render from frame
  SQL with no display logic in the templates.

## The three lists, for the human

**Definitional choices** (the round would ask these; relayed here because
F1 means it cannot):

1. **AP settlement** (0.55) — does any payment close a bill, or does the
   bill stay open for the remainder? 2,011 payments are short, but 1,265 of
   those are short by exactly 1/1.1, which looks like tax, not part payment.
2. **DPO's denominator** (0.70) — goods purchases only (USD 137.5M) or all
   supplier spend including expense (USD 155.6M)? Goods-only is the reading
   taken; the alternative shortens DPO by about 12%.
3. **Days in period** (0.80) — the month's own calendar days, or a fixed 30?
   Carries into DSO, DPO, DIO and therefore CCC.
4. **Balance basis** (0.75) — closing balance, or the average of opening and
   closing? The average needs a prior month and drops January.
5. **Gross revenue as net** (0.85) — nothing in the export reduces billing,
   but the absence of a credit-note table is weak evidence for the absence
   of credit notes.
6. **COGS basis** (0.85) — inventory issues at standard cost, or the sold
   line's own recorded cost? Both are the same standard cost applied at
   different points.
7. **CCC identity** (0.85) — DIO + DSO − DPO, or the operating cycle
   DIO + DSO?

**Data findings** — findings 1–5 and 8 above. The tax cluster and the
ORPHAN references are the two that need a business answer rather than a
definition.

**World-coverage wishes**:

- **An opening balance as of 2024-12-31**, or a prior-year extract. Without
  it the first months of DSO and DPO are structurally understated and the
  series cannot be read as a trend (finding 6).
- **A 2026 extract**, to separate the non-paying tail from right-censoring
  (finding 7). USD 40.4M of receivable turns on it.
- **The source system's date convention for `payments.date`**, per row or as
  a rule — or an ISO re-export. It would decide 2,466 payments and remove the
  largest single uncertainty in DPO.
- **A vendor master**, which would give the AP side a real dimension instead
  of a derived id domain, and let `behavior_evidence` reconcile the invoice
  half.

**The cohort, in full** — all seven agreed KPIs grounded, plus the six
component surfaces they are composed from. Nothing in the cohort failed to
ground. What is missing is not a metric but the axes on the composed ones
(F9) and the check functions: `inventory_rollforward` and
`ar_settles_in_full` are recorded as authored expectations with their
tolerances, but the `.rhai` check halves cannot be authored over the MCP
door, so they stand as expectations without a measuring voice.
