# Onboarding run 2: the medium export, second pass on the rebuilt server

2026-08-14, a wiped and freshly bootstrapped workspace (0 datasets, only
the shipped system: measurement library + KPI kit), the finance
generator's **medium** export, driven MCP-only. The generator's
`ground_truth.yaml`, `metadata_truth.yaml`, `entropy_map.yaml` and
`manifest.yaml` stayed sealed for the whole run; the CSVs were never
opened directly — everything below was learned through the door.

This is an acceptance run of a server rebuilt after
`2026-08-14-onboarding-run-fresh.md`. That earlier report was read only
after judging was complete, and only for the friction comparison in the
closing section. Stage 0 was delegated to the agent, so topic and cohort
are the agent's proposal.

## Stage 0 — topic and cohort (run-scoped assumptions)

**Topic** (`DECLARE DATASET fin SET (purpose: …)`): *working capital —
the cash conversion cycle: where cash is tied up between selling,
collecting, paying and holding stock.* **Confidence 0.8** — proposed from
the probed schemas of all 17 CSVs before anything landed. The export
carries both settlement halves whole (AR: `ar_invoices`→`receipts`; AP:
`invoices`→`payments`) plus a genuine inventory stock/flow pair
(`inventory_positions`/`stock_movements`), which is exactly the shape a
cash-cycle topic needs; that structural fit is what raises this above the
0.7 a name-based guess would earn. No human shaped it.

**Cohort proposed** (7 KPIs): DSO, DPO, DIO, CCC, monthly revenue, gross
margin %, overdue-AR share. **Confidence 0.8** that this is the right
cohort for the topic. All seven grounded — but see the honest caveat
below: three of them ground *arithmetically* while being untrustworthy as
levels, which is a finding rather than a success.

**Import as filter.** 10 of 17 files landed, 65 columns. Deliberately
excluded, each one `DECLARE RECIPE` away:

- `journal_entries`, `journal_lines`, `chart_of_accounts`,
  `trial_balance`, `balance_sheet` — the GL layer. The cash-cycle cohort
  is computable entirely from the subledgers, and the previous run
  measured what a 109-column long tail costs. **What this forecloses**: a
  GL tie-out of AR/AP/inventory, which is the one independent check on
  the balances below. Named as a wish, not a silent omission.
- `fx_rates` — measured irrelevant: every money column in every table is
  USD (14,770 AR / 16,817 AP / 157,851 GL lines, `count(distinct
  currency) = 1` throughout). The 8 shipped rate pairs convert nothing.
- `bank_transactions` — not landed as a table, but **read inside the
  `ap_payments` recipe** as the authoritative settlement date (below).

## What stands at the end

- **10 tables, 134,417 rows**, 0 dropped, cast accounts clean on all ten.
  Every grain verified exact (`COUNT(*)` vs `COUNT(DISTINCT …)`) before
  any entity verdict; `inventory_positions` confirmed composite
  (480 product-location pairs × 12 periods = 5,760).
- **10 relationships** declared from 29 candidates; the 19 rejects stay
  visible in the measurement. 9 of 10 carry 0 orphans.
- **65 columns fully vocabularied** — `meaning` and `role` on every one,
  `behavior` and `unit` on all 15 measures, `dimension` on all 29
  dimension-role axes. **The witnessed backlog is zero**: every
  `unassessed` row remaining is a measurement cache, not an unspoken
  claim.
- **14 metric surfaces** — 8 base extracts (revenue, cogs, purchases,
  supplier_invoices, accounts_receivable, overdue_receivables,
  accounts_payable, inventory_value) and 6 derived (dso, dpo, dio, ccc,
  gross_margin_pct, overdue_ar_share). Zero grounding collisions.
- **11 human rulings**, all folded in. 9 confirmations, 2 corrections.
- Cube and walk fuelled: 30 served faces including 4 disclosed rival
  series. `ATTEST(fin)` at close: **95 green, 1 red**.

## What the measurements caught

1. **`payments.date` is unparseable, and the bank fixes it.** The column
   carries **no ISO rows at all**: 5,030 as `%d-%b-%y` and 9,898 in a
   slash form assigned *per row at random* between `MM/DD/YYYY` and
   `DD/MM/YYYY` — 2,958 forced to DMY by a first part > 12, 2,958 forced
   to MDY, and 3,982 genuinely undecidable from the string. The
   invoice-date constraint resolves 1,516 of those; **2,265 stay
   undecidable**.

   Rather than guess, I checked whether another table carries the same
   fact. `bank_transactions` has a `payment_id` column, is 100% ISO across
   all 26,655 rows, and carries exactly 14,928 payment ids — one per
   payment. On the 10,946 payments whose original text *was* decidable,
   the bank date matches the payment date **exactly, on every single
   row**. So the recipe joins `bank_transactions` and lands `payment_date`
   from it. This is measured, not assumed: the defect is fully repaired
   rather than flagged, and the convention is deposited at SOURCE grain
   so the next export from this system inherits the knowledge.

2. **746 orphan AP payments** naming literal `ORPHAN-######` invoices —
   5.0% of payments, USD 6.74M. Anti-join confirms all 746 non-matching
   rows are the literal population. Crucially, the counterpart is
   identifiable: of the 2,635 AP invoices with no matching payment,
   1,889 are genuinely unpaid (open/cancelled/overdue) and **exactly 746
   are marked paid or partial** — those are the orphans' invoices. So
   this is corrupted key text on *settled* documents, not an unpaid
   population, and the edge gloss mandates `LEFT JOIN` for any cash-out
   total.

3. **`region` and `payment_terms` are perfectly confounded.**
   DACH↔net_30, Nordics↔net_60, Benelux↔net_90, UK&I↔due_on_receipt,
   exactly 100 customers each, λ = 1.0 both ways, g3 = 0.
   **Any "DSO by region" result is arithmetically identical to "DSO by
   credit terms".** Kept as separate axes (different concepts, not an
   alias to collapse) with the confound recorded in both `dimension`
   glosses. `segment` cross-cuts region cleanly (33–34 accounts in all 12
   cells), so it is the one genuinely independent customer axis.

4. **The stock/flow pair reconciles exactly, and the naive anchor is
   wrong.** `behavior_evidence` on `inventory_positions.units_on_hand`
   returned *two disagreeing anchors*: against `sales_order_lines` it
   says **flow** (support 0.478, 130 of 240 products); against
   `stock_movements` it says **stock** (support 0.977, 239 of 240,
   stock residual 1.7e-15 against flow residual 6.95). The sales-lines
   anchor sees only the issue half of the movements. I took the stock
   verdict and confirmed it independently: prior month-end level plus
   that month's own movements reproduces the level on **5,280 of 5,280**
   transitions. That same reconciliation also establishes that
   `period` labels a month whose **CLOSE** the level describes — recorded
   in `meaning`, because joining monthly flows to `period_start` without
   that knowledge mis-aligns stock against flow by a month.

5. **Three exact derivations, zero violations**: `line_amount = units ×
   unit_price` (23,613 rows), `stock_movements.value = units × unit_cost`
   (31,759), `inventory_positions.value = units_on_hand × unit_cost`
   (5,760). Plus one cross-table: `line_cost = units ×
   products.standard_cost` on all 23,613 lines — so standard cost is the
   single cost basis for inventory, movements and COGS alike.

6. **COGS agrees to the cent from two independent sources.** Sales-line
   costs and negated stock-issue values produce **identical monthly
   totals in all 12 months** (max difference 0.00). This closed a
   definitional fork by measurement rather than by asking — the choice of
   source cannot change any number.

7. **AP payment amounts disagree with their invoices on 2,516 of 14,064
   settled matches** (17.9%). The ratios are structured, not random:
   0.9091 (= 1/1.10) on 1,265 cases, 0.7692 (= 1/1.30) on 120, 1.2 on 92.
   But they spread across **112 of 120 vendors, all 12 months, and both
   categories at equal rates** (18.0% expense, 17.5% goods), which argues
   against a vendor tax rule and for injected amount noise. Stated
   plainly as ambiguous: the 1/1.10 concentration keeps a 10%-tax reading
   alive, and I could not kill it from the data. Payables are therefore
   measured from the invoice side, which the human subsequently ruled.

8. **Order date equals invoice date on all 14,770 orders** — zero
   order-to-invoice lag, so revenue and the receivable share one time
   axis. AR due dates are exactly `invoice_date + payment_terms` (0/30/
   60/90) on every row.

9. **Settlement structure is clean and complete.** At most one receipt
   per AR invoice; the 3,410 invoices with no receipt are *exactly* the
   1,178 open + 2,232 overdue. `paid` invoices settle to the cent
   (10,767/10,767); `partial` ones never do (0/593). Mean days-to-cash on
   settled invoices: **24.7 days**.

10. **Everything else is clean.** All dates outside `payments.date` are
    ISO with daily grain, zero gaps, full 365-day 2025 coverage. Zero
    orphans on receipts→invoices, lines→orders, lines→products,
    orders→customers. No null markers anywhere: every numeric and date
    column outside `payments.date` casts at 100%.

## The numbers

| metric | Jan | Dec | year | note |
|---|---|---|---|---|
| revenue | 13.57M | 17.30M | 178.03M | Q4 steps up ~30% |
| gross margin % | 30.6% | 30.4% | ~31% | **the one trustworthy series** |
| DIO | 41.0 d | 31.8 d | — | trustworthy: real opening level |
| DSO | 22.7 d | 75.6 d | — | a ramp, not a level |
| DPO | 11.0 d | 47.5 d | — | a ramp, not a level |
| CCC | 52.7 d | 59.9 d | — | inherits both distortions |
| overdue AR share | 30.1% | 67.5% | — | inflated by truncation |

**The level of DSO, DPO, CCC and overdue-share is not trustworthy for
2025, and the disclosed rival proves why.** AR builds monotonically from
zero (9.95M → 42.20M) because no opening position exists and collections
truncate at 2025-12-31 while invoicing continues to the same date. The
runnable alternative recorded on `dso` — mean days between invoice and
receipt on invoices that actually settled — runs **flat at ~25 days all
year** (25.3, 25.2, 25.3, 24.8, 24.9, 25.6, 25.1, 24.3, 24.9, 25.2, 24.1,
then 13.3 as truncation bites). Collections never deteriorated. The
entire 22.7→75.6 ramp is an artefact, and the rival series is what
demonstrates it rather than argues it.

Gross margin escapes entirely (both terms are within-period flows) and
DIO escapes because `inventory_positions` carries genuine month-end
levels (flat 12.34–12.47M all year). Every distorted grounding carries
the `scope` assumption saying so at confidence 1.0.

Closing balances tie out exactly: USD 178.03M billed − USD 135.83M
collected = USD 42.20M December receivables.

## What the round ruled

The question round fired on record reads exactly as designed — one form
per `ATTEST()`/`GLOSSARY()`/store-relation call, never interrupting a
landing, answers landing server-side with human standing. **11 rulings
across 8 metric aspects**: 9 confirmations, 2 corrections.

The two corrections and what they changed:

- **`dpo` denominator — corrected.** *"It covers all supplier invoices,
  if we only have goods purchases then it is that only."* I had used
  goods-only. Fold-in: declared a new `supplier_invoices` concept (all
  16,817 invoices, USD 155.6M) and re-composed `dpo` from it.
- **`accounts_payable` settlement — corrected.** *"only if the full
  payment lands."* I had dropped an invoice from payables on any payment;
  the ruling keeps the remainder outstanding on a short payment. Fold-in
  raised December payables from USD 18.23M to **USD 21.05M**.

The two partly offset on DPO (December 46.6 → 47.5 d), but the
composition is now the ruled one rather than mine.

**One thing worth flagging about the ruling set**: `purchases`'s
goods-only assumption was *confirmed* in the same batch where `dpo`'s
identically-worded goods-only assumption was *corrected*. Read literally
they conflict. I resolved them as targeting different aspects — the
concept `purchases` legitimately means inventory purchasing and keeps its
scope; the metric `dpo` moves to a different denominator — and recorded
that reconciliation in both groundings. The round serves per-assumption
and does not cross-check coherence between rulings; the agent has to do
it, and a less careful reconciliation would have silently dropped one
ruling.

**The fold-in mechanism worked.** A ruling lands as the judgment alone —
`{aspect, assumption, dimension, stance, note}` in the human's `ruling`
slot — never a copy of my body. My groundings stayed mine, and
`read.*()` served the corrected SQL immediately after I re-recorded. No
ruling was ever re-asked after its fold-in; the round advanced
monotonically through my remaining below-1.0 assumptions.

## Open judgment questions

The round was **still serving at close** — it works through
lowest-confidence assumptions in order, and every record read produced
another form. These stand on my judgment alone. Options are what I would
have offered.

1. **Is gross billed the same as net revenue?** (a) yes, the export
   carries no credit notes, returns or rebates (what I assumed); (b) no,
   a credit-note extract exists elsewhere and revenue is overstated.
   *Grounds: no credit-note table exists and `ar_invoices.amount` has no
   negative or zero rows in 14,770. Confidence 0.9 — but the absence of a
   table is weak evidence for the absence of the business fact.*
2. **Is standard cost the right COGS basis?** (a) yes, it is the only
   cost basis in the export (what I used); (b) no, actual purchase cost
   from goods AP invoices is the real COGS, and standard cost hides
   purchase price variance. *Confidence 0.9.*
3. **Is overdue computed from dates or from `status`?** (a) from the
   dates — unsettled at month end and past due_date (what I used);
   (b) from `ar_invoices.status`. *Grounds: status is a single
   as-of-export snapshot and cannot answer what was overdue in March.
   Confidence 0.9; affects both `overdue_receivables` and
   `overdue_ar_share`.*
4. **Days-in-period convention.** (a) actual calendar days (what I used);
   (b) a fixed 30-day month or 365/12. *Confidence 0.9; affects DSO, DPO,
   DIO and therefore CCC.*
5. **CCC identity.** (a) DIO + DSO − DPO (what I used, textbook);
   (b) the operating-cycle reading, DIO + DSO only. *Confidence 0.9.*
6. **Are the 2,516 mismatched AP payment amounts noise or a tax
   convention?** (a) injected noise — measure payables at the invoice
   (what I used, and the human ruled the invoice-side measurement);
   (b) a 10% tax treated inconsistently between invoice and payment, in
   which case the gap is meaningful and belongs in the model. *This is
   the one open item that is a fact about the business rather than a
   definition choice, and it is why I flagged the 1/1.10 cluster rather
   than dismissing it.*

## World-coverage wishes (documents, not decisions)

- **Opening AR and AP positions at 2024-12-31.** The single
  highest-value missing artefact: it is what turns DSO, DPO and CCC from
  shapes into levels. Every receivable and payable level shifts by it.
- **Collections and disbursements for 2026-01 onward**, or a stated
  as-of date for the export. The truncation is what inflates the late-year
  DSO and overdue share; without it the closing months cannot be read.
- **`payments.date` in the source system's own format**, or the export
  job's format spec. Not needed here — the bank join repaired it
  completely — but the repair depends on `bank_transactions` shipping in
  every future export.
- **A vendor master.** `ap_invoices.vendor_id` is 120 opaque ids with no
  names, terms or categories; AP spend can only be sliced by id.
- **A credit-note / returns extract**, or written confirmation that none
  exist — settles question 1.
- **Bad-debt provision and write-off policy.** 3,410 invoices (23% by
  count, USD 42.2M) were never collected in 2025 and nothing in the data
  ages, provisions or writes them off.
- **The GL package** (`journal_lines`, `chart_of_accounts`,
  `trial_balance`, `balance_sheet`) — deliberately out of scope, but it
  is the only independent tie-out for AR, AP and inventory balances. One
  `DECLARE RECIPE` each if the cohort ever needs verification rather than
  computation.

## The checks face at close

`ATTEST(fin)` — 96 rows, **95 green, 1 red**:

- All 95 `role` / `behavior` / `unit` slots green, score 0.0. No
  contested slot at close.
- **`fin :: metric_bands` — red, score 0.998** (`band_breach`).

**Judged, not just reported.** The red is correct behaviour and is
explained, not a fresh defect. The breaching months split into two known
causes: (a) a genuine Q4 step-change in the data — October revenue
18.92M against a p95 of 14.72M (PIT 0.999), with COGS, purchases and
supplier invoices all moving with it; and (b) the export truncation
already documented at 1.0 confidence in the groundings — `dpo` at 0.999
for Oct/Nov/Dec and `accounts_payable` at 0.999 for Sep/Nov as year-end
invoices sit unpaid, `dso` at 0.999 for Dec. Two low-side breaches are
also real: `gross_margin_pct` dipped below its corridor in September
(29.74%, PIT 0.0) and `dio` in October (PIT 0.0) as the Q4 volume step
lifted the COGS denominator against flat stock.

Unassessed backlog: 222 rows, **all measurement caches** —
`dimension_relevance` 45, `temporal_profile` 58, `behavior_evidence` 56,
`outlier_profile` 55, plus 8 table-grain candidates. The grid counts
every column for every measurement aspect whether or not it could apply.
**Zero witnessed fact-aspect rows are unassessed**, which is the number
that matters: nothing a human must speak to is unspoken.

**No standing validation was declared** — the check half of the metrics
framework needs a `.rhai` file on disk and this run was MCP-only. Per the
metrics skill's own instruction I say so here rather than shipping a
self-measured snapshot dressed as a standing check. The reconciliation
that most deserves promotion is the inventory one: prior level + month's
movements = closing level held on 5,280 of 5,280 transitions, which is
exactly the standing invariant §5 says to turn into a check.

## System friction

The run's chief value.

1. **`GLOSSARY()`'s columns are still not what the skills show.**
   `glossql-add-source` §1, `glossql-relationships` §1 and §5,
   `glossql-metrics` §3 and `glossql-add-source` §4 all write
   `SELECT value FROM GLOSSARY(…)`. There is no `value` column. Worse,
   the two call forms have *different* schemas and neither is documented:
   `GLOSSARY(fin)` serves `state` (which the core skill does use), while
   `GLOSSARY(fin, all => true)` serves `subject, aspect, kind, witness,
   actor, body, written_at` — no `state`, no `value`. The refusal is
   clear and names the valid fields, so it costs one round trip rather
   than silent wrongness, but five skill call-sites teach a column that
   does not exist.

2. **`metric_cube()`'s result cannot be read through the MCP door.** All
   three invocations returned 51–54 KB on one line and were dumped to a
   file by the harness rather than returned. The cache lands correctly and
   `metric_series()` is the intended read — that part works well and is
   genuinely pleasant to slice — but the function's own call is
   effectively write-only from the agent's side. Either the cube should
   return a summary (metrics × faces × rows) with the body reachable only
   through `metric_series()`, or the door should truncate it the way row
   results are capped.

3. **`behavior_evidence` still starves systematically on document-keyed
   event tables.** `ap_invoices.amount` and `ap_payments.amount`: *every*
   anchor abstains with `fewer than two entities carry 4+ periods` and
   `viable_entities: 0`, because the only declared edge between them is
   1:1 on invoice id and no vendor dimension is landed to group by. It
   reconciles beautifully where a shared dimension exists
   (`line_amount` 0.99 over 400/400 customers with a 2.1e-17 residual;
   `inventory_positions.value` 0.977 over 239/240 products). So in a
   cash-cycle dataset the two most obviously flow-shaped columns in the
   business are the ones the measurement cannot speak to. Unchanged from
   the previous run; the suggestion there (aggregate the
   dimension-aligned anchor at month grain, or treat "fact table with a
   declared `time_axis`" as evidence) still stands.

4. **Every grounding write stales both the cube and the walk, which makes
   the fold-in cycle expensive.** With 11 rulings arriving across several
   record reads, each fold-in invalidated both caches, so a consistent
   close required re-running `metric_cube` and `metric_bands` three
   times — the cube dumping 54 KB to a file each time. A batched
   "recompute the derived caches" call, or lazier invalidation that only
   re-walks the changed metrics, would cut this sharply.

5. **The round serves per-assumption without cross-checking coherence
   between rulings.** `purchases`'s goods-only assumption was confirmed in
   the same session where `dpo`'s identically-worded goods-only
   assumption was corrected to all-supplier. Both are legitimate on their
   own aspect, but nothing surfaces the tension, and an agent that folded
   them in literally and independently would produce a `dpo` whose
   denominator contradicts its own ruled component. Worth considering: when
   a ruling corrects an assumption whose wording matches a *confirmed*
   assumption on a sibling aspect, say so in the ruling note.

6. **A red band on a single-speaker measurement aspect surfaces as
   `contested`.** Mid-run `GLOSSARY(fin)` reported 1 contested slot; it
   was `fin::metric_bands`, whose only voice is the measurement function
   (`bands_w` has `speakers: []`). The core skill defines contested as
   "voices differ on one slot and the detector's score crossed the witness
   threshold", and the remedy it teaches — read the slots, re-ground,
   re-gloss — is meaningless when there is exactly one voice. The value is
   also withheld while contested, so the walk body becomes unreadable
   through `GLOSSARY()` at precisely the moment it is most interesting.
   It cleared on the next recompute, so it is transient, but the state
   name is wrong for the situation.

7. **`GLOSSARY(<source>)` requires a dataset in use, but `DECLARE SOURCE`
   and `PROBE` do not.** On a fresh workspace `DECLARE SOURCE fin_export
   …` and a 17-file schema probe both succeeded with no dataset, then
   `SELECT … FROM GLOSSARY(fin_export) WHERE aspect = 'conventions'` was
   refused with `no dataset in use — USE one first`. The add-source skill
   tells you to read source conventions *before* probing, and source-grain
   slots are explicitly workspace-wide ("a source-grain aspect's slots
   serve in every dataset"), so the one read that is supposed to be
   dataset-independent is the one that demands a dataset.

8. **The measurement fan-out is still expensive in context.** 65 columns
   × `profile()` at roughly 1.5 KB each, with 20 `top_values` per column,
   pushed tens of thousands of tokens through the agent for results that
   are immediately re-readable from the cache. Unchanged from the previous
   run's note; a `SELECT profile() FROM t.c` returning just
   `{subject, computed_at}`, or any suppression form, would make a wide
   workspace much cheaper.

9. **Alias collisions on scalar subqueries.** `SELECT 'x' j, (SELECT
   count(*) FROM t) base, count(*) joined FROM …` is refused with
   "Projections require unique expression names … `(<subquery>)` and
   `count(Int64(1)) AS count(*)` have the same name", even though both are
   explicitly aliased. Harmless once known; the natural way to write a
   grain check side by side with its baseline.

10. **A JSON parse error in a multi-statement GLOSS batch aborts the whole
    call with no statement attribution.** A literal newline inside one SQL
    string produced `ParserError("invalid JSON body at Line: 11, Column:
    21: control character…")`. The line/column pointer is excellent and
    found it instantly — but because parsing precedes execution, the
    refusal carries none of the usual "statement 2 of 3 refused —
    statement 1 landed" attribution, so it is not obvious from the message
    alone that nothing landed. It did not cost time here; noting it
    because the two refusal shapes read very differently.

### What worked notably well

- **The `LIMIT 0` schema rehearsal now works through the door.** A
  `PROBE … LIMIT 0` returns the full `columns` array with names and types
  at zero rows. Seventeen files rehearsed in two calls, every column
  visible before any recipe was authored — including the ones a row probe
  would hide. This is the fix the previous run's workaround existed for,
  and it removed an entire class of error from the run.
- **The ruling mechanism.** A ruling landing as the judgment alone,
  never a copy of the agent's body, is the difference between the two
  runs. My groundings stayed mine, corrections reached the read
  immediately, and no answered question was ever re-asked.
- **The disclosed rival series.** `alternative_sql` on `dso` did the
  single most valuable analytical work in the run — it turned "DSO is
  rising, is that bad?" into a demonstrated artefact by putting a flat
  25-day series next to a 22.7→75.6 ramp. The cube computed it without
  being asked twice.
- **Multi-source recipes.** `ap_payments` joining `payments.csv` to
  `bank_transactions.csv` at the source landed cleanly and the outcome
  reported both scanned sources with their row counts.
- Cast accounts, `relationship_coherence`'s temporal read (0 payments
  precede their invoice; 63% precede their *due date*, correctly
  distinguished), `detect_derivations`, `detect_hierarchies`' λ scoring
  catching the region/terms confound, grain checks, and supersession.

## Comparison against the previous run's friction list

**Fixed — no longer reproduces:**

- **#4, the `LIMIT 0` rehearsal not being runnable.** Confirmed fixed
  live; schema comes back at zero rows for every file. The
  land-a-rehearsal-recipe workaround is no longer needed.
- **#1, the human slot freezing a copy of the agent's body and outranking
  it wholesale.** The root cause of that run's three worst failures is
  gone. Rulings now carry only `{aspect, assumption, dimension, stance,
  note}`; the agent's grounding is never copied, never outranked
  wholesale, and a correction reaches `read.*()` as soon as it is folded
  in. Verified on both corrections.
- **#17, "sweep until quiet" not terminating.** The round advanced
  monotonically through 11 rulings; no question returned after its
  fold-in. It was still serving at close only because I still had
  genuine below-1.0 assumptions, which is correct behaviour.
- **#2, the round asking stock/flow questions.** Did not reproduce. All
  11 rulings were on `definition` or `scope`; not one `behavior`, `sign`
  or `grain` question was served. Partly a weaker test than the previous
  run — I recorded every behavior/sign/grain assumption at 1.0 citing its
  measurement, so the enforcement was never provoked from my side.
- **#7, `metric_bands` mis-aggregating a stock grounding.** Fixed. The
  walk now reports `aggregation: "latest-sum"` for all four stock
  groundings and the actuals are correct — `inventory_value` reads
  12,399,535.83 for 2025-07, the true month-end total, where the previous
  run saw 94,463.36 from a single row.
- **#5, `rate_tolerance` polarity undocumented**, and **#16, check
  aspects creating phantom backlog**: both now taught explicitly in
  glossql-metrics §5 (`breach_rate` is the violation share, "never report
  a pass rate under this key"; `WHEN entity = '…'` to scope a check).
  Not exercised — no checks were authorable MCP-only.
- **#12, DataFusion conjunct reordering.** The engine behaviour still
  reproduces (I hit it during the `payments.date` analysis: a `CASE`
  guard in a `WHERE` did not prevent `cast(substr('15-Jan-25',4,2) AS
  INT)` from evaluating), but it is now documented precisely in
  glossql-windows §7 with `try_cast` as the fix. Fixed as a teaching gap,
  not as an engine behaviour — and worth noting the trap bit me during
  *add-source* probing while its documentation lives in *windows*.

**Persists:**

- **#14, `GLOSSARY()` column drift** — unchanged and still in five skill
  call-sites. My friction 1.
- **#3, `behavior_evidence` starving on document-keyed tables** —
  unchanged, same abstention message, same shape. My friction 3.
- **#6, the validation half unreachable MCP-only** — unchanged
  mechanically, but the metrics skill now states it plainly and tells the
  agent to say so in the read-back, which is what I did. Documented
  rather than fixed.
- **#13, measurement fan-out cost** — unchanged. My friction 8.
- **#15, projection aliasing surprises** — a sibling case reproduced
  (scalar subquery + `count(*)`). My friction 9.

**Untested this run** (out of scope, not evidence of a fix): #8 semantic
grounding collisions (none of my 14 groundings were semantically
identical), #9 the schema/batches recipe refusal, #10 aspect-grain
enforcement on SOURCE subjects, #11 the connect-time brief undercounting
human writings — my brief correctly reported 0 at connect time on a fresh
workspace, and I did not reconnect after rulings landed.

**New this run:** frictions 2, 4, 5, 6, 7 and 10 above — the cube's
unreadable result, cache invalidation cost across fold-ins, incoherent
sibling rulings, `contested` on a single-speaker slot, `GLOSSARY(source)`
demanding a dataset, and parse-error attribution.
