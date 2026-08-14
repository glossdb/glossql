# Onboarding run: the medium export on a freshly rebuilt server

2026-08-14, a wiped and freshly bootstrapped workspace (0 datasets, only
the shipped system: measurement library + KPI kit), the finance
generator's **medium** export, driven MCP-only. The generator's
`ground_truth.yaml`, `metadata_truth.yaml`, `entropy_map.yaml` and
`manifest.yaml` stayed sealed for the whole run; comparing this record
against them is a separate step. Stage 0 was delegated to the agent, so
the topic and the cohort below are the agent's proposal, not a ruling.

This run is an acceptance test of the rebuilt server. The friction list
is the point; it sits at the end and is the longest section.

## Stage 0 — topic and cohort (run-scoped assumptions)

**Topic** (`DECLARE DATASET fin SET (purpose: …)`): month-end close and
working capital — order-to-cash from sales order to cash in the bank,
purchase-to-pay from vendor invoice to disbursement, inventory, and the
ledger that closes them. *Confidence 0.7* — proposed from the file
inventory and the probed schemas alone; no human shaped it.

**Cohort proposed** (7 KPIs + close integrity): net revenue, gross
margin %, DSO, DPO, DIO, cash conversion cycle, collections rate; plus
double-entry balance and a GL-to-trial-balance tie-out. *Confidence 0.7*
on the cohort being the right one for this business — all seven ground,
so the aim was not too high, and arguably too low: nothing in the cohort
forced the bank or balance-sheet tables into scope.

**Import as filter.** 14 of the 19 files landed. Deliberately excluded,
each one `DECLARE RECIPE` away: `bank_transactions` (the cohort needs no
cash-at-bank leg; it also proved a dead end as a payment-date cure —
only 12 of 26,655 rows carry a `PAY-%` reference), `balance_sheet`
(superseded by the trial balance for close integrity — in hindsight the
wrong call, see finding 3), `fx_rates` (single-currency export: USD on
every money column in every table), and the four sealed YAMLs.

## What stands at the end

- **14 tables**, 365,702 landed rows, all 14 grains verified exact
  (`COUNT(*)` vs `COUNT(DISTINCT …)`, zero duplicates, zero composite
  breaks) before any entity verdict.
- **17 declared relationships** judged from 45 candidates; the 28
  rejects stay visible in the measurement. 15 of 17 carry 0 orphans;
  the two that don't carry glossed populations.
- **94 columns fully vocabularied** — `meaning` and `role` on every
  one, `behavior` and `unit` on every measure, `dimension` on all 33
  dimension-role axes with measured grounds. Every `behavior` verdict
  cites `behavior_evidence`, including where it abstained.
- **11 metric surfaces** — 5 base extracts (revenue, cogs,
  ar_open_items, ap_open_items, inventory_value) and 6 derived
  (gross_margin_pct, dso, dpo, dio, cash_conversion_cycle,
  collections_rate). Ten were touched by the question round and
  **16 human writings stand**. One of them — `revenue` — is a
  confirmation the human later *corrected*, and the correction cannot
  reach the read: **the workspace does not currently serve the ruled
  revenue definition** (friction 1).
- **3 standing validations** with witnesses: journal balance (green),
  AP payment integrity (green at its own measured dirt), GL-to-trial-
  balance tie-out (**red**, correctly).
- Cube and walk fuelled: 28 served faces including the disclosed rival
  series; `band_breach` red at 0.998 — partly real, partly a defect
  (friction 7).

## What the measurements caught

1. **`net_amount` is the ledger; `debit`/`credit` are decorative and
   damaged.** `net_amount` sums to zero on **all 73,037 entries**;
   `debit − credit` balances on only 65,121. `debit` carries 4,211
   placeholder cells in eight spellings (`---`, `null`, `??`, `N/A`,
   `see note`, `PENDING`, `#ERR`, `TBD`) plus 524 already-empty cells;
   `credit` parses cleanly but holds negatives down to −3,325,212.00,
   which a credit column cannot. **I did not reconstruct `debit`.**
   `detect_derivations` proposes `debit = credit + net_amount` at only
   96.2% (5,811 violations), and reconstructing the dirty rows yields
   *negative debits* — so the identity is not sound enough to repair
   with. The cells stay NULL and the gloss says to read `net_amount`.
2. **The trial balance's columns are period turnover, not balances.**
   `behavior_evidence` reads both as flows (r_flow 0.058 vs r_stock
   1.003, 23/28 accounts, agreement 1.0), and a hand check confirms it:
   revenue account 4110 shows `debit_balance` 0.00 every month and
   2025-02 does not accumulate 2025-01. The names lie; the glosses say
   "Period Debit Turnover".
3. **The tie-out has two material breaks.** On the *net* basis 325 of
   336 account-periods tie exactly. The 11 that don't:
   **Inventory (1400) 2025-02, off by 289,836.61 with the sign flipped**
   (TB +248,950.44 vs GL −40,886.17), **Trade Receivables (1210)
   2025-08, off by 181,655.08**, Accumulated Depreciation 2025-10 off by
   −5,137.40, and eight at sub-dollar rounding. On the debit/credit
   basis only 46–67 of 336 tie — that apparent 2.6% gap is entirely an
   artifact of the damaged pair, not a close failure. Consequence: with
   `balance_sheet` left out, this workspace has **no true balance
   artifact at all**; the trial balance cannot supply one.
4. **`payments.date` mixes three formats with no marker** — `dd-Mon-yy`
   (5,030 rows, unambiguous) and `MM/DD/YYYY`/`DD/MM/YYYY` assigned per
   row at random (9,898 rows: 2,958 forced to DD/MM by a day > 12,
   2,958 forced to MM/DD, 3,982 genuinely ambiguous). The recipe
   resolves by a four-rung ladder — named month, then whichever slash
   reading is a valid date, then "a payment cannot precede its invoice"
   (resolves 1,516), then "a payment cannot postdate the ledger horizon
   of 2026-02-12" (resolves 143 more) — and lands
   **`payment_date_certain`** beside the date. 12,619 certain (84.5%),
   2,309 undecidable (15.5%), which default to MM/DD and are excluded
   from every AP ageing figure rather than guessed. I deliberately did
   *not* use `due_date` as a tiebreak: resolving payment dates by the
   terms and then measuring DPO would be circular.
   The flag validated itself — before the horizon rung, every payment
   dated past 2026-01-31 (up to 2026-12-02, ten months beyond the data)
   was flagged uncertain, while certain ones stopped at 2026-02-12,
   exactly the ledger's last entry.
5. **746 orphan payments** naming literal `ORPHAN-######` invoices —
   verified by anti-join that *all* 746 non-matching rows are the
   literal population, none a real missing invoice. 5.0% of
   disbursements; glossed on the edge, and the edge's gloss says use
   `LEFT JOIN` for any cash-out total.
6. **`region` and `payment_terms` are perfectly confounded.**
   DACH↔net_30, Nordics↔net_60, Benelux↔net_90, UK&I↔due_on_receipt,
   exactly 100 customers each, λ = 1.0 both ways. **Any "DSO by region"
   result is arithmetically identical to "DSO by credit terms"** — the
   two cannot be told apart in this data. I kept them as separate axes
   (they are different concepts, not an alias to collapse) and recorded
   the confound on the `ar_invoices.customer_id → customers` edge.
7. **Order date equals invoice date on all 14,770 orders**, zero lag —
   there is no order-to-invoice stage in this business, so revenue and
   COGS share one time axis and the order-to-cash cycle is invoice-to-
   cash only.
8. **Three-way agreement on the money.** Revenue reads
   **178,031,998.51** from the AR subledger, from order lines, and from
   the GL's three operating revenue accounts — to the cent. COGS reads
   **122,857,302.90** from order-line costs, from GL account 5100, and
   from the value of stock issues — to the cent. Adding the interest
   income the human ruled into scope brings revenue to
   **178,062,300.90**, which is exactly the GL's revenue-typed accounts
   including 4310 — so the ruled figure reconciles as cleanly as the
   unruled one. The definitional
   choice is therefore only about time axis and interest income, which
   is what the recorded alternatives say.
9. **`stock_movements.source_document` is polymorphic.** It names a
   sales order line on issues (23,613, all resolving), a vendor invoice
   on receipts (7,817) and a stock count on adjustments (329). The
   8,146 orphans are *exactly* receipts + adjustments. Declared with
   the slice stated in the gloss.
10. **Cancelled AP invoices never reach the ledger** — `entry_id` is
    NULL on exactly the 352 cancelled invoices, so the inner join to
    `journal_entries` silently drops them; the gloss mandates
    `LEFT JOIN`.
11. **Never-collected receivables are exactly the open+overdue
    population** — 3,410 AR invoices carry no receipt, and
    1,178 open + 2,232 overdue = 3,410. The 593 `partial` invoices do
    have a receipt, for less than the invoiced amount.
12. **Corridor breaches.** Revenue steps up ~30% in Q4 (Oct 18.92M
    against a p95 of 14.72M, PIT 0.999) and COGS moves with it (PIT
    0.999) — that reads as business, not defect. Collections did *not*
    follow (Oct PIT 0.0, 0.681 against p05 0.799), which is the
    interesting pairing: October billed hard and collected poorly.
    Gross margin dipped below its corridor in September (29.74%, PIT
    0.0). `inventory_value`'s two 0.999 breaches are **false alarms**
    from a measurement defect — friction 7.

## The numbers

| metric | Jan | Dec | note |
|---|---|---|---|
| revenue | 13.57M | 17.30M | Q4 step change |
| gross margin % | — | 30.36% | 31.0% for the year |
| DSO | 22.5 d | 72.4 d | rises mechanically, no opening AR |
| DPO | 15.6 d | 107.6 d | same, and Jan is depressed by an opening load |
| DIO | 41.0 d | 31.8 d | **the only trustworthy level** |
| CCC | 47.9 d | −3.4 d | inherits both distortions |
| collections rate | — | 75.6% | Jan structurally understated |

**The level of DSO, DPO and CCC is not trustworthy for 2025.** AR
builds monotonically 9.85M → 40.41M and goods AP 11.17M → 42.14M, both
from zero with no drawdown in any month, because no opening position
exists. DIO escapes this because `inventory_positions` carries real
month-end levels (flat at 12.34–12.51M all year). Every grounding says
so in its `scope` assumption at confidence 1.0.

## What the round ruled — and the correction it could not carry

The question round fired on record reads as designed — one form per
`ATTEST()`/store-relation call, ordered lowest-confidence first — and
landed answers server-side with human standing. **16 human writings
across 10 metric aspects**. Verified on two of them that a ruling copies
the agent's body and rewrites the served assumption's `basis` to
`human-ruled` with `confidence: 1.0`, leaving the others untouched. For
every *confirmation*, the SQL was unchanged and the reads returned
identical numbers, so no materialization was owed.

**Then the human corrected one, and the correction could not reach the
read.** Late in the run the lead ruled: *revenue includes interest
income* — rejecting the scope I had recorded. I re-grounded it as a
union of the AR subledger and GL account 4310, totalling
**178,062,300.90**, which matches the GL's revenue-typed accounts to the
cent, and updated the `definitions` registry in the same act.
`read.revenue()` still serves **178,031,998.51** — the rejected
definition.

The mechanism: the lead's *earlier* ruling on `revenue` (a confirmation,
13:11:27) wrote a human slot holding a frozen copy of my then-current
body, which excluded interest income. The human slot outranks the agent
slot wholesale and regardless of timestamp, so my corrected grounding
(13:25:02) is invisible to the read. The correction itself arrived
through the round's decline/correction channel, which writes no slot —
and per the language's own rule, "that a `GLOSS` was logged as human is
the entire record of an answer", so an unrecorded correction cannot
govern. Neither slot is `contested`; both read `current`.

**As it stands, the workspace serves a revenue definition the human
explicitly rejected, and no statement available to the agent can fix
it.** Superseding my own slot does not help; writing as `human` is not
the agent's to do; striking the human slot would override human standing.
This wants a ruling — see friction 1.

`inventory_value` was never served; its assumptions were already at 1.0.

## Open judgment questions

These stand on the agent's judgment alone. Options are what I would
have offered.

1. **Revenue scope — is gross billed the same as net revenue?**
   (a) yes, the export carries no credit notes, returns or rebates
   (what I assumed); (b) no, a credit-note extract exists elsewhere and
   revenue is overstated. *Grounds: `ar_invoices.amount` has no negative
   or zero rows in 14,770 and no credit-note table exists in the export.
   Confidence 0.7 — the absence of a table is weak evidence for the
   absence of the business fact.*
2. **Gross margin family.** (a) revenue − COGS (what I used, textbook);
   (b) revenue − all operating expenses, which would pull in the 18.1M
   of expense-category AP and read closer to operating margin.
   *Confidence 0.7; the rival is recorded runnably and the cube charts
   both.*
3. **DSO family.** (a) period-end AR over the same month's revenue
   (what I used); (b) countback/annualized, far less sensitive to the
   Q4 billing step — which matters here precisely because Q4 steps.
   *Confidence 0.7.*
4. **DPO denominator.** (a) goods purchases only, keeping DPO
   comparable with DIO for the cycle (what I used); (b) total spend
   including expense invoices, ~10% lower and a general payables
   metric. *Confidence 0.7.*
5. **Collections rate reading.** (a) cash received in a month over
   revenue billed that month — a cash-management view across cohorts
   (what I used); (b) cohort settlement: of invoices billed in month w,
   the share ever collected — a credit-quality view. *Confidence 0.6;
   human-ruled during the round, but recorded here because the two
   answer different questions and the choice deserves to be visible.*
6. **Are the 352 cancelled AP invoices in the payables population?**
   (a) keep them as never-settled (what I used); (b) exclude — they
   have no GL entry and never posted. *Confidence 0.6.*
7. **Do partially-paid AR invoices leave the receivable in full?**
   (a) yes, closed at receipt date (what I used); (b) no, the
   unreceipted remainder stays outstanding. 593 invoices, 4.0%.
   *Confidence 0.6.*
8. **Is regional credit policy real?** Every DACH customer is net_30,
   every Nordics customer net_60, and so on. (a) it is deliberate
   regional credit policy; (b) it is an export artifact and the two
   axes should not both be offered. *This one is not a definition
   choice but a fact about the business I cannot check — it changes
   whether "DSO by region" is a meaningful chart or a duplicate of
   "DSO by terms".*

## World-coverage wishes (documents, not decisions)

- **Opening AR and AP positions at 2024-12-31.** Every receivable and
  payable level shifts by them, and with them DSO, DPO and CCC become
  trustworthy as levels rather than only as shapes. This is the single
  highest-value missing artifact.
- **The close package for Inventory 2025-02 and Trade Receivables
  2025-08** — were the two material tie-out breaks manual close
  adjustments booked outside the journal, or a genuine break?
- **`payments.date` in the source system's own format**, or the export
  job's format spec — recovers the 2,309 dates that no constraint in
  the data can decide.
- **A credit-note / returns extract**, or written confirmation that
  none exist — settles question 1.
- **Bad-debt provision and write-off policy** — 3,410 invoices
  (23% by count) were never collected in 2025 and nothing in the data
  ages, provisions or writes them off.
- **`balance_sheet.csv` landed** — cheap, and the only route to a real
  carried balance now that the trial balance is known to be turnover.

## The checks face at close

`ATTEST(fin)` — 140 rows, **138 green, 2 red**:

- **`trial_balance :: tb_gl_tieout` — red, score 0.0327.** Correct and
  wanted: 11 of 336 account-periods breach a 0.0 tolerance, two of them
  material. This is the run's honest red corridor.
- **`fin :: metric_bands` — red, score 0.998** (`band_breach`). Partly
  real (the Q4 revenue/COGS step, the October collections miss, the
  September margin dip) and partly a defect: `inventory_value`'s two
  0.999 PITs are computed from wrong actuals — friction 7.
- `journal_lines :: journal_balanced` — green (breach rate 0.0).
- `ap_payments :: ap_payment_integrity` — green (breach 0.04997 against
  a tolerance of 0.05 set at the defect's own measured level).
- All 137 `role`/`behavior`/`unit` slots green.

Unassessed backlog: `outlier_profile` 94, `temporal_profile` 85,
`behavior_evidence` 83, `dimension_relevance` 61 — these are
measurement caches, not claims, and the grid counts every column for
every measurement aspect whether or not it could apply. The genuine
remainder is small: 9 `derivation_candidates`, 1 `hierarchy_candidates`,
and 2 phantom rows on the source `erp` (friction 10).

## System friction

The run's chief value. Ordered by how much each cost.

1. **The human slot freezes a copy of the agent's body and outranks it
   wholesale — so agent corrections can never reach the winning slot,
   and the round re-derives the same questions forever.** This one
   root cause produced three separate failures the lead had to stop the
   run for, and it is the most important finding of the run.

   When a ruling lands, the system copies the agent's *then-current*
   body into the human slot and rewrites only the served assumption.
   The human slot then outranks the agent slot entirely, at every read,
   regardless of timestamp. Consequences, all observed live:

   - **A human correction cannot govern.** The lead ruled that revenue
     includes interest income; I re-grounded; `read.revenue()` still
     serves the rejected definition, because their earlier confirmation
     froze the old body. No statement available to the agent fixes it.
   - **The round re-asks answered questions.** Because the correction
     writes no slot, the assumption stays below 1.0 in the winning slot
     and is re-derived on the next record read. The lead: *"If I add a
     correction, the question was answered, do not ask the same
     question again."*
   - **Agent fixes are invisible to the round.** I raised every
     `behavior` assumption to 1.0 with a measurement basis (below), but
     the round kept serving stock/flow questions — it derives from the
     *winning* slot, which is the human's frozen copy still carrying
     `behavior` at 0.8. Sweeping the round made this worse, not better:
     each sweep re-served from the stale copy.

   Candidate fixes, in order of how much they'd have helped here: a
   ruling should merge into the live agent body rather than freeze a
   copy (or store only the ruled assumption, not the whole body); a
   correction delivered through the round must write a slot, not just a
   message; and the round must derive from the composition of human
   ruling over current agent body, not from the frozen human body alone.

2. **The round asked the human stock/flow questions — the one thing
   ruled out on 2026-08-13 — and kept asking after they were closed.**
   The lead twice: *"I cannot answer stock vs. flow questions. YOU MUST
   USE THE BEHAVIOUR function."* Two compounding causes. First, the
   round derives a question from *any* assumption below confidence 1.0
   with no regard for its `dimension`, so an honest
   `{"dimension": "behavior", …, "confidence": 0.8}` — exactly what you
   write when `behavior_evidence` starves (friction 3) — becomes a human
   question. The skills forbid asking; nothing in the round enforces it.
   Second, once I closed them by measurement, friction 1 kept them open
   anyway. Fixes: the round should skip the dimensions the function map
   owns (`behavior`, `sign`, `grain`), *and* the skills should forbid
   recording a `behavior` assumption below 1.0 — require citing the
   measurement even when it abstains. I closed mine the second way
   (re-ran the function on four more columns, cited the GL mirror for
   the starved ones, raised to 1.0), but nothing in the system would
   have stopped me shipping the 0.8.
3. **`behavior_evidence` starves systematically on document-keyed event
   tables**, which is what created friction 2. Every 1:1 alignment at
   day grain returns `fewer than two entities carry 4+ periods` with
   `viable_entities: 0`. It reconciles well when a shared *dimension*
   exists (product_id, account_id: `line_cost` 0.947 over 234/240,
   `net_amount` 0.879 over 28/28, `inventory_positions.value` 0.977).
   It cannot settle `ap_payments.amount` at all (every anchor abstains),
   and settles `ar_invoices.amount` and `receipts.amount` only on 2–3
   voters. So in an order-to-cash dataset the *most obviously
   flow-shaped columns in the business* are precisely the ones the
   measurement cannot speak to. Worth considering: aggregate the
   dimension-aligned anchor at month rather than day grain, or treat
   "fact table with a declared `time_axis`" as evidence in its own
   right.
4. **The `LIMIT 0` schema rehearsal the add-source skill mandates is
   not runnable through the door.** The skill is emphatic — "Run it per
   file before authoring any recipe; row probes cannot replace it" —
   and it is right: the previous run lost three columns and a
   relationship this way. But `PROBE erp AS $$… LIMIT 0$$` returns
   `{"row_count":0,"rows":[],"truncated":false}`. The outcome shape has
   no schema field, so an empty result carries nothing. `DESCRIBE
   read_csv('x.csv')` does not parse at the source, and `struct(t.*)`
   does not either. **Workaround used**: land a `LIMIT 0` recipe under
   the final table name, `DESCRIBE` it, then supersede with the real
   recipe. It works and it earned its keep immediately — it exposed
   `customers.churned_date` and `products.discontinued_date`, both null
   in every sampled row and both invisible to row probes, i.e. two of
   the exact three columns the previous run missed. But the taught
   mechanism does not exist; either `PROBE` should return the schema on
   an empty result, or the skill should teach the rehearsal-landing.
5. **`rate_tolerance`'s `rate` is a *breach* rate, and nothing says
   so.** I wrote `journal_balanced` with `rate: 1.0` meaning "100% of
   entries balance" against `tolerance: 0.0` and it banded **red** at
   score 1.0. Restating as breach rate 0.0 turned it green. The metrics
   skill never states the polarity, and its one worked example
   (`"expected_rate": 0.895` for a known-dirt source) reads like a pass
   rate, which is what led me wrong. One sentence in the skill fixes
   this permanently.
6. **The validation half of the metrics framework is unreachable
   through the door.** §5's pattern is expectation gloss + *function
   voice*, and a function needs `DECLARE FUNCTION … FROM 'f.rhai'` —
   a file on disk. There is no in-language function body. An
   onboarding agent working MCP-only (as this run was constrained to
   be, and as the door's own premise suggests) cannot author a check at
   all; the pattern degrades to a single agent gloss carrying both the
   expectation and the measured rate, which is what I shipped. That
   also means `ACCEPTS (imports)` never fires, so my three checks will
   not recompute on the next import — they are snapshots, not standing
   checks. Either an in-language form is needed, or the skill should
   say plainly that checks require filesystem access.
7. **`metric_bands` mis-aggregates a `behavior: "stock"` grounding.**
   It reports `aggregation: "last"` and takes the last *row* of the
   month instead of summing entities within the month and taking the
   last month. `inventory_value` serves 480 rows per month, so the walk
   read actuals of 94,463.36 / 16,240.50 / 21,690.15 / 29,463.84 /
   3,133.44 / 111,888.64 where the true month-end totals are ~12.4M.
   `metric_cube` computes the *same metric* correctly
   (12,457,138.06 for 2025-01) — so the fix landed in the cube and not
   in the walk. Every band, PIT and breach verdict for a multi-row
   stock is meaningless, and two false 0.999 PITs feed `band_breach`'s
   red. This was already logged as an open product note on
   2026-08-13 for the cube; it is still live in the walk.
8. **`detect_grounding_collisions` misses semantic collisions.**
   `revenue` and `ar_open_items` serve **identical** monthly series
   (difference 0.0000 in all 12 months) and it reports zero collisions,
   because it buckets by canonical SQL and their SQL differs. This is
   exactly the failure the read exists to prevent. Bucketing by the
   served series — which the cube has already computed — would catch
   it. Related and worse: the cube's default face for an open-items
   extract is `sum(value)` per month, which for `ar_open_items` is
   *billings, not balance* — a plausible-looking, meaningless number,
   and nothing in the framework can mark an extract whose aggregate
   requires a window filter. The "open items" shape was forced on me
   because no balance table exists; the framework has no vocabulary for
   it.
9. **A recipe refusal named neither the statement nor the column.**
   `Error during planning: Mismatch between schema and batches` —
   sent as statement 2 of a 7-statement call, which aborted the
   remaining 5 silently; I had to read `imports` to discover what had
   landed. The cause was `CASE … THEN true ELSE false END`: a
   non-nullable boolean literal. `try_cast(CASE … THEN 1 ELSE 0 END AS
   BOOLEAN)` lands fine. Both the message and the abort-without-
   attribution cost real time.
10. **Aspect grain is not enforced on SOURCE subjects.** `entity`
   declares `grains: table`, yet `GLOSS entity ON erp` — a source —
   was **accepted**. The add-source skill states the opposite: "The `ON`
   grain in each declaration is the contract: glosses outside it are
   refused." The `unassessed` grid agrees with the bug, listing `erp`
   under both `entity` and `meaning`, so the backlog carries two rows
   that can never legitimately be filled and cannot walk to zero. (I
   deleted my probe gloss.)
11. **The connect-time brief said "0 human writings stand" while 16
    stood.** At the same moment `SELECT count(*) FROM glossary WHERE
    actor_kind='human'` returned 16 across 10 aspects. The brief is the
    first thing the core skill tells an agent to trust, and the one
    number in it that governs whether to sweep the round — so an agent
    resuming this workspace tomorrow would be told nothing stands and
    would skip the read-back the skill calls "the one unforgivable
    onboarding error" to miss.
12. **DataFusion conjunct reordering defeats guard predicates.**
    `WHERE date LIKE '__/__/____' AND cast(substr(date,4,2) AS INT) <= 12`
    evaluated the cast on rows the `LIKE` should have excluded —
    `substr('01-Jan-25',4,2)` is `'Ja'` — and errored. Inside a join
    subquery the guard simply does not guard; only `try_cast` is safe.
    Worth a line in glossql-windows, since the natural way to write a
    format-family filter is exactly this.
13. **A measurement cannot be triggered without pulling its whole
    body.** `SELECT subject FROM (SELECT profile() FROM t.c)` is
    refused — the planner reads `t.c` as a catalog path
    (`table 'datafusion.ar_invoices.status' not found`). With 94
    columns and profiles carrying 20 `top_values` each, the fan-out
    forces tens of thousands of tokens of cache-warming output through
    the agent's context for results it will re-read from `GLOSSARY()`
    anyway. A `SELECT profile() FROM t.c` that returned just
    `{subject, computed_at}` — or any suppression form — would make the
    measurement plane far cheaper on a wide workspace.
14. **`GLOSSARY()`'s columns are not what the skills show.** The skills
    write `SELECT value FROM GLOSSARY(erp) WHERE aspect = 'conventions'`
    and `SELECT subject, aspect, value FROM GLOSSARY(orders)`; the
    actual columns are `subject, aspect, kind, witness, actor, body,
    written_at`. There is no `value` and no `actor_kind` (the *store
    relation* `glossary` has `actor_kind`, the function does not).
    Worse, the wrong spelling failed silently on an empty result — my
    first conventions read returned 0 rows rather than an error, so the
    drift stayed invisible until a later query happened to be
    non-empty.
15. **Aliasing a projection to its own qualified source name is
    refused**: `round(j.gl_net,2) AS gl_net` gives "Schema contains
    qualified field name j.gl_net and unqualified field name gl_net
    which would be ambiguous". Harmless once known, surprising the
    first time, and the natural way to write a rounding projection.
16. **Check aspects declared `AS FACT ON TABLE` create phantom
    backlog.** Each of my three validations shows 14 unassessed rows —
    one per table — when the check applies to exactly one table. The
    grain vocabulary has no way to say "this subject only", so standing
    up 3 checks added 39 owed slots that should never be filled.
17. **Round cadence — the transport is right, the loop is not.** Forms
    rode record reads exactly as the 2026-08-14 ruling describes: one
    per call, lowest confidence first, never interrupting a landing.
    Answers landing server-side with human standing, never travelling
    through the agent's mouth, is a genuinely good mechanism and it
    worked. What does not work is the loop around it. The skill says
    "sweep the round until it stays quiet", but with friction 1 in play
    sweeping re-serves the same questions from the frozen slot, so the
    instruction actively harms: I made ~11 sweep calls and the lead had
    to stop me. Every write of mine also reopened the round, and the
    decline landed three times. Until a ruling merges rather than
    freezes, "sweep until quiet" is not a terminating loop — the agent
    needs either a served-question ledger or an explicit "this question
    is closed" state that a decline or a correction sets.

### What worked without complaint

Recipe supersede-and-reland (and its correct invalidation of the
superseded table's cached profiles — I re-ran the 7 `ap_payments`
profiles because of it). The cast account naming the actual tokens with
frequencies, which is what made the `debit` verdict possible in one
read. `relationship_coherence`'s temporal read. `detect_derivations`
independently reproducing the `debit = credit + net_amount` violation
count I had found by hand. Grain checks. The cube's rival series from
`alternative_sql`. Supersession and the `all => true` slot read.
