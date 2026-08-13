# Onboarding run: the medium export, end to end

2026-08-13, the fresh-workspace fixture (`~/glossql-ws`, rebuilt binary,
empty at start), the finance generator's **medium** export — the set
with planted dirt — driven MCP-only by the agent with the lead
answering the door's question forms live. The generator's
`ground_truth.yaml` stayed sealed for the whole run; comparing this
record against it is a separate step.

## What stands at the end

- 17 tables, ~431k rows, all casts clean after three recipe
  amendments; every grain verified exact before any entity verdict.
- 21 declared relationships (one composite), judged from 50
  candidates; the rejects stay visible in the measurement.
- 109 columns fully vocabularied (meaning, role; behavior and unit on
  every measure — behavior always from `behavior_evidence`, never
  guessed); dimension verdicts on 21 axes with measured grounds.
- 7 metric surfaces (revenue, cogs, gross_profit, ar_balance, dso,
  inventory_value, cash_position), every one **human-ruled** through
  the question round — 12 human writings govern the reads.
- 1 standing check: the journal line identity, green at 3.68%
  measured against 3.7% accepted dirt.
- The cube and the walk fueled; `band_breach` red at 0.998 — real,
  judged (below). `/app/metrics` serves the pulse, dossiers with the
  rivals charted, slices by account and cost center.

## What the measurements caught (the run's teeth)

1. **`journal_lines.debit` sentinels** — eight spellings (`TBD`,
   `N/A`, `null`, `---`, `PENDING`, `??`, `see note`, `#ERR`), 4,211
   cells. Recovered as `net_amount + credit`; the identity is now a
   standing check.
2. **5,811 violating lines ship in the export** (3.7%): parsed rows
   where `debit - credit ≠ net_amount`. Accepted as known dirt in the
   check's expectation; a rate above it means the pipeline got worse.
3. **`payments.date` mixes three formats** (`%m/%d/%Y`, `%d/%m/%Y`,
   `%d-%b-%y`). The temporal read caught the first cast landing
   misparses (month-start-heavy dates past the export's own end); the
   recipe rescues provable misparses via the export-window bound.
   The undetectable residue has a detector: **769 payments predate
   their invoice** (coherence temporal read).
4. **746 orphan payments** naming `ORPHAN-9xxxxx` invoices — a
   coherent planted population, glossed on the edge.
5. **The trial balance lies twice**: its columns are monthly
   *turnover*, not balances (`behavior_evidence`: flow at r_flow
   0.027 — the name lies), and it **collapses from 28 accounts to 4**
   in 2026-01/02 while the balance sheet stays whole (the composite
   edge's orphan finding).
6. **`ending_balance` is the true stock** — its monthly delta equals
   net journal postings to machine precision, sign as-posted, no
   negation. The "negative asset" reading dissolved: **the export
   provably carries no opening balances** (first month's level equals
   first month's postings to the cent), so levels are
   postings-since-2025-01; trends exact, absolutes offset by the
   unknown 2024-12-31 position.
7. **AR frozen at 21,145,181.49 for Dec–Feb** — receivables stop
   moving exactly where the trial balance collapses: the data
   boundary showing through, not a business event.
8. **The corridor breaches** (PIT ≥ 0.998): October's
   revenue + COGS + AR spike moves together — reads as business;
   inventory's September dip (PIT 0.0) and the frozen AR read as
   data artifacts.
9. **`region ↔ payment_terms` is a perfect alias** (λ = 1.0 both
   ways — each region carries exactly one term); 1,953 credits of
   exactly 10,000.0; 284 inventory positions predate their product's
   launch; 525 counterparty spellings carry suffix variants of the
   same firms; cancelled invoices carry no GL entry (clean semantics,
   glossed).

## What the lead ruled (through the forms)

All seven surfaces' definitional choices — COGS as 5100 alone, the
DSO conventions, revenue's interest-income inclusion and
recognized-vs-billed reading, gross profit vs the operating-profit
family, the cash composition, the AR source, the inventory scope —
confirmed or corrected via the round; the `cash_position` "wrong"
answer drove the opening-balance measurement in finding 6.

## World-coverage wishes (documents, not decisions)

- The **2024-12-31 closing balances** — every level shifts by them.
- A **canonical counterparty list** — unlocks the bank counterparty
  axis (ruled out until names resolve).
- A **trial balance re-export for 2026-01/02** — or confirmation the
  collapse is expected upstream.

## What the run changed in the product (fixed live)

1. **The round asks judgment only** (ruled): owed statistical claims
   (behavior, unit) left the human round — the shipped functions
   settle them; the skills now carry the function map. The lead's
   framing: it is easier to confirm a judgement than author an
   answer — the facts on the table, the decision still theirs.
2. **Sequential rulings composed from the wrong slot** — ruling a
   second assumption reverted the first (composed from the agent
   body), which re-derived and re-asked: the live loop. Fixed:
   rulings compose from the winning slot; regression test added.
3. **`relationship_coherence` choked on composite endpoints** — a
   tuple passes the two-segment guard and quotes as one impossible
   column. Fixed with the same skip `behavior_evidence` carries.
4. The add-source skill taught a wrong column name (`grain` →
   `grains` on the aspects relation).

## Remaining tail (agent work, small)

The judged-negative sweep (`behavior`/`unit` `none` on non-measure
columns) so the unassessed backlog walks to zero; pair-path `meaning`
on the remaining 14 declared edges; the counterparty resolution.

## Open product notes from the run

- `metric_cube`'s stock **total** series takes last-per-month of one
  row; for a multi-row-per-period stock extract that is one arbitrary
  entity, not the summed level. Dodged in this run by pre-summing
  cross-entity stocks in their groundings (scope disclosed); the
  honest fix is per-entity last, then sum.
- The round asked questions mid-run while the agent was still
  working the flows (forms fired on the agent's tool calls before
  the framework existed). Livable now that stats never ask, but the
  cadence of *when* the round engages the human may want a ruling.

## Validation against the sealed truth (unsealed after commit b53a789)

The generator planted **15 injections**. Scored: **7 clean catches**
(debit sentinels · obscured `vid`/`pt` · payment date corruption ·
the ORPHAN- references · `drift_formula` on trial_balance — caught
by `behavior_evidence` as "turnover, the name lies" · the trial
balance break · mutual-exclusivity showing as the 5,811 identity
violations), **4 partials** (cost_center nulls seen but explained
with an unverified story; the 10,000.0 credit spike flagged but not
connected to revenue; bank round-number mass noted, no Benford
check; the GL↔invoice match seen only as a rejected join candidate),
**3 misses** (temporal drift in bank amounts · mixed units in 1% of
invoice amounts · the payment↔bank match — whose join column
`bank_transactions.payment_id` was never landed).

**The landing miss that caused the relationship miss**: probe rows
omit null fields, so three all-null-leading columns
(`customers.churned_date`, `products.discontinued_date`,
`bank_transactions.payment_id`) were invisible in LIMIT-3 probes and
left out of the recipes. The add-source skill's LIMIT-0 schema
rehearsal exists for exactly this and was skipped. 18 of the 19
canonical edges were recovered; the 19th rode the unlanded column.

**Semantics: 14 of 14 stock/flow truths match** the behavior
verdicts, including both trial_balance columns (additive — the
discriminator out-judged the name) and the judged `fx_rates.rate`
point-in-time call. Entity and dimension verdicts consistent with
`table_roles`/`bus_matrix` on spot inspection.

**The rulings, decomposed** (the lead asked "were my rulings wrong"):

- **revenue** — the ruled GL reading is *contaminated, not
  mis-defined*: formula choice (canon credit-only vs ruled
  credit−debit) moves ±60k/month, but the injected credit outliers
  inflate the landed GL ~750k/month. The **disclosed rival (billed
  order lines) matches canon to within that month's other income**
  (Jan: 13,570,747 vs 13,573,300). The dossier's two-line story tile
  is this finding, drawn.
- **dso** — canon uses actual days (the disclosed rival), the ruling
  took flat-30: a real divergence, but small next to —
- **ar_balance** — the grounding (mine, confirmed by the ruling)
  used 1210 only; canon includes 1220 Other Receivables, which is
  the same size (Dec: 21.1M + 21.1M = canon's 42.2M exactly). Canon
  DSO ends 2025 at 75.6 days vs the served 34.5 — **the AR scope is
  the dominant error, and it was never disclosed as an assumption**,
  so no question ever surfaced it.
- **cogs** — the 5100-alone ruling matches canon scope; but my
  grounding nets credits (debit−credit) where canon takes debit-only
  (returns not netted): 420k/month divergence **never disclosed as
  an assumption**, so never asked.
- **cash_position, gross_profit, inventory_value** — match canon
  (inventory via the ties-to-GL invariant).

**The lesson with teeth**: every wrong number traces to an
*undisclosed* assumption or an unlanded column — never to a wrongly
answered question. The round worked; what it was shown was
incomplete. An assumption you leave out is a question nobody is ever
asked — measured here at 2× on DSO.

Not built, deliberately (scope, not error): purchases, expenses,
operating_income, margins, ap/dpo, dio, ccc, fcf, the db1 entity
family.
