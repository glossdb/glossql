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
