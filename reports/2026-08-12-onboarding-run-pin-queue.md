# 2026-08-12 — Onboarding run on a fresh workspace, up to the pin queue

The real run the pin loop owed (build line item 1). A fresh workspace
(`~/glossql-ws`, latest binary, empty at start), the finance
generator's clean export as the source, the agent driving every flow
through the MCP door by the skills as written, ending with the
definitional agenda served as the model app's pin queue. The human
gesture — signing pins in the browser — is the half that follows this
record.

## The run

~35 door calls. `fin` declared; `erp_export` (CSV, 17-file directory)
declared; the conventions read before probing came back empty —
first contact with this system. Probes confirmed clean parses
everywhere; eleven tables landed by authored recipes (chart,
entries, lines, trial balance, balance sheet, customers, orders,
order lines, products, AR invoices, receipts — 302k rows total), all
casts clean. Vocabulary (meaning/entity/role/behavior/unit +
dimension) declared with witnesses; every column glossed — including
the judged negatives: `unit: none` and `behavior: none` with grounds
on every non-magnitude column, the dimensions pattern applied beyond
its home aspect.

Relationships: 26 candidates from `detect_relationships`, 11
declared after judging (amount-value joins, a `period_date ->
launched_date` date coincidence, and composite "rescues" whose
anchors already resolved were rejected; a transitive
line-to-invoice edge left underived). Every declared join
grain-checked exactly. Dimensions: relevance scored on eight axes,
verdicts glossed with three judged negatives; all hierarchy
candidates rejected (one alias was real-but-coincidental, below).

Metrics: `revenue` grounded on the ledger lane (Product + Service
account families), `dso` recorded composing `FROM read.revenue()`,
formulas + definitions glossed thin-aspect style, no grounding
collisions, `metric_bands` walked both metrics over the twelve 2025
months.

## What the measurements caught (clean data, real teeth)

- **`trial_balance` lies about itself.** Its columns are monthly
  activity totals, not cumulative balances — `behavior_evidence`
  read r_flow ~ 1e-17 against the month's line debits before I did;
  my first entity gloss asserted "cumulative" from the name and was
  superseded after direct verification. The skills' "names lie"
  warning, demonstrated on the second table of the run.
- **`balance_sheet.ending_balance` is a proven stock**: cumulative
  `net_amount` at r_stock ~ 5e-16, 8/8 accounts — the standing
  invariant a future check function should carry.
- **The two revenue lanes are one fact**: ledger sales revenue
  equals invoiced amounts to the cent, every month of 2025.
- **The GL routes only ~50–55% of invoiced sales through Trade
  Receivables**, and neither payment-terms split explains the share.
  Recorded as a measured gap with no causal story (the
  relationships skill's rule); the credit-sales-only DSO denominator
  is therefore a world-coverage wish, not a formula choice.
- **2026 is settlement tail**: asset/liability pairs only, no P&L
  postings after 2025-12 — revenue and DSO have exactly twelve
  months of history, and the AR subledger ends 2025-12-31 while the
  GL continues.
- **`region ↔ payment_terms` is a perfect bijection** in this
  export — a data coincidence kept apart by meaning, glossed on both
  columns so no later reader trusts it as a rule.
- Postings are strictly leaf-account (verified 0 parent postings),
  so summing typed accounts cannot double-count the hierarchy.

All of it deposited where it belongs: dataset-local evidence in
dataset glosses, and the source-system facts — ISO dates, single
currency, the sign identity, the trial-balance naming wart, the
document flow — in a `conventions` gloss at SOURCE grain
(`AS FACT ON SOURCE`, the 2026-08-12 ruling, first real use). The
deposit half of the per-source story is now exercised; the
read-before-first-probe half still needs a second dataset from this
system.

## The agenda

Three genuinely open definitional choices, sized from the data,
glossed as `pin_questions` (six option rows, one proposed each):

1. **revenue scope** — Product + Service families only (proposed,
   0.7) vs all revenue-typed accounts. Interest income + FX gains
   are 30,302.39 of 178.06M (0.017%): materially irrelevant today,
   definitionally not.
2. **DSO numerator** — Trade Receivables 1210 only (proposed, 0.7)
   vs 1210 + 1220.
3. **DSO day-count** — actual calendar days (proposed, 0.6) vs the
   360-day banking year. Targets the `formulas` aspect, where the
   executable convention lives.

The model app serves all six rows; the frame and page were fetched
over HTTP and verified non-empty before handover.

## A wrinkle this run will demonstrate

Questions 1 and 2 both target `(fin, definitions)`, and the queue's
leave-by-derivation matches on (subject, aspect): the first
definitions pin will retire *both* questions, because whole-body
supersession means the signed body already carries a value for the
other field. That is self-consistent — once a human owns the map, a
second one-gesture pin would clobber their own choice — but it means
multi-question aspects converge over rounds: the agent's next
session reads the human map, re-composes any still-open questions
*on top of it*, and re-glosses a smaller agenda. The convention
holds; the report of the pin loop should teach the rounds explicitly
(and the F6 proposal shape inherits the same question: per-question
identity vs per-aspect supersession).

## The human half (same day)

The project lead signed two pins in the browser — "Philipp Suter" —
and both landed exactly as designed: HUMAN slots on `formulas`
(calendar days) and `definitions` (Trade Receivables numerator),
both outranking the agent slot at collapse, both `current`. The
predicted wrinkle bit for real: the numerator pin retired the
revenue question unanswered. The loop mechanics held; the surface
around them did not — five defects, all found by one person using
the page once:

1. **The pin button wore the chip class** — `.chip[title]` carries a
   help cursor, so the one action on the page read as a tooltip.
   Fixed: its own button style.
2. **The rounds could not serve.** The queue's leave-by-derivation
   matched on bare (subject, aspect), so once any human slot existed
   the aspect was retired forever — a re-composed agenda could never
   re-ask. Fixed: the derivation is timestamp-bounded (a human slot
   answers only agendas glossed at or before it); round 2 (the
   revenue question alone, both bodies composed on top of the pinned
   map) is live.
3. **The scenarios tile threw `Unrecognized type` on empty** — view-
   typed columns (`||`, `substr`, `json_get_str`) reached the browser
   reader. Fixed with classic casts; the empty state now states
   itself.
4. **The trajectory frame failed at plan time** — the substrate's
   common-subexpression optimizer drops the base column when dynamic
   json chains repeat across two unnest joins, and the measured
   companion wart: a dynamic-index extraction nulls the moment it
   crosses a projection boundary. bands.sql now unrolls the array
   positions statically (16 metric slots × 6 points, the
   measurement's own caps).
5. **The dossier rendered the formula and materialization twice** —
   the frames read `all => true` and a pin makes that two slots.
   Fixed: the faces serve the winning slot only (human outranks,
   total because supersession holds one slot per actor kind).

Parse-only frame coverage let 3–5 ship; the door suite now executes
every built-in frame end to end against seeded shapes and refuses
view types in any frame schema — plus regression tests for the
winner-dedupe and the rounds.

## The second round, and two more fixes (same day)

Round 2 worked end to end: the re-composed agenda (the revenue
question alone, both bodies on top of the pinned map) served through
the timestamp-bounded derivation, and the lead pinned the proposed
option — all three definitional choices are now human-held, the
queue empties honestly, and the whole loop record stands: agenda →
queue → pins → HUMAN slots → re-composed round → pin.

The trajectory tile came back empty a second time, and the root
cause was measured, not guessed: **the `glossary` ACCEPTS edge swept
`metric_bands` on every gloss write — the pin itself emptied the
walk it was standing next to.** Scoped same day (store): the edge
now fires on grounding (QUERY) writes only, which is what both of
its shipped consumers (`metric_bands`,
`detect_grounding_collisions`) actually read; a fact gloss — a pin
included — leaves their caches standing. Regression test in the
store suite; the strike path keeps its blunt full sweep (rare, and
over-invalidation there is safe). The finer per-metric keying (the
whatif pattern) remains open if this proves still too coarse.

The `/mcp` reconnect failure took two attempts, and the record keeps
both. Claude Code now declares protocol `2026-07-28`, whose
tools/list result carries the SEP-2322 `resultType` **plus
list-caching metadata** — `ttlMs` (number) and `cacheScope`
(`"public" | "private"`) — and its validator requires all three.
rmcp 3.1.2 models only the discriminator. The first fix omitted
`resultType` (reading the absent-means-complete rule as a bridge);
the client rightly refused — that bridge applies only to
earlier-revision servers. The fix that stands: `resultType` rides as
the library sends it, and a door middleware injects the two caching
fields on tools/list alone — the list is static per process (an
hour's TTL) and workspace-local (private) — until rmcp models them.
Pinned in the doors suite both ways: the full contract on
tools/list, no caching fields on tools/call; rmcp bumped 3.1.0 →
3.1.2 along the way (no behavior change in the bump itself).

One more observation, carried not fixed:

- **The stateless-client actor gap**: tool calls without a real
  initialize land under the transport's default name (this run's
  agent slots say `rmcp`). Real MCP clients are unaffected; the
  door-ergonomics list carries it.

## Left open, named
- The §5 validation pattern (journal_balanced expectation + check
  voice + detector) not exercised this run — proven in runs 5–9;
  the natural next leg on this workspace, with the balance-sheet
  reconciliation as the promoted invariant.
- **One resolve surface** (the lead's direction after pinning): the
  judgement queue and the pin queue should not read as two
  taxonomies — "needs judgement" vs "pin this" overwhelms; a
  judgement should be resolvable the way a pin is. Needs design,
  together with the F6 proposal shape: the common move is the agent
  pre-composing the body a resolution would write, whatever the
  aspect. Keep the semantics lean.
- The dimension judged-negative sweep covers the 12 judged axes,
  not all 68 columns; the backlog read shows the rest honestly.
- MCP door ergonomics, again: a metadata read inside a multi-
  statement call is capped (ATTEST truncated at 200) — the
  uncapped path needs the read sent alone. Known, unsequenced.
