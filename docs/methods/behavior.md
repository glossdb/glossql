# Behavior — stock/flow evidence and reconciliation

`behavior_evidence` measures whether a numeric column behaves as a
stock (a level, correct at a point in time) or a flow (a movement,
summable over a period). It runs at column grain
(`SELECT behavior_evidence() FROM trial_balance.debit_balance`) and
serves a summary — verdict, convention, support, the voted count, the
fit of both readings (`r_flow`, `r_stock`), the sign structure — with
every anchor's full evidence readable back through
`GLOSSARY(table.column::behavior_evidence)`.

Summing a stock corrupts a metric silently. The cube folds every
metric as a flow unless something detects a ratio or a stock: the
SQL's shape (a running total), this measurement's verdict on the
column the value is or is one `sum` of, or — where no verdict decided
— the grounding's `stock` marker or a `stock` gloss on that column,
the agent's word. The grounding write runs this measurement over that
column when no verdict stands there, so the verdict exists before
anyone asks; `metric_axes()` says which decided as `behavior_basis`
(`shape`, `evidence`, `marked`, `glossed`, or `default`).

## Why reconciliation, not shape statistics

A column's own trajectory cannot decide stock vs flow — a trending
flow and a mean-reverting stock look alike. The evidence must be
cross-table: an independent per-period movement, aggregated from a
related event table, scored against both hypotheses with scale-free
residuals — `flow: y ≈ m`, `stock: Δy ≈ m` — and the reading that
reconciles wins. The kernel reports the fit of both (`r_flow`,
`r_stock`), the voted convention, and the sign structure (primary /
mirror / both), so the loser stays visible.

An entity votes when the winning residual is under 0.05 and the loser
stands off by at least a third of their sum; otherwise it abstains.
The gate is the null model: two unrelated positive series of similar
size sit near 0.4 on the flow residual, and a true reconciliation
under 0.01. A measure that never moves is a dead value and abstains.
A convention decides an anchor when at least two entities voted, four
of five agree, and the winners are a majority of the common entities.
Under the majority the anchor abstains and the reason names the
counts.

## Mechanism

The `behavior_anchors` door discovers candidate anchors and holds the
policy — axes, alignments, grain, which terms are movements; the
reconcile kernel (`crates/scripts/src/lib.rs`) holds the arithmetic
behind the runtime seam. Anchors come from declared relationships
only; a composite (tuple) endpoint takes part like any other — every
leg an identifier, the entity key the tuple. Movement candidates are
the event table's numeric columns minus its identifiers — the legs of
declared edges and integer columns whose distinct count over filled is
at or above 0.9 — and every pair difference of them; the identifiers
are served as `identifier_columns`. Pairing is on the
intersection of (entity, period) cells present on both sides; the
anchor grain is the coarser of the two sides' native grains, and a
measure table whose only edges are document-keyed borrows its entity
one hop through the document. An anchor that cannot align abstains
with the reason and its `viable_entities` count rather than voting.

One anchor per alignment reads the measure's own shape instead of a
reconciliation: within each entity, at the axis's native grain and
inside the scope, does the value ever decrease? A column that never
does over 4+ periods votes stock under the convention `monotone`, with
the same Wilson support over the entities that carried enough periods.
It is the only evidence a cumulative with nothing to reconcile against
— season wins beside a table of finishing positions — can offer, and a
column that resets shows as the year scope deciding where the raw one
abstains. On equal support a reconciliation outranks it: the movement
explains the level, monotonicity only describes it.

The summary's winner is elected by one layered rule at both levels —
support first within a fixed epsilon, then the domain tiebreak
(reconciliation over monotone across anchors; fewer terms unless
ΔBIC > 10 within one), then the wider vote, then the anchor's name —
and where supports tied, the summary carries `event` and `tiebreak`
naming the winner and the layer that decided.

This is deliberately an evidence measurement and not a voice in the
`behavior` slots: a measured voice ranked against human claims would
put calibration back into the record. The judge reads, then
glosses.

## Limits

- Reconciliation needs a viable (entity, period) alignment; where
  none exists the anchors abstain (`viable_entities: 0`) and the
  column gets an abstention, not a verdict. A calendar gap between kept
  cells makes the difference span it and read as noise — the abstention
  covers that miss.
- Extraction serves the summary alone by design: the full anchor list
  is cached and read back on demand, because serving every anchor
  spends the judge's attention on what the summary already says.
- The verdict is per column against its dataset's anchors; a column
  that is a stock in one ledger convention and a flow in another is a
  vocabulary question, not a measurement question.
- A flow that rises every period for every entity, with no event
  column reconciling it, reads as a stock through the monotone anchor;
  the served convention says so, and the judge reads the anchor before
  glossing.
