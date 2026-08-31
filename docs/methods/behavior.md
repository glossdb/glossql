# Behavior — stock/flow evidence and reconciliation

`behavior_evidence` measures whether a numeric column behaves as a
stock (a level, correct at a point in time) or a flow (a movement,
summable over a period). It runs at column grain
(`SELECT behavior_evidence() FROM trial_balance.debit_balance`) and
serves a summary — verdict, convention, support, the voted count, the
fit of both readings (`r_flow`, `r_stock`), the sign structure — with
every anchor's full evidence readable back through
`GLOSSARY(table.column::behavior_evidence)`.

Summing a stock or point-reading a flow corrupts a metric silently;
`behavior` is the gloss that prevents it, and this measurement is the
evidence the judge reads before glossing it. The cube reads both: a
grounding with no `behavior` marker folds by the `behavior` gloss on
the column its value is, or is one `sum` of, and where no gloss speaks
by the verdict on that column — `metric_axes()` says which as
`behavior_basis = 'glossed'` or `'evidence'`.

## Why reconciliation, not shape statistics

A column's own trajectory cannot decide stock vs flow — a trending
flow and a mean-reverting stock look alike. The evidence must be
cross-table: an independent per-period movement, aggregated from a
related event table, scored against both hypotheses with scale-free
residuals — `flow: y ≈ m`, `stock: Δy ≈ m` — and the reading that
reconciles wins. The kernel reports the fit of both (`r_flow`,
`r_stock`), the voted convention, and the sign structure (primary /
mirror / both), so the loser stays visible.

## Mechanism

The `behavior_anchors` door discovers candidate anchors and holds the
policy — axes, alignments, grain, which terms are movements; the
reconcile kernel (`crates/scripts/src/lib.rs`) holds the arithmetic
behind the runtime seam. Anchors come from declared relationships
only; a composite (tuple) endpoint takes part like any other — every
leg an identifier, the entity key the tuple. Pairing is on the
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
It is what a cumulative with nothing to reconcile against — season wins
beside a table of finishing positions — has to say for itself, and a
column that resets shows as the year scope deciding where the raw one
abstains. On equal support a reconciliation outranks it: the movement
explains the level, monotonicity only describes it.

This is deliberately an evidence measurement and not a voice in the
`behavior` slots: a measured voice ranked against human claims would
smuggle calibration back into the record. The judge reads, then
glosses.

## Limits

- Reconciliation needs a viable (entity, period) alignment; where
  none exists the anchors abstain (`viable_entities: 0`) and the
  column gets honesty, not a verdict. A calendar gap between kept
  cells makes the difference span it and read as noise — abstention
  absorbs the miss.
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
