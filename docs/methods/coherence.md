# Coherence — what a declared join asserts, checked

`relationship_coherence` measures what each **declared** relationship
claims against the rows it now governs. It runs at dataset grain
(`SELECT relationship_coherence() FROM fin`) and reports, per
relationship: fill, orphan count and rate, and per column pair the
temporal precedence evidence (child-before-parent rate). Discovery
proposes and the judge declares ([discovery](discovery.md)); this
instrument watches what the declaration asserts from then on.

## Why these two facts

Both are invisible to any column-shaped check:

- **Orphan rate, exact.** A child key with no parent is the trace an
  invented or mispaired key leaves. Counted exactly, it catches the
  shapes sampling misses — including a single repeated orphan value,
  which defeats rare-category counting outright. On a
  high-cardinality key, every marginal engine measured exactly 0.0
  against these faults.
- **Date incoherence.** A child event dated before its parent exists
  is the trace a wrong pairing leaves even when the keys join. The
  per-pair `precedes_rate` carries it as evidence, not verdict — a
  payment before its invoice's due date is ordinary; before the
  invoice exists is the fault — which is why the judge reads the pair
  names and decides.

A composite endpoint joins on every leg of its tuple (the corpus's
composite ruling) instead of being dropped as unspellable.

A relationship within one table is one of two things, and the to-side
tells them apart. When the to-side is unique over the table — a key —
the edge is a self-reference (`accounts.parent_id ->
accounts.account_id`) and checks as a join of the table against
itself. Otherwise it is a nest, recorded finer → coarser (`city ->
country`): what it asserts is that every finer value determines one
coarser value, and its orphans are the rows whose finer value maps to
more than one. Neither carries temporal evidence — a date pair across
a table and itself says nothing.

## Mechanism

The `relationship_checks` door computes fill, orphans, and precedence
per declared relationship; the body
(`crates/scripts/functions/coherence.sql`) shapes them per
relationship with the temporal evidence nested per column pair. No
thresholds anywhere in the path: rates are served; thresholds belong
to the witness layer and the judge.

## Limits

- Declared relationships only — an undeclared join is nobody's
  assertion and is not watched.
- Precedence needs date columns on both sides, tried up to three per
  side; pairs without them contribute no temporal evidence.
- Orphan rate says a pairing is broken, not which side broke — the
  cure is a recipe or a re-declaration, and choosing is judgment.
