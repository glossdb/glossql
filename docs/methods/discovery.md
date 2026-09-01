# Discovery — containment-based relationship candidates

`detect_relationships` serves every plausible join pair across the
landed tables, with cardinality, overlap, match and orphan counts, and
distinct counts per side. It runs at dataset grain
(`SELECT detect_relationships() FROM fin`) and is the high-recall half
of the candidate → verified → declared arc: this measurement proposes,
the judge declares, and [coherence](coherence.md) watches what was
declared.

## Why containment

The statistic is containment: matched values over the *from* side's
distinct count. Join discovery asks "does this column's vocabulary
live inside that one" — a directional question, and containment is its
statistic: a small child key set fully contained in a large parent set
scores perfectly, whatever the size skew, and that size-skewed shape
is exactly what a real foreign key is.

## Mechanism

All columns land once through one union-of-distincts plan;
containment counts are the typed-key counts
(`crates/session/src/search.rs`). Pairs below 0.5 containment are not
served — the one floor in the door, placed to bound output, not to
judge. Composite keys are tried where the multi-tenant shape suggests
them — for each overlapping pair, the same two tables are tried with a
scoping leg in overlap order, and a composite that passes the floor
rides `key_columns` (the tuple is the key). Candidates rank orphan
evidence first, then overlap — a pair with orphans carries a judgment
call, while a perfectly clean 1.0 overlap is as often two parallel
surrogate sequences as an edge; the body
(`crates/scripts/functions/relationships.sql`) shapes and summarizes,
and never filters beyond the door's floor.

The judged read is half the method by construction: value containment
cannot see a spelling-mismatched key, and it cannot refuse a
lookup-shaped coincidence — a reader with context declares the one and
prunes the other, which is why the pass optimizes recall and leaves
declaring to the judge.

## Scale

The door computes distinct sets in memory — exact while the summed
distincts fit. The ladder past that is named in the door, unbuilt
until a dataset needs it: BINDER-style hash-range partitioning, then
bottom-k sketches.

## Limits

- Value containment cannot see a join the vocabularies do not share
  (surrogate-to-natural mappings); those arrive as declarations, not
  discoveries.
- A contained column is not necessarily a key — lookup-shaped
  coincidences pass the floor, and the judge removes them.
- The composite rescue tries width 2 only — a scoping leg beside the
  overlapping pair; wider composites are not searched.
- Within-table quadratic pair enumeration is the scale limit on very
  wide tables; recipes that land only relevant columns are the
  author-side fix.
