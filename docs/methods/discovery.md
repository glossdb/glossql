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

Every set operation in the door is a plan the engine runs, one pass
per column type: each table's same-typed columns unpivoted to
`(column, value)` rows, the distinct of those joined to itself on the
value, and the count per column pair — the containment numerator, and
on the diagonal each column's distinct count
(`crates/session/src/search.rs`). Values compare as stored; nothing is
cast. Pairs below 0.5 containment are not served — the one floor in
the door, placed to bound output, not to judge. Composite keys are
tried where the multi-tenant shape suggests them — for each
overlapping pair whose target is no key alone, the same two tables are
tried with a scoping leg in overlap order. A scoping leg that is
unique on its own is never tried, since the pair could identify no
more than that leg's own candidate already does; nor are two tables
that a key-like single pair already joins, since a scope could only
re-key a pair that has a key. The composite pass
first counts every combination's distinct pairs, an aggregate with no
join, and settles the key test from those counts; only the surviving
from-combinations are then joined to the surviving to-combinations. A
composite that passes the floor rides `key_columns` (the tuple is the
key). Candidates rank by what a reference looks like: its target is a
clean key (exactly unique in its table and not a Date or Timestamp),
it repeats (many-to-one before one-to-one), it reaches its key
(matched over the key's distinct count), and it resolves (overlap).
Key coverage stands before overlap because a small code set is
contained in every dense id space at overlap 1.0 and reaches almost
none of it; the clean-key tier stands first because a copied date
column and a sibling table's own foreign key both contain a
reference's values without being what it refers to, and a one-to-one
1.0 overlap is as often two parallel surrogate sequences as an edge.
The order demotes and never drops. The body
(`crates/scripts/functions/relationships.sql`) shapes and summarizes,
and never filters beyond the door's floor.

The judged read is half the method by construction: value containment
cannot see a spelling-mismatched key, and it cannot refuse a
lookup-shaped coincidence — a reader with context declares the one and
prunes the other, which is why the pass optimizes recall and leaves
declaring to the judge.

## Scale

No column's values are ever held outside the engine's memory pool:
the sorts, the merge join and the aggregates of a pass spill to disk
under pressure, and what leaves the engine is a matrix sized by the
schema. The cost of a pass is its fan-out — a value carried by many
columns of one type meets itself once per column pair — and the
composite pass bounds that to the pairs it was asked.

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
