# What settles a question — the function to run before anyone is asked

A question the shipped functions can settle is your work. The round
and your chat relays carry judgment only: definitions, conventions,
business meaning, choices between readings. Before any question
leaves the workspace, walk this map and run what answers it:

| the question | what settles it |
|---|---|
| what values a column holds — range, nulls, distincts, top values | `profile()` |
| outliers on a numeric column | `outliers()` (profile first) |
| a date column's grain, span, gaps | `temporal()` |
| **stock or flow — may it be summed** | `behavior_evidence()` over declared edges; its anchors carry the verdict, alternatives, and Wilson support. A grounding write runs it over the column the value sums; the cube folds by the verdict |
| **a sign convention** (source-signed vs natural) | the `sign` partition on a `behavior_evidence` anchor — primary/mirror counts, never column names |
| which columns join, and how well | `detect_relationships()`, then your anti-join judging; standing health is `relationship_coherence()` |
| whether a column derives from siblings (a = b × c) | `detect_derivations()` |
| which axes are worth slicing | `dimension_relevance()` (profiles first) |
| hierarchies inside dimensions | `detect_hierarchies()` |
| two metrics accidentally identical | `detect_grounding_collisions()` |
| whether a metric's month is surprising | `metric_bands()`, adjudicated by `band_breach` |
| the app's series and slices | `metric_series(grain => …)` — the cube, computed at read and landed under the cache, never recorded; `metric_axes()` says what it admitted and, per served column, what keeps the rest out |
| which rows look wrong, on a signal | `misfit.<frame>()` |
| whether an authored expectation holds | a check function's voice + `rate_tolerance`, read via `ATTEST()` |

A measurement is called in the SELECT list with its subject in FROM,
never as a table function: `SELECT detect_relationships() FROM ops`
for a dataset-grain one, `SELECT profile() FROM orders.amount` for a
column, `SELECT detect_hierarchies() FROM orders` for a table.
`SELECT * FROM detect_relationships()` is refused.

What remains askable after the map is walked is what the round
serves: an assumption whose basis is your judgment, held below full
confidence. The round enforces the boundary: it never serves an
assumption whose `dimension` is `behavior`, `sign`, or `grain` —
those are the functions' work, so record them at 1.0 citing the
measurement. When a measurement abstains, the abstention names why:
close the claim on your strongest remaining ground — a mirror table,
a reference system, the data's own shape — and cite it; relay it as a
judgment question only if it stays load-bearing, never as a raw
"which is it?".

The round is one of two registers. **Prose shapes the work; forms
rule the record.** Anything that decides what the work is — the
dataset's topic, which metrics to build, whether to widen the import
— is conversation: stop, present the facts, propose in prose,
interpret the human's prose. Forms carry only standing assumptions to
confirm or correct; they cannot replace conversation, because there
is nothing standing to confirm yet.
