# Vocabulary — read before glossing a column: role first, behavior by evidence

## Gloss the columns — role first

Read the measurements before speaking. Relevance is conditional: a
column owes `behavior` and `unit` only once its `role` says `measure`,
and `dimension` only on `role = 'dimension'` — so gloss role first and
the rest of the backlog derives from it.

- **meaning** — one sentence, specific to the business, saying what
  the column contains and how it is used; `term` is the name a report
  would print. Never state summability here — that verdict has one
  home.
- **role** — `key` · `measure` · `dimension` · `timestamp` ·
  `attribute`, judged from this table alone. Never call a column a
  foreign key here; references are `DECLARE RELATIONSHIP`.
- **behavior** — measures only. `stock` is a carried point-in-time
  level that must not be summed across periods; `flow` accumulates. A
  column's own trajectory cannot decide this — a trending flow and a
  mean-reverting stock look alike — so run `behavior_evidence`, which
  reconciles the column against period movements over *declared*
  edges. Each anchor is served raw and year-scoped: a cumulative that
  resets abstains at raw grain and reconciles as a stock on the year
  anchor; read the pair together. Names lie either way — a column
  called "total" can carry a per-period movement. The gloss is read:
  a grounding without its own `behavior` marker folds by the
  `behavior` gloss on the column its value sums, before any evidence.
- **unit** — where a magnitude has one; `source_column` names the
  column carrying it when it rides beside the value.

**When `behavior_evidence` starves** — every anchor abstains, no
entity persists across periods — climb the ladder: land the missing
dimension (a fact whose counterparty has no table starves only for
lack of a declared edge; `SELECT DISTINCT site_id FROM …` is a
legitimate recipe); then your own data test, cited as the basis; and last, on an
installation where a whole family of columns needs it, author a
workspace-scoped function that decides behavior the way *this* dataset
demands. That function is the installation's recorded thinking —
versioned, re-runnable, honest about its method in a way a one-off
judgment never is. Unwilling to climb? Don't gloss: absence shows as
an honest `unassessed` row, a guess does not.

"Does not apply" *within* relevance is still a judgment: a ratio is a
measure with no stock/flow nature, and that lands as
`{"value": "none", "grounds": "…"}`, never as a permanent unassessed
row.
