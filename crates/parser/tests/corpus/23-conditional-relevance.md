# 23 · Conditional relevance — RULED: WHEN narrows what a subject owes

Source: our own runs. A validated onboarding run
landed ~109 columns
against the kit's six column-grain vocabulary aspects and disclosed
~607 `unassessed` slots — behavior owed on text columns, dimension
owed on keys. Grain answers *which
subject class* an aspect speaks to; nothing could answer *which
subjects, given what the record already knows*. The relevance of
`behavior`, `unit`, and `dimension` is conditional on `role` — and
`role` is already an ordinary FACT aspect whose schema pins `value`
to an enum, so the condition references a vocabulary the store
already validates.

## The ruled form

```glossql
DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow", "none"]}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';
```

TRANSCRIBES — the kit's behavior aspect as shipped
(`crates/scripts/functions/kpi_kit.glossql`): owed on a column only
while that column's winning `role` slot carries `value = 'measure'`.

Semantics, sized to the mechanism:

- **The condition bounds `unassessed` disclosure only** — and every
  count derived from it (the coverage backlog, the checks backlog).
  Writes stay gated by grain alone: a spoken slot outside its
  condition serves normally, so a later re-ruling of `role` strands
  nothing (serve-and-mark, as everywhere).
- The condition reads the **winning sibling slot** on the same
  subject — human over agent, contest notwithstanding. No sibling
  slot spoken means nothing owed yet: `role` first, the rest follows,
  which is the walk's real order.
- At `DECLARE`, the referenced aspect must exist, and when its schema
  pins `value` to an enum the literal must be a member — a condition
  nobody could satisfy is a typo, refused at the door.
- `none` (the kit's judged negative — "examined, does not apply")
  stays meaningful *within* relevance: a ratio column is
  `role = 'measure'` yet `behavior = 'none'`; the condition removes
  the columns that were never candidates at all.

## The forks that lost

- **`WHERE` instead of `WHEN`** — reads as a full SQL predicate and
  invites one (paths, conjunctions, subqueries). `WHEN` states a
  single equality against a sibling aspect's `value`; a second form
  enters only when a fixture demands it.
- **Measurements close irrelevant slots** with explicit
  `applicable: false` glosses — 607 rows become 607 writes; noise
  moved, not removed.
- **Read-side joins in every surface** (frames deriving relevance
  from `role` themselves) — every reader re-implements the rule and
  the store's own counts stay inflated.
