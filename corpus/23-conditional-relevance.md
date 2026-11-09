# 23 · Conditional relevance — DECIDED: WHEN narrows what a subject requires

Source: our own runs. A validated onboarding run
stored ~109 columns
against the kit's six column-grain vocabulary aspects and disclosed
~607 `missing` values — behavior required on text columns, dimension
required on keys. Grain answers *which
subject class* an aspect speaks to; nothing could answer *which
subjects, given what the catalog already knows*. The relevance of
`behavior`, `unit`, and `dimension` is conditional on `role` — and
`role` is already an ordinary FACT aspect whose schema pins `value`
to an enum, so the condition references a vocabulary the store
already validates.

## The decided form

```glossql
DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow", "none"]}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';
```

TRANSCRIBES — the kit's behavior aspect as shipped
(`crates/scripts/functions/kpi_kit.glossql`): required on a column only
while that column's winning `role` value carries `value = 'measure'`.

Semantics, sized to the mechanism:

- **The condition bounds `missing` disclosure only** — and every
  count derived from it (the coverage backlog, the checks backlog).
  Writes stay gated by grain alone: a spoken value outside its
  condition serves normally, so a later re-ruling of `role` strands
  nothing (serve-and-mark, as everywhere).
- The condition reads the **winning sibling value** on the same
  subject — human over agent, contest notwithstanding. No sibling
  value spoken means nothing required yet: `role` first, the rest follows,
  which is the walk's real order.
- At `DECLARE`, the referenced aspect must exist, and when its schema
  pins `value` to an enum the literal must be a member — a condition
  nobody could satisfy is a typo, refused at the interface.
- `none` (the kit's judged negative — "examined, does not apply")
  stays meaningful *within* relevance: a ratio column is
  `role = 'measure'` yet `behavior = 'none'`; the condition removes
  the columns that were never candidates at all.

## The forks that lost

- **`WHERE` instead of `WHEN`** — reads as a full SQL predicate and
  invites one (paths, conjunctions, subqueries). `WHEN` states a
  single equality against a sibling aspect's `value`; a second form
  enters only when a fixture demands it.
- **Measurements close irrelevant values** with explicit
  `applicable: false` glosses — 607 rows become 607 writes; noise
  moved, not removed.
- **Read-side joins in every surface** (frames deriving relevance
  from `role` themselves) — every reader re-implements the rule and
  the store's own counts stay inflated.
