# The resolve surface — one queue, one verb (Leg D proposal)

Status: proposal for ruling, 2026-08-12. Ruling inputs already given
by the lead, taken as fixed: **one UX** — "needs judgement" and "pin"
merge; **pin is the verb** ("good, a bit coarse" — kept, refined by
copy, cheap to rename later since it is labels, not grammar); **login
simulation** — a button that sets a JWT cookie, no real login, for
current tests; **alpine.js** for the client interactivity the current
surface lacks; the frontend-design pass applies at build. Nothing
below touches grammar or the store.

## The unification, stated once

Everything a human resolves has one shape: **the agent pre-composes
the exact body a resolution would write; the human signs it.** A
definitional choice, a loose assumption, a recipe correction — same
shape, different question. Where the agent has not composed an answer
(an owed claim nobody wrote, evidence that needs eyes), the row is
not a second category: it is the same queue row without a pin action,
carrying its dossier link instead. One list, one ordering
(confidence, loosest first), no headings that split it into
taxonomies.

## The data — one convention grows, nothing new is invented

`pin_questions` stays the agenda convention and absorbs the rest:

1. **Definitional choices** — as today (fixture-taught, built).
2. **Loose assumptions** — the metrics skill's closing step gains one
   sentence: every assumption below full confidence that the agent
   *can* answer ships as an agenda entry whose option body is the
   full re-grounded gloss (the assumption at 1.0, its basis the
   human's act). The queue then shows the assumption once — as its
   question — not twice.
3. **Recipe corrections (F6)** — an agenda entry targeting a
   `recipe_change` FACT aspect (TABLE grain, workspace convention
   like `pin_questions` itself), body `{table, sql, reason}`. The pin
   writes the approval as a HUMAN gloss; **the agent executes the
   re-declare next session** on reading it. Approval is data; acts
   stay in agent sessions; the pin door keeps writing nothing the
   statement language could not.

The queue frame becomes one derivation: agenda entries (signable,
timestamp-bounded as built) ∪ loose assumptions and owed claims not
covered by an entry (investigate rows). The separate "Pin questions"
tile and "Needs judgement" tile retire.

## The login simulation

- `POST /app/session` with a name → `Set-Cookie: gl_actor=<JWT>`
  (HS256, per-boot secret, `sub` = name, no expiry games). A "sign
  in" affordance in the shell header; signed-in state shows the name;
  sign-out clears the cookie. Any name is accepted — a simulation,
  the slot where real auth lands later.
- The pin door takes the actor from a valid cookie; the per-row name
  field retires. No cookie → the pin action itself says "sign in to
  pin" and opens the affordance (an empty state that directs, not a
  refusal).

## The stack amendment

Vendor **alpine.js** beside htmx (same CSP posture, no external
hosts). Division of labor: htmx stays transport (posts, swaps);
alpine owns local state — option selection inside a question card,
the signed-in indicator, optimistic row state after a pin, keyboard
flow. This amends the 2026-08-07 htmx-only ruling at the lead's own
direction.

## The page

The standing view reorganizes around the queue — it was always
labeled "the front door"; now it looks like one:

```
┌──────────────────────────────────────────────────────────────┐
│ lede · census strip (facts · subjects · aspects · …)         │
├──────────────────────────────────────────────────────────────┤
│ OPEN QUESTIONS                                    signed in: ●│
│ ┌───────────────────────────────────────────────────────────┐│
│ │ ◐ 0.6  dso: which day-count convention?          [unfold] ││
│ │        ├ ● actual calendar days   — grounds…      [pin]   ││
│ │        └ ○ 360-day banking year   — grounds…      [pin]   ││
│ ├───────────────────────────────────────────────────────────┤│
│ │ ◐ 0.7  revenue: which account scope?             [unfold] ││
│ ├───────────────────────────────────────────────────────────┤│
│ │ – owed  behavior on fin_ap.bank_transactions.amount        ││
│ │         nobody has judged yet → dossier                    ││
│ └───────────────────────────────────────────────────────────┘│
│ metric surfaces · scenarios · coverage · slicing …           │
└──────────────────────────────────────────────────────────────┘
```

- **The signature element: the question card that retires in
  place.** A signed row does not vanish — it settles: "pinned ·
  Philipp Suter · just now", then compacts on the next load. The
  motion teaches the semantics (answers exist, nothing is
  dismissed) and is the one place the page spends its boldness.
  Everything else keeps the instrument language exactly — layered
  slate, mono data, provenance chips, the standing palette.
- **Copy discipline:** one verb through the whole flow — the button
  says "pin", the toast-equivalent says "pinned", the basis the
  agent later reads says the same word. The question does the
  differentiating work; the verb never forks.
- Investigate rows state their emptiness as direction ("nobody has
  judged yet → dossier"), never as a mood.

## Build scope (after the ruling)

`crates/apps` only, plus skills and tests: the merged queue frame,
the question-card template (alpine), `POST /app/session` + JWT verify
in the pin handler, alpine vendored, the frontend-design pass on the
standing view with screenshots against the live workspace; metrics
skill §9 extended (agenda covers loose assumptions), apps skill
updated; door tests for session/cookie pins and the merged frame;
the builtin registry updated. No grammar, no store change, no new
door power.

## Held for after this lands

Leg B's basis kinds take their names from this surface's settled
words (one human act = one kind). The verb's coarseness is a label
concern — revisit after real use, not before.
