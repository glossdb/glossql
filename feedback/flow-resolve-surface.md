# The resolve surface — one queue, one verb (Leg D proposal)

> **Built 2026-08-12** (D1–D5): multi-dataset first-by-name binding,
> the JWT sign-in simulation, the one queue with question cards that
> retire in place (alpine vendored for exactly that transition), the
> waiting-on-agent derivation counted on the front door and taught as
> the agent's session-open brief, the frontend-design pass against
> the live workspace (responsive floor verified at 390px). D6 — the
> connect-time brief at discover — remains, after which Leg B names
> its basis kinds on this surface's words: pin, the brief, waiting.

Status: proposal for ruling, 2026-08-12; §§1–4 accepted same day
(the lead: unification confirmed — every resolution is a human
writing; login sim "keep it simple"; alpine **only where client-side
transitions need it**; the retiring card "accurately describing what
we do"). Open: the loop's third leg (§ below) and the unjudged/open
surfacing ("must surface too — not yet there"). Ruling inputs taken
as fixed: **one UX** — "needs judgement" and "pin" merge; **pin is
the verb** ("good, a bit coarse" — kept, refined by copy, cheap to
rename later since it is labels, not grammar); **login simulation**
— a button that sets a JWT cookie, no real login, for current tests;
the frontend-design pass applies at build. Nothing below touches
grammar or the store.

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

## The loop's third leg — agent → user → agent

The lead's question (2026-08-12): once a pin lands, how does the
agent pick it up — and how does the user know an agent session must
close the loop? His three flows, plus two the enumeration was
missing:

- (a) **agent asks in the conversation** — sends the link (or an
  iframe), the user pins, tells the agent to go on. Synchronous; the
  kick is free because the conversation is already open.
- (b) **agent parks an ambiguity in the UI** — the user resolves
  later, then tells an agent.
- (c) **undefined territory** — both sides can see it; the user
  points, the agent defines, takes it into account.
- (d) *missing:* **the user-initiated correction** — nobody asked;
  the user contests or supersedes a standing claim of their own
  volition. Today this is the copy-the-statement relay; under one
  UX it is the same pin gesture on a claim's dossier, and it is the
  flow that most needs the third leg (dependents must re-ground).
- (e) *missing:* **the signal-initiated flow** — a band breach or a
  red validation starts the loop with no human and no agent asking;
  whoever sees it first (usually the surface) routes it: the agent
  investigates, the human rules.

The closure design rests on one honest distinction the *system*
makes and the user never has to:

1. **Many pins close themselves.** A pinned prose definition or a
   pinned assumption body needs no agent act — the human slot
   outranks at every read the moment it lands (an assumption pin's
   signed body is the full re-grounded gloss, SQL included). The
   card retires to "pinned · governs every read now".
2. **Some pins owe an agent act** — and the lead's catch (same day)
   put formula pins here: the formula gloss and the recorded
   materialization are one definition in two forms, and a pin
   rewrites only the text — `read.<metric>()` serves the recorded
   SQL until an agent recomposes it. Also here: a `recipe_change`
   approval (the re-declare must run), a contest (dependents
   re-judged), an answered round implying round n+1. These retire
   to a visibly different rest state — "pinned · waiting on the
   agent".

### The two briefs (the lead's shape, 2026-08-12)

Each party gets a brief **on connecting** — one derivation, two
renderings, future touchpoints (slack, email) ride the same reads:

- **The human's brief is the app's front door**: what awaits their
  pin (the queue, with the overrule always available — flow (d) is
  the same gesture on any standing claim), then what they pinned
  that was **not yet retriggered** — the waiting-on-agent list, one
  standing count. That count is how a user knows to poke their
  agent; the kick stays human for now ("go on", in the conversation
  they already have). A later phase may automate the kick; nothing
  here blocks it.
- **The agent's brief arrives at connect**: the changes that expect
  adaptation (human slots newer than the agent's last writing,
  unexecuted approvals, contests), new signals from new data (red
  bands, fresh imports), later long-run ops data. Delivery is the
  door telling live state where it already tells — the MCP
  initialize instructions gain one composed paragraph over the same
  reads — plus the identical read taught in the skills as
  session-open discipline, so a session that connected long ago
  sweeps the same brief before acting. Delivery researched
  2026-08-12 (`../reports/2026-08-12-brief-delivery-research.md`):
  the lean is **the brief as a read through the one tool** — it
  alone reaches already-connected agents ("a user changes something,
  tells the agent, the agent collects it"), needs no new door
  behavior, and respects the reversed-resources ruling; live
  instructions at discover (spec-blessed since 2026-07-28) follow as
  sugar over the same composition; `subscriptions/listen` push parks
  with the touchpoints phase. Decision shared with the lead.

One button, then: the user has one gesture (pin) and one indicator
(the waiting count); the agent has one entrypoint (the brief).
Everything else is state made visible.

Open beside this: the unjudged/open-topic surfacing across the
*whole* workspace — the app binds one dataset, so open topics on a
second dataset (`fin_ap` today) are invisible from the model app.
Named, not designed here.

## Held for after this lands

Leg B's basis kinds take their names from this surface's settled
words (one human act = one kind). The verb's coarseness is a label
concern — revisit after real use, not before.
