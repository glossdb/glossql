# Onboarding — what is built, what is not

Onboarding is the path from a company's exports and definitions to a
working glossed workspace: add-source → relationships → dimensions →
metrics, an agent driving the flows through the doors, a human
answering the questions only a human can. Evidence for every claim:
`../../reports/` (runs 5–9, the scorecard run, the monitoring
evaluation) and `../../feedback/2026-08-11-onboarding-run-glos.md` —
the first run against external company data (three MSSQL-style
parquet exports, 47 columns, ~50 door calls, one engineer answering
mid-run).

## Built

- **The four flows, end to end through the MCP door.** Graded against
  the finance generator's oracle (scorecard green, zero unexplained
  mismatches, 2026-08-06), rel-f1 (runs 5–6), a relational source over
  ADBC (runs 8–9), and real manufacturing data (2026-08-11). Nothing
  in the statement spine failed first contact.
- **Recipes as the cure surface.** Probe-first, authored typing,
  casts-clean accounting on file sources, supersede-and-reland for
  corrections (ruled 2026-08-06). The glos run cured four source warts
  this way, each triggered by measured evidence.
- **The judge discipline.** Measurements optimize recall, the agent
  removes false positives. Validated at both ends on glos: the judged
  read recovered edges the detector could not see (spelling-mismatched
  keys), and filtered ~100 hierarchy candidates to 5 declared nests.
- **Grain checks.** Cheap verdicts (anti-joins, count-before/after)
  recorded on every pair; all three glos verdicts would have corrupted
  metrics silently if skipped.
- **Composition.** A re-grounded component propagates into every
  composed read with no further act (`read.<aspect>()` in FROM
  position; only assumption prose needs a re-record).
- **The validation pattern.** Expectation gloss + check voice +
  detector + `ATTEST`; carries expected dirt honestly (bands green at
  a known non-zero defect rate; a 0.0 report bands red as
  overcleaning).
- **Supersession under correction.** One engineer correction
  propagated through eight glosses, each with actor and timestamp.
- **The backlog and triage reads.** `unassessed` disclosure, red-band
  `ATTEST` triage, the `datasets` relation and `DESCRIBE` for
  workspace entry (both landed after run 8 surfaced their absence).
- **Contested mechanics.** Convergence or strike; a strike invalidates
  detector verdicts.

## The onboarding build line (decisions 2026-08-12)

Priority order follows the 2026-08-11 evaluation's findings (F1–F6
there), sequenced with the project lead 2026-08-12. Infrastructure
debts live in `../system/`.

Approved — the build items:

1. **The pin loop through the app** (F3 + the question surface;
   approved and BUILT 2026-08-12 —
   `../../reports/2026-08-12-pin-loop.md`). The agenda lands as a
   `pin_questions` gloss; the model app serves it as a queue; a
   one-gesture approval writes the HUMAN slot under the pinner's
   name through the app's pin door; answered questions leave by
   derivation. The real run landed 2026-08-12
   (`../../reports/2026-08-12-onboarding-run-pin-queue.md`): a fresh
   workspace driven through every flow to a six-row queue on three
   genuine definitional choices; the human gesture landed same day —
   two pins signed in the browser, HUMAN slots outranking at
   collapse. The first use found five surface defects (all fixed
   same day, execution-level frame tests added) and the rounds gap:
   the queue's derivation is now timestamp-bounded so a re-composed
   agenda re-asks what a whole-map pin retired. Owed still: the F6
   half (recipe-correction approvals need their proposal shape),
   designed together with **one resolve surface** — the lead's
   direction 2026-08-12: judgements should resolve the way pins do,
   one gesture over an agent-composed body, without "needs
   judgement" vs "pin this" becoming two taxonomies.
2. **Judged negatives per witnessed aspect** (F2; ruled 2026-08-12:
   per-aspect explicit values — each witnessed aspect that can fail
   to apply declares `none` with grounds, the dimensions pattern; a
   generic body was rejected as hard to read on `GLOSSARY()`). Taught
   in the add-source skill same day; agents must remember to declare
   it when framing a vocabulary.
3. **Definitions-in-glosses convention** (F1; confirmed 2026-08-12).
   The definition of record — meaning, unit, owner, source — lives in
   a `definitions` FACT gloss where supersession and actor rank
   apply; the aspect `WITH` blob keeps schema, display label, and
   tooling flags only. The guard against contradicting the aspect
   later: a field lives in exactly one place, never both. Fixture
   edit and skill teaching landed same day.

Design owed before building:

4. **Per-source knowledge deposits** (F5, narrowed 2026-08-12:
   company-wide vocabulary postponed). RULED AND BUILT 2026-08-12:
   `AS FACT ON SOURCE` — source-grain slots read, supersede, and
   disclose across every dataset in the workspace; promotion is an
   ordinary re-speak at source grain; taught in the add-source
   skill. Both halves ran for real 2026-08-12: the fresh-workspace
   run banked seven conventions on `erp_export` at source grain, and
   the AP-lane run (`../../reports/2026-08-12-per-source-read-run.md`)
   read them from a second dataset before its first probe — one
   probe instead of six, supersession proven both directions. The
   fixture's corpus gate is met; entry is the lead's call. The
   Iceberg persistence rides the storage integration when it lands
   (a workspace-grain sibling namespace — see the proposal §5).
5. **Definition-dependency read** (F4). RULED IN PART 2026-08-12:
   Fork A of `../../feedback/flow-basis-vocabulary.md` — the basis
   becomes a structured reference validated by the grounding schema
   (no new aspect, statement, or relation; the dependency read is
   plain SQL). The kind vocabulary is deliberately unruled: its
   names must come from the streamlined resolve surface, so the
   build order is the temporal investigation (item 6), then the
   resolve surface, then this schema change on the settled words.
6. **Temporal typing and cast accounting on the relational path**
   (run 8, findings 1 and 4; interest confirmed 2026-08-12). Dates
   cannot land as a date type from a dialect-less source, and the
   cast safety net is absent over ADBC (`CastAccounting::Unchecked`).
   v0.3 solved the timestamp half in part — investigate its approach
   (`../dataraum-context`) before proposing; the 2026-08-07
   no-landing-side-machinery ruling stands until then.

Sequenced later (2026-08-12): **the full deletion cascade** — after
onboarding, the company-level glossary, and the storage integration
land; `DROP TABLE` refusal stays the stand-in. Dropped from this
list: light auth and the Iceberg branch/fast-forward tracking (both
were options, not needs).

Small door ergonomics, unsequenced: MCP sessions reaped between calls
answer a bare `Not Found` (client re-init plus `USE` replay).

## Rulings owed (project lead; not to be decided in passing)

- **Basis vocabulary modelling** (item 5): proposal, then ruling.

Ruled 2026-08-12 and folded into the build line above: the
judged-negative shape (per-aspect `none`), definitions-in-glosses
(confirmed), plural witnesses per aspect (allowed — each verdict
against its own witness's threshold, any crossing withholds), and
the per-source deposit (fork B, `AS FACT ON SOURCE`, built same
day).

## Open questions a run left with the engineer

- Three scorecard definitions (interest income in revenue, the
  gross-profit subtrahend, the DPO denominator) — 2026-08-06.
- The 365-day annualising convention — 2026-08-07.

## Deliberately not built (ruled)

- **Landing-side temporal machinery for dialect-less sources**
  (2026-08-07): dialect teaching carries it; SQLite is a test rig,
  not a product target. Reopened for investigation 2026-08-12 (build
  line item 6) — the ruling stands until the v0.3 investigation
  reports.
- **Sentinel lists** (2026-08-06): none may exist; cast accounting
  surfaces candidates, closure is an authored recipe amendment.
- **A begin-session skill** (2026-08-05): skills follow deliverables,
  never phase names.
- **Company-wide vocabulary** (postponed 2026-08-12): the held-open
  portability ruling; per-source deposits come first.
