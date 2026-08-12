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
   approved 2026-08-12). Onboarding flows already end with the
   pinning agenda; nothing serves the questions to the human, and a
   relayed answer lands in the *agent* slot. The build: questions
   raised during onboarding land as a queue in the model app
   (queue/dossier pattern, "humans do not volunteer disagreement —
   the UX is triage"); the human's answer is a one-gesture approval
   that writes the HUMAN slot. Recipe-borne semantic corrections (F6)
   ride the same approval surface.
2. **Judged negatives per witnessed aspect** (F2). "Not yet judged"
   and "never applicable" share the `unassessed` row, so the backlog
   read never converges (permanent 276-row floor at 47-column scale).
   The dimensions plane has the local solution (`none` with grounds).
   Ruling owed on the general shape before skills teach it (see the
   rulings list below).
3. **Definitions-in-glosses convention** (F1). Anything a company
   revises must live where supersession lives, and today that is only
   the gloss plane — aspect `WITH` blobs have no supersession story
   and go stale (the `x-unit: "pieces"` specimen). Convention: thin
   aspects, definition of record in gloss bodies. Fixture 18 §1 edit
   plus skill teaching, pending the lead's confirmation.

Design owed before building:

4. **Per-source knowledge deposits** (F5, narrowed 2026-08-12:
   company-wide vocabulary postponed; per-source is the interesting
   half). Source-system conventions — export dialect warts,
   placeholders, key spellings — deposited at SOURCE grain so the
   next dataset from the same system reads before probing. Needs a
   design: how the deposit persists in Iceberg for reuse across
   datasets, and how a dataset-local finding is promoted into it.
5. **Definition-dependency read** (F4). A definition change's blast
   radius is traced by hand today. Free-text `basis` matching was the
   first idea and the lead flagged it as unreliable (generated
   strings vary); the design likely needs a structured basis
   reference before any detector. Design owed.
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

- **The judged-negative shape** (item 2): per-aspect explicit values
  (the dimensions pattern — each enum gains a judged "none") vs one
  uniform not-applicable body convention admitted on any witnessed
  aspect. Corpus-first against the glos workspace's 276-row floor.
- **Definitions-in-glosses** (item 3): confirm the convention; then
  fixture 18 §1 and the skills carry it.
- **Plural witnesses on one aspect**: the 2026-08-12 review fix pairs
  each verdict with its own witness's threshold and withholds when
  any crosses (interim); whether a second witness per aspect should
  be refused outright is a language question.
- **Per-source deposit shape** (item 4): design proposal first, then
  the ruling.

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
