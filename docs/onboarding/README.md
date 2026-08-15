# Onboarding — what is built, what is not

Onboarding is the path from a company's exports and definitions to a
working glossed workspace: add-source → relationships → dimensions →
metrics, an agent driving the flows through the doors, a human
answering the questions only a human can. Evidence for every claim:
`../../reports/` (runs 5–9, the scorecard run, the monitoring
evaluation) and `../../reports/2026-08-11-onboarding-run-glos.md` —
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
   `pin_questions` gloss; the docket serves it as a queue; a
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
   agenda re-asks what a whole-map pin retired. **RETIRED
   2026-08-13** (`../../reports/2026-08-13-pin-retirement.md`): the
   pin door, sign-in, `pin_questions`, and the app's write
   affordances are removed. The **one resolve surface** the lead
   asked for (2026-08-12) resolved as the elicitation loop
   (`../../reports/2026-08-13-elicitation-spike.md`): the door asks
   through the client's question form (MRTR on the sessionless
   lifecycle, proven live in Claude Code), the answer lands as the
   human gloss, and the queue retires it by the same derivation.
   Questions are ephemeral — no ledger; a human `GLOSS` is the whole
   record (corpus fixture 22).
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
   Fork A of `../../reports/notes/flow-basis-vocabulary.md` — the basis
   becomes a structured reference validated by the grounding schema
   (no new aspect, statement, or relation; the dependency read is
   plain SQL). The kind vocabulary is deliberately unruled: its
   names must come from the streamlined resolve surface, so the
   build order is the temporal investigation (item 6), then the
   resolve surface, then this schema change on the settled words.
6. **Temporal typing and cast accounting on the relational path**
   (run 8, findings 1 and 4). INVESTIGATED 2026-08-12
   (`../../reports/2026-08-12-v03-temporal-investigation.md`): the
   predecessor solved format detection for untyped file sources (config-driven
   value patterns, two-gate confidence, generated typed tables — its
   generated DDL is glossql's recipe, authored instead); on the
   relational path it declined value verification with a stated
   rationale that holds here too, and its SQLite weak-typing
   handling is dead code. CLOSED 2026-08-12 (the lead, on the
   report): "it could be solved in the recipe as cast — it is
   VARCHAR or Parquet temporal and the agent could cast that. So ok
   not to fix it." Nothing to build; the two detector-shaped ideas
   from the report stay unsequenced observations.

7. **The question derivation, two faces** (approved 2026-08-13, one
   backlog with the validation surface — the lead, on the
   Metrik-Validierung sketch: everything maps, redesign free). One
   derivation — owed claims, loose assumptions, contested — rendered
   twice: (a) **the door's question round**: the MCP door serves
   derivable questions as MRTR forms between calls — an owed claim
   whose aspect schema carries an enum becomes a choice form
   (options straight from the schema), a loose assumption becomes
   confirm/correct — one question per tool call while open items
   exist; the spike's `--elicit-probe` dictation retires with it.
   (b) **the metric-validation surface** on the docket, adapted
   from the sketch: per-metric cards — status chip (open n / human-
   confirmed with date / measurement-closed), meaning with
   provenance, the commented query face, the grounding fold-out,
   live result tables, open questions, standing checks over
   `ATTEST()` — every string frame-derived, nothing authored on the
   page (the staleness risk resolves by construction). New design
   blessed; the rest of the docket adapts to it later.
   **Built 2026-08-13**, both faces: the round in the MCP door
   (owed → choice form, loose → confirm/correct, decline defers for
   the run; doors suite) and the validation surface (the chip
   ladder on metric surfaces, the standing-checks table, the
   question card on the dossier — amber spent only where judgement
   is wanted; apps suite). Verified end to end against a live door:
   seed → the round asks → answers land as the anonymous human
   gloss → chips flip to human-ruled, questions stop deriving. The
   run also caught a frame defect: the ruled-chip derivation read
   the workspace-wide glossary table and could claim another
   dataset's ruling — now scoped to `$dataset`.
8. **Metric queries carry their calculation** (guidance 2026-08-13):
   mechanics as SQL comments inside the recorded grounding/
   materialization SQL (a comment in the query cannot drift from
   it), judgment stays the assumptions array; plus the closure
   ladder the sketch demonstrated — a question data can decide is
   closed by measurement and watched by a standing check, and only
   what data cannot arbitrate goes to the human. Both taught in the
   metrics skill. **Built 2026-08-13**: §3 teaches comments-carry-
   mechanics / assumptions-carry-judgment (the example grounding
   leads with its comment line), §9 teaches the ladder — measure,
   then witness what must keep holding, ask only what survives.
9. **The `glossql-onboard` umbrella skill** — sequences add-source →
   relationships → dimensions → metrics and names the per-stage
   points where the agent stops for the human: brief → gloss
   honestly with confidence → the door asks → read the human slots
   back. Thin by ruling: no agenda convention exists to teach.
   **Built 2026-08-13, retired 2026-08-15**: the staged arc it fixed
   is now derived — the `workspace_next` read reports what the
   workspace affords and where it stands, and the nine skills
   collapsed to two (`glossql`, `glossql-metrics`). Open
   gap it exposed, for a ruling: no flow declares the behavior/unit
   witnesses, and owed questions derive only where a witness stands
   — whether add-source's framing stage should declare them is a
   design call, not a skill's to make.
10. **The SPEC.md diff for "actor rides the transport"** — the one
    sentence owed from the 2026-08-13 rulings: an elicited answer is
    server-witnessed and lands with human standing (fixture 22
    records the verdict). Proposed for the lead's review.
    **Proposed 2026-08-13**: the principle bullet generalizes
    connection → transport (two sentences added), the PoC note
    aligned; awaiting the lead's review, parser suite green.
11. **The fresh-user run** — claude in a scratch folder outside the
    repo, MCP config only, full onboarding through the question
    round; the run report is the verification and the answer to
    "where do users start claude". After item 7a.
12. **Claude Desktop measurement** — the elicitation loop is proven
    in Claude Code only; whether Desktop renders the form and drives
    the MRTR retry is unverified. Ten minutes with the probe door
    when Desktop is handy; the prose relay covers it either way.
13. **Apps on top of metrics — ideation round** (the lead,
    2026-08-13): how data-app creation, charting, and drill become
    an interactive experience over defined metrics — the agent
    authoring app artifacts, the human steering. Ideation before
    prose: competing forks against a real metric set, presented for
    ruling; the parked URL-mode elicitation is one candidate
    mechanism.
14. **The KPI kit** — built (accepted 2026-08-13):
    `crates/scripts/functions/kpi_kit.glossql` ships the semantic
    vocabulary at boot beside the measurement library — ten aspects
    (`meaning`, `entity`, `role`, `behavior`, `unit`, `dimension`,
    `conventions`, `formulas`, `definitions`, `recipe_change`) and
    seven witnesses, so owed questions derive from the first landed
    table with nothing hand-declared; `rate_tolerance` moved from
    metrics-skill prose into the shipped library. The agent brief
    counts open questions (owed + loose, the round's own derivation)
    and the core skill teaches the sweep. The doors test
    `the_kit_arms_the_round_with_nothing_hand_declared` is the
    acceptance.
15. **Skills cleanup, after the KPI kit** (the lead, 2026-08-13):
    streamline the skills to what the system really offers and does —
    the kit's conventions already left add-source/dimensions/metrics
    prose; the remaining pass trims each skill against the shipped
    system.
16. **Apps on metrics** — built (fork B ruled and accepted
    2026-08-13): the derived business surface. `metric_cube` (one
    measurement: per grounded metric the monthly total, slices along
    served dimension columns, and the rival series where an
    assumption discloses `alternative_sql`; caps stated in the body)
    serves through the `metric_series()` relation — metric names
    become data, which is what lets a static built-in frame slice any
    metric. The built-in docket renders it: pulse (latest month,
    move, axes, validation chip), dossier (story with the rival line,
    slice picker, formula, materialization, judgement, assumptions,
    corridor, the composes/feeds graph — the predecessor's
    metrics-as-graph idea, derived from the formulas registry and
    read.-mentions). The
    metric dossier left the docket — model verifies, metrics
    tells; queue/brief/surfaces link across. Grounded against
    Mosaic's architecture lesson (pre-aggregate, then interact);
    client-side arrow-js slicing and the transformers.js ask-box are
    the parked next steps if the URL round-trip feels slow. Fork A
    (the co-design protocol for bespoke apps) and gloss-carried
    artifacts (the wire-authoring gap) sequenced behind it, ruled
    same day.

Sequenced later (2026-08-12): **the full deletion cascade** — after
onboarding, the company-level glossary, and the storage integration
land; `DROP TABLE` refusal stays the stand-in. Approval decay folds
in here (the lead, 2026-08-13): a human slot is not more durable than
the reality under it, and once the glossary itself moves to Iceberg,
"evidence moved under an approved slot" becomes snapshot lineage —
a join, not machinery. Dropped from this
list: light auth and the Iceberg branch/fast-forward tracking (both
were options, not needs). Parked 2026-08-13: URL-mode elicitation
(the form is replaced by an approvable app page — the lead's noted
use: designing UI elements and data apps interactively, the agent
authoring artifacts and the human approving the rendered page;
revisit once form mode carries real onboarding questions); signed
human identity (the anonymous `human` actor stands until
identification is a need).

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
  line item 6) — the ruling stands until the temporal investigation
  reports.
- **Sentinel lists** (2026-08-06): none may exist; cast accounting
  surfaces candidates, closure is an authored recipe amendment.
- **A begin-session skill** (2026-08-05): skills follow deliverables,
  never phase names.
- **Company-wide vocabulary** (postponed 2026-08-12): the held-open
  portability ruling; per-source deposits come first.
