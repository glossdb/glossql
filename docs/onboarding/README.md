# Onboarding

Onboarding is the path from a company's exports and definitions to a
working glossed workspace: add-source → relationships → dimensions →
metrics, an agent driving the flows through the doors, a human
answering the questions only a human can. There is no manual sequence:
the `workspace_next` read reports what the workspace affords and where
it stands, and the agent asks the record instead of following a staged
arc.

## What stands

- **The four flows, end to end through the MCP door.** Add-source,
  relationships, dimensions, metrics — each exercised end to end
  against a live door by the server's own suite
  (`crates/serverd/tests/doors.rs`), file and relational (ADBC)
  sources alike.
- **Recipes as the cure surface.** Probe-first, authored typing,
  casts-clean accounting on file sources, supersede-and-reland for
  corrections — every cure triggered by measured evidence, never by
  convention.
- **The judge discipline.** Measurements optimize recall, the agent
  removes false positives: the judged read declares what no detector
  can see (a spelling-mismatched key) and prunes what no statistic can
  refuse (a lookup-shaped coincidence).
- **Grain checks.** Counts before and after a join must agree exactly,
  or the join multiplies every downstream aggregate;
  `relationship_coherence` keeps measuring what each declared join
  asserts.
- **Composition.** A re-grounded component propagates into every
  composed read with no further act (`read.<aspect>()` in FROM
  position); only assumption prose needs a re-record.
- **The validation pattern.** Expectation gloss + check voice +
  detector + `ATTEST`; carries expected dirt — the authored
  tolerance bands green at a known non-zero defect rate. The shipped
  detector is one-sided; catching overcleaning takes an authored
  two-sided detector.
- **Supersession under correction.** A correction is one `GLOSS`: the
  slot supersedes, every composed read serves the new value, and every
  row carries actor and timestamp.
- **The question round.** One derivation — the grounding assumptions
  disclosed below full confidence
  (`crates/session/reads/open_questions.sql`) — rendered twice: the
  MCP door serves one form per record-reading call (stands as stated ·
  wrong · unclear, with a standing ruling on the same key offered
  back), and the docket renders the same read as its open page. An
  answer lands as the human ruling; the question retires by
  derivation — questions are ephemeral, a human `GLOSS` is the whole
  record. Claims a measurement can settle (behavior, unit, role) never
  become human questions. The docket's ruling form is the one write
  for the human who stepped away.
- **The KPI kit.** The semantic vocabulary ships at boot beside the
  measurement library — grained, conditioned aspects and their
  witnesses, so the record owes claims (role, behavior, unit) from
  the first landed table with nothing hand-declared. What a
  measurement can settle stays the agent's backlog, never a human
  question.
- **Source-grain deposits.** `AS FACT ON SOURCE` banks source-system
  conventions once; a second dataset reads them before its first
  probe. Promotion is an ordinary re-speak at source grain.
- **Backlog and triage reads.** `unassessed` disclosure, red-band
  `ATTEST` triage, `datasets` and `DESCRIBE` for workspace entry.
- **Contested mechanics.** A slot contests only across voices — a
  lone voice shows its band, never a withheld body
  (`crates/glossary/src/rules.rs`). The collapsed read withholds with
  band and score; resolution is convergence, a re-speak that closes
  the gap. Row removal refuses while the substrate cannot delete —
  superseding the slot is the cure.

## Conventions the flows rely on

- **Definitions in glosses.** The definition of record — meaning,
  unit, owner, source — lives in a `definitions` FACT gloss where
  supersession and actor rank apply; the aspect `WITH` blob keeps
  schema, display label, and tooling flags only. A field lives in
  exactly one place, never both.
- **Judged negatives per witnessed aspect.** Each witnessed aspect
  that can fail to apply declares `none` with grounds — an absent slot
  and a judged-absent slot are different facts.
- **Mechanics in the query, judgment in the assumptions.** Metric
  mechanics ride as SQL comments inside the recorded grounding (a
  comment in the query cannot drift from it); judgment stays in the
  assumptions array. The closure ladder: a question data can decide is
  closed by measurement and watched by a standing check; only what
  data cannot arbitrate goes to the human.
- **The deposit decomposition.** Company definitions (a held-open
  cross-workspace ruling) / source-system conventions (source grain,
  built) / dataset-local evidence (correctly local; the assumption
  `basis` is the seam). The cost curve this serves: onboarding N+1
  reads what N deposited.
- **Assumption disclosure, rehearsed.** A wrong served number traces
  to an undisclosed assumption or an unlanded column, not to a wrongly
  answered question — so ground with a `LIMIT 0` rehearsal, disclose
  every assumption with its key, then serve.
- **Closure is authored.** Cast accounting surfaces candidate values —
  placeholder dates, magic numbers, format warts — and the cure is a
  recipe amendment the author writes, so the record shows who decided
  what a sentinel means. Temporal formats ride the same path: the
  recipe cast carries the fix, and what is learned banks at source
  grain.
