# Quality — what is built, what is not

The quality layer guards the soundness of the numbers; the performance
layer reads them. Ordinary growth, seasonality, and business change
belong to performance monitoring — the quality layer's job is to stay
silent about them (measured: a few percent of yearly drift moves no
quality instrument), and to speak when the pipeline, not the business,
moved.

Evidence for every claim below: `../../../tfmeval/FINDINGS.md`
(2026-08-09..11) and `../../reports/`. Grounded on the finance
generator's oracle corpora; not yet validated on a design partner's
data.

## Built

Shipped measurement functions (declared at boot, run by the agent,
judged by the actor — a measurement proposes, it never verdicts):

- **`detect_derivations`** — row-grain arithmetic identities among a
  table's numeric columns (`a = b·c`, `a = b+c`) with violation
  counts. This is the instrument that separates "the pipeline broke"
  from "the business changed": a silently scaled slice violates the
  lineage identity at exactly its row coverage while every marginal
  statistic confuses it with a real price move. Confirmed identities,
  re-checked per batch, are the admission check for subtle corruption.
- **`relationship_coherence`** — what each declared join asserts,
  measured: orphan rate (exact; catches invented-key shapes including
  the single repeated orphan that defeats rare-category counting) and
  child-before-parent date incoherence (the trace a wrong pairing
  leaves). No column-shaped check can see any of this on a
  high-cardinality key — every marginal engine measured exactly 0.0.
- **`detect_relationships`, `profile`, `outliers`, `temporal`,
  `behavior_evidence`, `dimension_relevance`, `detect_hierarchies`,
  `detect_grounding_collisions`, `slot_entropy`** — the pre-existing
  library: structure discovery and per-column shape, the substrate the
  quality reads stand on.

Practices established by measurement, taught in skills rather than
shipped as code:

- **Slice-grain watching.** Two opposing changes leave the aggregate
  flat; per-member evidence names both. Metric watching happens per
  dimension member, not only in total.
- **Reference discipline.** Vocabulary and membership live on the FULL
  admitted history, never on a sampled context (a uniform sample
  fabricates ~16 points of false novelty on a long-tail dimension).
  On living dimensions, novelty is a rate question — a few percent of
  members are genuinely new every month.
- **Completeness guards.** Partial periods abstain instead of scoring
  (relative to the batch's own history, not an absolute floor).

## Deliberately not built (ruled)

- **Calibrated expectation bands on monthly metrics** — with 1–2 years
  of history no method produces bands that mean what they claim
  (best measured coverage 0.61 at nominal 0.80). Whether a movement is
  surprising is the actor's judgement, informed by the evidence.
  Re-measure when workspaces have longer histories. (2026-08-10)
- **TabPFN** — license incompatible with the product path; never
  evaluated. Effort goes into TabICL. (2026-08-10)
- **Bigger single-table anomaly models** for subtle corruption — the
  information is in the lineage, not the table; doubling model compute
  moved detection not at all. (2026-08-11)
- **Learned cause classifiers** — given good evidence a hand rule ties
  the learned model; the evidence extraction is the asset, the judge
  removes false positives. (2026-08-10)
- **Interaction what-ifs composed from single observations** — the
  interaction is not in the data; compose additively and label it, or
  observe a joint scenario. (2026-08-10)
- **Any matching of one written text against another** — equality or
  similarity (token overlap, BM25, embeddings), anywhere in the
  system. Agents do not phrase a claim the same way twice and humans
  phrase it differently again, so a text match passes its tests and
  then produces wrong joins, invisible side effects, and undebuggable
  behavior in production. Identity is an explicit key the author
  writes; where no key exists, the feature does less and says so.
  This retired the ruling round's prose matching in favour of the
  assumption `key` — with three gaps kept open deliberately rather
  than closed by a matcher: an unkeyed assumption is never asked; the
  same claim under two keys reads as two claims and nothing detects
  it; a key dropped from the agent's body clears its fold-in debt.
  (2026-08-14)

## Not covered yet — the backlog

1. **Quality reported at metric grain.** Verdicts land at table /
   column / relationship grain; the operator thinks in metrics. "Which
   metrics are impaired by this batch" is lineage arithmetic the
   glossary already knows; nobody computes it as a standing read. The
   detection is solved — the open work is the read shape and its
   presentation.
2. **Wrong-join decisions.** Joint plausibility ranks wrong pairings
   well (AUROC 0.93 where nothing deterministic reaches), but there is
   no established policy for turning a recall-oriented ranking into an
   admission decision without flooding the judge. Also gated on Phase 2
   serving (below).
3. **Restatement watch.** Already-admitted history changing later —
   late postings are normal to a point, and that point differs per
   workspace. Snapshots make the change detectable; normal-vs-incident
   has no method yet.
4. **Aging tables, honestly monitored.** The current answer to
   lifecycle maturation is excluding the lifecycle columns — blindness,
   not monitoring. "Is this month's status mix normal for its age"
   needs lifecycle timestamps or snapshots; no instrument exists.
5. **Slow, small changes.** A 2% scoped move is invisible in any single
   month. Accumulation across months (sequential testing) is the
   classical answer; untried, and possibly unworkable at six-month
   histories.
6. **Slice false-alarm budgets.** Per-slice surprise misfires on clean
   data already at three segments; at hundreds of slices it drowns the
   judge without a false-alarm budget across slices. Standard
   statistics, unproven here.

## Phase 2 — the model track

The two TabICL read-outs worth shipping (what-if bands with honest
uncertainty; join-suspect ranking) cannot run on the rhai runtime.
Serving path evaluated 2026-08-11: hand-port to candle, ONNX ruled out
(an exported graph silently specializes to one context size — fatal
for an in-context model). ~1.5–2k lines over 7 modules, weights
convert mechanically to safetensors, Metal first-class; gated by an
export-fidelity experiment against the harness numbers. See
`../../reports/2026-08-11-tabicl-export-evaluation.md`.
