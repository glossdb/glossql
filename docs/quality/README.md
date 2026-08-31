# Quality

The quality layer guards the soundness of the numbers; the performance
layer reads them. Ordinary growth, seasonality, and business change
belong to performance monitoring — the quality layer's job is to stay
silent about them and to speak when the pipeline, not the business,
moved. The instruments ship with ground-truthed oracle tests
(`crates/scripts/tests/`); not yet validated on a design partner's
data.

## The discipline

- **Measurements propose, they never verdict.** A measurement is
  served as fact; the model answers only what no statistic settles;
  abstention is a complete answer; a failed judgment means the stats
  stand. The judge pattern: measurements optimize recall, the agent
  judge removes false positives.
- **No statistic ports without its oracle.** Every shipped statistic
  has a ground-truthed acceptance test
  (`behavior_oracle.rs`, `dimensions_oracle.rs`);
  outputs earn existence by consumers — no aspect is declared that
  nothing reads.
- **Red exists only where a detector computes it.** Humans do not
  volunteer disagreement, so the detector library is the human-side
  bottleneck: triage quality is bounded by what gets measured.
- **No function voice on FACT slots.** A measured voice ranked against
  claims smuggles calibration back in; the shape is an evidence
  measurement the agent judges.
- **Identity is an explicit key the author writes.** There is no text
  matching — equality or similarity — anywhere in the system: agents
  do not phrase a claim the same way twice and humans phrase it
  differently again, so identity never rides prose. Rulings join on
  the assumption's `key`; where no key exists, the feature does less
  and says so, and the gaps this leaves stay visible instead of being
  papered over by a matcher.
- **Bands inform; the actor judges.** Whether a movement is surprising
  is the actor's judgment, informed by the evidence — no instrument
  turns a band into a verdict.

## Built

Shipped measurement functions (declared at boot, run by the agent,
judged by the actor):

- **`detect_derivations`** — row-grain arithmetic identities among a
  table's numeric columns (`a = b·c`, `a = b+c`) with violation
  counts. This is the instrument that separates "the pipeline broke"
  from "the business changed": a silently scaled slice violates the
  lineage identity at exactly its row coverage while every marginal
  statistic confuses it with a real price move. A confirmed identity,
  re-checked on each landing, is the standing instrument for subtle
  corruption.
- **`relationship_coherence`** — what each declared join asserts,
  measured: orphan rate (exact; catches invented-key shapes including
  the single repeated orphan that defeats rare-category counting) and
  child-before-parent date incoherence (the trace a wrong pairing
  leaves). No column-shaped check can see either on a
  high-cardinality key.
- **`detect_relationships`, `profile`, `outliers`, `temporal`,
  `behavior_evidence`, `dimension_relevance`, `detect_hierarchies`,
  `detect_grounding_collisions`** — structure discovery and per-column
  shape, the substrate the quality reads stand on.
- **Three detectors adjudicate at read** — `slot_entropy` across an
  aspect's slots, `band_breach` over the walked bands,
  `rate_tolerance` between an authored expectation and its check
  voice.
- **The band plane** — `tabicl_bands`, a native kernel over the
  sibling candle port (weights digest-verified; Metal by default, CPU
  fallback), with `metric_bands` and `band_breach` in the library —
  the walk withholding a partial trailing month on a judged
  sub-monthly axis, the detector scoring the newest complete one —
  and `whatif.<scenario>()` replaying rewritten plans across
  bracketed band grids.

Practices established by measurement, taught in skills rather than
shipped as code:

- **Slice-grain watching.** Two opposing changes leave the aggregate
  flat; per-member evidence names both. Metric watching happens per
  dimension member, not only in total.
- **Reference discipline.** Vocabulary and membership live on the FULL
  admitted history, never on a sampled context — a uniform sample
  fabricates false novelty on a long-tail dimension. On living
  dimensions, novelty is a rate question: some members are genuinely
  new every month.
- **Completeness guards.** A period short against the batch's own
  history — not the trailing month the walk withholds, an earlier one
  a late-landing source left thin — is the reader's call, not a
  score.

## What watches what

Cross-chain reconciliation — confirmed derivations re-checked on each
landing — catches the value faults no marginal statistic sees;
`relationship_coherence` watches what every declared join asserts;
`misfit.<frame>()` ranks new rows against a declared frame of
known-good history; the band walk asks, for each recent month, what
range it should have landed in. A clean corpus stays green at the
judge.
