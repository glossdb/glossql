# 06 · Claim witnesses + reliabilities — slots TRANSCRIBE · calibration DROPPED BY DESIGN

Source: `claim_witnesses` (engine schema.sql):

```sql
CREATE TABLE claim_witnesses (
    target VARCHAR NOT NULL,
    claim_field VARCHAR NOT NULL,        -- claim-slot id, e.g. "null_token:TBD"
    witness_id VARCHAR NOT NULL,         -- ≠ detector_id, part of the UNIQUE key
    distribution JSONB,
    reliability FLOAT NOT NULL,
    detector_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    CONSTRAINT uq UNIQUE (target, claim_field, witness_id, run_id)
);
```

Source: `dataraum-config/entropy/reliabilities.yaml` — reliability calibrated
**per witness within a measurement**, not per detector:

```yaml
witnesses:
  null_semantics:
    quarantine_clustering: 0.8681
    type_claim: 0.2658
    null_vocabulary: 0.944
  temporal_behavior:
    llm_claim: 0.838                    # measured 2026-06-10, stratified corpus
    structural_reconciliation: 0.889    # measured 2026-06-11, wave-2 rig
```

Plus per-measurement calibration provenance: `calibrated: true/false`,
corpus_version, estimator, per_class_accuracy, brier, sample sizes, dates.

## Transcription

The slot model replaces distributions-with-weights: per (subject, aspect) one
current value per speaker — the function's cached output, the agent's gloss,
the human's gloss. The detector adjudicates across slots and returns band +
score; ATTEST serves it.

```glossql
DECLARE ASPECT behavior WITH $${
  "type": "object",
  "properties": {"value": {"enum": ["stock", "flow"]}}
}$$ AS FACT ON COLUMN;

DECLARE FUNCTION temporal_behavior FOR GLOBAL FROM 'functions/temporal_behavior.rhai'
  RETURNS behavior;

DECLARE FUNCTION behavior_entropy FOR GLOBAL FROM 'functions/behavior_entropy.rhai';

DECLARE WITNESS behavior_w ON behavior BY (AGENT, HUMAN)
  DETECTOR behavior_entropy THRESHOLD 0.7;

SELECT * FROM ATTEST(orders.amount::behavior);
```

`claim_witnesses.distribution` becomes the value function's cached JSON output
(detail reachable by function SELECT); `run_id` is cache bookkeeping.

## Findings

- **Slots TRANSCRIBE.** target→subject, claim_field→aspect, the detector's
  verdict→(band, score). A human re-gloss supersedes the human slot; a
  contested state is a red/orange band, not a flag.
- **The data-grounded voice is a `RETURNS` binding** (respelled
  2026-08-04): `temporal_behavior` speaks the `behavior` aspect by
  returning it — v0.3's `structural_reconciliation` witness, made a typed
  function instead of a `BY (FUNCTION …)` entry. Its output validates
  against the aspect it speaks; the `BY` gate is for actors only. The
  detector lost its RETURNS transcription: no RETURNS *is* the detector
  shape, and the attest contract is the engine's. *The example function
  is superseded (2026-08-05): a trajectory read of behavior was falsified
  (a trending flow and a mean-reverting stock look alike), and the shipped
  wiring serves `behavior_evidence` as a MEASUREMENT the agent reads
  before glossing. The voice mechanism itself lives on — fixture 16 §5's
  validation checks speak their aspects exactly this way.*
- **DROPPED BY DESIGN — the calibration theater.** Per-witness calibrated
  reliabilities, calibration provenance (corpus id, estimator, Brier),
  placeholder priors, pooling math: all of it is the DETECTOR function's
  internal logic — swappable code, not grammar.
- What the log loses: adjudication is no longer reproducible from statements
  alone — it is reproducible from statements + the detector script. Accepted
  as part of functions-as-scripts.
