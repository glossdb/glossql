# Validation

A **validation** is four declared parts: an authored expectation (a
FACT gloss), a measuring check (a function voice on the same aspect),
a detector that bands across the slots, and `ATTEST` as the verdict
surface. No part writes a verdict into data — judgment lives in the
detector and in read policy, and every verdict is recomputed from the
current slots at read. Normative definition:
[SPEC.md §7](../../SPEC.md).

## Witnesses

A **witness** is declared per aspect. Per (subject, aspect) it holds
one slot per speaker: each function voice, the agent's gloss, the
human's gloss.

- `BY` lists the actor kinds admitted to gloss the aspect (`AGENT`,
  `HUMAN`). Function voices are not gated here — a function speaks by
  `RETURNS`.
- `DETECTOR` names a function without `RETURNS` that examines the
  slots and returns band + score.
- `THRESHOLD` (0..1) is the cutoff: a score above it withholds the
  value at the collapsed `GLOSSARY()` read and serves `contested` —
  but only where voices actually differ. A single voice cannot
  contest itself: its crossing shows as the band beside the value,
  never as a withheld body.

A verdict belongs to its witness, not its detector: one detector
serving three witnesses holds three verdicts, each computed from its
own witness's slots against its own threshold. More than one witness
may be declared on an aspect; any witness's crossing withholds.

## The pattern

```glossql
DECLARE ASPECT hours_reconcile WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"},
                 "breach_rate": {"type": "number"}}
}$$ AS FACT ON TABLE WHEN entity = 'work log line';
GLOSS hours_reconcile ON work_logs AS $${
  "outcome": "Logged minutes match the order's recorded duration, exactly.", "tolerance": 0.0}$$;
DECLARE WITNESS hours_w ON hours_reconcile BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.0;
```

The expectation is authored prose plus a tolerance; the check is a
function whose `RETURNS` names the same aspect and whose measured
`breach_rate` lands as a voice beside the authored slot
([functions](functions.md)); the shipped `rate_tolerance` detector
bands the two against each other.

## Expected dirt

The expectation is authored, never assumed zero. Nobody knows a
source's defects in advance: the agent measures them at landing, and
the measured rate is the expectation — green today, red when the
source drifts. `breach_rate` is the violation share — 0.0 means fully
passing — compared upward against `tolerance`; the key is named for
its polarity because a pass rate reported under it bands a
fully-passing check red. The shipped `rate_tolerance` detector is
one-sided: green at or under the tolerance, red above it, yellow when
no check voice has landed. A check that reports 0.0 where the profile
showed dirt does not see the dirt; the fix is the check's SQL. A
source that must also catch a recipe filtering the dirt away declares
its own detector that goes red on both sides.

## Reading verdicts

```glossql
SELECT * FROM ATTEST(orders.amount::behavior);
SELECT subject, band FROM ATTEST(fin.trial_balance) WHERE band = 'red';
```

The attest shape is fixed: `(subject, aspect, witness, band, score,
computed_at, current, error)` — `band` in green | yellow | orange |
red | error, `score` the disagreement in 0..1, `current` whether
every function voice the verdict read is at the read's pin,
`error` the failure text where the detector itself could not answer
(band `error` — never a judgment, so it never withholds a value, and
every other witness still speaks). Sweeps are WHERE clauses, never a special form;
with no argument, `ATTEST()` sweeps the `USE`'d dataset.

The collapsed `GLOSSARY()` read carries the same judgment as `state`:
`unassessed` (a witness exists, nobody spoke — the row still appears;
absence is visible, never omitted), `contested` (withheld, with band
and score), `current`, and `stale` (served **and marked** — staleness
never suppresses judgment; it shows beside it).

## Lineage

The witness model comes from the observer-reliability literature:
several imperfect observers of one claim, plus an estimate of each
observer's accuracy — Dawid & Skene (1979) is the classic estimator.
glossql keeps the multiple observers but does not estimate
reliabilities. Precedence is fixed — human > agent > function — and
the detector's band reports disagreement instead of resolving it. An
estimated reliability becomes an optimization target once it decides
verdicts (Goodhart's law); a fixed precedence and a visible band keep
the judgment in two inspectable places: the detector's declared rule
and the human's slot.
