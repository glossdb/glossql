# The judge pattern

A measurement proposes; it never verdicts. Every instrument in this
directory is built to one division of labor: the statistical pass
optimizes recall and serves everything it found as fact, and the agent
reading the measurement removes the false positives against the data.
Judgment lives in the reader, in detectors, and in read policy — never
inside a result.

## Why this division

A threshold buried deep in machinery is a judgment nobody can argue
with, made once, by the author, without the data in front of them.
Keeping the bodies recall-oriented keeps the arguable constants
visible — the bars are written in the measurement's own SQL (the
derivation body's `match_rate >= 0.95 AND support >= 20`), while the
doors beneath prune only on cost and algebra: the derivation door
skips triples whose operand magnitudes cannot land within 30× of the
target, and the discovery door's 0.5 containment floor bounds output,
not judgment (`crates/session/src/search.rs`).

The division is load-bearing in both directions: a reader with
context declares edges no value statistic can see, and refuses
coincidences no statistic can — which is why every pass serves
candidates, never conclusions.

## The rules the pattern rests on

- **Abstention is a complete answer.** A function that cannot ground
  its output names the absent inputs (`missing_aspects`) instead of
  guessing. A failed judgment means the statistics stand.
- **No statistic ports without its oracle.** The ported statistics
  carry ground-truthed acceptance tests
  (`crates/scripts/tests/behavior_oracle.rs`, `dimensions_oracle.rs`);
  an instrument whose accuracy cannot be measured does not ship.
- **Outputs earn existence by consumers.** No aspect is declared that
  nothing reads.
- **Red exists only where a detector computes it.** Humans do not
  volunteer disagreement; triage quality is bounded by the detector
  library, not by anyone's diligence.
- **No function voice on FACT slots.** A measured voice ranked against
  claims smuggles calibration back in; the shape is an evidence
  measurement the agent judges (see [behavior](behavior.md)).

## Limits

The pattern moves cost to the reader: a generous candidate list is
only as good as the judge that prunes it, and the judge's attention is
the scarce resource every serving decision protects (summaries served,
detail read back on demand).
