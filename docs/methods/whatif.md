# What-if — scenarios as plan-rewrite replay

The `whatif.` door serves a declared scenario — a FACT aspect carrying
column overrides (a real column, a factor, a start month) — as bands
over recipe replay: per concept and month, the mechanical
recomputation at the declared factors (`replay`) beside the model's
bands, both halves visible.

## Why plan rewrite, not stored counterfactuals

The override never touches storage. It is a plan rewrite: every scan
of an overridden table gains a projection scaling the overridden
column from the scenario's start month, and each concept's *current*
QUERY grounding is replayed under that rewrite. This buys three
properties at once: the scenario always reads the grounding as it
stands (a re-grounded metric changes the scenario with no further
act), storage carries no synthetic rows to distinguish from real
ones, and the whole computation is one plan set — recomputed per
read, never stored.

## Mechanism

The server replays the grounding at a bracketing grid of strengths
(`crates/session/src/whatif.rs`: factors placed on both sides of the
declared one), so the declared point is always interpolation, never
extrapolation — the support worlds surround it. The band kernel then
reads across the worlds with the factors as features, at the
measurement layer's quantiles.

Judgment rides the `basis` column, never a hidden guess: a concept
whose grounding is not current says so; one the overrides cannot move
is refused with the reason (no declared path from the overridden
column — `detect_derivations` proposes the missing identities); one
whose grounding lacks a time axis or a value column says which.

## Limits

- A scenario's overrides compose additively and say so — an
  interaction between interventions is not in single-intervention
  data; observing a joint scenario is the way to have one.
- A scenario can only move concepts connected to the overridden
  column by declared derivations and groundings — the refusal names
  the gap instead of silently serving an unchanged number.
