# Validations — read when a number needs a check that says why it holds

## Stand up validations

The authored expectation is a FACT gloss; the check is a function
voice on the same aspect; a detector bands across both slots;
`ATTEST` is the verdict surface.

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

- **Scope the check with `WHEN`.** A check declared bare `ON TABLE`
  owes an unassessed row on every table in the workspace — a handful
  of unscoped checks fills the backlog with unfillable rows.
- **`breach_rate` is the violation share.** 0.0 is fully passing, and
  it is compared against `tolerance` upward. Reporting a pass rate
  under that key bands red.
- **The expectation is the rate you measured, never an assumed zero.**
  Nobody knows a source's defects in advance. You measured them at
  landing; author that rate as the tolerance, so the check is green
  today and red when the source drifts. A check reporting 0.0 where
  the profile showed dirt does not see the dirt — fix the check's SQL.
- **Promote confirmed reconciliations.** A `behavior_evidence`
  convention that reconciled at ~0 residual is a standing invariant —
  make it a check.
- **A window has two bounds.** A readmission, a return, a repeat
  within N days tests the next event against both ends: after the
  index event and inside the window. Probe the rows that pass with a
  negative interval — an event that begins inside the index event
  counts when only the upper bound is tested.
- **Promote basis claims.** A grounding assumption whose basis names
  a second route to the number — "ties to GL 4* net" — is a
  reconciliation in prose. Author the route as the check voice and
  band its residual against the served frame; the claim then re-runs
  at every pin move instead of drifting from the SQL it describes.

**The check half is a function, and you write it here** — the body
rides its declaration, so an expectation without a measuring voice is
a choice rather than a limit. `glossql-functions` has
the contract, the kernels and the abstention rule; `rate_tolerance` is
the detector that bands an authored expectation against a check voice:

```glossql
DECLARE FUNCTION hours_reconcile_check FOR ops AS $$
  SELECT 'measured: logged minutes against recorded durations' AS outcome,
         CASE WHEN count(*) = 0 THEN 0.0
              ELSE CAST(count(*) FILTER (WHERE abs(logged - recorded) > 0.5) AS DOUBLE) / count(*)
         END AS breach_rate
  FROM (SELECT order_id, sum(minutes) AS logged, max(duration_min) AS recorded
        FROM work_logs GROUP BY order_id)
$$ RETURNS hours_reconcile;
```

A voice speaks the aspect's own schema — `outcome` like any slot, the
measurement beside it. One schema, every speaker.
