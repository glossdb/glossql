-- Derivation candidates: row-grain arithmetic identities among a
-- table's numeric columns — `a = b * c` and `a = b + c` — with their
-- violation counts. Runs at table grain
-- (`SELECT detect_derivations() FROM orders`).
--
-- Why this exists: a scoped unit-mix artifact and
-- a real price change move a metric identically; the only instrument
-- that separates them is the derivation the lineage carries —
-- `line_amount = units * unit_price` held at violation rate 0.0 on
-- every clean corpus and fired at the artifact's exact row coverage.
-- No marginal statistic reaches this. The door optimizes recall; the
-- two constants anyone might argue with are right here: an identity
-- HOLDS at match rate >= 0.95 over >= 20 supporting rows, and the
-- judge confirms which are real derivations rather than coincidence.
-- A confirmed identity re-checked per batch is the admission
-- instrument. Ties in the ordering keep the door's enumeration order.
SELECT
  bool_and(applicable) AS applicable,
  min(reason) AS reason,
  CASE WHEN bool_and(applicable) THEN max(rows) END AS rows,
  CASE WHEN bool_and(applicable) THEN coalesce(
    array_agg(named_struct(
      'target', target, 'form', form,
      'operands', [operand_1, operand_2],
      'support', support, 'violations', violations, 'match_rate', match_rate
    ) ORDER BY match_rate DESC, seq) FILTER (WHERE match_rate >= 0.95 AND support >= 20),
    []) END AS candidates,
  CASE WHEN bool_and(applicable) THEN bool_or(truncated) END AS truncated
FROM derivation_candidates($subject)
