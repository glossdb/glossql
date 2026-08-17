-- Hierarchy candidates: pairwise FD screens at high recall over one
-- table's dimension-like columns — the cheap SQL core of v0.3's
-- dimension-identity stack (analysis/hierarchies, transcribed
-- 2026-08-05). Runs at table grain
-- (`SELECT detect_hierarchies() FROM journal_lines`).
--
-- The measurement's job is recall; the judge removes false positives.
-- The door (`hierarchy_candidates`) serves every screened direction
-- with its g3, λ and rows_per_value; the constants anyone might argue
-- with are right here: candidates ship at g3 <= 0.05 (v0.3 asserted
-- edges at 0.01 — the extra band is the judge's to kill against the
-- data), and a perfect near-copy BOTH ways at the asserted line is
-- served as kind "alias" — whether that is a code↔label relabeling or
-- a coincidence is exactly what no statistic can settle; the identity
-- judgment stays with the reader (the glossql-dimensions skill carries
-- the lessons). λ is served beside every candidate, never gated:
-- λ < 0.5 is the recorded vacuous-skew signature the judge reads.
SELECT
  bool_and(applicable) AS applicable,
  min(reason) AS reason,
  CASE WHEN bool_and(applicable) THEN max(rows) END AS rows,
  CASE WHEN bool_and(applicable) THEN coalesce(
    array_agg(named_struct(
      'from', from_col, 'to', to_col,
      'distinct_from', distinct_from, 'distinct_to', distinct_to,
      'pair_groups', pair_groups, 'g3', g3, 'lambda', lambda,
      'rows_per_value', rows_per_value,
      'kind', CASE WHEN g3 <= 0.01 AND g3_reverse <= 0.01 THEN 'alias' ELSE 'edge' END
    ) ORDER BY seq) FILTER (WHERE g3 <= 0.05), []) END AS candidates
FROM hierarchy_candidates($subject)
