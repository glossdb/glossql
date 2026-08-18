-- Relationship coherence: what the declared joins assert, checked
-- against the rows. Runs at dataset grain
-- (`SELECT relationship_coherence() FROM fin`) over DECLARED
-- relationships only — the candidate plane proposes, the judge
-- declares, this measures what the declaration now claims. The door
-- (`relationship_checks`) carries the two facts no column-shaped check
-- can see — orphans and the temporal precedence evidence — and, since
-- the port, a composite endpoint joins on every leg of its tuple
-- (fixture 14) instead of being dropped as unspellable.
WITH r AS (
  SELECT applicable, reason, seq, relationship, filled, orphans, orphan_rate,
         coalesce(array_agg(named_struct(
           'child_column', child_column, 'parent_column', parent_column,
           'joined', joined, 'precedes', precedes, 'precedes_rate', precedes_rate
         ) ORDER BY pair_seq) FILTER (WHERE child_column IS NOT NULL), []) AS temporal
  FROM relationship_checks($subject)
  GROUP BY applicable, reason, seq, relationship, filled, orphans, orphan_rate
)
SELECT
  bool_and(applicable) AS applicable,
  min(reason) AS reason,
  CASE WHEN bool_and(applicable) THEN coalesce(array_agg(named_struct(
    'relationship', relationship, 'filled', filled, 'orphans', orphans,
    'orphan_rate', orphan_rate, 'temporal', temporal
  ) ORDER BY seq) FILTER (WHERE relationship IS NOT NULL), []) END AS relationships
FROM r
