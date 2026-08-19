-- Relationship candidates (fixture 12): the high-recall half of the
-- candidate -> verified -> declared arc. Runs at dataset grain
-- (`SELECT detect_relationships() FROM fin`) and serves every plausible
-- join pair the door measured across the landed tables — generous by
-- design: the statistical pass optimizes recall, and
-- the judge reading this measurement removes the false positives
-- against the data; this body never does. Core fields (from, to,
-- cardinality, overlap) answer the aspect schema; the rest of the
-- evidence rides its open remainder, and a composite endpoint rides
-- key_columns (the tuple is the key, fixture 14). Candidates order by
-- overlap, ties in the door's enumeration order.
SELECT named_struct('candidates', coalesce(
  array_agg(named_struct(
    'from', from_col, 'to', to_col, 'cardinality', cardinality,
    'overlap', overlap, 'matched', matched, 'orphans', orphans,
    'from_distinct', from_distinct, 'to_distinct', to_distinct,
    'key_columns', CASE WHEN kc_from IS NOT NULL
                        THEN [named_struct('from', kc_from, 'to', kc_to)] END
  ) ORDER BY overlap DESC, seq) FILTER (WHERE from_col IS NOT NULL),
  [])) AS result
FROM relationship_candidates($subject)
