-- Relationship candidates (fixture 12): the high-recall half of the
-- candidate -> verified -> declared arc. Runs at dataset grain
-- (`SELECT detect_relationships() FROM fin`) and serves every plausible
-- join pair the door measured across the landed tables — generous by
-- design: the statistical pass optimizes recall, and
-- the judge reading this measurement removes the false positives
-- against the data; this body never does. Core fields (from, to,
-- cardinality, overlap) answer the aspect schema; the rest of the
-- evidence rides its open remainder, and a composite endpoint rides
-- key_columns (the tuple is the key, fixture 14). Candidates rank by
-- what a reference looks like: its target is a clean key (exactly
-- unique in its table and not a point in time), it repeats
-- (many-to-one before one-to-one), it reaches its key (matched over
-- the key's distinct count), and it resolves (overlap), ties in the
-- door's enumeration order. Key coverage stands before overlap because
-- a small code set is contained in every dense id space at overlap
-- 1.0 and reaches almost none of it; the clean-key tier stands first
-- because a copied date column and a sibling table's own foreign key
-- both contain a reference's values without being what it refers to.
-- Demotion only — nothing leaves the list. Extraction serves the
-- summary alone — the count and the top of the ranking — and the full
-- list reads back via GLOSSARY(dataset::relationship_candidates).
WITH c AS (
  SELECT coalesce(
    array_agg(named_struct(
      'from', from_col, 'to', to_col, 'cardinality', cardinality,
      'overlap', overlap, 'matched', matched, 'orphans', orphans,
      'from_distinct', from_distinct, 'to_distinct', to_distinct,
      'to_unique', to_unique, 'to_temporal', to_temporal,
      'key_columns', CASE WHEN kc_from IS NOT NULL
                          THEN [named_struct('from', kc_from, 'to', kc_to)] END
    ) ORDER BY CASE WHEN to_unique AND NOT to_temporal THEN 0 ELSE 1 END,
               CASE cardinality WHEN 'many-to-one' THEN 0 ELSE 1 END,
               CAST(matched AS DOUBLE) / to_distinct DESC, overlap DESC, seq)
      FILTER (WHERE from_col IS NOT NULL),
    []) AS candidates
  FROM relationship_candidates($subject)
)
SELECT named_struct(
  'candidates', candidates,
  'summary', named_struct(
    'candidates', cardinality(candidates),
    'top', array_slice(candidates, 1, 16),
    'note', 'top 16: clean keys first (unique target, not a date), then many-to-one, then key coverage, then overlap — every candidate reads back via GLOSSARY(dataset::relationship_candidates)'
  )) AS result
FROM c
