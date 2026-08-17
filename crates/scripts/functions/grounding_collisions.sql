-- Grounding collisions: two concepts grounding to the same extract make
-- every ratio between them compute 1.0, silently — the cheapest wrong
-- number there is. Runs at dataset grain
-- (`SELECT detect_grounding_collisions() FROM fin`). The door buckets
-- every current grounding by its canonical SQL and by its served
-- monthly series; a bucket holding two or more concepts is a collision
-- — reported, never resolved: deliberate synonyms exist, and telling
-- them from errors is the judge's call against the definitions.
SELECT
  max(groundings) > 0 AS applicable,
  max(groundings) AS groundings,
  coalesce(array_agg(named_struct(
    'kind', kind, 'sql', sql, 'months', months,
    'aspects', aspects, 'subjects', subjects
  ) ORDER BY seq) FILTER (WHERE kind IS NOT NULL), []) AS collisions
FROM grounding_collisions($subject)
