-- The metric's assumptions ledger: every dimension of the claim with
-- its basis and confidence. Full confidence renders fixed (●), less
-- renders loose (◐) — until the read discloses actor kind, the glyph
-- speaks to firmness, not to who holds it. Each row carries the
-- contest statement — the whole metric gloss, because the gloss is
-- the unit of writing: an assumption is a row inside it, edited in
-- place while the rest rides along.
WITH a AS (
  SELECT
    json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dim,
    json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
    json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
    coalesce(json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis'), '') AS basis,
    arrow_cast('GLOSS ' || g.aspect || ' ON ' || CAST($dataset AS VARCHAR) || ' AS $$' || g.body || '$$;', 'Utf8') AS statement
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query' AND g.aspect = $metric
    AND i.i < json_length(g.body, 'assumptions')
    -- the winning slot only: human outranks agent, one slot per actor
    -- kind under the supersession key, so this anti-join is total
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
)
SELECT
  CASE WHEN conf >= 1.0 THEN '●' ELSE '◐' END AS glyph,
  CASE WHEN conf >= 1.0 THEN 'g-fix' ELSE 'g-jud' END AS gcls,
  conf, dim, what, basis, statement
FROM a
ORDER BY conf, dim
