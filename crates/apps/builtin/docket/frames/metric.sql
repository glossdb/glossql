-- One metric surface, both faces: the standing formula from the
-- workspace's formulas gloss, and the recorded materialization that
-- must keep matching it. One row per metric: the winning slot only —
-- a human writing outranks the agent slot (the supersession key holds
-- one slot per actor kind, so the anti-join below is total). The json
-- chains stay in place on the base scan: a dynamic extraction parked
-- in a CTE column comes back null.
SELECT q.aspect,
  coalesce(json_get_str(a.schema, 'title'), q.aspect) AS title,
  -- `unit` and `meaning` ride the `definitions` registry, never the
  -- aspect blob: a declaration cannot be superseded, and both are the
  -- company's to revise (the blob's `x-unit` goes stale on revision).
  arrow_cast(coalesce(
    json_get_str(json_get(json_get(d.value, 'definitions'), q.aspect), 'unit'),
    ''), 'Utf8') AS unit,
  arrow_cast(coalesce(
    json_get_str(json_get(json_get(d.value, 'definitions'), q.aspect), 'meaning'),
    'no meaning written — the definitions registry has no entry for this metric'),
    'Utf8') AS meaning,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  json_get_str(q.body, 'sql') AS sql,
  -- the meta line, separators only between present parts — an unset
  -- unit must not leave a dangling dot in the tile
  arrow_cast(concat_ws(' · ',
    coalesce(json_get_str(a.schema, 'title'), q.aspect),
    nullif(coalesce(json_get_str(a.schema, 'x-kind'), ''), ''),
    nullif(json_get_str(json_get(json_get(d.value, 'definitions'), q.aspect), 'unit'), '')),
    'Utf8') AS meta,
  arrow_cast(coalesce(json_get_str(json_get(f.value, 'formulas'), CAST($metric AS VARCHAR)),
           'no recorded formula'), 'Utf8') AS formula,
  q.actor,
  arrow_cast(substr(q.written_at, 1, 10), 'Utf8') AS written,
  arrow_cast('GLOSS ' || q.aspect || ' ON '
    || (SELECT dataset FROM current_dataset)
    || ' AS $$' || q.body || '$$;', 'Utf8') AS statement
FROM GLOSSARY(all => true) q
JOIN aspects a ON a.name = q.aspect
LEFT JOIN GLOSSARY() f ON f.aspect = 'formulas'
LEFT JOIN GLOSSARY() d ON d.aspect = 'definitions'
WHERE q.kind = 'query' AND q.aspect = $metric
  -- The outer scan is GLOSSARY(), which serves this dataset; these two
  -- drop into the raw relation, which serves the workspace. Without the
  -- dataset leg the second one *suppresses*: a human writing on a
  -- same-named subject in another dataset makes this dataset's agent
  -- row vanish from the tile, which is worse than showing too much.
  AND (EXISTS (SELECT 1 FROM glossary me
               WHERE me.dataset = (SELECT dataset FROM current_dataset)
                 AND me.subject = q.subject AND me.aspect = q.aspect
                 AND me.actor_id = q.actor AND me.actor_kind = 'human')
       OR NOT EXISTS (SELECT 1 FROM glossary h
                      WHERE h.dataset = (SELECT dataset FROM current_dataset)
                        AND h.subject = q.subject AND h.aspect = q.aspect
                        AND h.actor_kind = 'human'))
