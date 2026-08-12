-- One metric surface, both faces: the pinned formula from the
-- workspace's formulas gloss, and the recorded materialization that
-- must keep matching it. One row per metric: the winning slot only —
-- a human writing outranks the agent slot (the supersession key holds
-- one slot per actor kind, so the anti-join below is total). The json
-- chains stay in place on the base scan: a dynamic extraction parked
-- in a CTE column comes back null (measured 2026-08-12).
SELECT q.aspect,
  coalesce(json_get_str(a.schema, 'title'), q.aspect) AS title,
  coalesce(json_get_str(a.schema, 'x-unit'), '') AS unit,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  json_get_str(q.body, 'sql') AS sql,
  arrow_cast(coalesce(json_get_str(json_get(f.value, 'formulas'), CAST($metric AS VARCHAR)),
           'no pinned formula'), 'Utf8') AS formula,
  q.actor,
  arrow_cast(substr(q.written_at, 1, 10), 'Utf8') AS written,
  'GLOSS ' || q.aspect || ' ON ' || d.name || ' AS $$' || q.body || '$$;' AS statement
FROM GLOSSARY(all => true) q
JOIN aspects a ON a.name = q.aspect
CROSS JOIN datasets d
LEFT JOIN GLOSSARY() f ON f.aspect = 'formulas'
WHERE q.kind = 'query' AND q.aspect = $metric
  AND (EXISTS (SELECT 1 FROM glossary me
               WHERE me.subject = q.subject AND me.aspect = q.aspect
                 AND me.actor_id = q.actor AND me.actor_kind = 'human')
       OR NOT EXISTS (SELECT 1 FROM glossary h
                      WHERE h.subject = q.subject AND h.aspect = q.aspect
                        AND h.actor_kind = 'human'))
