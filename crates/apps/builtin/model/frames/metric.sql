-- One metric surface, both faces: the pinned formula from the
-- workspace's formulas gloss, and the recorded materialization that
-- must keep matching it.
SELECT q.aspect,
  coalesce(json_get_str(a.schema, 'title'), q.aspect) AS title,
  coalesce(json_get_str(a.schema, 'x-unit'), '') AS unit,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  json_get_str(q.body, 'sql') AS sql,
  coalesce(json_get_str(json_get(f.body, 'formulas'), CAST($metric AS VARCHAR)),
           'no pinned formula') AS formula,
  q.actor,
  arrow_cast(substr(q.written_at, 1, 10), 'Utf8') AS written,
  'GLOSS ' || q.aspect || ' ON ' || d.name || ' AS $$' || q.body || '$$;' AS statement
FROM GLOSSARY(all => true) q
JOIN aspects a ON a.name = q.aspect
CROSS JOIN datasets d
LEFT JOIN GLOSSARY(all => true) f ON f.aspect = 'formulas'
WHERE q.kind = 'query' AND q.aspect = $metric
