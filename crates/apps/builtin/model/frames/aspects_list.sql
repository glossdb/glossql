-- The vocabulary itself: every declared aspect, its kind, the grains
-- it may attach to, and the values it admits — read from the declared
-- schema, which is also where a future contest form would get its
-- options.
SELECT a.name, a.kind,
  coalesce(a.grains, '') AS grains,
  coalesce(json_get_str(a.schema, 'title'), '') AS title,
  coalesce(
    replace(replace(replace(replace(
      json_as_text(json_get(json_get(json_get(a.schema, 'properties'), 'value'), 'enum')),
      '["', ''), '"]', ''), '", "', ' | '), '","', ' | '),
    'free text') AS admits
FROM aspects a
ORDER BY CASE a.kind WHEN 'fact' THEN 0 WHEN 'measurement' THEN 1 ELSE 2 END, a.name
