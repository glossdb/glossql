-- One metric surface, both faces: the standing formula from the
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
  -- the meta line, separators only between present parts — an unset
  -- unit must not leave a dangling dot in the tile
  arrow_cast(concat_ws(' · ',
    coalesce(json_get_str(a.schema, 'title'), q.aspect),
    nullif(coalesce(json_get_str(a.schema, 'x-kind'), ''), ''),
    nullif(coalesce(json_get_str(a.schema, 'x-unit'), ''), '')), 'Utf8') AS meta,
  -- The formula, from the one place it is written: the grounding's own
  -- opening comment. This used to read a `formulas` gloss — a kit
  -- aspect neither skill teaches, so nothing ever wrote one and the
  -- face was empty in every workspace that ever existed. The practice
  -- skill already requires the mechanics to be said as comments inside
  -- the SQL, where they cannot drift from the query; the first line is
  -- the author's one-sentence statement of what this metric is.
  -- The opening block, not just its first line: an author writes a
  -- sentence and wraps it, so line one alone stops mid-clause. Five
  -- lines is the bound — a formula that needs more than that is a
  -- description, and the whole SQL is one tile down.
  arrow_cast(coalesce(nullif(trim(concat_ws(' ',
    CASE WHEN starts_with(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 1)), '--')
         THEN ltrim(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 1)), '- ') END,
    CASE WHEN starts_with(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 2)), '--')
         THEN ltrim(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 2)), '- ') END,
    CASE WHEN starts_with(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 3)), '--')
         THEN ltrim(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 3)), '- ') END,
    CASE WHEN starts_with(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 4)), '--')
         THEN ltrim(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 4)), '- ') END,
    CASE WHEN starts_with(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 5)), '--')
         THEN ltrim(ltrim(split_part(json_get_str(q.body, 'sql'), chr(10), 5)), '- ') END)), ''),
    'the grounding opens with no comment — say what this metric is above its SQL'
  ), 'Utf8') AS formula,
  q.actor,
  arrow_cast(substr(q.written_at, 1, 10), 'Utf8') AS written,
  arrow_cast('GLOSS ' || q.aspect || ' ON ' || CAST($dataset AS VARCHAR) || ' AS $$' || q.body || '$$;', 'Utf8') AS statement
FROM GLOSSARY(all => true) q
JOIN aspects a ON a.name = q.aspect
WHERE q.kind = 'query' AND q.aspect = $metric
  AND (EXISTS (SELECT 1 FROM glossary me
               WHERE me.subject = q.subject AND me.aspect = q.aspect
                 AND me.actor_id = q.actor AND me.actor_kind = 'human')
       OR NOT EXISTS (SELECT 1 FROM glossary h
                      WHERE h.subject = q.subject AND h.aspect = q.aspect
                        AND h.actor_kind = 'human'))
