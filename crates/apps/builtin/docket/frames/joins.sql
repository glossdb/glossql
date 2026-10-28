-- The joins: every declared relationship with what the record says of
-- it — the meaning entry on the pair path, a person's over the
-- agent's — and what the coherence measurement found: every row
-- resolving, or how many do not. A join nobody measured says so. The
-- ones leaving rows unresolved come first; a row opens the left
-- table's page. Two page params narrow the list: `table` to the joins
-- a table takes part in, `column` to the joins one column takes part
-- in; empty, every join serves.
WITH coh AS (
  SELECT json_get_str(rel, 'relationship') AS rel,
         json_get_int(rel, 'orphans') AS orphans,
         json_get_int(rel, 'filled') AS filled
  FROM (SELECT json_get(json_get(g.body, 'relationships'), r.i) AS rel
        FROM GLOSSARY(all => true) g CROSS JOIN generate_series(0, 99) AS r(i)
        WHERE g.kind = 'measurement' AND g.current AND g.aspect = 'relationship_coherence'
          AND r.i < json_length(g.body, 'relationships'))),
m AS (
  SELECT g.subject, json_get_str(g.body, 'value') AS meaning, g.actor_kind,
         row_number() OVER (PARTITION BY g.subject
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect = 'meaning' AND strpos(g.subject, ' -> ') > 0)
SELECT arrow_cast(r.left_path || ' → ' || r.right_path, 'Utf8') AS rel,
       arrow_cast(coalesce(m.meaning, ''), 'Utf8') AS meaning,
       arrow_cast(CASE WHEN m.actor_kind = 'human' THEN 'ruled' ELSE '' END, 'Utf8') AS mark,
       arrow_cast(CASE WHEN c.rel IS NULL THEN 'not yet measured'
                       WHEN c.orphans = 0 THEN 'every row resolves'
                       ELSE CAST(c.orphans AS VARCHAR) || ' of ' || CAST(c.filled AS VARCHAR)
                         || ' rows without a ' || split_part(r.right_path, '.', 1) END, 'Utf8') AS coherence,
       coalesce(c.orphans, 0) AS orphans,
       arrow_cast('?table=' || split_part(r.left_path, '.', 1), 'Utf8') AS link
FROM relationships r
JOIN current_dataset d ON d.dataset = r.dataset
LEFT JOIN coh c ON c.rel = r.left_path || ' -> ' || r.right_path
LEFT JOIN m ON m.subject = r.left_path || ' -> ' || r.right_path AND m.rn = 1
WHERE (coalesce(CAST($table AS VARCHAR), '') = ''
       OR split_part(r.left_path, '.', 1) = CAST($table AS VARCHAR)
       OR split_part(r.right_path, '.', 1) = CAST($table AS VARCHAR))
  AND (coalesce(CAST($column AS VARCHAR), '') = ''
       OR r.left_path = CAST($column AS VARCHAR)
       OR r.right_path = CAST($column AS VARCHAR))
ORDER BY orphans DESC, rel
