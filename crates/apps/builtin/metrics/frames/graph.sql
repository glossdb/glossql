-- The metric graph, one node's edges: which sibling surfaces this
-- metric composes (named in its standing formula, or read through
-- read.<name>() in its recorded materialization) and which compose it.
-- A textual mention, stated as such — the same honesty the model app's
-- travels list keeps. Each edge opens the sibling's dossier.
--
-- Shape note: the one json chain lives in the `formulas` CTE and only
-- STRINGS leave it — a json value parked in a CTE column comes back
-- null (measured 2026-08-12), and repeating the chain across the union
-- branches trips the optimizer's subexpression extraction (a
-- qualified/unqualified field collision, found 2026-08-13).
WITH grounded AS (
  SELECT g.aspect, json_get_str(g.body, 'sql') AS sql
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'query'
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
),
m AS (SELECT name FROM aspects WHERE kind = 'query'),
formulas AS (
  SELECT m.name, coalesce(json_get_str(json_get(f.value, 'formulas'), m.name), '') AS formula
  FROM m
  LEFT JOIN GLOSSARY() f ON f.aspect = 'formulas'
)
SELECT
  arrow_cast(dir, 'Utf8') AS dir,
  other,
  arrow_cast(CASE WHEN in_formula > 0 THEN formula_why ELSE sql_why END, 'Utf8') AS why,
  arrow_cast('?metric=' || other, 'Utf8') AS link
FROM (
  SELECT 'composes' AS dir, m.name AS other,
    'named in the formula' AS formula_why,
    'read in the materialization' AS sql_why,
    strpos(coalesce(fme.formula, ''), m.name) AS in_formula,
    strpos(coalesce(gq.sql, ''), 'read.' || m.name) AS in_sql
  FROM m
  LEFT JOIN formulas fme ON fme.name = CAST($metric AS VARCHAR)
  LEFT JOIN grounded gq ON gq.aspect = CAST($metric AS VARCHAR)
  WHERE m.name <> CAST($metric AS VARCHAR)
  UNION ALL
  SELECT 'feeds', m.name,
    'names it in the formula',
    'reads it in the materialization',
    strpos(coalesce(fo.formula, ''), CAST($metric AS VARCHAR)),
    strpos(coalesce(gm.sql, ''), 'read.' || CAST($metric AS VARCHAR))
  FROM m
  LEFT JOIN formulas fo ON fo.name = m.name
  LEFT JOIN grounded gm ON gm.aspect = m.name
  WHERE m.name <> CAST($metric AS VARCHAR)
)
WHERE in_formula > 0 OR in_sql > 0
ORDER BY dir, other
