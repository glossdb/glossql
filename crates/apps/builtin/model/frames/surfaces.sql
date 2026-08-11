-- The declared metric surfaces: every QUERY aspect with its pinned
-- formula where the formulas gloss names one, and whether a recorded
-- materialization exists to serve reads. The list is the navigation —
-- each row opens the metric's dossier.
SELECT
  coalesce(json_get_str(a.schema, 'title'), a.name) AS title,
  a.name,
  coalesce(json_get_str(a.schema, 'x-unit'), '') AS unit,
  coalesce(json_get_str(a.schema, 'x-kind'), '') AS mkind,
  coalesce(json_get_str(json_get(f.body, 'formulas'), a.name),
           'a base concept — grounded as an extract, no formula') AS formula,
  CASE WHEN q.aspect IS NULL THEN 'nothing recorded' ELSE 'grounded' END AS status,
  CASE WHEN q.aspect IS NULL THEN 'warn' ELSE 'ok' END AS scls,
  arrow_cast('?metric=' || a.name, 'Utf8') AS link
FROM aspects a
LEFT JOIN (SELECT DISTINCT aspect FROM GLOSSARY(all => true) WHERE kind = 'query') q
  ON q.aspect = a.name
LEFT JOIN GLOSSARY(all => true) f ON f.aspect = 'formulas' AND f.kind = 'fact'
WHERE a.kind = 'query'
ORDER BY CASE WHEN coalesce(json_get_str(a.schema, 'x-kind'), '') = 'metric'
              THEN 0 ELSE 1 END, a.name
