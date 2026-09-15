-- Coverage with denominators from the model's own rules: behavior and
-- unit are owed only where role = measure. A table with no measures
-- shows an em dash — not applicable, not a gap. This is the column
-- grid: a subject counts only where the dataset holds that column,
-- so an app's parts (`<app>.<part>`, dotted the same way) and the
-- composite and relationship subjects stay out.
WITH cols AS (
  SELECT arrow_cast(c.table_name, 'Utf8') AS t,
         g.subject, g.aspect, g.body
  FROM GLOSSARY(all => true) g
  JOIN current_dataset d ON true
  JOIN information_schema.columns c
    ON c.table_schema = d.dataset AND c.table_name || '.' || c.column_name = g.subject
  WHERE g.kind = 'fact'
),
per AS (
  SELECT t,
    count(DISTINCT subject) AS cols,
    count(*) FILTER (WHERE aspect = 'meaning') AS meaning,
    count(*) FILTER (WHERE aspect = 'behavior') AS behavior,
    count(*) FILTER (WHERE aspect = 'unit') AS unit,
    count(*) FILTER (WHERE aspect = 'role'
      AND json_get_str(body, 'value') = 'measure') AS owed,
    count(*) FILTER (WHERE aspect = 'dimension'
      AND json_get_str(body, 'value') IN ('primary', 'supporting')) AS dims
  FROM cols GROUP BY t
)
SELECT t, cols, meaning, dims,
  arrow_cast(CASE WHEN owed = 0 THEN '–'
       ELSE CAST(behavior AS VARCHAR) || '/' || CAST(owed AS VARCHAR) END, 'Utf8') AS btxt,
  CASE WHEN behavior < owed THEN 'r-gap' ELSE '' END AS bcls,
  arrow_cast(CASE WHEN owed = 0 THEN '–'
       ELSE CAST(unit AS VARCHAR) || '/' || CAST(owed AS VARCHAR) END, 'Utf8') AS utxt,
  CASE WHEN unit < owed THEN 'r-gap' ELSE '' END AS ucls
FROM per
ORDER BY cols DESC
