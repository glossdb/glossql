-- Coverage with denominators from the model's own rules: behavior and
-- unit are owed only where role = measure. A table with no measures
-- shows an em dash — not applicable, not a gap. Composite and
-- relationship subjects stay out; this is the column grid.
WITH cols AS (
  SELECT arrow_cast(substr(subject, 1, strpos(subject, '.') - 1), 'Utf8') AS t,
         subject, aspect, body
  FROM GLOSSARY(all => true)
  WHERE kind = 'fact'
    AND strpos(subject, '.') > 0
    AND strpos(subject, '(') = 0
    AND strpos(subject, '->') = 0
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
