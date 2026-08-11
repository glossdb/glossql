-- The measurement plane on one subject. An abstention is testimony:
-- it renders with its named reason, never as a blank. The profile
-- summarizes to its load-bearing numbers; anything else shows its
-- body's head, honestly truncated.
SELECT g.aspect, g.actor AS fn,
  CASE WHEN json_get_bool(g.body, 'applicable') = false THEN '◌' ELSE '○' END AS glyph,
  CASE WHEN json_get_bool(g.body, 'applicable') = false THEN 'g-abs' ELSE 'g-mea' END AS gcls,
  arrow_cast(CASE
    WHEN json_get_bool(g.body, 'applicable') = false THEN
      'abstained — ' || coalesce(json_get_str(g.body, 'reason'), 'no reason recorded')
    WHEN g.aspect = 'column_profile' THEN
      'rows ' || CAST(json_get_int(g.body, 'total') AS VARCHAR)
      || ' · nulls ' || CAST(round(json_get_float(g.body, 'null_ratio') * 100) AS VARCHAR)
      || '% · distinct ' || CAST(json_get_int(g.body, 'distinct') AS VARCHAR)
    ELSE substr(g.body, 1, 200)
  END, 'Utf8') AS text
FROM GLOSSARY(all => true) g
WHERE g.subject = $subject AND g.kind = 'measurement'
ORDER BY g.aspect
