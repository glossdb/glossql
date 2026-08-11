-- Every judged slice axis — primary, supporting, and the ruled-out —
-- ranked by measured relevance. The judged fact carries the grounds
-- prose; the measurement carries the number the bar draws. Ruled-out
-- axes keep their grounds: why the model excluded an axis is knowledge
-- too, and hiding it would make the list look complete when it is
-- selective.
SELECT d.subject,
  json_get_str(d.body, 'value') AS val,
  CASE json_get_str(d.body, 'value')
    WHEN 'primary' THEN 'ok' WHEN 'supporting' THEN '' ELSE 'off' END AS vcls,
  arrow_cast(coalesce(
    CAST(round(json_get_float(m.body, 'relevance') * 1000) / 1000 AS VARCHAR),
    '·'), 'Utf8') AS relevance,
  coalesce(round(json_get_float(m.body, 'relevance') * 100), 0) AS pct,
  coalesce(json_get_str(d.body, 'grounds'), '') AS grounds,
  arrow_cast('?subject=' || d.subject, 'Utf8') AS link
FROM GLOSSARY(all => true) d
LEFT JOIN GLOSSARY(all => true) m
  ON m.subject = d.subject AND m.aspect = 'dimension_relevance'
WHERE d.aspect = 'dimension'
ORDER BY CASE json_get_str(d.body, 'value')
    WHEN 'primary' THEN 0 WHEN 'supporting' THEN 1 ELSE 2 END,
  json_get_float(m.body, 'relevance') DESC NULLS LAST, d.subject
