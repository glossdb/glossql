-- Declared what-if scenarios, generically: the vocabulary names them
-- (FACT aspects with x-kind "scenario"), the collapsed read carries
-- what currently runs, the cache says whether the whatif door has
-- read it. The chart for a scenario is an authored tile in a data app
-- (glossql-apps) -- its name sits in the frame SQL, which a built-in
-- cannot know; this list is the generic surface every workspace gets.
SELECT a.name,
  arrow_cast(coalesce(json_get_str(a.schema, 'title'), a.name), 'Utf8') AS title,
  arrow_cast(coalesce(
    json_get_str(json_as_text(json_get(json_as_text(json_get(g.value, 'overrides')), 0)), 'column')
      || ' x ' || arrow_cast(json_get_float(json_as_text(json_get(json_as_text(json_get(g.value, 'overrides')), 0)), 'factor'), 'Utf8')
      || ' from ' || json_get_str(json_as_text(json_get(json_as_text(json_get(g.value, 'overrides')), 0)), 'from')
      || CASE WHEN json_get(json_as_text(json_get(g.value, 'overrides')), 1) IS NOT NULL
              THEN ' (+ more)' ELSE '' END,
    'no overrides glossed yet'), 'Utf8') AS lever,
  arrow_cast(coalesce(g.state, 'unassessed'), 'Utf8') AS state,
  arrow_cast(CASE WHEN c.computed_at IS NOT NULL
       THEN 'read ' || substr(c.computed_at, 1, 10)
       ELSE 'not read' END, 'Utf8') AS status
FROM aspects a
LEFT JOIN GLOSSARY() g ON g.aspect = a.name
LEFT JOIN (SELECT witness, max(computed_at) AS computed_at
           FROM cache WHERE function = 'whatif' GROUP BY witness) c
       ON c.witness = a.name
WHERE a.kind = 'fact' AND json_get_str(a.schema, 'x-kind') = 'scenario'
ORDER BY a.name
