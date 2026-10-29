-- What the vocabulary still owes, by name: every slot a witnessed
-- fact aspect is owed on — the aspect's grain and condition name the
-- subject — that no voice has filled, grouped by aspect and, under
-- it, by table. The names, not the counts; the row opens the aspect
-- on the Glossary tab, where each name has its entry.
WITH unspoken AS (
  SELECT g.aspect, g.subject
  FROM GLOSSARY() g JOIN aspects a ON a.name = g.aspect
  WHERE a.kind = 'fact' AND g.state = 'unassessed'
),
named AS (
  SELECT aspect,
         CASE WHEN strpos(subject, ' -> ') > 0 THEN 'joins'
              WHEN strpos(subject, '.') > 0 THEN split_part(subject, '.', 1)
              ELSE 'whole tables' END AS grp,
         CASE WHEN strpos(subject, ' -> ') > 0 OR strpos(subject, '.') = 0 THEN subject
              ELSE split_part(subject, '.', 2) END AS name
  FROM unspoken
),
grouped AS (
  SELECT aspect, grp, count(*) AS n, array_to_string(array_agg(name ORDER BY name), ', ') AS names
  FROM named
  GROUP BY aspect, grp
)
SELECT arrow_cast(aspect, 'Utf8') AS aspect,
       sum(n) AS owed,
       arrow_cast(array_to_string(array_agg(
         grp || ': ' || names ORDER BY CASE grp WHEN 'whole tables' THEN 0 WHEN 'joins' THEN 2 ELSE 1 END, grp), ' · '), 'Utf8') AS names,
       arrow_cast('/' || d.dataset || '/app/docket/p/glossary?aspect=' || aspect, 'Utf8') AS link
FROM grouped CROSS JOIN current_dataset d
GROUP BY aspect, d.dataset
ORDER BY owed DESC, aspect
