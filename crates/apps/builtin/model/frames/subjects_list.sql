-- Everything a claim attaches to, sorted by what it is: the dataset,
-- its tables, their columns, the declared relationships. The slot
-- count is how much is claimed on each.
SELECT s.subject,
  CASE WHEN strpos(s.subject, '->') > 0 THEN 'relationship'
       WHEN d.name IS NOT NULL THEN 'dataset'
       WHEN strpos(s.subject, '.') > 0 THEN 'column'
       ELSE 'table' END AS skind,
  s.n,
  arrow_cast('?subject=' || s.subject, 'Utf8') AS link
FROM (SELECT subject, count(*) AS n FROM GLOSSARY(all => true) GROUP BY subject) s
LEFT JOIN datasets d ON d.name = s.subject
ORDER BY CASE skind WHEN 'dataset' THEN 0 WHEN 'table' THEN 1
              WHEN 'column' THEN 2 ELSE 3 END, s.subject
