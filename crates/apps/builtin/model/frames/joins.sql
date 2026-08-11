-- The declared relationships touching this subject — the join
-- structure groundings lean on. Each relationship is a subject of its
-- own; its dossier holds the grain-check claims.
SELECT DISTINCT g.subject AS rel,
  arrow_cast('?subject=' || g.subject, 'Utf8') AS link
FROM GLOSSARY(all => true) g
WHERE strpos(g.subject, '->') > 0
  AND (g.subject LIKE $subject || '%' OR g.subject LIKE '% ' || $subject || '%')
ORDER BY rel
