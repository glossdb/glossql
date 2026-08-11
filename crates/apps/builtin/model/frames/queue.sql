-- The judgement queue: what the model does not yet hold firmly,
-- loosest first. Two sources — claims the model's own rules owe and
-- nobody wrote (measures missing behavior or unit, from the collapsed
-- read's unassessed disclosure), and judged metric assumptions below full
-- confidence (the assumptions convention: every metric writing
-- carries [{dimension, assumption, basis, confidence}]). Glyphs and
-- links are the frame's job; the template stays dumb. No cap — the
-- queue is the whole of what is owed, and the page scrolls it.
WITH owed AS (
  SELECT '–' AS glyph, 'g-una' AS gcls,
         CAST(NULL AS DOUBLE) AS conf,
         c.subject AS subj, c.aspect AS asp,
         CASE c.aspect
           WHEN 'behavior' THEN 'a measure with no summability claim — nothing under any sum of it'
           ELSE 'a measure with no unit' END AS what,
         '?subject=' || c.subject AS link
  FROM GLOSSARY() c
  JOIN (SELECT subject FROM GLOSSARY(all => true)
        WHERE aspect = 'role' AND json_get_str(body, 'value') = 'measure') r
    ON r.subject = c.subject
  WHERE c.state = 'unassessed' AND c.aspect IN ('behavior', 'unit')
),
loose AS (
  SELECT '◐' AS glyph, 'g-jud' AS gcls,
         json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
         g.aspect AS subj,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS asp,
         json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
         '?metric=' || g.aspect AS link
  FROM GLOSSARY(all => true) g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
)
SELECT * FROM (SELECT * FROM owed UNION ALL SELECT * FROM loose)
ORDER BY conf ASC NULLS FIRST, subj, asp
