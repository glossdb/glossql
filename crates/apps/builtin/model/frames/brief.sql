-- The waiting half of the human's brief (ruled 2026-08-12): human
-- writings that owe an agent act, derived from data — never a status flag anyone
-- maintains. Three sources:
--   1. recipe_change approvals with no import of the named table
--      since the approval (the re-declare has not run);
--   2. formula answers newer than a metric's recorded materialization
--      (the two forms of one definition have not been re-aligned —
--      the lead's catch: read.<metric>() serves the recorded SQL
--      until an agent recomposes it);
--   3. contested slots (dependents await re-judging).
-- Static json paths project through CTEs; the one dynamic path
-- (which metric a formulas map names) stays in place at its base.
WITH approvals AS (
  SELECT h.subject, h.written_at, json_get_str(h.body, 'table') AS tbl
  FROM glossary h
  WHERE h.aspect = 'recipe_change' AND h.actor_kind = 'human'
),
waiting_recipes AS (
  SELECT 'recipe change on ' || a.tbl AS what,
         'approved — the re-declare has not run' AS why,
         arrow_cast(replace(substr(a.written_at, 1, 16), 'T', ' '), 'Utf8') AS since,
         '?subject=' || a.tbl AS link
  FROM approvals a
  WHERE a.tbl IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM imports i
                    WHERE i.table_name = a.tbl AND i.imported_at >= a.written_at)
),
waiting_formulas AS (
  SELECT 'formula answer on ' || g.aspect AS what,
         'the recorded materialization predates it' AS why,
         arrow_cast(replace(substr(h.written_at, 1, 16), 'T', ' '), 'Utf8') AS since,
         '?metric=' || g.aspect AS link
  FROM glossary g
  JOIN aspects a ON a.name = g.aspect AND a.kind = 'query'
  JOIN glossary h ON h.subject = g.subject AND h.aspect = 'formulas'
    AND h.actor_kind = 'human' AND h.written_at > g.written_at
  WHERE g.actor_kind = 'agent'
    -- the store keeps superseded writings; only the live slot counts
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND NOT EXISTS (SELECT 1 FROM glossary h2
                    WHERE h2.subject = h.subject AND h2.aspect = h.aspect
                      AND h2.actor_kind = 'human' AND h2.written_at > h.written_at)
    AND json_get_str(json_get(h.body, 'formulas'), g.aspect) IS NOT NULL
),
contested AS (
  SELECT 'contested: ' || c.subject || ' ' || c.aspect AS what,
         'withheld at read — a detector crossed or voices differ; the signal is yours to run down' AS why,
         '' AS since,
         '?subject=' || c.subject AS link
  FROM GLOSSARY() c
  WHERE c.state = 'contested'
)
SELECT arrow_cast(what, 'Utf8') AS what,
       arrow_cast(why, 'Utf8') AS why,
       arrow_cast(since, 'Utf8') AS since,
       arrow_cast(link, 'Utf8') AS link
FROM (SELECT * FROM waiting_recipes
      UNION ALL SELECT * FROM waiting_formulas
      UNION ALL SELECT * FROM contested)
ORDER BY since DESC, what
