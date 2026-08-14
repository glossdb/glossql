-- owed — what stands waiting on an act, from the data alone.
--
-- Never a status flag anyone maintains: each row derives from a
-- mismatch that the act itself resolves, so nothing has to be marked
-- done. Four sources:
--
--   recipe   an approved recipe change with no import of that table
--            since the approval — the re-declare has not run;
--   formula  a human formula answer newer than the metric's recorded
--            materialization — `read.<metric>()` keeps serving the
--            recorded SQL until an agent recomposes it;
--   contest  a slot withheld at read because voices differ or a
--            detector crossed;
--   fold-in  a ruling whose key still stands below full confidence in
--            the agent's body.
--
-- `subject` names what the act is about, so a caller can link to it;
-- glyphs, links and ordering stay the caller's business.
--
-- The one dynamic json path — which metric a formulas map names —
-- stays at its base, where the column it indexes by is in scope.
WITH approvals AS (
  SELECT h.subject AS subject, h.written_at AS written_at,
         json_get_str(h.body, 'table') AS tbl
  FROM glossary h
  WHERE h.aspect = 'recipe_change' AND h.actor_kind = 'human'
),
recipes AS (
  SELECT 'recipe' AS kind, a.tbl AS subject,
         'recipe change on ' || a.tbl AS what,
         'approved — the re-declare has not run' AS why,
         a.written_at AS since
  FROM approvals a
  WHERE a.tbl IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM imports i
                    WHERE i.table_name = a.tbl AND i.imported_at >= a.written_at)
),
formulas AS (
  SELECT 'formula' AS kind, g.aspect AS subject,
         'formula answer on ' || g.aspect AS what,
         'the recorded materialization predates it' AS why,
         h.written_at AS since
  FROM glossary g
  JOIN aspects a ON a.name = g.aspect AND a.kind = 'query'
  JOIN glossary h ON h.subject = g.subject AND h.aspect = 'formulas'
    AND h.actor_kind = 'human' AND h.written_at > g.written_at
  WHERE g.actor_kind = 'agent'
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND NOT EXISTS (SELECT 1 FROM glossary h2
                    WHERE h2.subject = h.subject AND h2.aspect = h.aspect
                      AND h2.actor_kind = 'human' AND h2.written_at > h.written_at)
    AND json_get_str(json_get(h.body, 'formulas'), g.aspect) IS NOT NULL
),
contests AS (
  SELECT 'contest' AS kind, c.subject AS subject,
         'contested: ' || c.subject || ' ' || c.aspect AS what,
         'withheld at read — a detector crossed or voices differ' AS why,
         '' AS since
  FROM GLOSSARY() c
  WHERE c.state = 'contested'
),
foldins AS (
  SELECT 'fold-in' AS kind, r.aspect AS subject,
         'ruling on ' || r.aspect AS what,
         'ruled (' || r.stance || ') — the fold-in has not run' AS why,
         r.written_at AS since
  FROM ruling_entries r
  WHERE NOT r.folded_in
)
SELECT * FROM recipes
UNION ALL SELECT * FROM formulas
UNION ALL SELECT * FROM contests
UNION ALL SELECT * FROM foldins
