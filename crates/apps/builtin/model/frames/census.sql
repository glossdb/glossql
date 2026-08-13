-- The standing counts, one row. `needs` is the one queue's honest
-- total: judged assumptions below full confidence — judgment only
-- (ruled 2026-08-13). Unassessed witnessed claims (behavior, unit)
-- count under `waiting` instead: a claim a measurement can settle
-- waits on the agent's functions, never on a human answer.
WITH raw AS (SELECT * FROM GLOSSARY(all => true)),
loose AS (
  SELECT count(*) AS n
  FROM raw g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.kind = 'query'
    AND i.i < json_length(g.body, 'assumptions')
    AND json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') < 1.0
    -- the winning slot only, as in frames/queue.sql
    AND (EXISTS (SELECT 1 FROM glossary me
                 WHERE me.subject = g.subject AND me.aspect = g.aspect
                   AND me.actor_id = g.actor AND me.actor_kind = 'human')
         OR NOT EXISTS (SELECT 1 FROM glossary h
                        WHERE h.subject = g.subject AND h.aspect = g.aspect
                          AND h.actor_kind = 'human'))
),
owed AS (
  SELECT count(*) AS n
  FROM GLOSSARY() c
  JOIN (SELECT subject FROM raw
        WHERE aspect = 'role' AND json_get_str(body, 'value') = 'measure') r
    ON r.subject = c.subject
  WHERE c.state = 'unassessed' AND c.aspect IN ('behavior', 'unit')
),
-- The waiting count mirrors frames/brief.sql: human writings that owe
-- an agent act (unexecuted recipe approvals, formula answers newer
-- than their metric's recorded materialization, contested slots).
approvals AS (
  SELECT h.subject, h.written_at, json_get_str(h.body, 'table') AS tbl
  FROM glossary h
  WHERE h.aspect = 'recipe_change' AND h.actor_kind = 'human'
),
waiting AS (
  SELECT
    (SELECT count(*) FROM approvals a
     WHERE a.tbl IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM imports i
                       WHERE i.table_name = a.tbl AND i.imported_at >= a.written_at))
  + (SELECT count(*) FROM glossary g
     JOIN aspects a ON a.name = g.aspect AND a.kind = 'query'
     JOIN glossary h ON h.subject = g.subject AND h.aspect = 'formulas'
       AND h.actor_kind = 'human' AND h.written_at > g.written_at
     WHERE g.actor_kind = 'agent'
       -- the store keeps superseded writings; only live slots count
       AND NOT EXISTS (SELECT 1 FROM glossary g2
                       WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                         AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
       AND NOT EXISTS (SELECT 1 FROM glossary h2
                       WHERE h2.subject = h.subject AND h2.aspect = h.aspect
                         AND h2.actor_kind = 'human' AND h2.written_at > h.written_at)
       AND json_get_str(json_get(h.body, 'formulas'), g.aspect) IS NOT NULL)
  + (SELECT count(*) FROM GLOSSARY() c WHERE c.state = 'contested') AS n
)
SELECT
  (SELECT count(*) FROM raw WHERE kind = 'fact') AS facts,
  (SELECT count(DISTINCT subject) FROM raw) AS subjects,
  (SELECT count(*) FROM aspects) AS aspects,
  (SELECT count(*) FROM aspects WHERE kind = 'query') AS metrics,
  (SELECT count(*) FROM witnesses) AS witnesses,
  (SELECT count(*) FROM raw WHERE kind = 'measurement') AS measurements,
  (SELECT n FROM loose) AS needs,
  (SELECT n FROM waiting) + (SELECT n FROM owed) AS waiting
