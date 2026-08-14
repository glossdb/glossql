-- agent_assumptions — every assumption the agent currently discloses.
--
-- The winning agent slot only: the store keeps superseded writings, and
-- a disclosed assumption means the one the agent stands on now. No
-- gates — `open_questions` decides which of these a human should be
-- asked about, and `ruling_entries` uses the same rows to decide
-- whether a ruling has been folded in. Both used to unnest this
-- themselves.
--
-- Flat, with the json accessors in the same SELECT as the scan:
-- datafusion-functions-json's rewrite does not survive a CTE that
-- projects the body column.
SELECT g.subject AS subject,
       g.aspect AS aspect,
       i.i AS idx,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dimension,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS assumption,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis') AS basis,
       json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf
FROM glossary g
CROSS JOIN generate_series(0, 19) AS i(i)
WHERE g.actor_kind = 'agent'
  AND NOT EXISTS (SELECT 1 FROM glossary g2
                  WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                    AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
  AND i.i < json_length(g.body, 'assumptions')
