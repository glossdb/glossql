-- agent_assumptions — every assumption the agent currently discloses.
--
-- The winning agent slot only: the store keeps superseded writings, and
-- a disclosed assumption means the one the agent stands on now. No
-- gates — `open_questions` decides which of these a human should be
-- asked about, and `ruling_entries` uses the same rows to decide
-- whether a ruling has been folded in. Both used to unnest this
-- themselves.
--
-- `dataset` rides along because the plain `glossary` relation is
-- workspace-wide: a consumer bound to one dataset must say so — and it
-- is part of the supersession key below, not merely carried.
--
-- The store's collapse has one exception this does not: a source-grain
-- row (its subject names a declared source AND its aspect opted into
-- SOURCE grain) supersedes workspace-wide. Unreachable here — a
-- grounding is glossed on a dataset or a table, never on a source — so
-- the key is unconditional. **A source-grain aspect entering this read
-- would need the exception.**
--
-- `alternative` is the rival reading the agent named beside the claim,
-- absent when none was named. It is the strongest thing the round can
-- offer as an answer, because it is a reading and not a stance.
--
-- `body` rides along as the whole writing the assumption sits inside,
-- so a caller can show it or re-issue it as a statement. Read the
-- columns above rather than reaching into it: json accessors applied
-- to a body that arrived through a read hit the rewrite problem below.
--
-- Flat, with the json accessors in the same SELECT as the scan:
-- datafusion-functions-json's rewrite does not survive a CTE that
-- projects the body column.
SELECT g.dataset AS dataset,
       g.subject AS subject,
       g.aspect AS aspect,
       i.i AS idx,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dimension,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS assumption,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis') AS basis,
       json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'alternative') AS alternative,
       json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
       g.body AS body
FROM glossary g
CROSS JOIN generate_series(0, 19) AS i(i)
WHERE g.actor_kind = 'agent'
  -- The supersession key carries `dataset`, matching the store's own
  -- collapse: a dataset-scoped row is superseded only within its
  -- dataset. Without this leg two datasets holding a same-named subject
  -- collapse into one and the older dataset's assumptions vanish from
  -- every read built on this — measured, not theorised.
  AND NOT EXISTS (SELECT 1 FROM glossary g2
                  WHERE g2.dataset = g.dataset
                    AND g2.subject = g.subject AND g2.aspect = g.aspect
                    AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
  AND i.i < json_length(g.body, 'assumptions')
