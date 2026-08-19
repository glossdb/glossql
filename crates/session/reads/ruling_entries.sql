-- ruling_entries — the human's standing judgments, one row per entry.
--
-- A ruling gloss carries a list; this flattens it and keeps only the
-- newest writing per subject, which is what "standing" means for a
-- superseded slot. Every derivation that asks "what has the human
-- already ruled" builds on this instead of copying the unnest — it was
-- written out six times before this file existed.
--
-- `key` names the claim and is the only join column: prose is never
-- matched against prose anywhere in this system.
--
-- `folded_in` is the debt, answered: the agent owes a re-record of the
-- ruled grounding, and the ruling stands unfolded while that key is
-- still disclosed below full confidence. It clears when the re-record
-- lands, so nothing tracks it and nothing has to be marked done.
--
-- The json accessors sit in the same SELECT as the scan they read
-- from — lifting the newest-writing filter into a CTE that projects
-- `body` breaks datafusion-functions-json's rewrite, and planning
-- fails with "No field named c.body". The outer SELECT only reads
-- columns the inner one already extracted, which is safe.
--
-- `folded_in` is a LEFT JOIN and a count, not a correlated NOT EXISTS:
-- a correlated subquery over a read that extracts json defeats
-- `decorrelate_predicate_subquery`, which looks for the original column
-- and finds `__datafusion_extracted_*` in its place.
WITH entries AS (
  SELECT r.dataset AS dataset,
         r.subject AS subject,
       j.j AS idx,
       json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'aspect') AS aspect,
       json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'key') AS key,
       json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'stance') AS stance,
       json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'dimension') AS dimension,
       coalesce(json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'assumption'), '') AS assumption,
       json_get_str(json_get(json_get(r.body, 'rulings'), j.j), 'note') AS note,
       r.written_at AS written_at
FROM glossary r
CROSS JOIN generate_series(0, 199) AS j(j)
WHERE r.aspect = 'ruling' AND r.actor_kind = 'human'
  AND NOT EXISTS (SELECT 1 FROM glossary r2
                  WHERE r2.subject = r.subject AND r2.aspect = 'ruling'
                    AND r2.actor_kind = 'human' AND r2.written_at > r.written_at)
  AND j.j < json_length(r.body, 'rulings')
)
SELECT e.dataset, e.subject, e.idx, e.aspect, e.key, e.stance, e.dimension,
       e.assumption, e.note, e.written_at,
       count(a.key) = 0 AS folded_in
FROM entries e
LEFT JOIN agent_assumptions a
  ON a.subject = e.subject AND a.aspect = e.aspect
 AND a.key = e.key AND a.conf < 1.0
GROUP BY e.dataset, e.subject, e.idx, e.aspect, e.key, e.stance, e.dimension,
         e.assumption, e.note, e.written_at
