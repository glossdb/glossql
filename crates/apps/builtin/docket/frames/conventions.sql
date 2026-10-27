-- The Explore tab's source conventions: for every source a recipe of
-- this dataset reads, each entry of its `conventions` deposit — what
-- an onboarding learned about the system that wrote the export:
-- identifier forms, units, date formats, status codes. The deposit is
-- one object per source; its keys are the rows here. A human's entry
-- outranks the agent's, the newest wins.
WITH c AS (
  SELECT g.subject AS source, g.body,
         row_number() OVER (PARTITION BY g.subject
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, g.written_at DESC) AS rn
  FROM glossary g
  WHERE g.aspect = 'conventions'
    AND g.subject IN (SELECT r.source FROM recipes r JOIN current_dataset d ON d.dataset = r.dataset)
),
keyed AS (
  SELECT source, body, unnest(json_object_keys(body)) AS key FROM c WHERE rn = 1
)
SELECT arrow_cast(source, 'Utf8') AS source,
       arrow_cast(key, 'Utf8') AS key,
       arrow_cast(coalesce(json_get_str(body, key), CAST(json_get(body, key) AS VARCHAR)), 'Utf8') AS value
FROM keyed
ORDER BY source, key
