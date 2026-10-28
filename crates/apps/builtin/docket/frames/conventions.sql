-- The sources: for every source a recipe of this dataset reads, what
-- an onboarding learned about the system that wrote the export — the
-- conventions deposit, one object per source, its `system` entry as
-- the line under the name and every other key folded as a line. A
-- person's entry outranks the agent's, the newest wins. A source
-- without a deposit is listed with none.
WITH srcs AS (
  SELECT DISTINCT r.source FROM recipes r JOIN current_dataset d ON d.dataset = r.dataset
),
c AS (
  SELECT g.subject AS source, g.body,
         row_number() OVER (PARTITION BY g.subject
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, g.written_at DESC) AS rn
  FROM glossary g
  WHERE g.aspect = 'conventions' AND g.subject IN (SELECT source FROM srcs)
),
keyed AS (
  SELECT source, body, unnest(json_object_keys(body)) AS key FROM c WHERE rn = 1
),
lines AS (
  SELECT source,
         array_to_string(array_agg(
           key || '  ·  ' || coalesce(json_get_str(body, key), CAST(json_get(body, key) AS VARCHAR))
           ORDER BY key), chr(10)) AS lines,
         count(*) AS n
  FROM keyed
  WHERE key <> 'system'
  GROUP BY source
)
SELECT arrow_cast(s.source, 'Utf8') AS source,
       arrow_cast(coalesce(json_get_str(c.body, 'system'), 'no conventions entry yet'), 'Utf8') AS system,
       arrow_cast(coalesce(l.lines, ''), 'Utf8') AS lines
FROM srcs s
LEFT JOIN c ON c.source = s.source AND c.rn = 1
LEFT JOIN lines l ON l.source = s.source
ORDER BY source
