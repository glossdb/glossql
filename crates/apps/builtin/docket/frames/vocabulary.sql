-- The vocabulary: every declared aspect of the kind the page asks for
-- — `fact`, `query`, `measurement`, or `own` for the workspace's own
-- six (the app's five and the ruling), folded — with what it holds
-- from its schema (an enum's members, else its property names, else
-- its title), where it is owed from its grain and condition, how many
-- owed subjects carry a value and how many are contested, who may
-- speak it (the witness's speakers, the function that lands it, or
-- the agent and a person for a grounding), and whether a witness
-- judges it. A row opens the aspect's page. The counts are the
-- collapsed read's: a slot is owed where the aspect's grain and
-- condition name the subject and a witness stands, or where a voice
-- spoke.
WITH a AS (
  SELECT name,
         CAST(kind AS VARCHAR) AS kind,
         CAST(grains AS VARCHAR) AS grains,
         CAST(condition AS VARCHAR) AS condition,
         schema,
         CASE WHEN name IN ('app', 'app_page', 'app_frame', 'app_spec', 'app_release', 'ruling') THEN 'own'
              ELSE CAST(kind AS VARCHAR) END AS side
  FROM aspects
),
counts AS (
  SELECT aspect,
         count(*) AS owed,
         count(*) FILTER (WHERE state <> 'unassessed') AS spoken,
         count(*) FILTER (WHERE state = 'contested') AS contested
  FROM GLOSSARY()
  GROUP BY aspect
),
w AS (
  SELECT aspect,
         max(CASE WHEN detector IS NOT NULL THEN 1 ELSE 0 END) AS judged,
         max(CAST(speakers AS VARCHAR)) AS speakers
  FROM witnesses
  GROUP BY aspect
),
f AS (
  SELECT CAST(returns AS VARCHAR) AS aspect, array_to_string(array_agg(name ORDER BY name), ', ') AS fns
  FROM functions
  WHERE returns IS NOT NULL
  GROUP BY returns
)
SELECT arrow_cast(a.name, 'Utf8') AS aspect,
       arrow_cast(CASE
         WHEN a.kind = 'query' THEN 'SQL with its assumptions disclosed, served as a view under the name'
         WHEN json_get(json_get(json_get(a.schema, 'properties'), 'value'), 'enum') IS NOT NULL
           THEN 'one of ' || replace(regexp_replace(replace(replace(replace(
                  CAST(json_get(json_get(json_get(a.schema, 'properties'), 'value'), 'enum') AS VARCHAR),
                  '"', ''), '[', ''), ']', ''), '[[:space:]]+', ' ', 'g'), ', ', ' · ')
         WHEN json_object_keys(a.schema, 'properties') IS NOT NULL
           THEN array_to_string(json_object_keys(a.schema, 'properties'), ', ')
         WHEN json_get_str(a.schema, 'title') IS NOT NULL THEN json_get_str(a.schema, 'title')
         ELSE 'a JSON object' END, 'Utf8') AS holds,
       arrow_cast(CASE
         WHEN a.grains IS NULL THEN 'anywhere'
         WHEN a.grains = 'dataset' THEN 'on the dataset'
         WHEN strpos(a.grains, ',') = 0 THEN 'on a ' || replace(a.grains, 'relationship', 'join')
         ELSE 'on a ' || replace(replace(a.grains, 'relationship', 'join'), ',', ', a ') END
         || coalesce(' where ' || replace(a.condition, '''', ''), ''), 'Utf8') AS owed_on,
       arrow_cast(CAST(coalesce(c.spoken, 0) AS VARCHAR) || ' of ' || CAST(coalesce(c.owed, 0) AS VARCHAR)
         || CASE WHEN coalesce(c.contested, 0) > 0 THEN ' · ' || CAST(c.contested AS VARCHAR) || ' contested' ELSE '' END, 'Utf8') AS spoken,
       CASE WHEN coalesce(c.owed, 0) = 0 THEN 0 ELSE CAST(round(c.spoken * 100.0 / c.owed) AS BIGINT) END AS share,
       arrow_cast(CASE WHEN coalesce(c.owed, 0) > 0 AND c.spoken = c.owed THEN 'full' ELSE '' END, 'Utf8') AS full,
       arrow_cast(CASE
         WHEN a.kind = 'measurement' THEN coalesce(f.fns, 'no function returns it')
         WHEN w.speakers IS NOT NULL
           THEN replace(replace(replace(replace(replace(replace(w.speakers, '"', ''), '[', ''), ']', ''), ',', ', '), 'human', 'a person'), 'agent', 'the agent')
         ELSE 'the agent, a person' END
         || CASE WHEN coalesce(w.judged, 0) = 1 THEN ' · judged' ELSE '' END, 'Utf8') AS speakers,
       arrow_cast('?aspect=' || a.name, 'Utf8') AS link
FROM a
LEFT JOIN counts c ON c.aspect = a.name
LEFT JOIN w ON w.aspect = a.name
LEFT JOIN f ON f.aspect = a.name
WHERE a.side = CAST($kind AS VARCHAR)
ORDER BY CASE a.grains WHEN 'column' THEN 0 WHEN 'table' THEN 1 WHEN 'table,column,relationship' THEN 2
                       WHEN 'relationship' THEN 3 WHEN 'source' THEN 4 WHEN 'dataset' THEN 5 ELSE 6 END,
         share DESC, aspect
