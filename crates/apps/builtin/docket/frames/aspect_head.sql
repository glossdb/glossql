-- One aspect's declaration: its kind, what it holds from its schema,
-- where it is owed from its grain and condition, who may speak it,
-- the witness that stands on it with its detector and threshold, the
-- schema's own description where it has one, and the schema itself.
-- One row, or none for a name no aspect carries. The name filter
-- casts through `arrow_cast`, and what the schema holds is read in
-- the same step as the row: a plain cast the planner unwraps, or a
-- join on the cast name, lands in a view-typed comparison the engine
-- refuses once the schema's JSON is read beside it.
WITH a AS (
  SELECT CAST(name AS VARCHAR) AS name, CAST(kind AS VARCHAR) AS kind, CAST(grains AS VARCHAR) AS grains,
         CAST(condition AS VARCHAR) AS condition, schema,
         CASE
           WHEN json_get(json_get(json_get(schema, 'properties'), 'value'), 'enum') IS NOT NULL
             THEN 'one of ' || replace(regexp_replace(replace(replace(replace(
                    CAST(json_get(json_get(json_get(schema, 'properties'), 'value'), 'enum') AS VARCHAR),
                    '"', ''), '[', ''), ']', ''), '[[:space:]]+', ' ', 'g'), ', ', ' · ')
           WHEN json_object_keys(schema, 'properties') IS NOT NULL
             THEN arrow_cast(array_to_string(json_object_keys(schema, 'properties'), ', '), 'Utf8')
           ELSE 'a JSON object' END AS holds
  FROM aspects WHERE arrow_cast(name, 'Utf8') = CAST($aspect AS VARCHAR)),
w AS (
  SELECT array_to_string(array_agg(
           CAST(name AS VARCHAR) || coalesce(' by ' || replace(replace(replace(replace(CAST(speakers AS VARCHAR), '"', ''), '[', ''), ']', ''), ',', ', '), '')
             || coalesce(', judged by ' || CAST(detector AS VARCHAR) || coalesce(' at ' || CAST(threshold AS VARCHAR), ''), '')
           ORDER BY CAST(name AS VARCHAR)), ' · ') AS witnesses
  FROM witnesses WHERE CAST(aspect AS VARCHAR) = CAST($aspect AS VARCHAR)),
f AS (
  SELECT array_to_string(array_agg(CAST(name AS VARCHAR) ORDER BY CAST(name AS VARCHAR)), ', ') AS fns
  FROM functions WHERE CAST(returns AS VARCHAR) = CAST($aspect AS VARCHAR)),
counts AS (
  SELECT count(*) AS owed, count(*) FILTER (WHERE state <> 'unassessed') AS spoken,
         count(*) FILTER (WHERE state = 'contested') AS contested
  FROM GLOSSARY() WHERE CAST(aspect AS VARCHAR) = CAST($aspect AS VARCHAR))
SELECT arrow_cast(a.name, 'Utf8') AS name,
       arrow_cast(CASE a.kind WHEN 'fact' THEN 'a fact: said by the agent or a person'
                              WHEN 'query' THEN 'a grounding: SQL with its assumptions disclosed, served as a view'
                              WHEN 'measurement' THEN 'a measurement: landed by a function, never written by hand'
                              ELSE a.kind END, 'Utf8') AS kind,
       arrow_cast(CASE
         WHEN a.kind = 'query' THEN 'SQL with its assumptions disclosed, served as a view under the name'
         ELSE a.holds END, 'Utf8') AS holds,
       arrow_cast(CASE
         WHEN a.grains IS NULL THEN 'anywhere'
         WHEN a.grains = 'dataset' THEN 'on the dataset'
         WHEN strpos(a.grains, ',') = 0 THEN 'on a ' || replace(a.grains, 'relationship', 'join')
         ELSE 'on a ' || replace(replace(a.grains, 'relationship', 'join'), ',', ', a ') END
         || coalesce(' where ' || replace(a.condition, '''', ''), ''), 'Utf8') AS owed_on,
       arrow_cast(CASE WHEN a.kind = 'measurement' THEN coalesce('landed by ' || f.fns, 'no function returns it')
                       ELSE coalesce('witness ' || w.witnesses, 'no witness: anyone may speak, nobody judges') END, 'Utf8') AS guard,
       arrow_cast(CAST(c.spoken AS VARCHAR) || ' of ' || CAST(c.owed AS VARCHAR) || ' owed subjects carry a value'
         || CASE WHEN c.contested > 0 THEN ', ' || CAST(c.contested AS VARCHAR) || ' contested' ELSE '' END, 'Utf8') AS spoken,
       arrow_cast(coalesce(json_get_str(a.schema, 'description'), ''), 'Utf8') AS description,
       arrow_cast(regexp_replace(CAST(a.schema AS VARCHAR), '[[:space:]]+', ' ', 'g'), 'Utf8') AS schema
FROM a CROSS JOIN w CROSS JOIN f CROSS JOIN counts c
