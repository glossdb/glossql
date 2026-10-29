-- One aspect's values: every subject the collapsed read serves for it
-- — the value in words, who spoke it (a person's over the agent's;
-- the function for a measurement), the slot's standing and when —
-- and every subject it is owed on where nobody spoke, said so. The
-- contested first, then the stale, then the spoken, then the owed; a
-- subject opens its page where it has one.
WITH st AS (
  SELECT subject, state, value FROM GLOSSARY() WHERE aspect = CAST($aspect AS VARCHAR)),
said AS (
  SELECT g.subject, g.actor_kind, g.actor_id, g.body, g.written_at,
         row_number() OVER (PARTITION BY g.subject
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect = CAST($aspect AS VARCHAR)),
fn AS (
  SELECT g.subject, g.actor, g.written_at, g.body
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.aspect = CAST($aspect AS VARCHAR) AND g.current),
landed AS (SELECT DISTINCT i.table_name FROM imports i JOIN current_dataset d ON d.dataset = i.dataset),
a AS (SELECT CAST(kind AS VARCHAR) AS kind FROM aspects WHERE name = CAST($aspect AS VARCHAR))
SELECT arrow_cast(st.subject, 'Utf8') AS subject,
       arrow_cast(CASE
         WHEN st.state = 'unassessed' THEN CASE WHEN a.kind = 'measurement' THEN 'not yet measured' ELSE 'not yet said' END
         WHEN st.state = 'contested' THEN 'withheld: the voices differ'
         WHEN a.kind = 'measurement' THEN substr(CAST(f.body AS VARCHAR), 1, 200)
         WHEN a.kind = 'query' THEN 'a grounding in SQL; its page has the assumptions and the SQL'
         ELSE CASE CAST($aspect AS VARCHAR)
           WHEN 'entity' THEN json_get_str(s.body, 'value')
             || coalesce(' · one row per ' || regexp_replace(replace(replace(replace(CAST(json_get(s.body, 'grain') AS VARCHAR), '"', ''), '[', ''), ']', ''), ',[[:space:]]*', ', ', 'g'), '')
             || coalesce(' · time axis ' || json_get_str(s.body, 'time_axis'), '')
           WHEN 'unit' THEN json_get_str(s.body, 'value') || coalesce(', varying by ' || json_get_str(s.body, 'source_column'), '')
           WHEN 'behavior' THEN json_get_str(s.body, 'value') || coalesce(' — ' || json_get_str(s.body, 'grounds'), '')
           WHEN 'dimension' THEN json_get_str(s.body, 'value') || coalesce(' — ' || json_get_str(s.body, 'grounds'), '')
           WHEN 'meaning' THEN json_get_str(s.body, 'value') || coalesce(' (the term: ' || json_get_str(s.body, 'term') || ')', '')
           WHEN 'conventions' THEN array_to_string(json_object_keys(s.body), ', ')
           WHEN 'definitions' THEN CAST(json_length(s.body, 'definitions') AS VARCHAR) || ' definitions'
           ELSE coalesce(json_get_str(s.body, 'value'), substr(CAST(s.body AS VARCHAR), 1, 200)) END END, 'Utf8') AS value,
       arrow_cast(CASE WHEN st.state = 'unassessed' THEN ''
                       WHEN a.kind = 'measurement' THEN coalesce(f.actor, '')
                       WHEN s.actor_kind = 'human' THEN 'a person'
                       WHEN s.actor_kind = 'agent' THEN 'the agent'
                       ELSE coalesce(s.actor_kind, '') END, 'Utf8') AS voice,
       arrow_cast(st.state, 'Utf8') AS state,
       arrow_cast(coalesce(to_char(CAST(coalesce(s.written_at, f.written_at) AS TIMESTAMP), '%d %b %Y %H:%M'), ''), 'Utf8') AS at,
       arrow_cast(CASE WHEN st.state = 'contested' THEN 'contested' WHEN st.state = 'stale' THEN 'stale'
                       WHEN s.actor_kind = 'human' THEN 'ruled' ELSE '' END, 'Utf8') AS mark,
       arrow_cast(CASE WHEN st.state = 'contested' THEN 'v-open' WHEN st.state = 'stale' THEN 'v-stale'
                       WHEN s.actor_kind = 'human' THEN 'v-ruled' ELSE 'v-none' END, 'Utf8') AS mcls,
       arrow_cast(CASE WHEN st.state = 'contested' THEN 'the voices differ; the witness withholds the value'
                       WHEN st.state = 'stale' THEN 'written before the subject last changed'
                       WHEN s.actor_kind = 'human' THEN 'spoken by ' || s.actor_id
                       ELSE '' END, 'Utf8') AS mark_title,
       arrow_cast(CASE
         WHEN a.kind = 'query' THEN '/' || d.dataset || '/app/docket/p/metrics?metric=' || CAST($aspect AS VARCHAR)
         WHEN strpos(st.subject, ' -> ') > 0 THEN ''
         WHEN strpos(st.subject, '.') > 0 THEN '?column=' || st.subject
         WHEN l.table_name IS NOT NULL THEN '?table=' || st.subject
         ELSE '' END, 'Utf8') AS link
FROM st
CROSS JOIN a
CROSS JOIN current_dataset d
LEFT JOIN said s ON s.subject = st.subject AND s.rn = 1
LEFT JOIN fn f ON f.subject = st.subject
LEFT JOIN landed l ON l.table_name = st.subject
ORDER BY CASE state WHEN 'contested' THEN 0 WHEN 'stale' THEN 1 WHEN 'current' THEN 2 ELSE 3 END, subject
