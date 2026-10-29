-- One column's slots, every one: each authored aspect with every
-- voice on it — a person's first, then the agent's — its value in
-- words, who spoke, when, and the slot's standing from the collapsed
-- read; each measurement a function landed, summarised from its
-- body; and each slot the record owes and nobody filled, said so.
-- The mark says a person's word, a contested slot or a stale one;
-- nothing for the agent's unchallenged word.
WITH states AS (
  SELECT aspect, state FROM GLOSSARY() WHERE subject = CAST($column AS VARCHAR)),
said AS (
  SELECT g.aspect, g.actor_kind, g.actor_id, g.body, g.written_at,
         row_number() OVER (PARTITION BY g.aspect
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.subject = CAST($column AS VARCHAR)),
authored AS (
  SELECT s.aspect, 'said' AS kind,
         CASE s.aspect
           WHEN 'unit' THEN json_get_str(s.body, 'value') || coalesce(', varying by ' || json_get_str(s.body, 'source_column'), '')
           WHEN 'behavior' THEN json_get_str(s.body, 'value') || coalesce(' — ' || json_get_str(s.body, 'grounds'), '')
           WHEN 'dimension' THEN json_get_str(s.body, 'value') || coalesce(' — ' || json_get_str(s.body, 'grounds'), '')
           WHEN 'meaning' THEN json_get_str(s.body, 'value') || coalesce(' (the term: ' || json_get_str(s.body, 'term') || ')', '')
           ELSE coalesce(json_get_str(s.body, 'value'), substr(CAST(s.body AS VARCHAR), 1, 200)) END AS value,
         CASE s.actor_kind WHEN 'human' THEN 'a person' WHEN 'agent' THEN 'the agent' ELSE s.actor_kind END AS voice,
         s.actor_id AS actor,
         CASE WHEN s.rn = 1 THEN coalesce(st.state, 'current') ELSE 'outranked' END AS state,
         s.written_at AS at,
         s.rn
  FROM said s LEFT JOIN states st ON st.aspect = s.aspect),
measured AS (
  SELECT g.aspect, 'measured' AS kind,
         CASE WHEN NOT coalesce(json_get_bool(g.body, 'applicable'), true)
                THEN 'not applicable' || coalesce(': ' || json_get_str(g.body, 'reason'), '')
           ELSE CASE g.aspect
             WHEN 'temporal_profile' THEN coalesce(json_get_str(g.body, 'granularity'), 'no grain found')
               || coalesce(', ' || CAST(json_get_int(json_get(g.body, 'completeness'), 'actual') AS VARCHAR)
                    || ' of ' || CAST(json_get_int(json_get(g.body, 'completeness'), 'expected') AS VARCHAR) || ' periods', '')
               || coalesce(', ' || CAST(json_get_int(json_get(g.body, 'gaps'), 'count') AS VARCHAR) || ' gaps', '')
               || coalesce(', from ' || json_get_str(g.body, 'min') || ' to ' || json_get_str(g.body, 'max'), '')
             WHEN 'dimension_relevance' THEN coalesce('covers ' || CAST(round(json_get_float(g.body, 'coverage') * 100) AS VARCHAR) || '% of the rows', 'relevance ' || CAST(json_get_float(g.body, 'relevance') AS VARCHAR))
               || coalesce(', ' || CAST(json_get_int(g.body, 'groups') AS VARCHAR) || ' groups', '')
             WHEN 'column_profile' THEN coalesce(CAST(json_get_int(g.body, 'total') AS VARCHAR) || ' values', 'profiled')
               || coalesce(', ' || CAST(round(json_get_float(g.body, 'null_ratio') * 100) AS VARCHAR) || '% null', '')
               || coalesce(', ' || CAST(json_get_int(g.body, 'distinct') AS VARCHAR) || ' distinct', '')
             WHEN 'behavior_evidence' THEN 'the evidence says ' || coalesce(json_get_str(json_get(g.body, 'summary'), 'verdict'), 'nothing')
             ELSE substr(CAST(g.body AS VARCHAR), 1, 200) END END AS value,
         g.actor AS voice,
         g.actor AS actor,
         CASE WHEN g.current THEN 'current' ELSE 'stale' END AS state,
         g.written_at AS at,
         1 AS rn
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.subject = CAST($column AS VARCHAR)),
unfilled AS (
  SELECT st.aspect, CASE WHEN a.kind = 'measurement' THEN 'measured' ELSE 'said' END AS kind,
         CASE WHEN a.kind = 'measurement' THEN 'not yet measured' ELSE 'not yet said' END AS value,
         '' AS voice, '' AS actor, 'unassessed' AS state, CAST(NULL AS VARCHAR) AS at, 1 AS rn
  FROM states st JOIN aspects a ON a.name = st.aspect
  WHERE st.state = 'unassessed'),
every AS (SELECT * FROM authored UNION ALL SELECT * FROM measured UNION ALL SELECT * FROM unfilled)
SELECT arrow_cast(aspect, 'Utf8') AS aspect,
       arrow_cast(kind, 'Utf8') AS kind,
       arrow_cast(value, 'Utf8') AS value,
       arrow_cast(voice, 'Utf8') AS voice,
       arrow_cast(state, 'Utf8') AS state,
       arrow_cast(coalesce(to_char(CAST(at AS TIMESTAMP), '%d %b %Y %H:%M'), ''), 'Utf8') AS at,
       arrow_cast(CASE WHEN state = 'contested' THEN 'contested' WHEN state = 'stale' THEN 'stale'
                       WHEN state = 'outranked' THEN 'outranked'
                       WHEN voice = 'a person' THEN 'ruled' ELSE '' END, 'Utf8') AS mark,
       arrow_cast(CASE WHEN state = 'contested' THEN 'v-open' WHEN state = 'stale' THEN 'v-stale'
                       WHEN state = 'outranked' THEN 'v-stale'
                       WHEN voice = 'a person' THEN 'v-ruled' ELSE 'v-none' END, 'Utf8') AS mcls,
       arrow_cast(CASE WHEN state = 'contested' THEN 'the voices differ; the witness withholds the value'
                       WHEN state = 'stale' THEN 'written before the column last changed'
                       WHEN state = 'outranked' THEN 'a later or higher voice wins'
                       WHEN voice = 'a person' THEN 'spoken by ' || actor
                       ELSE '' END, 'Utf8') AS mark_title
FROM every
ORDER BY CASE kind WHEN 'said' THEN 0 ELSE 1 END,
         CASE aspect WHEN 'role' THEN 0 WHEN 'meaning' THEN 1 WHEN 'unit' THEN 2 WHEN 'behavior' THEN 3 WHEN 'dimension' THEN 4 ELSE 5 END,
         aspect, rn
