-- One table's columns, every slot the record holds on each: the
-- judged role with the axis verdict beside a dimension (the engine's
-- type where none is judged), the unit and behaviour where owed, the
-- meaning or `not yet said` where the meaning is owed and nobody
-- spoke, and what was measured on the column — the temporal profile,
-- the dimension's coverage, the column's profile, an identity it
-- takes part in, the cells its cast refused at the newest landing.
-- The mark is the column's worst standing across its authored
-- slots: contested over stale over a person's word; nothing for the
-- agent's unchallenged word. A row opens the column's page.
WITH slots AS (
  SELECT g.subject, g.aspect, g.actor_kind, g.body, g.written_at,
         row_number() OVER (PARTITION BY g.subject, g.aspect
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.subject LIKE CAST($table AS VARCHAR) || '.%'
    AND g.aspect IN ('role', 'meaning', 'unit', 'behavior', 'dimension')),
cur AS (SELECT subject, aspect, actor_kind, written_at, json_get_str(body, 'value') AS value FROM slots WHERE rn = 1),
states AS (
  SELECT subject,
         max(CASE WHEN state = 'contested' THEN 2 WHEN state = 'stale' THEN 1 ELSE 0 END) AS worst,
         count(*) FILTER (WHERE aspect = 'meaning' AND state = 'unassessed') AS meaning_owed
  FROM GLOSSARY()
  WHERE subject LIKE CAST($table AS VARCHAR) || '.%'
    AND aspect IN ('role', 'meaning', 'unit', 'behavior', 'dimension')
  GROUP BY subject),
human AS (
  SELECT subject, max(written_at) AS at FROM cur WHERE actor_kind = 'human' GROUP BY subject),
entity AS (
  SELECT json_get_str(g.body, 'time_axis') AS axis,
         row_number() OVER (ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect = 'entity' AND g.subject = CAST($table AS VARCHAR)),
measured AS (
  SELECT g.subject, g.aspect, g.body
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.current AND g.subject LIKE CAST($table AS VARCHAR) || '.%'),
summaries AS (
  SELECT subject,
         array_to_string(array_agg(
           CASE WHEN NOT coalesce(json_get_bool(body, 'applicable'), true) THEN aspect || ': not applicable'
             ELSE CASE aspect
               WHEN 'temporal_profile' THEN coalesce(json_get_str(body, 'granularity'), 'no grain found')
                 || coalesce(', ' || CAST(json_get_int(json_get(body, 'completeness'), 'actual') AS VARCHAR)
                      || ' of ' || CAST(json_get_int(json_get(body, 'completeness'), 'expected') AS VARCHAR) || ' periods', '')
                 || coalesce(', ' || CAST(json_get_int(json_get(body, 'gaps'), 'count') AS VARCHAR) || ' gaps', '')
               WHEN 'dimension_relevance' THEN coalesce('covers ' || CAST(round(json_get_float(body, 'coverage') * 100) AS VARCHAR) || '% of the rows', 'relevance ' || CAST(json_get_float(body, 'relevance') AS VARCHAR))
                 || coalesce(', ' || CAST(json_get_int(body, 'groups') AS VARCHAR) || ' groups', '')
               WHEN 'column_profile' THEN coalesce(CAST(json_get_int(body, 'total') AS VARCHAR) || ' values', 'profiled')
                 || coalesce(', ' || CAST(round(json_get_float(body, 'null_ratio') * 100) AS VARCHAR) || '% null', '')
                 || coalesce(', ' || CAST(json_get_int(body, 'distinct') AS VARCHAR) || ' distinct', '')
               WHEN 'behavior_evidence' THEN 'evidence says ' || coalesce(json_get_str(json_get(body, 'summary'), 'verdict'), 'nothing')
               WHEN 'outlier_profile' THEN 'outliers profiled'
               ELSE aspect END END
           ORDER BY aspect), ' · ') AS measured
  FROM measured GROUP BY subject),
identities AS (
  SELECT CAST($table AS VARCHAR) || '.' || json_get_str(cand, 'target') AS subject,
         '= ' || json_get_str(json_get(cand, 'operands'), 0)
           || CASE json_get_str(cand, 'form') WHEN 'product' THEN ' × ' ELSE ' + ' END
           || json_get_str(json_get(cand, 'operands'), 1)
           || ' on ' || CAST(json_get_int(cand, 'support') AS VARCHAR) || ' rows' AS identity
  FROM (SELECT json_get(json_get(g.body, 'candidates'), c.i) AS cand
        FROM GLOSSARY(all => true) g CROSS JOIN generate_series(0, 49) AS c(i)
        WHERE g.kind = 'measurement' AND g.current AND g.aspect = 'derivation_candidates'
          AND g.subject = CAST($table AS VARCHAR) AND c.i < json_length(g.body, 'candidates'))),
newest AS (
  SELECT i.cast_failures, row_number() OVER (ORDER BY i.imported_at DESC) AS rn
  FROM imports i JOIN current_dataset d ON d.dataset = i.dataset
  WHERE i.table_name = CAST($table AS VARCHAR)),
refused AS (
  SELECT CAST($table AS VARCHAR) || '.' || json_get_str(chk, 'column') AS subject,
         json_get_int(chk, 'failed') AS failed
  FROM (SELECT json_get(json_get(n.cast_failures, 'checked'), c.i) AS chk
        FROM newest n CROSS JOIN generate_series(0, 99) AS c(i)
        WHERE n.rn = 1 AND c.i < json_length(n.cast_failures, 'checked'))
  WHERE json_get_int(chk, 'failed') > 0),
cols AS (
  SELECT c.column_name, c.ordinal_position, c.table_name || '.' || c.column_name AS subject,
         CASE
           WHEN c.data_type LIKE 'Utf8%' THEN 'text'
           WHEN c.data_type LIKE 'Int%' OR c.data_type LIKE 'UInt%' OR c.data_type LIKE 'Float%' OR c.data_type LIKE 'Decimal%' THEN 'number'
           WHEN c.data_type LIKE 'Date%' THEN 'date'
           WHEN c.data_type LIKE 'Timestamp%' THEN 'timestamp'
           WHEN c.data_type = 'Boolean' THEN 'flag'
           ELSE lower(c.data_type) END AS typed
  FROM information_schema.columns c JOIN current_dataset d ON d.dataset = c.table_schema
  WHERE c.table_name = CAST($table AS VARCHAR))
SELECT arrow_cast(c.column_name, 'Utf8') AS name,
       arrow_cast(coalesce(r.value, c.typed), 'Utf8') AS role,
       arrow_cast(CASE WHEN r.value IS NULL THEN 'engine type, no role judged' ELSE '' END, 'Utf8') AS role_note,
       arrow_cast(concat_ws(' · ', u.value, b.value), 'Utf8') AS unit,
       arrow_cast(CASE WHEN e.axis = c.column_name THEN 'time axis' ELSE coalesce(x.value, '') END, 'Utf8') AS axis,
       arrow_cast(coalesce(m.value, CASE WHEN coalesce(st.meaning_owed, 0) > 0 THEN 'not yet said' ELSE '' END), 'Utf8') AS meaning,
       arrow_cast(concat_ws(' · ', s.measured, i.identity,
         CAST(f.failed AS VARCHAR) || CASE WHEN f.failed = 1 THEN ' cell' ELSE ' cells' END || ' refused at landing'), 'Utf8') AS measured,
       arrow_cast(CASE WHEN st.worst = 2 THEN 'contested' WHEN st.worst = 1 THEN 'stale' WHEN h.subject IS NOT NULL THEN 'ruled' ELSE '' END, 'Utf8') AS mark,
       arrow_cast(CASE WHEN st.worst = 2 THEN 'v-open' WHEN st.worst = 1 THEN 'v-stale' WHEN h.subject IS NOT NULL THEN 'v-ruled' ELSE 'v-none' END, 'Utf8') AS mcls,
       arrow_cast(CASE WHEN st.worst = 2 THEN 'the voices on a slot differ; the witness withholds it'
                       WHEN st.worst = 1 THEN 'a slot was written before the column last changed'
                       WHEN h.subject IS NOT NULL THEN 'a person spoke on this column, ' || to_char(CAST(h.at AS TIMESTAMP), '%d %b %Y %H:%M')
                       ELSE '' END, 'Utf8') AS mark_title,
       arrow_cast('?column=' || c.subject, 'Utf8') AS link
FROM cols c
LEFT JOIN cur r ON r.subject = c.subject AND r.aspect = 'role'
LEFT JOIN cur x ON x.subject = c.subject AND x.aspect = 'dimension'
LEFT JOIN cur m ON m.subject = c.subject AND m.aspect = 'meaning'
LEFT JOIN cur u ON u.subject = c.subject AND u.aspect = 'unit'
LEFT JOIN cur b ON b.subject = c.subject AND b.aspect = 'behavior'
LEFT JOIN states st ON st.subject = c.subject
LEFT JOIN human h ON h.subject = c.subject
LEFT JOIN entity e ON e.rn = 1
LEFT JOIN summaries s ON s.subject = c.subject
LEFT JOIN identities i ON i.subject = c.subject
LEFT JOIN refused f ON f.subject = c.subject
ORDER BY c.ordinal_position
