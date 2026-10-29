-- What the functions measured: one row per landed measurement aspect
-- with what it found, in words from its own body, how many subjects
-- it reached and when. The shipped aspects have their own sentence;
-- another is counted. A measurement proposes and never judges: a
-- defect here becomes a finding above, a verdict only through a
-- witness. The aspects nobody has run are `unmeasured`'s rows.
WITH m AS (
  SELECT g.subject, g.aspect, g.body, g.written_at
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.current
),
rels AS (
  SELECT json_get(json_get(m.body, 'relationships'), r.i) AS rel
  FROM m CROSS JOIN generate_series(0, 99) AS r(i)
  WHERE m.aspect = 'relationship_coherence' AND r.i < json_length(m.body, 'relationships')),
coherence AS (
  SELECT 'relationship_coherence' AS aspect,
         CAST(count(*) AS VARCHAR) || CASE WHEN count(*) = 1 THEN ' join' ELSE ' joins' END AS subjects,
         CAST(count(*) FILTER (WHERE json_get_int(rel, 'orphans') = 0) AS VARCHAR) || ' of '
           || CAST(count(*) AS VARCHAR) || ' joins resolve every row.'
           || coalesce(' ' || array_to_string(array_agg(
                json_get_str(rel, 'relationship') || ' leaves ' || CAST(json_get_int(rel, 'orphans') AS VARCHAR)
                  || ' of ' || CAST(json_get_int(rel, 'filled') AS VARCHAR) || ' rows')
                FILTER (WHERE json_get_int(rel, 'orphans') > 0), '; ') || '.', '') AS found
  FROM rels
  HAVING count(*) > 0),
temporal AS (
  SELECT 'temporal_profile' AS aspect,
         CAST(count(*) AS VARCHAR) || CASE WHEN count(*) = 1 THEN ' date column' ELSE ' date columns' END AS subjects,
         array_to_string(array_agg(
           subject || ': ' || coalesce(json_get_str(body, 'granularity'), 'no grain found')
             || coalesce(', ' || CAST(json_get_int(json_get(body, 'completeness'), 'actual') AS VARCHAR)
                  || ' of ' || CAST(json_get_int(json_get(body, 'completeness'), 'expected') AS VARCHAR) || ' periods', '')
             || coalesce(', ' || CAST(json_get_int(json_get(body, 'gaps'), 'count') AS VARCHAR) || ' gaps', '')
           ORDER BY subject), '; ') || '.' AS found
  FROM m
  WHERE aspect = 'temporal_profile'
  HAVING count(*) > 0),
cands AS (
  SELECT m.subject, json_get(json_get(m.body, 'candidates'), c.i) AS cand
  FROM m CROSS JOIN generate_series(0, 49) AS c(i)
  WHERE m.aspect = 'derivation_candidates' AND c.i < json_length(m.body, 'candidates')),
derivations AS (
  SELECT 'derivation_candidates' AS aspect,
         CAST(count(DISTINCT subject) AS VARCHAR) || CASE WHEN count(DISTINCT subject) = 1 THEN ' table' ELSE ' tables' END AS subjects,
         array_to_string(array_agg(
           subject || ': ' || json_get_str(cand, 'target') || ' = '
             || json_get_str(json_get(cand, 'operands'), 0)
             || CASE json_get_str(cand, 'form') WHEN 'product' THEN ' × ' ELSE ' + ' END
             || json_get_str(json_get(cand, 'operands'), 1)
             || ' holds on ' || CAST(json_get_int(cand, 'support') AS VARCHAR) || ' rows, '
             || CAST(json_get_int(cand, 'violations') AS VARCHAR) || ' break it'
           ORDER BY subject), '; ') || '.' AS found
  FROM cands
  HAVING count(*) > 0),
relevance AS (
  SELECT 'dimension_relevance' AS aspect,
         CAST(count(*) AS VARCHAR) || CASE WHEN count(*) = 1 THEN ' dimension column' ELSE ' dimension columns' END AS subjects,
         concat_ws(', ',
           'Relevance from ' || CAST(round(min(json_get_float(body, 'relevance')) * 100) / 100 AS VARCHAR)
             || ' to ' || CAST(round(max(json_get_float(body, 'relevance')) * 100) / 100 AS VARCHAR),
           'coverage from ' || CAST(round(min(json_get_float(body, 'coverage')) * 100) AS VARCHAR) || '% to '
             || CAST(round(max(json_get_float(body, 'coverage')) * 100) AS VARCHAR) || '% of the rows',
           CAST(min(json_get_int(body, 'groups')) AS VARCHAR) || ' to '
             || CAST(max(json_get_int(body, 'groups')) AS VARCHAR) || ' groups') || '.' AS found
  FROM m
  WHERE aspect = 'dimension_relevance' AND json_get_bool(body, 'applicable')
  HAVING count(*) > 0),
collisions AS (
  SELECT 'grounding_collisions' AS aspect,
         'the dataset' AS subjects,
         CAST(json_get_int(body, 'groundings') AS VARCHAR) || ' groundings, '
           || CAST(json_length(body, 'collisions') AS VARCHAR) || ' serving one name differently.' AS found
  FROM m
  WHERE aspect = 'grounding_collisions'),
candidates AS (
  SELECT 'relationship_candidates' AS aspect,
         'the dataset' AS subjects,
         CAST(json_length(body, 'candidates') AS VARCHAR) || ' candidate joins found in the data.' AS found
  FROM m
  WHERE aspect = 'relationship_candidates'),
walked AS (
  SELECT json_get(json_get(m.body, 'metrics'), w.i) AS metric
  FROM m CROSS JOIN generate_series(0, 99) AS w(i)
  WHERE m.aspect = 'metric_bands' AND w.i < json_length(m.body, 'metrics')),
bands AS (
  SELECT 'metric_bands' AS aspect,
         'the dataset' AS subjects,
         CAST(count(*) FILTER (WHERE json_get_bool(metric, 'applicable')) AS VARCHAR) || ' of '
           || CAST(count(*) AS VARCHAR) || ' metrics walked at month grain'
           || coalesce(', trained on up to ' || CAST(max(json_get_int(metric, 'trained_on')) AS VARCHAR) || ' months', '')
           || coalesce(', served by ' || max(json_get_str(metric, 'served_by')), '') || '.' AS found
  FROM walked
  HAVING count(*) > 0),
ahead AS (
  SELECT json_get(json_get(m.body, 'metrics'), f.i) AS metric
  FROM m CROSS JOIN generate_series(0, 99) AS f(i)
  WHERE m.aspect = 'metric_forecast' AND f.i < json_length(m.body, 'metrics')),
forecast AS (
  SELECT 'metric_forecast' AS aspect,
         'the dataset' AS subjects,
         CAST(count(*) FILTER (WHERE json_get_bool(metric, 'applicable')) AS VARCHAR) || ' of '
           || CAST(count(*) AS VARCHAR) || ' metrics forecast up to '
           || CAST(max(json_length(metric, 'points')) AS VARCHAR) || ' months ahead.' AS found
  FROM ahead
  HAVING count(*) > 0),
worded AS (
  SELECT * FROM coherence
  UNION ALL SELECT * FROM temporal
  UNION ALL SELECT * FROM derivations
  UNION ALL SELECT * FROM relevance
  UNION ALL SELECT * FROM collisions
  UNION ALL SELECT * FROM candidates
  UNION ALL SELECT * FROM bands
  UNION ALL SELECT * FROM forecast),
others AS (
  SELECT aspect,
         CAST(count(*) AS VARCHAR) || CASE WHEN count(*) = 1 THEN ' subject' ELSE ' subjects' END AS subjects,
         'Measured; the page has no words for this aspect beyond the count.' AS found
  FROM m
  WHERE aspect NOT IN (SELECT aspect FROM worded)
  GROUP BY aspect),
every AS (SELECT * FROM worded UNION ALL SELECT * FROM others),
backlog AS (
  SELECT g.aspect, count(*) FILTER (WHERE g.state = 'unassessed') AS unspoken
  FROM GLOSSARY() g JOIN aspects a ON a.name = g.aspect
  WHERE a.kind = 'measurement'
  GROUP BY g.aspect),
landed AS (SELECT aspect, max(written_at) AS newest FROM m GROUP BY aspect)
SELECT arrow_cast(e.aspect, 'Utf8') AS aspect,
       arrow_cast(e.subjects || CASE WHEN coalesce(o.unspoken, 0) > 0
                                     THEN ', ' || CAST(o.unspoken AS VARCHAR) || ' not yet'
                                     ELSE '' END, 'Utf8') AS subjects,
       arrow_cast(e.found, 'Utf8') AS found,
       arrow_cast(coalesce(to_char(CAST(t.newest AS TIMESTAMP), '%d %b %Y %H:%M'), ''), 'Utf8') AS at
FROM every e
LEFT JOIN backlog o ON o.aspect = e.aspect
LEFT JOIN landed t ON t.aspect = e.aspect
ORDER BY CASE aspect WHEN 'relationship_coherence' THEN 0 WHEN 'temporal_profile' THEN 1
                       WHEN 'derivation_candidates' THEN 2 WHEN 'dimension_relevance' THEN 3
                       WHEN 'grounding_collisions' THEN 4 WHEN 'relationship_candidates' THEN 5
                       WHEN 'metric_bands' THEN 6 WHEN 'metric_forecast' THEN 7 ELSE 8 END, aspect
