-- What holds among one table's columns: every identity the
-- derivation measurement found — the target, the operands, how many
-- rows it holds on and how many break it — and what the hierarchy
-- search found, or that neither has run. Rows only for the table the
-- page asks for. Each read of the landed body stands on its own: the
-- planner's JSON extraction does not survive one body read twice
-- through a shared table expression.
WITH cands AS (
  SELECT json_get(json_get(g.body, 'candidates'), c.i) AS cand
  FROM GLOSSARY(all => true) g CROSS JOIN generate_series(0, 49) AS c(i)
  WHERE g.kind = 'measurement' AND g.current AND g.subject = CAST($table AS VARCHAR)
    AND g.aspect = 'derivation_candidates' AND c.i < json_length(g.body, 'candidates')),
identities AS (
  SELECT 'identity' AS kind,
         json_get_str(cand, 'target') || ' = ' || json_get_str(json_get(cand, 'operands'), 0)
           || CASE json_get_str(cand, 'form') WHEN 'product' THEN ' × ' ELSE ' + ' END
           || json_get_str(json_get(cand, 'operands'), 1) AS text,
         'holds on ' || CAST(json_get_int(cand, 'support') AS VARCHAR) || ' rows'
           || CASE WHEN json_get_int(cand, 'violations') > 0
                   THEN ' · ' || CAST(json_get_int(cand, 'violations') AS VARCHAR) || ' break it'
                   ELSE ' · none break it' END AS note,
         0 AS ord
  FROM cands),
found AS (SELECT count(*) AS n FROM cands),
ran AS (
  SELECT count(*) FILTER (WHERE g.aspect = 'derivation_candidates') AS derivations,
         count(*) FILTER (WHERE g.aspect = 'hierarchy_candidates') AS hierarchies,
         max(CASE WHEN g.aspect = 'hierarchy_candidates' THEN json_length(g.body, 'candidates') END) AS trees
  FROM GLOSSARY(all => true) g
  WHERE g.kind = 'measurement' AND g.current AND g.subject = CAST($table AS VARCHAR)
    AND g.aspect IN ('derivation_candidates', 'hierarchy_candidates')),
searched AS (
  SELECT 'identities' AS kind,
         CASE WHEN r.derivations = 0 THEN 'identities not yet searched'
              WHEN f.n = 0 THEN 'no identity holds among the columns'
              ELSE '' END AS text,
         '' AS note, 1 AS ord
  FROM ran r CROSS JOIN found f),
hierarchies AS (
  SELECT 'hierarchies' AS kind,
         CASE WHEN r.hierarchies = 0 THEN 'hierarchies not yet searched'
              ELSE CAST(coalesce(r.trees, 0) AS VARCHAR) || ' hierarchies found among the columns' END AS text,
         '' AS note, 2 AS ord
  FROM ran r)
SELECT arrow_cast(kind, 'Utf8') AS kind, arrow_cast(text, 'Utf8') AS text, arrow_cast(note, 'Utf8') AS note
FROM (SELECT * FROM identities UNION ALL SELECT * FROM searched UNION ALL SELECT * FROM hierarchies)
WHERE text <> ''
ORDER BY ord, text
