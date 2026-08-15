-- workspace_next — what this workspace can be extended through, and
-- where it stands on each.
--
-- Not a task queue and not an order to follow. It is the map of
-- surfaces: what kind of thing you can declare or write here, how much
-- of it stands, and what is open on it. The agent reads this instead
-- of a staged manual — the four flow skills used to carry the order as
-- prose, which made the work look like a pipeline it is not. Judgment
-- about what to do next stays the agent's; this only says what the
-- system affords and what the record already holds.
--
-- `stands` counts what exists. `open` counts what is unfinished on
-- that surface, and `open` is 0 for surfaces where nothing can be
-- owed. `how` is the statement that extends the surface.
--
-- Order is the caller's: a reader wants it by surface, an agent by
-- what is open.
--
-- The heavy reads are counted once in CTEs rather than named twice in
-- the branches below: each mention expands the whole read into the
-- plan, and four expansions of `open_questions` and `ruling_entries`
-- across a nine-branch union overflowed the planner's stack outright.
WITH asked AS (
  SELECT count(*) AS n, count(DISTINCT aspect) AS metrics FROM open_questions
),
ruled AS (
  SELECT count(*) AS n,
         count(*) FILTER (WHERE NOT folded_in) AS owed
  FROM ruling_entries
),
apps AS (SELECT count(DISTINCT app) AS n FROM app_parts)
SELECT 'sources' AS surface,
       'DECLARE SOURCE, then PROBE it — rehearse with LIMIT 0 to see every column before authoring a recipe' AS how,
       (SELECT count(*) FROM sources) AS stands,
       0 AS open
UNION ALL
SELECT 'tables' AS surface,
       'author a recipe and extract it — typing is the recipe''s work, this is not ETL' AS how,
       (SELECT count(DISTINCT table_name) FROM imports) AS stands,
       (SELECT count(*) FROM imports WHERE dropped_rows_count > 0 OR cast_failures > 0) AS open
UNION ALL
SELECT 'relationships' AS surface,
       'DECLARE RELATIONSHIP between two endpoints — detect_relationships proposes, you judge' AS how,
       (SELECT count(*) FROM relationships) AS stands,
       0 AS open
UNION ALL
SELECT 'aspects' AS surface,
       'DECLARE ASPECT to add vocabulary — AS FACT, MEASUREMENT or QUERY, grained with ON' AS how,
       (SELECT count(*) FROM aspects) AS stands,
       0 AS open
UNION ALL
SELECT 'claims' AS surface,
       'GLOSS a subject with an aspect — the write verb; a human writing outranks the agent slot at every read' AS how,
       (SELECT count(*) FROM glossary) AS stands,
       (SELECT n FROM asked) AS open
UNION ALL
SELECT 'functions' AS surface,
       'run one as a measurement, or DECLARE FUNCTION your own — statistics are the functions'' work, never a human question' AS how,
       (SELECT count(*) FROM functions) AS stands,
       (SELECT count(*) FROM functions f
        WHERE NOT EXISTS (SELECT 1 FROM cache c WHERE c.function = f.name)) AS open
UNION ALL
SELECT 'metrics' AS surface,
       'GLOSS a QUERY aspect with its SQL and its assumptions — read.<name>() then serves it' AS how,
       (SELECT count(*) FROM aspects WHERE kind = 'query') AS stands,
       (SELECT metrics FROM asked) AS open
UNION ALL
SELECT 'rulings' AS surface,
       'a human rules a disclosed assumption; the agent owes the re-record that folds it in' AS how,
       (SELECT n FROM ruled) AS stands,
       (SELECT owed FROM ruled) AS open
UNION ALL
SELECT 'apps' AS surface,
       'GLOSS app, app_page, app_frame, app_spec — one gloss per part, so a surface is written like anything else' AS how,
       (SELECT n FROM apps) AS stands,
       0 AS open
