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
-- Every count spans the workspace, deliberately: this is the read an
-- agent runs to orient itself, and the MCP door's channel is bound to
-- nothing when it does. So the counts are keyed the way the workspace
-- identifies a thing — vocabulary by name, everything a dataset owns
-- by (dataset, name) — rather than narrowed to one dataset. The
-- surfaces that ARE dataset-scoped are read through `open_questions`,
-- `ruling_entries` and `app_parts`, which carry `dataset` for exactly
-- this reason.
--
-- Order is the caller's: a reader wants it by surface, an agent by
-- what is open.
--
-- SHAPE, and why it is not the obvious one. This was nine UNION ALL
-- branches, each projecting a literal surface name beside its own
-- scalar subqueries. Two things went wrong with that, both met in
-- runs rather than reasoned about:
--
--   1. Naming a heavy read twice across the branches expands it into
--      the plan each time, and four expansions of `open_questions` and
--      `ruling_entries` overflowed the planner's stack outright.
--   2. Worse, and found in run 4: `WHERE surface = 'aspects'` over that
--      union came back with ALL NINE ROWS and every other surface's
--      `stands` reported as 0. The predicate is pushed into the
--      branches, constant-folds to false against the literal, and the
--      optimizer answers by replacing the table scans inside that
--      branch's scalar subqueries with an empty relation — while
--      keeping the branch's row. A filter that is silently dropped is
--      bad; one that also zeroes the numbers it returns is a read
--      nobody can trust.
--
-- So the counts are taken once, in a single row, and the surfaces are a
-- literal relation joined to it. A predicate on `surface` now lands on
-- plain VALUES rows and cannot reach a subquery at all.
-- `count(DISTINCT aspect)` here is deliberate and not the identity
-- slip the two DISTINCTs below were: an aspect IS workspace
-- vocabulary — the `aspects` relation carries no dataset — so `dso`
-- with open questions in two datasets is one metric with open
-- questions, which is what `n_metrics` counts it as.
WITH asked AS (
  SELECT count(*) AS n, count(DISTINCT aspect) AS metrics FROM open_questions
),
ruled AS (
  SELECT count(*) AS n,
         count(*) FILTER (WHERE NOT folded_in) AS owed
  FROM ruling_entries
),
-- Same identity rule as tables: the app door serves
-- `/<dataset>/app/<name>`, so a `docket` in two datasets is two apps
-- with two sets of parts.
apps AS (SELECT count(*) AS n FROM
           (SELECT DISTINCT dataset, app FROM app_parts) t),
-- A landing is open work when its casts nulled cells: kept rows with
-- holes in them, which is the one thing about an import that wants an
-- author's attention. Run 4 read 11 of 11 clean tables as open, because
-- this used to be `cast_failures > 0` on a column holding JSON text —
-- '{' sorts above '0', so every landing qualified. The dropped-row
-- count is deliberately not part of this: which rows a WHERE excluded
-- is the author's question answered on the files,
-- and the counter is only meaningful for a
-- single-relation row-preserving recipe anyway.
nulled AS (
  SELECT count(*) AS n FROM (
    SELECT DISTINCT i.dataset, i.table_name
    FROM imports i
    CROSS JOIN generate_series(0, 99) AS c(i)
    WHERE c.i < json_length(i.cast_failures, 'checked')
      AND json_get_int(json_get(json_get(i.cast_failures, 'checked'), c.i), 'failed') > 0
  ) t
),
counts AS (
  SELECT (SELECT count(*) FROM sources) AS n_sources,
         -- A table is identified by (dataset, name), never by name:
         -- `imports` is workspace-wide and two datasets each holding
         -- `orders` are two tables. A bare DISTINCT on the name
         -- collapses them and this surface *under*-reports, which is
         -- the opposite failure from the rest of this read and the
         -- easier one to miss.
         (SELECT count(*) FROM
            (SELECT DISTINCT dataset, table_name FROM imports) t) AS n_tables,
         (SELECT n FROM nulled) AS open_tables,
         (SELECT count(*) FROM relationships) AS n_relationships,
         (SELECT count(*) FROM aspects) AS n_aspects,
         (SELECT count(*) FROM glossary) AS n_claims,
         (SELECT n FROM asked) AS open_claims,
         -- A function is never "open": it is called, and computes when a
         -- read needs it — so this surface owes 0.
         (SELECT count(*) FROM functions) AS n_functions,
         -- A sample frame is a QUERY aspect too, and would count as a
         -- metric here — the two doors have their own surfaces.
         -- Narrowed by exclusion, not by requiring
         -- `x-kind = 'metric'`: a grounding declared without the tag is
         -- still a metric, and dropping it from the map would be worse
         -- than the miscount this fixes.
         (SELECT count(*) FROM aspects
          WHERE kind = 'query'
            AND coalesce(json_get_str(schema, 'x-kind'), '') <> 'sample') AS n_metrics,
         (SELECT metrics FROM asked) AS open_metrics,
         -- A validation stands when its witness is declared; it is
         -- open while nobody has spoken on the witnessed aspect — no
         -- authored expectation in the glossary and no function voice
         -- in the measurements. Light scalar subqueries, kept in this
         -- single row like the rest (see SHAPE above).
         (SELECT count(*) FROM witnesses) AS n_validations,
         (SELECT count(*) FROM witnesses w
          WHERE NOT EXISTS (SELECT 1 FROM glossary g WHERE g.aspect = w.aspect)
            AND NOT EXISTS (SELECT 1 FROM measurements m WHERE m.aspect = w.aspect)) AS open_validations,
         -- The two model doors. Both are planner doors rather than rows
         -- in `functions`, so nothing else on this map or in the
         -- relations would ever mention them: an agent could not find
         -- what it could not name. `open` is vocabulary standing
         -- without a body — declared, never glossed, so the door has
         -- nothing to serve.
         (SELECT count(*) FROM aspects
          WHERE kind = 'fact'
            AND json_get_str(schema, 'x-kind') = 'scenario') AS n_scenarios,
         (SELECT count(*) FROM aspects a
          WHERE a.kind = 'fact'
            AND json_get_str(a.schema, 'x-kind') = 'scenario'
            AND NOT EXISTS (SELECT 1 FROM glossary g WHERE g.aspect = a.name)) AS open_scenarios,
         (SELECT count(*) FROM aspects
          WHERE kind = 'query'
            AND json_get_str(schema, 'x-kind') = 'sample') AS n_samples,
         (SELECT count(*) FROM aspects a
          WHERE a.kind = 'query'
            AND json_get_str(a.schema, 'x-kind') = 'sample'
            AND NOT EXISTS (SELECT 1 FROM glossary g WHERE g.aspect = a.name)) AS open_samples,
         (SELECT n FROM ruled) AS n_rulings,
         (SELECT owed FROM ruled) AS open_rulings,
         (SELECT n FROM apps) AS n_apps
)
SELECT s.surface AS surface,
       s.how AS how,
       CASE s.surface
         WHEN 'sources' THEN c.n_sources
         WHEN 'tables' THEN c.n_tables
         WHEN 'relationships' THEN c.n_relationships
         WHEN 'aspects' THEN c.n_aspects
         WHEN 'claims' THEN c.n_claims
         WHEN 'functions' THEN c.n_functions
         WHEN 'metrics' THEN c.n_metrics
         WHEN 'validations' THEN c.n_validations
         WHEN 'scenarios' THEN c.n_scenarios
         WHEN 'samples' THEN c.n_samples
         WHEN 'rulings' THEN c.n_rulings
         ELSE c.n_apps
       END AS stands,
       CASE s.surface
         WHEN 'tables' THEN c.open_tables
         WHEN 'claims' THEN c.open_claims
         WHEN 'functions' THEN 0
         WHEN 'metrics' THEN c.open_metrics
         WHEN 'validations' THEN c.open_validations
         WHEN 'scenarios' THEN c.open_scenarios
         WHEN 'samples' THEN c.open_samples
         WHEN 'rulings' THEN c.open_rulings
         ELSE 0
       END AS open
FROM counts c
CROSS JOIN (VALUES
  ('sources', 'DECLARE SOURCE, then PROBE it — rehearse with LIMIT 0 to see every column before authoring a recipe'),
  ('tables', 'author a recipe and extract it — typing is the recipe''s work, this is not ETL'),
  ('relationships', 'DECLARE RELATIONSHIP between two endpoints — detect_relationships proposes, you judge'),
  ('aspects', 'DECLARE ASPECT to add vocabulary — AS FACT, MEASUREMENT or QUERY, grained with ON'),
  ('claims', 'GLOSS a subject with an aspect — the write verb; a human writing outranks the agent slot at every read'),
  ('functions', 'run one as a measurement, or DECLARE FUNCTION your own — statistics are the functions'' work, never a human question'),
  ('metrics', 'GLOSS a QUERY aspect with its SQL and its assumptions — read.<name>() then serves it'),
  ('validations', 'GLOSS the expectation on a FACT aspect, DECLARE FUNCTION the check that RETURNS it, DECLARE WITNESS with a DETECTOR banding them — ATTEST() serves the verdicts; a reconciliation run by hand becomes a standing check'),
  ('scenarios', 'DECLARE ASPECT ... AS FACT with x-kind scenario, GLOSS its column overrides and their basis — whatif.<name>() then replays the recipes and bands the result'),
  ('samples', 'DECLARE ASPECT ... AS QUERY with x-kind sample, GLOSS one SELECT holding known-good history and the suspects together — misfit.<name>() then ranks the rows'),
  ('rulings', 'a human rules a disclosed assumption; the agent owes the re-record that folds it in'),
  ('apps', 'GLOSS app, app_page, app_frame, app_spec — one gloss per part, so a surface is written like anything else')
) AS s(surface, how)
