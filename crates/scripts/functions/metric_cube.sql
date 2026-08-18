-- The metric cube: every grounded metric's monthly series — the total,
-- the slices along its served dimension columns, and the named rival
-- where a grounding discloses one — landed as one measurement. What it
-- exists for: a static frame cannot name a metric in FROM, but the
-- metric_cube_slices door turns metric names into rows once and the
-- metric_series() read serves them to any frame with plain value
-- filters. Runs at dataset grain (`SELECT metric_cube() FROM fin`).
-- The caps and the three monthly verbs are the door's; extraction
-- serves the summary alone (ruled 2026-08-14: the 54 KB body was
-- write-only through the door) — the full cube is the landed value's
-- business: metric_series() slices it, GLOSSARY reads it whole. Cube
-- rows are records since stage 5 — the tuple form was a script-ism
-- arrow could not carry.
-- CTE names carry the mc_ prefix: the planner seam resolves a
-- pinned workspace table ahead of a same-named CTE, so a dataset
-- carrying a table named `cells` would otherwise capture the join
-- (found at the port's own suite, 2026-08-18).
WITH mc_d AS (SELECT * FROM metric_cube_slices($subject)),
mc_facts AS (
  SELECT seq, metric, applicable, reason, behavior, dims, bucketed,
         alternative, alternative_error
  FROM mc_d WHERE fact
),
mc_cells AS (
  SELECT seq,
         coalesce(array_agg(named_struct(
           'dimension', dimension, 'member', member,
           'period', period, 'value', value
         ) ORDER BY cell_seq) FILTER (WHERE period IS NOT NULL), []) AS rows,
         count(period) AS cell_count
  FROM mc_d GROUP BY seq
),
mc_m AS (
  SELECT f.seq, f.metric, f.applicable, f.reason, f.behavior, f.dims,
         f.bucketed, f.alternative, f.alternative_error,
         CASE WHEN f.applicable THEN c.rows END AS rows,
         c.cell_count
  FROM mc_facts f JOIN mc_cells c ON f.seq = c.seq
)
SELECT
  count(metric) > 0 AS applicable,
  named_struct('dims', 4, 'members', 24, 'months', 48) AS caps,
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'behavior', behavior, 'dims', dims, 'bucketed', bucketed, 'rows', rows,
    'alternative', alternative, 'alternative_error', alternative_error
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics,
  named_struct(
    'metrics', coalesce(array_agg(metric ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []),
    'rows', coalesce(sum(cell_count), 0),
    'note', 'cached — slice with metric_series(); the whole cube reads back via GLOSSARY(<dataset>::metric_cube)'
  ) AS summary
FROM mc_m
