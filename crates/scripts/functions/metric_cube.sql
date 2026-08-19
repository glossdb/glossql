-- The metric cube: every grounded metric's monthly series — the total,
-- the slices along its served dimension columns, and the named rival
-- where a grounding discloses one — landed as one measurement. What it
-- exists for: a static frame cannot name a metric in FROM, but the
-- metric_cube_slices door turns metric names into rows once and the
-- metric_series() read serves them to any frame with plain value
-- filters. Runs at dataset grain (`SELECT metric_cube() FROM fin`).
-- The caps and the three monthly verbs are the door's; extraction
-- serves the summary alone (the 54 KB body was
-- write-only through the door) — the full cube is the landed value's
-- business: metric_series() slices it, GLOSSARY reads it whole. Cube
-- rows are records since stage 5 — the tuple form was a script-ism
-- arrow could not carry.
-- The axes come from judged verdicts, never from the data's own
-- shape: a served column enters as a dimension when its collapsed
-- dimension_relevance is applicable (human over agent over function),
-- relevance orders the admitted, and the time axis is the served date
-- column whose collapsed temporal_profile shows a named cadence,
-- highest completeness first. A column without a verdict is a gap,
-- not a candidate. Counting admits nothing; `members` is a bucketing
-- target, NOT a cutoff (34 sales orgs could never enter at a hard
-- 24): at 24 or under every member is named, and above that the top
-- 23 by weight keep their names while the rest fold into 'other' — so
-- a wide axis enters bucketed instead of falling off. Each metric's
-- `bucketed` field names the dimensions this
-- happened to, so 'other' is never mistaken for a business member.
-- Bucketing loses nothing: a bucketed flow or stock axis still sums
-- to the metric's own total, and a ratio member — 'other' included —
-- divides its own summed halves (member ratios never sum to the
-- total; that is the defect the ratio verb exists for).
-- CTE names carry the mc_ prefix: the planner seam resolves a
-- pinned workspace table ahead of a same-named CTE, so a dataset
-- carrying a table named `cells` would otherwise capture the join.
WITH mc_d AS (SELECT * FROM metric_cube_slices($subject)),
mc_facts AS (
  SELECT seq, metric, applicable, reason, behavior, dims, bucketed,
         alternative, alternative_error
  FROM mc_d WHERE fact
),
mc_cells AS (
  -- A ratio cell carries its summed halves (num, den) — a coarser
  -- window re-derives the division from them; other verbs leave them
  -- NULL and the landed body omits the keys.
  SELECT seq,
         coalesce(array_agg(named_struct(
           'dimension', dimension, 'member', member,
           'period', period, 'value', value, 'num', num, 'den', den
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
  coalesce(array_agg(named_struct(
    'metric', metric, 'applicable', applicable, 'reason', reason,
    'behavior', behavior, 'dims', dims, 'bucketed', bucketed, 'rows', rows,
    'alternative', alternative, 'alternative_error', alternative_error
  ) ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []) AS metrics,
  named_struct(
    'metrics', coalesce(array_agg(metric ORDER BY seq) FILTER (WHERE metric IS NOT NULL), []),
    'rows', coalesce(sum(cell_count), 0),
    'note', 'cached — slice with metric_series(); the whole cube reads back via GLOSSARY(<dataset>::metric_cube). A dimension wider than 24 members is bucketed to its top 23 by weight plus an "other" member — read the bucketed field on each metric before taking "other" for a business member'
  ) AS summary
FROM mc_m
