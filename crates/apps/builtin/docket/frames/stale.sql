-- One row only while the served cube is stale, so the page shows the
-- note with no template logic (display logic lives in frame SQL,
-- 2026-08-10). metric_series() serves the last landed cube after any
-- workspace write, marked current = false; recomputing is a pull that
-- belongs to an agent session, never to a page load.
SELECT arrow_cast(
  'showing the last landed cube — the workspace has been written since. '
  || 'Ask your agent to re-run it: SELECT metric_cube() FROM the dataset.',
  'Utf8') AS note
FROM (SELECT DISTINCT current FROM metric_series())
WHERE NOT current
