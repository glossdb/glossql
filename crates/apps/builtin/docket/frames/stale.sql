-- One row only while the served cube is stale, so the page shows the
-- note with no template logic (display logic lives in frame SQL,
-- 2026-08-10). The source is the affordance map's cube surface — open
-- while the recompute is owed — rather than metric_series() directly:
-- staleness is a fact about the record, and reading it through the
-- record keeps this frame `record`-class, so a ruling refreshes the
-- banner in the same breath as the panels (a metric_series() read
-- would class `data` and the banner itself would go stale).
-- Recomputing stays a pull that belongs to an agent session, never to
-- a page load.
SELECT arrow_cast(
  'showing the last landed cube — the workspace has been written since. '
  || 'Ask your agent to re-run it: SELECT metric_cube() FROM the dataset.',
  'Utf8') AS note
FROM workspace_next WHERE surface = 'cube' AND open > 0
