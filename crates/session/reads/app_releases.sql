-- app_releases — what is released of each glossed app: one row per
-- app that carries a release.
--
-- A release is a gloss of the `app_release` aspect on the app's name.
-- The door serves the app's parts as they stood at `at` — the
-- release's own writing time, or the earlier release the body's `at`
-- names — and everything written since is the draft, served under
-- `?draft`. `released_at` is when the serving release was written:
-- what a later release names in `at` to return to this one.
--
-- Two collapses, in this order: the newest release per (dataset, app,
-- actor kind), then the human's over the agent's — the same precedence
-- every other read serves. Workspace-wide, with `dataset` on every
-- row, like `app_parts`.
SELECT
  arrow_cast(g.dataset, 'Utf8') AS dataset,
  arrow_cast(g.subject, 'Utf8') AS app,
  arrow_cast(coalesce(json_get_str(g.body, 'at'), g.written_at), 'Utf8') AS at,
  arrow_cast(json_get_str(g.body, 'note'), 'Utf8') AS note,
  arrow_cast(g.actor_kind, 'Utf8') AS actor_kind,
  arrow_cast(g.written_at, 'Utf8') AS released_at
FROM glossary g
WHERE g.aspect = 'app_release'
  AND NOT EXISTS (SELECT 1 FROM glossary g2
                  WHERE g2.dataset = g.dataset
                    AND g2.subject = g.subject AND g2.aspect = g.aspect
                    AND g2.actor_kind = g.actor_kind
                    AND g2.written_at > g.written_at)
  AND NOT EXISTS (SELECT 1 FROM glossary g3
                  WHERE g3.dataset = g.dataset
                    AND g3.subject = g.subject AND g3.aspect = g.aspect
                    AND g3.actor_kind = 'human' AND g.actor_kind = 'agent')
