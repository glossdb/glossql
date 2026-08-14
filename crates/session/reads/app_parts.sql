-- app_parts — apps authored as glosses, one row per file.
--
-- An app used to be a directory and nothing else, read from disk per
-- request, which is why an agent connected over MCP could not author
-- one: it has no filesystem, only statements. Each part is now its own
-- gloss, so writing an app is writing glosses — supersession versions
-- each part on its own, and actor kind records whose hand shaped it.
--
-- The aspect says what kind of file it is and the subject says where
-- it goes: `app` on `<app>` is the manifest, `app_page` on
-- `<app>.<page>` is a page, `app_frame` a query, `app_spec` a chart
-- spec. One gloss per part (ruled 2026-08-15), so an author edits one
-- frame without rewriting the app.
--
-- Two collapses, in this order: the newest writing per (subject,
-- aspect, actor kind), then the human's over the agent's — the same
-- precedence every other read serves.
--
-- `arrow_cast` on the way out: a shipped read should have a schema its
-- consumers can rely on, and json_get_str's return type is not one the
-- Rust side should have to guess at.
SELECT
  arrow_cast(split_part(g.subject, '.', 1), 'Utf8') AS app,
  arrow_cast(
    CASE g.aspect
      WHEN 'app' THEN 'app'
      WHEN 'app_page' THEN split_part(g.subject, '.', 2) || '.html'
      WHEN 'app_frame' THEN 'frames/' || split_part(g.subject, '.', 2) || '.sql'
      ELSE 'specs/' || split_part(g.subject, '.', 2) || '.vl.json'
    END, 'Utf8') AS path,
  arrow_cast(
    CASE g.aspect
      WHEN 'app_page' THEN json_get_str(g.body, 'html')
      WHEN 'app_frame' THEN json_get_str(g.body, 'sql')
      WHEN 'app_spec' THEN json_get_str(g.body, 'spec')
      ELSE g.body
    END, 'Utf8') AS text,
  arrow_cast(g.actor_kind, 'Utf8') AS actor_kind
FROM glossary g
WHERE g.aspect IN ('app', 'app_page', 'app_frame', 'app_spec')
  AND (g.aspect = 'app' OR split_part(g.subject, '.', 2) <> '')
  AND NOT EXISTS (SELECT 1 FROM glossary g2
                  WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                    AND g2.actor_kind = g.actor_kind
                    AND g2.written_at > g.written_at)
  AND NOT EXISTS (SELECT 1 FROM glossary g3
                  WHERE g3.subject = g.subject AND g3.aspect = g.aspect
                    AND g3.actor_kind = 'human' AND g.actor_kind = 'agent')
