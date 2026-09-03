-- metric_surfaces — every declared metric, with where it stands.
--
-- One row per QUERY aspect: what it is called, its unit and meaning,
-- its formula, whether a grounding with `sql` is recorded, and the
-- author's `stopped` text where the current grounding is a stop. The pulse list and
-- the dossier header both render this; before it existed the pulse
-- frame counted loose assumptions with its own unnest, which is how
-- the chip and the queue came to disagree.
--
-- The record only. The numbers — a metric's latest period, its move,
-- the axes the cube admitted — are the data at a grain, served by
-- `metric_series()` and `metric_axes()`; a caller that wants them
-- beside these rows joins on `metric`, the key. Keeping them apart is
-- what lets a ruling refresh this read without touching the cube.
--
-- No open count and no ruled-at here: both live in workspace-wide
-- relations, and which dataset they should answer for is the caller's
-- question, not this read's. Callers join `open_questions` and
-- `ruling_entries` narrowed to a dataset — `current_dataset` names the
-- bound one, so that is one line each against reads that already exist.
-- Formatting, glyphs and links are the caller's.
WITH grounded AS (
  SELECT DISTINCT aspect FROM GLOSSARY(all => true)
  WHERE kind = 'query' AND json_get_str(body, 'sql') IS NOT NULL
),
-- The collapsed grounding, human over agent: a human `sql` over an
-- agent `stopped` serves, a human `stopped` over an agent `sql` stops
-- (SPEC.md §5.2). The text is the author's finding, carried as written.
stopped AS (
  SELECT aspect, max(json_get_str(value, 'stopped')) AS why
  FROM GLOSSARY()
  WHERE json_get_str(value, 'stopped') IS NOT NULL
  GROUP BY aspect
)
SELECT a.name AS metric,
       coalesce(json_get_str(a.schema, 'title'), a.name) AS title,
       coalesce(json_get_str(a.schema, 'x-kind'), '') AS kind,
       arrow_cast(coalesce(
         json_get_str(json_get(json_get(d.value, 'definitions'), a.name), 'unit'),
         ''), 'Utf8') AS unit,
       arrow_cast(coalesce(
         json_get_str(json_get(json_get(d.value, 'definitions'), a.name), 'meaning'),
         ''), 'Utf8') AS meaning,
       arrow_cast(coalesce(json_get_str(json_get(f.value, 'formulas'), a.name),
                  'a base concept — grounded as an extract, no formula'), 'Utf8') AS formula,
       g.aspect IS NOT NULL AS grounded,
       arrow_cast(coalesce(s.why, ''), 'Utf8') AS stopped
FROM aspects a
LEFT JOIN grounded g ON g.aspect = a.name
LEFT JOIN stopped s ON s.aspect = a.name
-- The two dataset registries, each a collapsed read: one winning slot,
-- human over agent. Both paths are indexed by the metric's own name, so
-- they stay here where that column is in scope.
--
-- `title` and `kind` come from the aspect blob — the display label and
-- the tooling flag, which is all the blob keeps. `unit` and `meaning`
-- come from `definitions`, because a declaration cannot be superseded
-- and the company revises both — serving `x-unit` from the blob
-- goes stale on the first revision.
-- A field lives in exactly one place, never both.
LEFT JOIN GLOSSARY() f ON f.aspect = 'formulas'
LEFT JOIN GLOSSARY() d ON d.aspect = 'definitions'
WHERE a.kind = 'query'
  -- The vocabulary is workspace-wide, the record is not: a metric is
  -- this dataset's when a grounding stands here, served or stopped,
  -- or one of its two registries names it. A metric declared and neither glossed nor
  -- defined belongs to no dataset yet and shows in none — its
  -- `definitions` entry is the act that claims it; `workspace_next`
  -- counts it meanwhile.
  AND (g.aspect IS NOT NULL
       OR s.aspect IS NOT NULL
       OR coalesce(json_contains(json_get(d.value, 'definitions'), a.name), false)
       OR coalesce(json_contains(json_get(f.value, 'formulas'), a.name), false))
