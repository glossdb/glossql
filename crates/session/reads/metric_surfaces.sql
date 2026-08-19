-- metric_surfaces — every declared metric, with where it stands.
--
-- One row per QUERY aspect: what it is called, its latest cube month
-- and the move into it, the axes the cube admitted, how many questions
-- it still owes a human, and when a human last ruled on it. The pulse
-- list and the dossier header both render this; before it existed the
-- pulse frame counted loose assumptions with its own unnest, which is
-- how the chip and the queue came to disagree.
--
-- No open count and no ruled-at here: both live in workspace-wide
-- relations, and only the caller knows which dataset it is bound to.
-- Callers join `open_questions` and `ruling_entries` scoped to their
-- own dataset — one line each, against reads that already exist.
--
-- Values come from the `metric_cube` measurement: a metric with
-- no cube rows carries nulls here until `SELECT metric_cube() FROM`
-- the dataset runs. Formatting, glyphs and links are the caller's.
WITH latest AS (
  SELECT metric, period, value,
         value - lag(value) OVER (PARTITION BY metric ORDER BY period) AS delta
  FROM metric_series() WHERE dimension = ''
  QUALIFY row_number() OVER (PARTITION BY metric ORDER BY period DESC) = 1
),
axes AS (
  SELECT metric, string_agg(dimension, ' · ') AS axes
  FROM (SELECT DISTINCT metric, dimension FROM metric_series()
        WHERE dimension NOT IN ('', 'alternative'))
  GROUP BY metric
),
grounded AS (
  SELECT DISTINCT aspect FROM GLOSSARY(all => true) WHERE kind = 'query'
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
       l.period AS period,
       l.value AS value,
       l.delta AS delta,
       x.axes AS axes,
       arrow_cast(coalesce(json_get_str(json_get(f.value, 'formulas'), a.name),
                  'a base concept — grounded as an extract, no formula'), 'Utf8') AS formula,
       g.aspect IS NOT NULL AS grounded
FROM aspects a
LEFT JOIN latest l ON l.metric = a.name
LEFT JOIN axes x ON x.metric = a.name
LEFT JOIN grounded g ON g.aspect = a.name
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
