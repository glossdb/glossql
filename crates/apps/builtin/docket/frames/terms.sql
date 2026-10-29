-- The terms: every metric and view with its agreed meaning — the
-- definitions registry as metric_surfaces serves it — and every
-- column whose meaning entry names a term, in one alphabet. A
-- person's ruling on a metric's definition, or a person's meaning on
-- a column, shows as a mark with the time on hover; a contested
-- meaning shows as one. A row opens the metric's page or the
-- column's. A stopped surface carries its author's text; a surface
-- without an entry says so.
WITH rulings AS (
  SELECT r.aspect, max(r.written_at) AS at, count(*) AS n
  FROM ruling_entries r JOIN current_dataset d ON d.dataset = r.dataset
  WHERE r.stance IN ('confirmed', 'corrected')
  GROUP BY r.aspect),
slots AS (
  SELECT g.subject, g.aspect, g.actor_kind, g.actor_id, g.body, g.written_at,
         row_number() OVER (PARTITION BY g.subject, g.aspect
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.written_at DESC) AS rn
  FROM glossary g JOIN current_dataset d ON d.dataset = g.dataset
  WHERE g.aspect IN ('meaning', 'role', 'unit', 'behavior', 'dimension')),
cur AS (SELECT subject, aspect, actor_kind, actor_id, body, written_at FROM slots WHERE rn = 1),
states AS (SELECT subject, state FROM GLOSSARY() WHERE aspect = 'meaning'),
joined AS (
  SELECT path, count(*) AS n FROM (
    SELECT r.left_path AS path FROM relationships r JOIN current_dataset d ON d.dataset = r.dataset
    UNION ALL
    SELECT r.right_path FROM relationships r JOIN current_dataset d ON d.dataset = r.dataset)
  GROUP BY path),
surfaces AS (
  SELECT s.title AS term, s.name,
         CASE WHEN s.kind = 'relation' THEN 'view' WHEN s.kind IN ('', 'measure', 'metric') THEN 'metric' ELSE s.kind END AS kind,
         CASE WHEN s.stopped <> '' THEN 'stopped: ' || s.stopped
              ELSE coalesce(nullif(s.meaning, ''), 'no definition yet') END AS meaning,
         coalesce(s.unit, '') AS aside,
         CASE WHEN r.n > 0 THEN 'ruled' ELSE '' END AS mark,
         CASE WHEN r.n > 0 THEN 'a person ruled on the definition, ' || to_char(CAST(r.at AS TIMESTAMP), '%d %b %Y %H:%M') ELSE '' END AS mark_title,
         '/' || d.dataset || '/app/docket/p/metrics?metric=' || s.name
           || CASE WHEN s.kind IN ('relation', 'fact') THEN '&kind=' || s.kind ELSE '' END AS link
  FROM metric_surfaces s
  CROSS JOIN current_dataset d
  LEFT JOIN rulings r ON r.aspect = s.name
  WHERE s.kind <> 'plan'),
cols AS (
  SELECT json_get_str(m.body, 'term') AS term, m.subject AS name, 'column' AS kind,
         json_get_str(m.body, 'value') AS meaning,
         concat_ws(' · ',
           json_get_str(r.body, 'value') || coalesce(', ' || json_get_str(x.body, 'value'), ''),
           json_get_str(b.body, 'value'),
           json_get_str(u.body, 'value'),
           CASE WHEN j.n > 0 THEN CAST(j.n AS VARCHAR) || CASE WHEN j.n = 1 THEN ' join' ELSE ' joins' END END) AS aside,
         CASE WHEN st.state = 'contested' THEN 'contested'
              WHEN st.state = 'stale' THEN 'stale'
              WHEN m.actor_kind = 'human' THEN 'ruled' ELSE '' END AS mark,
         CASE WHEN st.state = 'contested' THEN 'the voices on the meaning differ; the witness withholds it'
              WHEN st.state = 'stale' THEN 'written before the column last changed'
              WHEN m.actor_kind = 'human' THEN 'a person''s word, ' || to_char(CAST(m.written_at AS TIMESTAMP), '%d %b %Y %H:%M')
              ELSE '' END AS mark_title,
         '/' || d.dataset || '/app/docket/p/glossary?column=' || m.subject AS link
  FROM cur m
  CROSS JOIN current_dataset d
  LEFT JOIN cur r ON r.subject = m.subject AND r.aspect = 'role'
  LEFT JOIN cur x ON x.subject = m.subject AND x.aspect = 'dimension'
  LEFT JOIN cur b ON b.subject = m.subject AND b.aspect = 'behavior'
  LEFT JOIN cur u ON u.subject = m.subject AND u.aspect = 'unit'
  LEFT JOIN joined j ON j.path = m.subject
  LEFT JOIN states st ON st.subject = m.subject
  WHERE m.aspect = 'meaning' AND json_get_str(m.body, 'term') IS NOT NULL),
every AS (SELECT * FROM surfaces UNION ALL SELECT * FROM cols)
SELECT arrow_cast(term, 'Utf8') AS term,
       arrow_cast(name, 'Utf8') AS name,
       arrow_cast(kind, 'Utf8') AS kind,
       arrow_cast(meaning, 'Utf8') AS meaning,
       arrow_cast(aside, 'Utf8') AS aside,
       arrow_cast(mark, 'Utf8') AS mark,
       arrow_cast(CASE mark WHEN 'ruled' THEN 'v-ruled' WHEN 'contested' THEN 'v-open' WHEN 'stale' THEN 'v-stale' ELSE 'v-none' END, 'Utf8') AS mcls,
       arrow_cast(mark_title, 'Utf8') AS mark_title,
       arrow_cast(link, 'Utf8') AS link
FROM every
ORDER BY lower(term), name
