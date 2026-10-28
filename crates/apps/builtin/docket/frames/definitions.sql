-- The Glossary's definitions: every metric, view and fact of the
-- dataset with the words the company agreed for it — the
-- `definitions` registry's meaning and unit, as `metric_surfaces`
-- serves them — and what kind of surface it is, linked to its page.
-- A surface without an entry says so, which is itself a finding: a
-- number nobody has defined. A stopped surface carries its author's
-- text in place of a meaning. A plan is no definition: it stands on
-- its metric's page.
SELECT s.title,
       s.name,
       arrow_cast(CASE WHEN s.kind IN ('', 'measure', 'metric') THEN 'metric'
                       WHEN s.kind = 'relation' THEN 'view'
                       ELSE s.kind END, 'Utf8') AS kind,
       arrow_cast(CASE WHEN s.stopped <> '' THEN 'stopped: ' || s.stopped
                       ELSE coalesce(nullif(s.meaning, ''), 'no definitions entry yet') END, 'Utf8') AS meaning,
       arrow_cast(coalesce(s.unit, ''), 'Utf8') AS unit,
       arrow_cast('/' || d.dataset || '/app/docket/p/metrics?metric=' || s.name
         || CASE WHEN s.kind IN ('relation', 'fact') THEN '&kind=' || s.kind ELSE '' END, 'Utf8') AS link
FROM metric_surfaces s CROSS JOIN current_dataset d
WHERE s.kind <> 'plan'
ORDER BY CASE kind WHEN 'metric' THEN 0 WHEN 'view' THEN 1 ELSE 2 END, title
