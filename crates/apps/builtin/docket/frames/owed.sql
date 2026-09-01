-- What waits on an act: the shipped `owed` read, which holds the five
-- sources and why each derives. Display only — a readable timestamp,
-- a link chosen by kind, the order. A `re-measure` row links to the
-- metrics page, where the banner's button runs the act.
SELECT arrow_cast(o.what, 'Utf8') AS what,
       arrow_cast(o.why, 'Utf8') AS why,
       arrow_cast(replace(substr(o.since, 1, 16), 'T', ' '), 'Utf8') AS since,
       arrow_cast(CASE o.kind
         WHEN 'recipe' THEN '/app/docket/p/record'
         WHEN 'contest' THEN '/app/docket/p/record'
         WHEN 're-measure' THEN '/app/docket/p/metrics'
         ELSE '/app/docket/p/metrics?metric=' || o.subject
       END, 'Utf8') AS link
FROM owed o
ORDER BY since DESC, what
