-- The waiting half of the human's brief: the shipped `owed` read,
-- which holds the four sources and why each one derives. What is left
-- here is display — a readable timestamp, a link chosen by kind, and
-- the order.
SELECT arrow_cast(o.what, 'Utf8') AS what,
       arrow_cast(o.why, 'Utf8') AS why,
       arrow_cast(replace(substr(o.since, 1, 16), 'T', ' '), 'Utf8') AS since,
       arrow_cast(
         CASE o.kind
           WHEN 'recipe' THEN '?subject=' || o.subject
           WHEN 'contest' THEN '?subject=' || o.subject
           ELSE '/app/metrics?metric=' || o.subject
         END, 'Utf8') AS link
FROM owed o
-- the projected alias, not o.since: naming both makes the sort ambiguous
ORDER BY since DESC, what
