-- The docket's open band: what stands open for a human to judge,
-- straight from `open_questions` — the same rows the door asks as
-- forms. Display only: the confidence, the link, the order. The
-- verdict column stays empty here, which is what "open" looks like.
SELECT o.conf,
       arrow_cast(o.aspect, 'Utf8') AS subj,
       arrow_cast(coalesce(o.dimension, '-') || ' · ' || o.key, 'Utf8') AS asp,
       arrow_cast(o.assumption, 'Utf8') AS what,
       arrow_cast(coalesce(o.basis, 'unstated'), 'Utf8') AS basis,
       arrow_cast(coalesce(o.sibling, ''), 'Utf8') AS sibling,
       arrow_cast('/app/docket/p/metrics?metric=' || o.aspect, 'Utf8') AS link
FROM open_questions o
WHERE o.dataset = CAST($dataset AS VARCHAR)
ORDER BY o.conf ASC, subj, asp
