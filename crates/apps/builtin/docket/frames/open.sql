-- The docket's open band: what stands open for a human to judge,
-- straight from `open_questions` — the same rows the door asks as
-- forms. Display only: the confidence, the link, the order. The
-- verdict column stays empty here, which is what "open" looks like.
-- `pct` fills the confidence rail: the gutter is the page's one
-- signature, and read down the column it says how settled this
-- workspace is.
SELECT o.conf,
       arrow_cast(CAST(CAST(round(o.conf * 100) AS INT) AS VARCHAR), 'Utf8') AS pct,
       -- The confidence as the record holds it, to two places, so a
       -- column of them lines up under the rails.
       arrow_cast(CASE WHEN o.conf >= 1.0 THEN '1.00'
                       ELSE '0.' || lpad(CAST(CAST(round(o.conf * 100) AS INT) AS VARCHAR), 2, '0')
                  END, 'Utf8') AS cf,
       arrow_cast(o.aspect, 'Utf8') AS subj,
       arrow_cast(coalesce(o.dimension, '-') || ' · ' || o.key, 'Utf8') AS asp,
       arrow_cast(o.assumption, 'Utf8') AS what,
       arrow_cast(coalesce(o.basis, 'unstated'), 'Utf8') AS basis,
       arrow_cast(coalesce(o.sibling, ''), 'Utf8') AS sibling,
       arrow_cast('/app/docket/p/metrics?metric=' || o.aspect, 'Utf8') AS link
FROM open_questions o
WHERE o.dataset = CAST($dataset AS VARCHAR)
ORDER BY o.conf ASC, subj, asp
