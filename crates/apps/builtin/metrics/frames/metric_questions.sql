-- One metric's open questions (the sketch's "your check" box,
-- 2026-08-13): the shipped `open_questions` read narrowed to this
-- metric. Literally the same rows the door asks as forms — one file
-- holds the gates and the reasons, and this frame only picks the
-- metric, names the columns the template wants, and orders them.
-- Answered rows stop deriving; nothing is stored or dismissed.
SELECT o.conf,
       arrow_cast(coalesce(o.dimension, '-'), 'Utf8') AS dim,
       arrow_cast(o.assumption, 'Utf8') AS what,
       arrow_cast(coalesce(o.basis, 'unstated'), 'Utf8') AS basis
FROM open_questions o
WHERE o.aspect = $metric
ORDER BY o.conf ASC
