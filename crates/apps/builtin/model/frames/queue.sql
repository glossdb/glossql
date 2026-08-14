-- The one queue (informative since 2026-08-13: the app carries no
-- write — answers travel through a session and land as RULINGS, ruled
-- 2026-08-14): judgment only, and the same rows the door's round
-- serves, because both read `open_questions` — the shipped read that
-- holds the derivation and its reasons. What is left here is display:
-- glyphs, a link, and the order. The template stays dumb. No cap — the
-- read is the whole of what is owed.
SELECT '◐' AS glyph, 'g-jud' AS gcls,
       o.conf,
       o.aspect AS subj,
       o.dimension AS asp,
       o.assumption AS what,
       arrow_cast('/app/metrics?metric=' || o.aspect, 'Utf8') AS link
FROM open_questions o
ORDER BY o.conf ASC, subj, asp
