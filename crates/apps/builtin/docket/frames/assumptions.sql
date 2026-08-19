-- The metric's assumptions ledger: `agent_assumptions` narrowed to
-- this metric, each row joined to its standing ruling on the declared
-- `key` — prose is display, never a join column.
-- The row shows the judgment behind the agreed fact and, while the
-- assumption still sits below full confidence, that the fold-in is
-- owed. Full confidence renders fixed (●), less renders loose (◐);
-- amber marks what still wants judgement, the hue law.
--
-- The contest statement is the whole metric gloss, because the gloss
-- is the unit of writing: an assumption is a row inside it, edited in
-- place while the rest rides along.
SELECT CASE WHEN a.conf >= 1.0 THEN '●' ELSE '◐' END AS glyph,
       CASE WHEN a.conf >= 1.0 THEN 'g-jud' ELSE 'g-fix' END AS gcls,
       a.conf,
       arrow_cast(a.dimension, 'Utf8') AS dim,
       arrow_cast(a.assumption, 'Utf8') AS what,
       arrow_cast(coalesce(a.basis, ''), 'Utf8') AS basis,
       arrow_cast(CASE
         WHEN r.stance IS NULL THEN ''
         ELSE 'ruled: ' || r.stance
              || CASE WHEN coalesce(r.note, '') <> '' THEN ' — ' || r.note ELSE '' END
              || CASE WHEN a.conf < 1.0 THEN ' · awaiting the fold-in' ELSE '' END
       END, 'Utf8') AS ruling,
       arrow_cast('GLOSS ' || a.aspect || ' ON ' || CAST($dataset AS VARCHAR)
                  || ' AS $$' || a.body || '$$;', 'Utf8') AS statement
FROM agent_assumptions a
LEFT JOIN ruling_entries r
  ON r.subject = a.subject AND r.aspect = a.aspect AND r.key = a.key
WHERE a.aspect = $metric
ORDER BY a.conf, dim
