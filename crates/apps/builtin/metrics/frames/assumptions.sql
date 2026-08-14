-- The metric's assumptions ledger: every dimension of the claim with
-- its basis and confidence, read from the AGENT's current grounding —
-- the one record that moves as the work moves (ruled 2026-08-14: the
-- human slot carries rulings, never a body copy). Each assumption
-- joins its standing ruling on its declared `key` — prose is display,
-- never a join column (ruled 2026-08-14) — so the row shows the judgment
-- that led to the agreed fact — and, while the assumption still sits
-- below full confidence, that the fold-in is owed. Full confidence
-- renders fixed (●), less renders loose (◐). Each row carries the
-- contest statement — the whole metric gloss, because the gloss is
-- the unit of writing: an assumption is a row inside it, edited in
-- place while the rest rides along.
WITH ruled AS (
  SELECT r.subject AS subject,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'aspect') AS aspect,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'key') AS key,
         json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'stance') AS stance,
         coalesce(json_get_str(json_get(json_get(r.body, 'rulings'), rj.j), 'note'), '') AS note
  FROM glossary r
  CROSS JOIN generate_series(0, 199) AS rj(j)
  WHERE r.aspect = 'ruling' AND r.actor_kind = 'human'
    AND NOT EXISTS (SELECT 1 FROM glossary r2
                    WHERE r2.subject = r.subject AND r2.aspect = 'ruling'
                      AND r2.actor_kind = 'human' AND r2.written_at > r.written_at)
    AND rj.j < json_length(r.body, 'rulings')
),
a AS (
  SELECT
    g.subject AS subject,
    json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'dimension') AS dim,
    json_get_float(json_get(json_get(g.body, 'assumptions'), i.i), 'confidence') AS conf,
    json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'key') AS key,
    json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'assumption') AS what,
    coalesce(json_get_str(json_get(json_get(g.body, 'assumptions'), i.i), 'basis'), '') AS basis,
    arrow_cast('GLOSS ' || g.aspect || ' ON ' || CAST($dataset AS VARCHAR) || ' AS $$' || g.body || '$$;', 'Utf8') AS statement
  FROM glossary g
  CROSS JOIN generate_series(0, 19) AS i(i)
  WHERE g.aspect = $metric AND g.actor_kind = 'agent'
    AND NOT EXISTS (SELECT 1 FROM glossary g2
                    WHERE g2.subject = g.subject AND g2.aspect = g.aspect
                      AND g2.actor_kind = 'agent' AND g2.written_at > g.written_at)
    AND i.i < json_length(g.body, 'assumptions')
)
SELECT
  CASE WHEN a.conf >= 1.0 THEN '●' ELSE '◐' END AS glyph,
  -- amber marks what still wants judgement (the hue law): the loose
  -- glyph carries it, the settled one stays quiet ink
  CASE WHEN a.conf >= 1.0 THEN 'g-jud' ELSE 'g-fix' END AS gcls,
  a.conf, a.dim, a.what, a.basis,
  arrow_cast(CASE
    WHEN r.stance IS NULL THEN ''
    ELSE 'ruled: ' || r.stance
         || CASE WHEN r.note <> '' THEN ' — ' || r.note ELSE '' END
         || CASE WHEN a.conf < 1.0 THEN ' · awaiting the fold-in' ELSE '' END
  END, 'Utf8') AS ruling,
  a.statement
FROM a
LEFT JOIN ruled r
  ON r.subject = a.subject AND r.aspect = $metric AND r.key = a.key
ORDER BY a.conf, a.dim
