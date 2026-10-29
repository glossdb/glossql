-- A voice check's evidence: every subject whose slots differ — the
-- witness's verdicts that are not green — with every voice on the
-- slot side by side, by actor kind, the human's first; the agreeing
-- subjects are counted on the check's row, not listed, so a red is
-- never buried under sixty green ones. The voices are the glossary's
-- own rows on the subject and aspect; the band and score are
-- ATTEST()'s. Rows only for a witness whose detector is slot_entropy.
WITH w AS (
  SELECT name, aspect FROM witnesses WHERE name = $check AND detector = 'slot_entropy'
),
voices AS (
  SELECT g.subject, g.aspect,
         array_to_string(array_agg(
           g.actor_kind || ': ' || coalesce(json_get_str(g.body, 'value'), g.body)
           ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 WHEN 'agent' THEN 1 ELSE 2 END, g.actor_id, g.written_at), ' · ') AS voices
  FROM glossary g
  JOIN current_dataset d ON d.dataset = g.dataset
  JOIN w ON w.aspect = g.aspect
  GROUP BY g.subject, g.aspect
)
SELECT arrow_cast(a.subject, 'Utf8') AS subject,
       arrow_cast(coalesce(v.voices, 'no voice yet'), 'Utf8') AS voices,
       arrow_cast(CAST(round(a.score * 1000) / 1000 AS VARCHAR), 'Utf8') AS score,
       arrow_cast(a.band, 'Utf8') AS band,
       CASE WHEN a.band = 'green' THEN 'ok' ELSE 'warn' END AS bcls
FROM ATTEST() a
JOIN w ON w.name = a.witness
LEFT JOIN voices v ON v.subject = a.subject AND v.aspect = a.aspect
WHERE a.band <> 'green'
ORDER BY CAST(score AS DOUBLE) DESC, subject
