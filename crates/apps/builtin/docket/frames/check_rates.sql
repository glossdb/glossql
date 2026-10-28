-- A validation's evidence: per subject, the authored expectation — the
-- outcome in words and the tolerance written beside it — against the
-- breach rate the check measured, and the band the detector gave the
-- pair. The expectation is the human's or the agent's slot, the
-- measurement a function voice's; both ride the glossary's rows on
-- the subject and aspect. Rows only for a witness whose detector is
-- rate_tolerance.
WITH w AS (
  SELECT name, aspect, threshold FROM witnesses WHERE name = $check AND detector = 'rate_tolerance'
),
slots AS (
  SELECT g.subject, g.aspect, g.actor_kind, g.body
  FROM glossary g
  JOIN current_dataset d ON d.dataset = g.dataset
  JOIN w ON w.aspect = g.aspect
),
expected AS (
  SELECT subject, aspect,
         max(json_get_str(body, 'outcome')) AS outcome,
         max(json_get_float(body, 'tolerance')) AS tolerance
  FROM slots WHERE actor_kind IN ('human', 'agent') GROUP BY subject, aspect
),
measured AS (
  SELECT subject, aspect, max(json_get_float(body, 'breach_rate')) AS rate
  FROM slots WHERE actor_kind NOT IN ('human', 'agent') GROUP BY subject, aspect
)
SELECT arrow_cast(a.subject, 'Utf8') AS subject,
       arrow_cast(coalesce(e.outcome, 'no expectation written'), 'Utf8') AS outcome,
       arrow_cast(coalesce('tolerance ' || CAST(e.tolerance AS VARCHAR), 'tolerance ' || coalesce(w.threshold, '0')), 'Utf8') AS tolerance,
       arrow_cast(coalesce('measured ' || CAST(m.rate AS VARCHAR), 'no check has spoken'), 'Utf8') AS measured,
       arrow_cast(a.band, 'Utf8') AS band,
       CASE WHEN a.band = 'green' THEN 'ok' ELSE 'warn' END AS bcls
FROM ATTEST() a
JOIN w ON w.name = a.witness
LEFT JOIN expected e ON e.subject = a.subject AND e.aspect = a.aspect
LEFT JOIN measured m ON m.subject = a.subject AND m.aspect = a.aspect
ORDER BY CASE band WHEN 'red' THEN 0 WHEN 'orange' THEN 1 WHEN 'yellow' THEN 2 WHEN 'error' THEN 3 ELSE 4 END, subject
