-- One number: witnessed slots nobody has spoken to yet — the agent's
-- measurement backlog, bounded by conditional relevance (ruled
-- 2026-08-14: a column owes behavior/unit/dimension only where its
-- role makes them meaningful). The owed detail per table lives in
-- Coverage; the verdicts live in frames/checks.sql.
SELECT count(*) AS n
FROM witnesses w
JOIN GLOSSARY() c ON c.aspect = w.aspect
WHERE c.state = 'unassessed'
