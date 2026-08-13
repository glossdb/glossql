-- One number: witnessed slots nobody has spoken to yet — the agent's
-- measurement backlog. The same join as frames/checks.sql, the other
-- half of the filter; the owed detail per table lives in Coverage.
SELECT count(*) AS n
FROM witnesses w
JOIN GLOSSARY() c ON c.aspect = w.aspect
WHERE c.state = 'unassessed'
