-- The measurement aspects nobody has run: owed on their subjects by
-- grain and condition, with no landed body on any. The agent's
-- backlog, not a question for a person; folded under the measured.
SELECT arrow_cast(g.aspect, 'Utf8') AS aspect,
       arrow_cast('owed on ' || CAST(count(*) AS VARCHAR) || ' '
         || CASE WHEN CAST(a.grains AS VARCHAR) LIKE '%column%' THEN 'columns'
                 WHEN CAST(a.grains AS VARCHAR) LIKE '%table%' THEN 'tables'
                 ELSE 'subjects' END
         || coalesce(' where ' || CAST(a.condition AS VARCHAR), '')
         || ', none measured', 'Utf8') AS owed
FROM GLOSSARY() g JOIN aspects a ON a.name = g.aspect
WHERE a.kind = 'measurement'
GROUP BY g.aspect, a.grains, a.condition
HAVING count(*) FILTER (WHERE g.state <> 'unassessed') = 0
ORDER BY aspect
