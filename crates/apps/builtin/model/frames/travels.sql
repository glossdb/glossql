-- Where the column's meaning travels: metric surfaces whose recorded
-- SQL mentions its bare name. A textual mention, stated as such — the
-- honest reach until composition is a declared relation.
SELECT q.aspect AS metric, '?metric=' || q.aspect AS link
FROM GLOSSARY(all => true) q
WHERE q.kind = 'query'
  AND strpos(json_get_str(q.body, 'sql'),
             arrow_cast(substr($subject, strpos($subject, '.') + 1), 'Utf8')) > 0
ORDER BY q.aspect
