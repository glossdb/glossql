-- One row while any measurement in the dataset stands from before the
-- last change — a function voice landed at an earlier pin, served and
-- marked (the raw read's `current`) — or while one the cube's fact
-- rows or a witness reads was never made (`owed`'s `never measured`
-- rows: a served column nobody profiled, the walk that never ran).
-- The numbers are current: the cube rebuilds at every pin; the judged
-- axes, the bands and the check verdicts stand on those voices until
-- they run again, or run at all. Record-class, so the banner appears
-- with the write that caused it and clears when re-measure lands. The
-- hint is composed here — formatting is this frame's.
WITH stale AS (
  SELECT actor, count(*) AS n FROM GLOSSARY(all => true)
  WHERE NOT current GROUP BY actor
),
unmade AS (
  SELECT subject AS actor FROM owed WHERE kind = 'never measured'
),
counts AS (
  SELECT (SELECT coalesce(sum(n), 0) FROM stale) AS n,
         (SELECT count(*) FROM unmade) AS m,
         (SELECT string_agg(actor, ', ' ORDER BY actor)
          FROM (SELECT actor FROM stale UNION SELECT actor FROM unmade)) AS functions
)
SELECT n, m,
       arrow_cast(coalesce(functions, ''), 'Utf8') AS functions,
       arrow_cast(concat_ws(', ',
         CASE WHEN n > 0 THEN CAST(n AS VARCHAR) || ' measurements stand from before the last change' END,
         CASE WHEN m > 0 THEN CAST(m AS VARCHAR) || ' the cube or a witness reads were never made' END)
         || ' (' || coalesce(functions, '') || ') — the numbers are current; the judged axes, the bands and the check verdicts may not be.',
         'Utf8') AS hint
FROM counts
WHERE n > 0 OR m > 0
