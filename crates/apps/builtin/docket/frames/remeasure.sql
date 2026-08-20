-- One row while any measurement in the dataset stands from before the
-- last change — a function voice landed at an earlier pin, served and
-- marked (the raw read's `current`). The numbers are current: the cube
-- rebuilds at every pin; the judged axes and the check verdicts stand
-- on those voices until they run again. Record-class, so the banner
-- appears with the write that caused it and clears when re-measure
-- lands.
SELECT * FROM (
  SELECT sum(n) AS n,
         arrow_cast(coalesce(string_agg(actor, ', '), ''), 'Utf8') AS functions
  FROM (SELECT actor, count(*) AS n FROM GLOSSARY(all => true)
        WHERE NOT current GROUP BY actor ORDER BY actor)
) WHERE n > 0
