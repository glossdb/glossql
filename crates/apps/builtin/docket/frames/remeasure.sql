-- One row while any metric's axes stand on verdicts judged at an
-- earlier pin — the workspace has been written or re-imported since
-- the profilers ran. The numbers are current (the cube rebuilds at
-- every pin); the axes may not be. Record-class, so the banner appears
-- with the ruling that caused it and clears when re-measure lands.
SELECT * FROM (
  SELECT count(*) AS n,
         arrow_cast(coalesce(string_agg(metric, ', '), ''), 'Utf8') AS metrics
  FROM (SELECT metric FROM metric_axes() WHERE NOT judged_current ORDER BY metric)
) WHERE n > 0
