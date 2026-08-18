-- Outliers (fixture 11): IQR fences and the modified Z-score — universal
-- distribution facts, no domain rules. The fences derive from the same
-- `profile` aggregate the profile measurement lands, composed inline
-- (§7e) — so the missing_aspects abstention this function served while
-- the profile had not landed no longer exists: the first ask computes.
-- Modified Z-score |0.6745 (x - median) / MAD| > 3.5, folded into
-- fences; MAD 0 (half the values identical) makes every deviation
-- infinite, so that check abstains to the IQR fences and counts zero.
-- TRY_CAST is identity on a typed numeric column and keeps the door
-- honest on an untyped one; it reads through the display form because
-- arrow has no Date-to-Float cast at all — the count arm must survive
-- the non-numeric columns the applicable gate then rules out, and a
-- number's display parses back to the same number.
WITH p AS (SELECT profile(v) AS pr FROM subject_column($subject)),
fences AS (
  SELECT
    pr['numeric'] IS NOT NULL AS ok,
    pr['numeric']['percentiles']['p25']
      - 1.5 * (pr['numeric']['percentiles']['p75'] - pr['numeric']['percentiles']['p25']) AS iqr_low,
    pr['numeric']['percentiles']['p75']
      + 1.5 * (pr['numeric']['percentiles']['p75'] - pr['numeric']['percentiles']['p25']) AS iqr_high,
    pr['numeric']['percentiles']['p50'] AS median,
    pr['numeric']['mad'] AS mad
  FROM p
),
counts AS (
  SELECT
    count(TRY_CAST(CAST(v AS VARCHAR) AS DOUBLE)) AS parsed,
    count(CASE WHEN TRY_CAST(CAST(v AS VARCHAR) AS DOUBLE) < f.iqr_low
                 OR TRY_CAST(CAST(v AS VARCHAR) AS DOUBLE) > f.iqr_high THEN 1 END) AS iqr_out,
    count(CASE WHEN TRY_CAST(CAST(v AS VARCHAR) AS DOUBLE) < f.median - 3.5 * f.mad / 0.6745
                 OR TRY_CAST(CAST(v AS VARCHAR) AS DOUBLE) > f.median + 3.5 * f.mad / 0.6745 THEN 1 END) AS z_raw
  FROM subject_column($subject) CROSS JOIN fences f
)
SELECT
  f.ok AS applicable,
  CASE WHEN f.ok THEN named_struct(
    'lower', f.iqr_low,
    'upper', f.iqr_high,
    'count', c.iqr_out,
    'ratio', CASE WHEN c.parsed = 0 THEN 0.0
                  ELSE CAST(c.iqr_out AS DOUBLE) / c.parsed END
  ) END AS iqr,
  CASE WHEN f.ok THEN named_struct(
    'count', CASE WHEN f.mad > 0.0 THEN c.z_raw ELSE 0 END,
    'ratio', CASE WHEN c.parsed = 0 OR f.mad <= 0.0 THEN 0.0
                  ELSE CAST(c.z_raw AS DOUBLE) / c.parsed END
  ) END AS zscore
FROM fences f CROSS JOIN counts c
