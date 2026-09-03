-- Temporal profile (fixture 11): the deterministic temporal family as one
-- measurement — window, cadence, completeness, gaps. Ported from v0.3's
-- analyze_basic_temporal (analysis/temporal/detection.py,
-- verified). Cadence is the nearest named grain to the median gap
-- between DISTINCT instants, so duplicate-heavy fact columns count each
-- day once; the grain windows are disjoint, so "nearest wins" flattens
-- to one CASE ladder. Completeness counts calendar buckets over the
-- column's own window — a grain's nominal seconds serve inference only,
-- never a period denominator (months vary); the bucket count computes
-- per grain and the ladder picks. Gaps are the stretches beyond twice the median; the sample
-- keeps the 20 largest and the count stays true. Two v0.3 fields stayed
-- behind: is_stale (a verdict about now is judgment, and judgment lives
-- in detectors and read policy, never in results) and
-- last_period_complete (low-signal by its own documentation).
-- A point in time bounds a window; durations and times-of-day do not
-- (v0.3's TIME_POINT_TYPES rule, by type never by name).
-- The casts land on microseconds, never nanoseconds: an open-ended
-- sentinel such as 9999-01-01 sits past the nanosecond range (which
-- ends in 2262) and the engine refuses the cast even as TRY_CAST. The
-- same range rules out date_trunc for the bucket keys — its calendar
-- grains scale every unit to nanoseconds first (datafusion-functions
-- 54.1.0 date_trunc.rs:707) — so buckets are epoch arithmetic for the
-- fixed grains and calendar extraction for the rest.
WITH bounds AS (
  SELECT count(v) AS present, min(v) AS min_v, max(v) AS max_v,
         arrow_typeof(min(v)) AS ty
  FROM subject_column($subject)
),
w AS (
  SELECT present, ty,
         (ty LIKE 'Date%' OR ty LIKE 'Timestamp%') AS temporal,
         CAST(min_v AS VARCHAR) AS min_s,
         CAST(max_v AS VARCHAR) AS max_s,
         to_unixtime(TRY_CAST(max_v AS TIMESTAMP(6)))
           - to_unixtime(TRY_CAST(min_v AS TIMESTAMP(6))) AS span_seconds
  FROM bounds
),
ordered_ts AS (
  SELECT DISTINCT TRY_CAST(v AS TIMESTAMP(6)) AS t
  FROM subject_column($subject) WHERE v IS NOT NULL
),
gap AS (
  SELECT t AS gap_start, lead(t) OVER (ORDER BY t) AS gap_end,
         CAST(to_unixtime(lead(t) OVER (ORDER BY t)) - to_unixtime(t) AS DOUBLE) AS gap_seconds
  FROM ordered_ts
),
g AS (
  SELECT median(gap_seconds) AS med, min(gap_seconds) AS lo, max(gap_seconds) AS hi
  FROM gap WHERE gap_seconds IS NOT NULL
),
-- [grain, nominal seconds, tolerance] — v0.3's table, windows disjoint.
grain AS (
  SELECT med, lo, hi,
    CASE
      WHEN med IS NULL THEN 'unknown'
      WHEN abs(med - 1.0) < 0.5 THEN 'second'
      WHEN abs(med - 60.0) < 5.0 THEN 'minute'
      WHEN abs(med - 3600.0) < 300.0 THEN 'hour'
      WHEN abs(med - 86400.0) < 3600.0 THEN 'day'
      WHEN abs(med - 604800.0) < 86400.0 THEN 'week'
      WHEN abs(med - 2592000.0) < 259200.0 THEN 'month'
      WHEN abs(med - 7776000.0) < 777600.0 THEN 'quarter'
      WHEN abs(med - 31536000.0) < 3153600.0 THEN 'year'
      ELSE 'irregular'
    END AS granularity
  FROM g
),
conf AS (
  SELECT granularity, med,
    CASE
      WHEN med IS NULL THEN 0.0
      WHEN granularity = 'irregular' THEN 0.3
      -- v0.3's exact rule: a zero min gap (sub-second neighbours floored
      -- to 0) keeps the flat default instead of the variation formula;
      -- the 0.5 cap doubles as the floor.
      WHEN lo > 0.0 AND hi > 0.0 AND med > 0.0
        THEN 1.0 - least((hi - lo) / med / 10.0, 0.5)
      ELSE 0.7
    END AS confidence
  FROM grain
),
big AS (
  -- The "twice the median" rule, spelled once for the count and the
  -- sample alike.
  SELECT gap_start, gap_end, gap_seconds, med
  FROM gap CROSS JOIN g
  WHERE med IS NOT NULL AND med > 0.0 AND gap_seconds > med * 2.0
),
sig AS (
  SELECT count(*) AS n, max(gap_seconds) / 86400.0 AS largest_days FROM big
),
largest AS (
  -- The tie-breaker is new with the port: the old ORDER BY had none, so
  -- the order among equal-length gaps (and the sampled set, when a tie
  -- crossed the cap) was the engine's accident. Earliest-first is the
  -- rule now, and the sample stops moving between captures.
  SELECT gap_start, gap_end, gap_seconds, med
  FROM big
  ORDER BY gap_seconds DESC, gap_start LIMIT 20
),
samp AS (
  -- start/end are the observations around the hole, not the hole's own
  -- endpoints.
  SELECT array_agg(named_struct(
           'start', CAST(gap_start AS VARCHAR),
           'end', CAST(gap_end AS VARCHAR),
           'days', gap_seconds / 86400.0,
           'missing_periods', CAST(gap_seconds / med AS BIGINT) - 1,
           'severity', CASE WHEN gap_seconds > med * 10.0 THEN 'severe'
                            WHEN gap_seconds > med * 5.0 THEN 'moderate'
                            ELSE 'minor' END
         ) ORDER BY gap_seconds DESC, gap_start) AS sample
  FROM largest
),
comp AS (
  SELECT
    min(t) AS lo_t, max(t) AS hi_t,
    count(DISTINCT to_unixtime(t)) AS a_second,
    count(DISTINCT floor(to_unixtime(t) / 60.0)) AS a_minute,
    count(DISTINCT floor(to_unixtime(t) / 3600.0)) AS a_hour,
    count(DISTINCT floor(to_unixtime(t) / 86400.0)) AS a_day,
    -- ISO weeks start Monday; the epoch is a Thursday, three days in.
    count(DISTINCT floor((to_unixtime(t) + 259200) / 604800.0)) AS a_week,
    count(DISTINCT extract(year FROM t) * 12 + extract(month FROM t)) AS a_month,
    count(DISTINCT extract(year FROM t) * 4 + extract(quarter FROM t)) AS a_quarter,
    count(DISTINCT extract(year FROM t)) AS a_year
  FROM ordered_ts WHERE t IS NOT NULL
),
-- Calendar grains count by calendar arithmetic; fixed grains by epoch
-- steps between truncated bounds — both inclusive.
expect AS (
  SELECT
    CASE granularity
      WHEN 'year' THEN CAST(extract(year FROM hi_t) - extract(year FROM lo_t) + 1 AS BIGINT)
      WHEN 'quarter' THEN CAST((extract(year FROM hi_t) - extract(year FROM lo_t)) * 4
          + extract(quarter FROM hi_t) - extract(quarter FROM lo_t) + 1 AS BIGINT)
      WHEN 'month' THEN CAST((extract(year FROM hi_t) - extract(year FROM lo_t)) * 12
          + extract(month FROM hi_t) - extract(month FROM lo_t) + 1 AS BIGINT)
      WHEN 'week' THEN CAST(floor((to_unixtime(hi_t) + 259200) / 604800.0)
          - floor((to_unixtime(lo_t) + 259200) / 604800.0) + 1 AS BIGINT)
      WHEN 'day' THEN CAST(floor(to_unixtime(hi_t) / 86400.0)
          - floor(to_unixtime(lo_t) / 86400.0) + 1 AS BIGINT)
      WHEN 'hour' THEN CAST(floor(to_unixtime(hi_t) / 3600.0)
          - floor(to_unixtime(lo_t) / 3600.0) + 1 AS BIGINT)
      WHEN 'minute' THEN CAST(floor(to_unixtime(hi_t) / 60.0)
          - floor(to_unixtime(lo_t) / 60.0) + 1 AS BIGINT)
      WHEN 'second' THEN CAST(to_unixtime(hi_t) - to_unixtime(lo_t) + 1 AS BIGINT)
    END AS expected,
    CASE granularity
      WHEN 'year' THEN a_year WHEN 'quarter' THEN a_quarter
      WHEN 'month' THEN a_month WHEN 'week' THEN a_week
      WHEN 'day' THEN a_day WHEN 'hour' THEN a_hour
      WHEN 'minute' THEN a_minute WHEN 'second' THEN a_second
    END AS actual
  FROM comp CROSS JOIN grain
)
SELECT
  (w.temporal AND w.present > 0) AS applicable,
  CASE
    WHEN NOT w.temporal THEN 'column type is ' || w.ty
      || ', not Date/Timestamp — a date landed as text needs typing in the recipe'
    WHEN w.present = 0 THEN 'no non-null values — nothing bounds a window'
  END AS reason,
  CASE WHEN w.temporal AND w.present > 0 THEN w.min_s END AS min,
  CASE WHEN w.temporal AND w.present > 0 THEN w.max_s END AS max,
  CASE WHEN w.temporal AND w.present > 0 THEN w.span_seconds / 86400.0 END AS span_days,
  CASE WHEN w.temporal AND w.present > 0 THEN c.granularity END AS granularity,
  CASE WHEN w.temporal AND w.present > 0 THEN c.confidence END AS confidence,
  CASE WHEN w.temporal AND w.present > 0 THEN named_struct(
    'count', sig.n, 'largest_days', sig.largest_days, 'sample', samp.sample
  ) END AS gaps,
  CASE WHEN w.temporal AND w.present > 0
        AND c.granularity NOT IN ('unknown', 'irregular') THEN named_struct(
    'expected', e.expected, 'actual', e.actual,
    'ratio', CAST(e.actual AS DOUBLE) / CAST(e.expected AS DOUBLE)
  ) END AS completeness
FROM w
CROSS JOIN conf c CROSS JOIN sig CROSS JOIN samp CROSS JOIN expect e
