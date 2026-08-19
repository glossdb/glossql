-- Dimension relevance (transcribed from v0.3's
-- analysis/slicing/relevance.py):
--
--     relevance = coverage × evenness
--
-- coverage is the fraction of rows landing in a named bucket; evenness
-- is Pielou's index — Shannon entropy over its maximum for that many
-- buckets, H / ln(k). Read together: of the data this axis can speak
-- about, how much of its structure does it actually resolve? No free
-- parameters — nothing tuned, no threshold to calibrate.
--
-- The falsification that shaped it (v0.3's own record): the
-- effective-group ratio exp(H)/k was tried first and scored a 99/1
-- near-constant boolean at 0.53 — the same as a merely skewed four-way
-- split. Pielou separates them (0.08 vs 0.62) and stays scale-free in
-- k: an even 4-way axis and an even 800-way axis both score 1.0. What
-- the score deliberately does NOT do: prefer few groups to many —
-- which axis a reader wants first is business judgment, and the number
-- never overrules it.
--
-- The profile composes inline (§7e), reading the EXACT entropy scalar
-- over the full distribution — never the top_values display buckets
-- (a display cap must not silently become a statistics cap).
-- Admission gates are v0.3's recorded inventory: at least two buckets
-- with NULL counted as one (a null-coded binary is a lane, not a
-- silent constant-drop), null_ratio <= 0.5, and near-key as a FRACTION
-- ceiling of 0.9 — the absolute-count version was a recorded bug.
-- The relevance clamps at [0, 1] rather than trusting the arithmetic:
-- exactly-uniform k in {5, 13, 19, ...} floats to 1.0000000000000002.
WITH p AS (SELECT profile(v) AS pr FROM subject_column($subject)),
f AS (
  SELECT pr['total'] AS total, pr['distinct'] AS "distinct",
         pr['null_ratio'] AS null_ratio,
         pr['cardinality_ratio'] AS cardinality_ratio,
         pr['entropy'] AS entropy
  FROM p
),
g AS (
  SELECT *,
         CASE WHEN null_ratio > 0.0 THEN 1 ELSE 0 END AS null_bucket,
         CAST(total - CAST(round(CAST(total AS DOUBLE) * null_ratio) AS BIGINT) AS DOUBLE)
           / total AS coverage
  FROM f
),
-- The admission predicate, computed once — the gates below can never
-- drift apart from `applicable`.
a AS (
  SELECT *,
         (total <> 0 AND "distinct" + null_bucket >= 2 AND null_ratio <= 0.5
          AND cardinality_ratio < 0.9) AS ok
  FROM g
)
SELECT
  ok AS applicable,
  CASE WHEN total = 0 THEN 'empty column'
       WHEN "distinct" + null_bucket < 2 THEN 'constant axis: one bucket, NULL included'
       WHEN null_ratio > 0.5 THEN 'nulls dominate (ratio > 0.5)'
       WHEN cardinality_ratio >= 0.9 THEN 'near-key axis (distinct/filled >= 0.9)'
  END AS reason,
  CASE WHEN ok THEN
    CASE WHEN "distinct" < 2 THEN 0.0
         ELSE greatest(0.0, least(1.0,
                coverage * (entropy / ln(CAST("distinct" AS DOUBLE))))) END
  END AS relevance,
  CASE WHEN ok THEN coverage END AS coverage,
  CASE WHEN ok THEN
    CASE WHEN "distinct" < 2 THEN 0.0
         ELSE entropy / ln(CAST("distinct" AS DOUBLE)) END
  END AS evenness,
  CASE WHEN ok THEN "distinct" END AS groups
FROM a
