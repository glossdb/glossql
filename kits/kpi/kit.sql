-- The KPI kit: the vocabulary a workspace starts with, the detector that
-- judges the vocabulary, two measurements that profile the columns, and
-- the tests that bind them. The core ships no aspect; what an actor
-- must learn about these words is SKILL.md beside this file. Load it with
--
--   FROM glossql_kit('kits/kpi/kit.sql');
--
-- Every statement is glossql, so a fork is an edit.

-- Column vocabulary. role classes every column; behavior and unit are the
-- claims on measures, dimension the claim on dimensions. WHEN narrows what
-- a column owes to what its role makes meaningful: a role-less column owes
-- only role, so the backlog counts what is real instead of columns ×
-- vocabulary. `none` is the judged negative: examined, does not apply. A
-- metric's view is a table of its dataset, so its columns carry these
-- words too: how its value combines over periods and members is the
-- behavior on its value column — a flow sums, a stock keeps its last
-- level, a ratio re-divides its summed halves, the columns numerator and
-- denominator name.

DECLARE ASPECT meaning WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"}, "term": {"type": "string"}}
}$$ AS FACT ON TABLE, COLUMN, RELATIONSHIP;

DECLARE ASPECT role WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["key", "measure", "dimension",
                                    "timestamp", "attribute"]}}
}$$ AS FACT ON COLUMN;

DECLARE ASPECT behavior WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["stock", "flow", "ratio", "none"]},
                 "numerator": {"type": "string"},
                 "denominator": {"type": "string"},
                 "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';

DECLARE ASPECT unit WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "source_column": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';

DECLARE ASPECT dimension WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["primary", "supporting", "none"]},
                 "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'dimension';

-- Table identity: the entity verdict every aggregate stands on.

DECLARE ASPECT entity WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "role": {"enum": ["fact", "dimension"]},
                 "grain": {"type": "array", "items": {"type": "string"}},
                 "time_axis": {"type": "string"},
                 "identity_columns": {"type": "array", "items": {"type": "string"}}}
}$$ AS FACT ON TABLE;

-- Source conventions: fiscal calendar, sign conventions, what an export's
-- nulls mean. Read and superseded workspace-wide.

DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;

-- The handbook: a metric's aspect stays thin (title, x-kind), and what the
-- company revises (meaning, unit, owner, source, and a derived metric's
-- formula over its siblings) lives here, keyed by the metric's name, where a
-- correction supersedes with actor and time.

DECLARE ASPECT definitions WITH $${
  "type": "object", "properties": {"definitions": {"type": "object"}}
}$$ AS FACT ON DATASET;

-- The detector: one query over the test's slots, a (subject, band,
-- score) row per subject. Score is the fraction of extra distinct values
-- across a subject's slots; the band reads it against the test's
-- threshold, bound as $threshold.

DECLARE FUNCTION slot_entropy FOR GLOBAL AS $$
  WITH s AS (
    SELECT subject,
           CASE WHEN count(*) <= 1 THEN 0.0
                ELSE (count(DISTINCT body->>'value') - 1.0) / (count(*) - 1.0)
           END AS score
    FROM slots GROUP BY subject)
  SELECT subject,
         CASE WHEN score = 0.0 THEN 'green'
              WHEN score <= coalesce($threshold, 1.0) THEN 'yellow'
              ELSE 'red' END AS band,
         score
  FROM s
$$;

-- Measurements: a function with RETURNS fills its aspect at extraction,
-- SELECT column_profile() FROM orders.amount; the output meets the aspect's
-- schema, lands in measurements, and GLOSSARY() serves it beside the glosses.

DECLARE ASPECT column_profile WITH $${
  "type": "object", "required": ["total", "null_ratio", "distinct"],
  "properties": {"total": {"type": "integer"}, "null_ratio": {"type": "number"},
                 "distinct": {"type": "integer"}, "min": {}, "max": {}}
}$$ AS MEASUREMENT ON COLUMN;

DECLARE FUNCTION column_profile FOR GLOBAL AS $$
  SELECT count(*) AS total,
         CASE WHEN count(*) = 0 THEN 0.0 ELSE (count(*) - count(v)) / count(*)::DOUBLE END AS null_ratio,
         count(DISTINCT v) AS "distinct",
         min(v)::VARCHAR AS min, max(v)::VARCHAR AS max
  FROM subject_column($subject)
$$ RETURNS column_profile;

-- Outliers: IQR fences and the modified Z-score, distribution facts with no
-- domain rule. A column that does not parse as numbers is examined and not
-- applicable; MAD 0 makes every deviation infinite, so that check counts
-- zero and the fences stand alone.

DECLARE ASPECT outlier_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}, "iqr": {}, "zscore": {}}
}$$ AS MEASUREMENT ON COLUMN;

DECLARE FUNCTION outlier_profile FOR GLOBAL AS $$
  WITH x AS (SELECT TRY_CAST(v::VARCHAR AS DOUBLE) AS x FROM subject_column($subject)),
  f AS (
    SELECT count(x) > 0 AS ok, count(x) AS parsed,
           quantile_cont(x, 0.25) - 1.5 * (quantile_cont(x, 0.75) - quantile_cont(x, 0.25)) AS lo,
           quantile_cont(x, 0.75) + 1.5 * (quantile_cont(x, 0.75) - quantile_cont(x, 0.25)) AS hi,
           median(x) AS med, mad(x) AS mad
    FROM x),
  c AS (
    SELECT count(*) FILTER (WHERE x.x < f.lo OR x.x > f.hi) AS iqr_out,
           count(*) FILTER (WHERE f.mad > 0 AND abs(0.6745 * (x.x - f.med) / f.mad) > 3.5) AS z_out
    FROM x CROSS JOIN f)
  SELECT f.ok AS applicable,
         CASE WHEN f.ok THEN {'lower': f.lo, 'upper': f.hi, 'count': c.iqr_out,
                              'ratio': c.iqr_out / f.parsed::DOUBLE} END AS iqr,
         CASE WHEN f.ok THEN {'count': c.z_out, 'ratio': c.z_out / f.parsed::DOUBLE} END AS zscore
  FROM f CROSS JOIN c
$$ RETURNS outlier_profile;

-- The tests, last: a test names an aspect and a detector that stand.
-- BY says who may speak; slot_entropy judges the enum claims.

DECLARE TEST meaning_w ON meaning BY (AGENT, HUMAN);
DECLARE TEST entity_w ON entity BY (AGENT, HUMAN);
DECLARE TEST role_w ON role BY (AGENT, HUMAN) DETECTOR slot_entropy THRESHOLD 0.7;
DECLARE TEST behavior_w ON behavior BY (AGENT, HUMAN) DETECTOR slot_entropy THRESHOLD 0.7;
DECLARE TEST unit_w ON unit BY (AGENT, HUMAN) DETECTOR slot_entropy THRESHOLD 0.7;
DECLARE TEST dimension_w ON dimension BY (AGENT, HUMAN);
