-- The KPI kit: the vocabulary a workspace starts with, the detector that
-- judges the vocabulary, two measurements that profile the columns, and
-- the tests that bind them. The core ships no property; what an actor
-- must learn about these words is SKILL.md beside this file. Load it with
--
--   FROM glossql_kit('kits/kpi/kit.sql');
--
-- Every statement is glossql, so a fork is an edit.

-- Column vocabulary. role classes every column; additivity and unit are the
-- claims on measures, dimension the claim on dimensions. WHEN narrows what
-- a column owes to what its role makes meaningful: a role-less column owes
-- only role, so the backlog counts what is real instead of columns ×
-- vocabulary. `none` is the judged negative: examined, does not apply. A
-- metric's view is a table of its dataset, so its columns carry these
-- words too: how its value combines over periods and members is the
-- additivity on its value column — a flow sums, a stock keeps its last
-- level, a ratio re-divides its summed halves, the columns numerator and
-- denominator name.

DECLARE PROPERTY meaning WITH $${
  "description": "What the table, column or relationship means, in the words of the business.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"}, "term": {"type": "string"}}
}$$ AS FACT ON TABLE, COLUMN, RELATIONSHIP;

DECLARE PROPERTY role WITH $${
  "description": "What a column is in the semantic model: a key, a measure, a dimension, a timestamp or an attribute.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["key", "measure", "dimension",
                                    "timestamp", "attribute"]}}
}$$ AS FACT ON COLUMN;

DECLARE PROPERTY additivity WITH $${
  "description": "How a measure adds up over time and over the members of a dimension.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["semi-additive", "additive", "ratio", "non-additive"]},
                 "numerator": {"type": "string"},
                 "denominator": {"type": "string"},
                 "basis": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';

DECLARE PROPERTY unit WITH $${
  "description": "The unit a measure is in.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "source_column": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';

DECLARE PROPERTY dimension WITH $${
  "description": "Whether a dimension is a primary way to slice the data, a supporting one, or none.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["primary", "supporting", "none"]},
                 "basis": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'dimension';

-- Table identity: the entity every aggregate rests on.

DECLARE PROPERTY entity WITH $${
  "description": "What one row of the table is, with its grain and its time axis.",
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "string"},
                 "role": {"enum": ["fact", "dimension"]},
                 "grain": {"type": "array", "items": {"type": "string"}},
                 "time_axis": {"type": "string"},
                 "identity_columns": {"type": "array", "items": {"type": "string"}}}
}$$ AS FACT ON TABLE;

-- Source conventions: fiscal calendar, sign conventions, what an export's
-- nulls mean. Read and superseded workspace-wide.

DECLARE PROPERTY conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;

-- The detector: one query over the test's annotations, a (subject, state,
-- score) row per subject. Score is the fraction of extra distinct values
-- across a subject's annotations; the state reads it against the test's
-- threshold, bound as $threshold.

DECLARE FUNCTION value_entropy FOR GLOBAL AS $$
  WITH s AS (
    SELECT subject,
           CASE WHEN count(*) <= 1 THEN 0.0
                ELSE (count(DISTINCT body->>'value') - 1.0) / (count(*) - 1.0)
           END AS score
    FROM annotations GROUP BY subject)
  SELECT subject,
         CASE WHEN score = 0.0 THEN 'green'
              WHEN score <= coalesce($threshold, 1.0) THEN 'yellow'
              ELSE 'red' END AS state,
         score
  FROM s
$$;

-- Measurements: a function with RETURNS fills its property at extraction,
-- SELECT column_profile() FROM orders.amount; the output meets the property's
-- schema, is stored in results, and GLOSSARY() serves it beside the glosses.

DECLARE PROPERTY column_profile WITH $${
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
-- zero and the fences remain alone.

DECLARE PROPERTY outlier_profile WITH $${
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

-- The tests, last: a test names a property and a detector that are declared.
-- BY says who may annotate; value_entropy judges the enum claims.

DECLARE TEST meaning_test ON meaning BY (AGENT, HUMAN);
DECLARE TEST entity_test ON entity BY (AGENT, HUMAN);
DECLARE TEST role_test ON role BY (AGENT, HUMAN) DETECTOR value_entropy THRESHOLD 0.7;
DECLARE TEST additivity_test ON additivity BY (AGENT, HUMAN) DETECTOR value_entropy THRESHOLD 0.7;
DECLARE TEST unit_test ON unit BY (AGENT, HUMAN) DETECTOR value_entropy THRESHOLD 0.7;
DECLARE TEST dimension_test ON dimension BY (AGENT, HUMAN);
