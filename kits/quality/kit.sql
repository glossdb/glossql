-- The quality kit: what the data is like and how it holds together.
-- Loaded after the KPI kit, whose words its readers condition on. What an
-- actor must learn about these words is SKILL.md beside this file.
--
--   FROM glossql_kit('kits/kpi/kit.sql');
--   FROM glossql_kit('kits/quality/kit.sql');
--
-- The screens are table macros over dataset_cells(subject): every cell of a
-- dataset's tables, or of one table, as (table, column, ci, column_type, rn,
-- val). A screen is SQL over that relation: a join on rn puts a row's columns
-- side by side, a join on val matches values across columns, within one
-- stored type. Nothing outside SQL names a column. Each screen's measurement
-- function reads the screen, keeps what crosses its line, and lands the
-- aspect; the thresholds are the function's, the counts the screen's. A
-- key a screen did not reach is null, and the schemas say so.

--===------------------------------------------------------------------===--
-- hierarchy_candidates(table): functional dependencies between a table's
-- columns, one row per screened direction. Measures stay out by type
-- (floating and decimal); a column with a distinct value per filled row is
-- out, since it determines everything; NULL is a category. Per direction:
-- g3 is the share of rows that break from → to, g3_reverse the other way,
-- gk_lambda Goodman–Kruskal's lambda for predicting `to` from `from`,
-- rows_per_value the evidence per from-value. Fewer than two such columns,
-- or no rows, is served as one row that says why.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO hierarchy_candidates(tbl) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(tbl)),
stats AS (
  SELECT ci, "column", count(*) AS total, count(val) AS filled, count(DISTINCT val) AS distinct_vals,
         count(DISTINCT val) + (count(*) > count(val))::INTEGER AS groups
  FROM cells
  WHERE column_type NOT IN ('DOUBLE', 'FLOAT', 'REAL') AND column_type NOT LIKE 'DECIMAL%'
  GROUP BY ci, "column"),
pool AS (SELECT * FROM stats WHERE groups >= 2 AND NOT (filled > 0 AND distinct_vals = filled)),
n AS (SELECT max(total) AS rows FROM stats),
reason AS (
  SELECT CASE WHEN (SELECT count(*) FROM cells) = 0 THEN 'empty table'
              WHEN (SELECT count(*) FROM pool) < 2 THEN 'fewer than two dimension-like columns' END AS why),
pairs AS (
  SELECT a.ci AS fi, b.ci AS ti, a.val AS fv, b.val AS tv, count(*) AS c
  FROM cells a JOIN cells b ON a.rn = b.rn AND a.ci <> b.ci
  WHERE a.ci IN (SELECT ci FROM pool) AND b.ci IN (SELECT ci FROM pool)
  GROUP BY ALL),
per_from AS (SELECT fi, ti, fv, max(c) AS mx, count(*) AS ng FROM pairs GROUP BY ALL),
per_to AS (SELECT fi, ti, tv, sum(c) AS n_t FROM pairs GROUP BY ALL),
dir AS (
  SELECT f.fi, f.ti, sum(f.mx) AS agree, sum(f.ng) AS pair_groups, t.modal
  FROM per_from f
  JOIN (SELECT fi, ti, max(n_t) AS modal FROM per_to GROUP BY fi, ti) t USING (fi, ti)
  GROUP BY f.fi, f.ti, t.modal),
served AS (
  SELECT d.fi, d.ti, pf."column" AS from_col, pt."column" AS to_col,
         pf.groups AS distinct_from, pt.groups AS distinct_to, d.pair_groups,
         (n.rows - d.agree) / n.rows::DOUBLE AS g3,
         (n.rows - r.agree) / n.rows::DOUBLE AS g3_reverse,
         CASE WHEN n.rows = d.modal THEN 0.0 ELSE (d.agree - d.modal) / (n.rows - d.modal)::DOUBLE END AS gk_lambda,
         n.rows / pf.groups::DOUBLE AS rows_per_value, n.rows
  FROM dir d JOIN dir r ON r.fi = d.ti AND r.ti = d.fi
  JOIN pool pf ON pf.ci = d.fi JOIN pool pt ON pt.ci = d.ti, n)
SELECT true AS applicable, NULL::VARCHAR AS reason, rows,
       row_number() OVER (ORDER BY least(fi, ti), greatest(fi, ti), CASE WHEN fi < ti THEN 0 ELSE 1 END) AS seq,
       from_col, to_col, distinct_from, distinct_to, pair_groups, g3, g3_reverse, gk_lambda, rows_per_value
FROM served WHERE (SELECT why FROM reason) IS NULL
UNION ALL
SELECT false, why, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM reason WHERE why IS NOT NULL
ORDER BY seq;

--===------------------------------------------------------------------===--
-- relationship_candidates(dataset): which column of one table keys into
-- which column of another, by value. Two columns are compared within one
-- stored type. A target is key-like when nearly every filled row is distinct
-- (nine in ten) and it has two values or more; a pair is admitted when half
-- the source's distinct values or more appear in the target. A single
-- candidate is an admitted pair whose target is key-like. Where a table
-- pair has no such key, a second column pair on the same two tables may
-- scope the first: the composite rescue, served with key_columns. One all-
-- NULL row where nothing is found.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO relationship_candidates(ds) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(ds)),
cols AS (
  SELECT "table", "column", ci, column_type, count(*) AS rows, count(val) AS filled, count(DISTINCT val) AS distinct_vals
  FROM cells GROUP BY ALL),
vals AS (SELECT DISTINCT ci, column_type, val FROM cells WHERE val IS NOT NULL),
m AS (SELECT a.ci AS fi, b.ci AS ki, count(*) AS m FROM vals a JOIN vals b ON a.val = b.val AND a.column_type = b.column_type GROUP BY ALL),
cs AS (
  SELECT c.*, (filled > 0 AND distinct_vals >= 2 AND distinct_vals >= 0.9 * filled) AS key_like,
         (distinct_vals = filled) AS uniq, (column_type = 'DATE' OR column_type LIKE 'TIMESTAMP%') AS temporal
  FROM cols c),
admitted AS (
  SELECT f.ci AS fi, k.ci AS ki, f."table" AS ft, f."column" AS fc, k."table" AS kt, k."column" AS kc,
         f.distinct_vals AS df, k.distinct_vals AS dk, k.filled AS kfilled, k.rows AS krows,
         coalesce(m.m, 0) AS matched, coalesce(m.m, 0) / f.distinct_vals::DOUBLE AS overlap,
         k.key_like AS k_key, f.uniq AS f_uniq, k.uniq AS k_uniq, k.temporal AS k_temporal
  FROM cs f JOIN cs k ON (f."table" <> k."table" OR f."column" <> k."column") AND f.column_type = k.column_type
  LEFT JOIN m ON m.fi = f.ci AND m.ki = k.ci
  WHERE k.distinct_vals >= 2 AND f.distinct_vals > 0 AND coalesce(m.m, 0) / f.distinct_vals::DOUBLE >= 0.5),
single AS (SELECT * FROM admitted WHERE k_key),
keyed_tables AS (SELECT DISTINCT least(ft, kt) AS t1, greatest(ft, kt) AS t2 FROM single),
-- the rescue: an admitted pair with no key-like target, on a table pair no key joins
anchors AS (
  SELECT a.* FROM admitted a
  WHERE NOT k_key AND NOT EXISTS (SELECT 1 FROM keyed_tables k WHERE k.t1 = least(a.ft, a.kt) AND k.t2 = greatest(a.ft, a.kt))),
scoped AS (
  SELECT p.fi AS pf, p.ki AS pk, s.fi AS sf, s.ki AS sk, p.ft, p.kt, p.fc AS pfc, p.kc AS pkc, s.fc AS sfc, s.kc AS skc,
         p.dk AS pdk, s.dk AS sdk, s.overlap AS s_overlap, p.k_temporal, s.k_temporal AS s_temporal
  FROM anchors p JOIN admitted s ON s.ft = p.ft AND s.kt = p.kt AND s.fc <> p.fc AND s.kc <> p.kc AND NOT s.k_uniq
  WHERE p.dk * s.dk >= 0.9 * greatest(0, p.kfilled + s.kfilled - p.krows)),
-- a row's two columns side by side, for every ordered column pair of a table
duo AS (
  SELECT a."table", a.ci AS c0, b.ci AS c1, a.rn, a.val AS v0, b.val AS v1
  FROM cells a JOIN cells b ON a."table" = b."table" AND a.rn = b.rn AND a.ci <> b.ci),
duo_stats AS (
  SELECT "table", c0, c1, count(*) FILTER (WHERE v0 IS NOT NULL AND v1 IS NOT NULL) AS filled2,
         count(DISTINCT (v0, v1)) FILTER (WHERE v0 IS NOT NULL AND v1 IS NOT NULL) AS distinct2
  FROM duo GROUP BY ALL),
attempts AS (
  SELECT s.*, fd.filled2 AS from_filled, fd.distinct2 AS df, td.filled2 AS to_filled, td.distinct2 AS dt
  FROM scoped s
  JOIN duo_stats fd ON fd."table" = s.ft AND fd.c0 = s.pf AND fd.c1 = s.sf
  JOIN duo_stats td ON td."table" = s.kt AND td.c0 = s.pk AND td.c1 = s.sk
  WHERE td.filled2 > 0 AND s.pdk * s.sdk >= 0.9 * td.filled2
    AND td.distinct2 >= 2 AND td.distinct2 >= 0.9 * td.filled2
    AND fd.distinct2 > 0 AND td.distinct2 >= 0.5 * fd.distinct2 AND td.distinct2 > s.sdk),
crossed AS (
  SELECT a.pf, a.pk, a.sf, a.sk, count(*) AS matched
  FROM attempts a
  JOIN (SELECT DISTINCT "table", c0, c1, v0, v1 FROM duo WHERE v0 IS NOT NULL AND v1 IS NOT NULL) f
    ON f."table" = a.ft AND f.c0 = a.pf AND f.c1 = a.sf
  JOIN (SELECT DISTINCT "table", c0, c1, v0, v1 FROM duo WHERE v0 IS NOT NULL AND v1 IS NOT NULL) t
    ON t."table" = a.kt AND t.c0 = a.pk AND t.c1 = a.sk AND t.v0 = f.v0 AND t.v1 = f.v1
  GROUP BY ALL),
rescues AS (
  SELECT a.*, c.matched, c.matched / a.df::DOUBLE AS overlap,
         row_number() OVER (PARTITION BY a.pf, a.pk ORDER BY a.s_overlap DESC, a.sf, a.sk) AS pick
  FROM attempts a JOIN crossed c USING (pf, pk, sf, sk)
  WHERE c.matched / a.df::DOUBLE >= 0.5),
rescued AS (
  SELECT CASE WHEN pdk >= sdk THEN ft || '.' || pfc ELSE ft || '.' || sfc END AS from_col,
         CASE WHEN pdk >= sdk THEN kt || '.' || pkc ELSE kt || '.' || skc END AS to_col,
         CASE WHEN df = from_filled THEN 'one-to-one' ELSE 'many-to-one' END AS cardinality,
         overlap, matched, df - matched AS orphans, df AS from_distinct, dt AS to_distinct,
         dt = to_filled AS to_unique, CASE WHEN pdk >= sdk THEN k_temporal ELSE s_temporal END AS to_temporal,
         CASE WHEN pdk >= sdk THEN ft || '.' || sfc ELSE ft || '.' || pfc END AS kc_from,
         CASE WHEN pdk >= sdk THEN kt || '.' || skc ELSE kt || '.' || pkc END AS kc_to,
         least(pf, sf) AS o1, greatest(pf, sf) AS o2
  FROM rescues WHERE pick = 1),
found AS (
  SELECT ft || '.' || fc AS from_col, kt || '.' || kc AS to_col,
         CASE WHEN f_uniq THEN 'one-to-one' ELSE 'many-to-one' END AS cardinality,
         overlap, matched, df - matched AS orphans, df AS from_distinct, dk AS to_distinct,
         k_uniq AS to_unique, k_temporal AS to_temporal, NULL::VARCHAR AS kc_from, NULL::VARCHAR AS kc_to,
         0 AS phase, ki AS o1, fi AS o2
  FROM single
  UNION ALL
  SELECT DISTINCT from_col, to_col, cardinality, overlap, matched, orphans, from_distinct, to_distinct,
         to_unique, to_temporal, kc_from, kc_to, 1, o1, o2
  FROM rescued)
SELECT row_number() OVER (ORDER BY phase, o1, o2, from_col, to_col) AS seq,
       from_col, to_col, cardinality, overlap, matched, orphans, from_distinct, to_distinct, to_unique, to_temporal, kc_from, kc_to
FROM found
UNION ALL
SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM found)
ORDER BY seq;

--===------------------------------------------------------------------===--
-- relationship_checks(dataset): whether the declared relationships hold.
-- Per edge: filled rows, the orphans (child values no parent row carries;
-- inside one table, child values that reach more than one parent), and
-- for an edge across two tables the temporal pairs: the first three date
-- columns of each side, joined on the edge, how often the child's date
-- precedes the parent's. Legs are one column or a pair; an endpoint is
-- spelled as the record stores it, dataset.table.column. No declared
-- relationships is one row that says so.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO relationship_checks(ds) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(ds)),
edges AS (
  SELECT row_number() OVER (ORDER BY seq) - 1 AS seq,
         left_side || ' ' || op || ' ' || right_side AS relationship,
         split_part(left_side, '.', 2) AS ct, split_part(right_side, '.', 2) AS pt,
         CASE WHEN left_side LIKE '%(%' THEN string_split(regexp_extract(left_side, '\((.*)\)', 1), ', ')
              ELSE [substr(left_side, length(ds) + length(split_part(left_side, '.', 2)) + 3)] END AS cl,
         CASE WHEN right_side LIKE '%(%' THEN string_split(regexp_extract(right_side, '\((.*)\)', 1), ', ')
              ELSE [substr(right_side, length(ds) + length(split_part(right_side, '.', 2)) + 3)] END AS pl
  FROM glossql.relationships WHERE dataset = ds),
legs AS (SELECT * FROM edges WHERE len(cl) = len(pl) AND len(cl) BETWEEN 1 AND 2),
-- each side's rows pivoted onto the edge's legs
child AS (
  SELECT e.seq, c.rn,
         max(CASE WHEN c."column" = e.cl[1] THEN c.val END) AS k1,
         max(CASE WHEN len(e.cl) = 2 AND c."column" = e.cl[2] THEN c.val END) AS k2
  FROM legs e JOIN cells c ON c."table" = e.ct
  GROUP BY e.seq, c.rn),
parent AS (
  SELECT e.seq, c.rn,
         max(CASE WHEN c."column" = e.pl[1] THEN c.val END) AS k1,
         max(CASE WHEN len(e.pl) = 2 AND c."column" = e.pl[2] THEN c.val END) AS k2
  FROM legs e JOIN cells c ON c."table" = e.pt
  GROUP BY e.seq, c.rn),
cfull AS (SELECT c.* FROM child c JOIN legs e USING (seq) WHERE k1 IS NOT NULL AND (len(e.cl) = 1 OR k2 IS NOT NULL)),
pfull AS (SELECT p.* FROM parent p JOIN legs e USING (seq) WHERE k1 IS NOT NULL AND (len(e.pl) = 1 OR k2 IS NOT NULL)),
filled AS (SELECT seq, count(*) AS filled FROM cfull GROUP BY seq),
nested AS (
  SELECT e.seq, count(DISTINCT (p.k1, p.k2)) <> count(*) AS is_nest
  FROM legs e JOIN pfull p USING (seq) WHERE e.ct = e.pt GROUP BY e.seq),
nest_orphans AS (
  SELECT seq, coalesce(sum(n) FILTER (WHERE k > 1), 0) AS orphans
  FROM (SELECT seq, k1, k2, count(DISTINCT (p1, p2)) AS k, count(*) AS n
        FROM (SELECT c.seq, c.k1, c.k2, p.k1 AS p1, p.k2 AS p2 FROM cfull c JOIN pfull p ON p.seq = c.seq AND p.rn = c.rn)
        GROUP BY seq, k1, k2)
  GROUP BY seq),
join_orphans AS (
  SELECT c.seq, count(*) AS orphans
  FROM cfull c
  WHERE NOT EXISTS (SELECT 1 FROM pfull p WHERE p.seq = c.seq AND p.k1 = c.k1 AND p.k2 IS NOT DISTINCT FROM c.k2)
  GROUP BY c.seq),
per_edge AS (
  SELECT e.seq, e.relationship, coalesce(f.filled, 0) AS filled,
         CASE WHEN coalesce(n.is_nest, false) THEN coalesce(no.orphans, 0) ELSE coalesce(jo.orphans, 0) END AS orphans,
         e.ct, e.pt
  FROM legs e LEFT JOIN filled f USING (seq) LEFT JOIN nested n USING (seq)
  LEFT JOIN nest_orphans no USING (seq) LEFT JOIN join_orphans jo USING (seq)),
dates AS (
  SELECT "table", "column", ci, row_number() OVER (PARTITION BY "table" ORDER BY ci) AS nth
  FROM (SELECT DISTINCT "table", "column", ci FROM cells WHERE column_type = 'DATE' OR column_type LIKE 'TIMESTAMP%')),
date_pairs AS (
  SELECT e.seq, cd."column" AS child_column, pd."column" AS parent_column,
         row_number() OVER (PARTITION BY e.seq ORDER BY cd.nth, pd.nth) AS pair_seq, cd.ci AS cci, pd.ci AS pci
  FROM legs e JOIN dates cd ON cd."table" = e.ct AND cd.nth <= 3 JOIN dates pd ON pd."table" = e.pt AND pd.nth <= 3
  WHERE e.ct <> e.pt),
temporal AS (
  SELECT dp.seq, dp.pair_seq, dp.child_column, dp.parent_column,
         count(*) FILTER (WHERE cdv.val IS NOT NULL AND pdv.val IS NOT NULL) AS joined,
         count(*) FILTER (WHERE cdv.val::TIMESTAMP < pdv.val::TIMESTAMP) AS precedes
  FROM date_pairs dp
  JOIN cfull c ON c.seq = dp.seq
  JOIN pfull p ON p.seq = c.seq AND p.k1 = c.k1 AND p.k2 IS NOT DISTINCT FROM c.k2
  JOIN legs e ON e.seq = dp.seq
  JOIN cells cdv ON cdv."table" = e.ct AND cdv.rn = c.rn AND cdv.ci = dp.cci
  JOIN cells pdv ON pdv."table" = e.pt AND pdv.rn = p.rn AND pdv.ci = dp.pci
  GROUP BY ALL HAVING count(*) FILTER (WHERE cdv.val IS NOT NULL AND pdv.val IS NOT NULL) > 0)
SELECT true AS applicable, NULL::VARCHAR AS reason, pe.seq, pe.relationship, pe.filled, pe.orphans,
       CASE WHEN pe.filled = 0 THEN 0.0 ELSE pe.orphans / pe.filled::DOUBLE END AS orphan_rate,
       t.pair_seq, t.child_column, t.parent_column, t.joined, t.precedes,
       CASE WHEN t.joined IS NULL THEN NULL ELSE t.precedes / t.joined::DOUBLE END AS precedes_rate
FROM per_edge pe LEFT JOIN temporal t USING (seq)
UNION ALL
SELECT false, 'no declared relationships', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM legs)
ORDER BY seq, pair_seq;

--===------------------------------------------------------------------===--
-- derivation_candidates(table): which numeric column is another two
-- combined, as a product or a sum. Numeric columns in table order, twelve at
-- most; a target and two operands each filled on twenty rows or more; the
-- form admitted when the magnitudes fit within a factor of thirty. A row
-- supports a triple when all three are filled and violates it when the
-- target misses the form by more than 0.011 or a millionth of itself.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO derivation_candidates(tbl) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(tbl)),
numeric AS (
  SELECT ci, "column", count(val) AS filled, avg(abs(TRY_CAST(val AS DOUBLE))) AS mag, count(*) AS total,
         row_number() OVER (ORDER BY ci) AS nth
  FROM cells
  WHERE column_type IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER',
                        'UBIGINT', 'UHUGEINT', 'FLOAT', 'DOUBLE', 'REAL')
     OR column_type LIKE 'DECIMAL%'
  GROUP BY ci, "column"),
capped AS (SELECT * FROM numeric WHERE nth <= 12),
facts AS (SELECT count(*) AS numeric_columns, bool_or(nth > 12) AS truncated, max(filled) AS rows FROM numeric),
triples AS (
  SELECT a.ci AS ai, a."column" AS target, b.ci AS bi, b."column" AS operand_1, c.ci AS ci2, c."column" AS operand_2, form
  FROM capped a, capped b, capped c, (SELECT unnest(['product', 'sum']) AS form)
  WHERE b.nth < c.nth AND a.ci <> b.ci AND a.ci <> c.ci AND a.filled >= 20 AND b.filled >= 20 AND c.filled >= 20
    AND CASE form
          WHEN 'product' THEN coalesce(a.mag, 0) > 0 AND coalesce(b.mag, 0) > 0 AND coalesce(c.mag, 0) > 0
                             AND b.mag * c.mag < 30 * a.mag AND a.mag < 30 * b.mag * c.mag
          ELSE coalesce(b.mag, 0) + coalesce(c.mag, 0) > 0 AND coalesce(a.mag, 0) > 0
               AND b.mag + c.mag < 30 * a.mag AND a.mag < 30 * (b.mag + c.mag) END),
rows3 AS (
  SELECT t.ai, t.bi, t.ci2, t.form, t.target, t.operand_1, t.operand_2,
         TRY_CAST(a.val AS DOUBLE) AS av, TRY_CAST(b.val AS DOUBLE) AS bv, TRY_CAST(c.val AS DOUBLE) AS cv
  FROM triples t
  JOIN cells a ON a.ci = t.ai JOIN cells b ON b.ci = t.bi AND b.rn = a.rn JOIN cells c ON c.ci = t.ci2 AND c.rn = a.rn),
measured AS (
  SELECT target, form, operand_1, operand_2, ai, bi, ci2,
         count(*) FILTER (WHERE av IS NOT NULL AND bv IS NOT NULL AND cv IS NOT NULL) AS support,
         count(*) FILTER (WHERE av IS NOT NULL AND bv IS NOT NULL AND cv IS NOT NULL
                          AND abs(av - CASE form WHEN 'product' THEN bv * cv ELSE bv + cv END) > greatest(0.011, abs(av) * 0.000001)) AS violations
  FROM rows3 GROUP BY ALL)
SELECT true AS applicable, NULL::VARCHAR AS reason, f.rows, f.truncated,
       row_number() OVER (ORDER BY ai, bi, ci2, CASE form WHEN 'product' THEN 0 ELSE 1 END) AS seq,
       target, form, operand_1, operand_2, support, violations,
       CASE WHEN support = 0 THEN NULL ELSE 1 - violations / support::DOUBLE END AS match_rate
FROM measured, facts f WHERE f.numeric_columns >= 3
UNION ALL
SELECT true, NULL, f.rows, f.truncated, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM facts f WHERE f.numeric_columns >= 3 AND NOT EXISTS (SELECT 1 FROM measured)
UNION ALL
SELECT false, 'fewer than three numeric columns', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM facts f WHERE f.numeric_columns < 3
ORDER BY seq;

--===------------------------------------------------------------------===--
-- grounding_collisions(dataset): metrics that are the same thing twice.
-- Two kinds: groundings whose SQL is the same statement once parsed, case
-- and spacing aside, under different aspects; and groundings whose monthly
-- series agree over every month though their SQL differs. The series pass
-- reads each grounding's view by the claims on its columns, as a metrics
-- kit does: the period is the column glossed role timestamp (one, or the
-- one named period), the value the column glossed role measure (one, or
-- the one named value); without the claims, the columns named period and
-- value, the first date column standing in for the period. The behavior
-- glossed on the value column folds the month: a flow summed, a stock the
-- month's last level, a ratio its summed halves re-divided, the columns
-- its numerator and denominator name; none claimed reads as flow. A
-- collision is two groundings of one flavor, the aspect's x-kind: a
-- grounding flavored otherwise stands beside a metric and is never its
-- twin. No groundings is one row with groundings 0.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO grounding_collisions(ds) AS TABLE
WITH slots AS (
  SELECT g.aspect, g.subject, g.body, coalesce(json_extract_string(a.schema, '$."x-kind"'), '') AS flavor,
         row_number() OVER (PARTITION BY g.aspect, g.subject ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.seq DESC) AS rk
  FROM glossql.glossary g JOIN glossql.aspects a ON a.name = g.aspect
  WHERE g.dataset = ds AND a.kind = 'QUERY' AND json_extract_string(g.body, '$.sql') IS NOT NULL),
groundings AS (SELECT aspect, subject, flavor, json_extract_string(body, '$.sql') AS sql FROM slots WHERE rk = 1),
canon AS (
  SELECT aspect, subject, flavor, sql,
         lower(coalesce(regexp_replace(json_serialize_sql(sql)::VARCHAR, '"query_location(_length)?":\d+,?', '', 'g'),
                        regexp_replace(trim(sql), '\s+', ' ', 'g'))) AS canon
  FROM groundings),
sql_hits AS (
  SELECT canon, list(DISTINCT aspect ORDER BY aspect) AS aspects, list(DISTINCT subject ORDER BY subject) AS subjects,
         min(sql) AS sql, count(DISTINCT aspect) AS n
  FROM canon GROUP BY canon, flavor HAVING count(DISTINCT aspect) >= 2),
vcells AS (SELECT * FROM dataset_cells(ds, 'views') WHERE "table" IN (SELECT aspect FROM groundings)),
vcols AS (SELECT DISTINCT "table", "column", ci, column_type FROM vcells),
claims AS (
  SELECT split_part(subject, '.', 2) AS aspect, split_part(subject, '.', 3) AS col, g.aspect AS claim, value
  FROM glossql_glossary(ds, NULL, false) g
  WHERE g.aspect IN ('role', 'behavior') AND state IN ('current', 'stale') AND value IS NOT NULL
    AND len(string_split(subject, '.')) = 3 AND split_part(subject, '.', 2) IN (SELECT aspect FROM groundings)),
roles AS (
  SELECT aspect, value->>'value' AS role,
         list(col ORDER BY CASE WHEN col IN ('period', 'value') THEN 0 ELSE 1 END, col) AS cols
  FROM claims WHERE claim = 'role' GROUP BY aspect, value->>'value'),
chosen AS (
  SELECT g.aspect,
         coalesce(CASE WHEN len(p.cols) = 1 OR p.cols[1] = 'period' THEN p.cols[1] END,
                  (SELECT c."column" FROM vcols c WHERE c."table" = g.aspect AND c."column" = 'period'),
                  (SELECT c."column" FROM vcols c WHERE c."table" = g.aspect AND (c.column_type = 'DATE' OR c.column_type LIKE 'TIMESTAMP%')
                   ORDER BY c.ci LIMIT 1)) AS period_column,
         coalesce(CASE WHEN len(m.cols) = 1 OR m.cols[1] = 'value' THEN m.cols[1] END,
                  (SELECT c."column" FROM vcols c WHERE c."table" = g.aspect AND c."column" = 'value')) AS value_column
  FROM groundings g
  LEFT JOIN roles p ON p.aspect = g.aspect AND p.role = 'timestamp'
  LEFT JOIN roles m ON m.aspect = g.aspect AND m.role = 'measure'),
folds AS (
  SELECT c.aspect, c.period_column, c.value_column,
         coalesce(b.value->>'value', 'flow') AS verb, b.value->>'numerator' AS num, b.value->>'denominator' AS den
  FROM chosen c LEFT JOIN claims b ON b.claim = 'behavior' AND b.aspect = c.aspect AND b.col = c.value_column
  WHERE c.period_column IS NOT NULL AND c.value_column IS NOT NULL),
series AS (
  SELECT f.aspect, date_trunc('month', TRY_CAST(p.val AS TIMESTAMP)) AS month,
         CASE f.verb
           WHEN 'stock' THEN arg_max(TRY_CAST(v.val AS DOUBLE), TRY_CAST(p.val AS TIMESTAMP)) FILTER (WHERE v."column" = f.value_column)
           WHEN 'ratio' THEN sum(TRY_CAST(v.val AS DOUBLE)) FILTER (WHERE v."column" = f.num)
                             / sum(TRY_CAST(v.val AS DOUBLE)) FILTER (WHERE v."column" = f.den)
           ELSE sum(TRY_CAST(v.val AS DOUBLE)) FILTER (WHERE v."column" = f.value_column) END AS value,
         bool_or(v.val IS NULL) FILTER (WHERE v."column" = f.value_column) AS has_null
  FROM folds f
  JOIN vcells p ON p."table" = f.aspect AND p."column" = f.period_column AND TRY_CAST(p.val AS TIMESTAMP) IS NOT NULL
  JOIN vcells v ON v."table" = f.aspect AND v.rn = p.rn AND v."column" IN (f.value_column, f.num, f.den)
  GROUP BY f.aspect, date_trunc('month', TRY_CAST(p.val AS TIMESTAMP)), f.verb, f.value_column, f.num, f.den),
prints AS (
  SELECT aspect, string_agg(strftime(month, '%Y-%m') || '=' || round(value, 2)::VARCHAR, ';' ORDER BY month) AS fingerprint,
         count(*) AS months
  FROM series GROUP BY aspect HAVING NOT bool_or(coalesce(has_null, false) OR value IS NULL)),
series_hits AS (
  SELECT fingerprint, list(DISTINCT p.aspect ORDER BY p.aspect) AS aspects,
         list(DISTINCT c.subject ORDER BY c.subject) AS subjects, max(months) AS months
  FROM prints p JOIN canon c USING (aspect)
  GROUP BY fingerprint, c.flavor HAVING count(DISTINCT p.aspect) >= 2 AND count(DISTINCT c.canon) >= 2),
hits AS (
  SELECT 0 AS phase, 'sql' AS kind, sql, NULL::BIGINT AS months, aspects, subjects FROM sql_hits
  UNION ALL
  SELECT 1, 'served_series', '', months, aspects, subjects FROM series_hits)
SELECT (SELECT count(*) FROM groundings) AS groundings,
       row_number() OVER (ORDER BY phase, aspects[1]) AS seq, kind, sql, months, aspects, subjects
FROM hits
UNION ALL
SELECT (SELECT count(*) FROM groundings), NULL, NULL, NULL, NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM hits)
ORDER BY seq;

--===------------------------------------------------------------------===--
-- The aspects each screen lands, and the functions that land them. A
-- function reads its screen, keeps what crosses its line, and shapes the
-- value the aspect's schema requires. Each is FOR GLOBAL: any dataset.
--===------------------------------------------------------------------===--

DECLARE ASPECT hierarchy_candidates WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}, "rows": {"type": ["integer", "null"]},
                 "candidates": {"type": ["array", "null"], "items": {"type": "object",
                   "properties": {"from": {"type": "string"}, "to": {"type": "string"},
                                  "g3": {"type": "number"}, "lambda": {"type": "number"},
                                  "kind": {"type": "string", "enum": ["edge", "alias"]}}}}}
}$$ AS MEASUREMENT ON TABLE;

DECLARE FUNCTION detect_hierarchies FOR GLOBAL AS $$
  SELECT bool_and(applicable) AS applicable, min(reason) AS reason,
         CASE WHEN bool_and(applicable) THEN max(rows) END AS rows,
         CASE WHEN bool_and(applicable) THEN coalesce(list({
           'from': from_col, 'to': to_col, 'distinct_from': distinct_from, 'distinct_to': distinct_to,
           'pair_groups': pair_groups, 'g3': g3, 'lambda': gk_lambda, 'rows_per_value': rows_per_value,
           'kind': CASE WHEN g3 <= 0.01 AND g3_reverse <= 0.01 THEN 'alias' ELSE 'edge' END
         } ORDER BY seq) FILTER (WHERE g3 <= 0.05), []) END AS candidates
  FROM hierarchy_candidates($subject)
$$ RETURNS hierarchy_candidates;

DECLARE ASPECT relationship_candidates WITH $${
  "type": "object",
  "properties": {"candidates": {"type": "array", "items": {"type": "object",
                   "properties": {"from": {"type": "string"}, "to": {"type": "string"},
                                  "cardinality": {"type": "string"}, "overlap": {"type": "number"}}}},
                 "summary": {"type": "object"}}
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION detect_relationships FOR GLOBAL AS $$
  WITH c AS (
    SELECT coalesce(list({
      'from': from_col, 'to': to_col, 'cardinality': cardinality, 'overlap': overlap, 'matched': matched,
      'orphans': orphans, 'from_distinct': from_distinct, 'to_distinct': to_distinct,
      'to_unique': to_unique, 'to_temporal': to_temporal,
      'key_columns': CASE WHEN kc_from IS NOT NULL THEN [{'from': kc_from, 'to': kc_to}] END
    } ORDER BY CASE WHEN to_unique AND NOT to_temporal THEN 0 ELSE 1 END,
               CASE cardinality WHEN 'many-to-one' THEN 0 ELSE 1 END,
               overlap DESC, matched / to_distinct::DOUBLE DESC, seq)
      FILTER (WHERE from_col IS NOT NULL), []) AS candidates
    FROM relationship_candidates($subject))
  SELECT {
    'candidates': candidates,
    'summary': {
      'candidates': len(candidates),
      'top': list_slice(candidates, 1, 16),
      'note': 'top 16: clean keys first (unique target, not a date), then many-to-one, then overlap, then key coverage — every candidate reads back via GLOSSARY(dataset::relationship_candidates)'
    }} AS result
  FROM c
$$ RETURNS relationship_candidates;

DECLARE ASPECT relationship_coherence WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "relationships": {"type": ["array", "null"], "items": {"type": "object",
                   "properties": {"relationship": {"type": "string"}, "filled": {"type": "integer"},
                                  "orphans": {"type": "integer"}, "orphan_rate": {"type": "number"},
                                  "temporal": {"type": "array", "items": {"type": "object",
                                    "properties": {"child_column": {"type": "string"}, "parent_column": {"type": "string"},
                                                   "joined": {"type": "integer"}, "precedes": {"type": "integer"},
                                                   "precedes_rate": {"type": "number"}}}}}}}}
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION relationship_coherence FOR GLOBAL AS $$
  WITH r AS (
    SELECT applicable, reason, seq, relationship, filled, orphans, orphan_rate,
           coalesce(list({'child_column': child_column, 'parent_column': parent_column, 'joined': joined,
                          'precedes': precedes, 'precedes_rate': precedes_rate} ORDER BY pair_seq)
                    FILTER (WHERE child_column IS NOT NULL), []) AS temporal
    FROM relationship_checks($subject)
    GROUP BY applicable, reason, seq, relationship, filled, orphans, orphan_rate)
  SELECT bool_and(applicable) AS applicable, min(reason) AS reason,
         CASE WHEN bool_and(applicable) THEN coalesce(list({
           'relationship': relationship, 'filled': filled, 'orphans': orphans, 'orphan_rate': orphan_rate, 'temporal': temporal
         } ORDER BY seq) FILTER (WHERE relationship IS NOT NULL), []) END AS relationships
  FROM r
$$ RETURNS relationship_coherence;

DECLARE ASPECT derivation_candidates WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}, "rows": {"type": ["integer", "null"]}, "truncated": {"type": ["boolean", "null"]},
                 "candidates": {"type": ["array", "null"], "items": {"type": "object",
                   "properties": {"target": {"type": "string"}, "form": {"type": "string", "enum": ["product", "sum"]},
                                  "operands": {"type": "array", "items": {"type": "string"}},
                                  "support": {"type": "integer"}, "violations": {"type": "integer"},
                                  "match_rate": {"type": "number"}}}}}
}$$ AS MEASUREMENT ON TABLE;

DECLARE FUNCTION detect_derivations FOR GLOBAL AS $$
  SELECT bool_and(applicable) AS applicable, min(reason) AS reason,
         CASE WHEN bool_and(applicable) THEN max(rows) END AS rows,
         CASE WHEN bool_and(applicable) THEN coalesce(list({
           'target': target, 'form': form, 'operands': [operand_1, operand_2],
           'support': support, 'violations': violations, 'match_rate': match_rate
         } ORDER BY match_rate DESC, seq) FILTER (WHERE match_rate >= 0.95 AND support >= 20), []) END AS candidates,
         CASE WHEN bool_and(applicable) THEN bool_or(truncated) END AS truncated
  FROM derivation_candidates($subject)
$$ RETURNS derivation_candidates;

DECLARE ASPECT grounding_collisions WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}, "groundings": {"type": "integer"},
                 "collisions": {"type": "array", "items": {"type": "object",
                   "properties": {"sql": {"type": "string"},
                                  "aspects": {"type": "array", "items": {"type": "string"}},
                                  "subjects": {"type": "array", "items": {"type": "string"}}}}}}
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION detect_grounding_collisions FOR GLOBAL AS $$
  SELECT max(groundings) > 0 AS applicable, max(groundings) AS groundings,
         coalesce(list({'kind': kind, 'sql': sql, 'months': months, 'aspects': aspects, 'subjects': subjects}
                       ORDER BY seq) FILTER (WHERE kind IS NOT NULL), []) AS collisions
  FROM grounding_collisions($subject)
$$ RETURNS grounding_collisions;

--===------------------------------------------------------------------===--
-- The temporal profile: a date column's window, cadence, completeness and
-- gaps, as a reader that windows a series needs them. Cadence is the nearest
-- named grain to the median gap between distinct instants; completeness
-- counts calendar buckets over the column's own window; a gap is a stretch
-- beyond twice the median, the twenty largest sampled. A point in time
-- bounds a window; a number or a text does not, and says so.
--===------------------------------------------------------------------===--

DECLARE ASPECT temporal_profile WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"},
                 "granularity": {"type": ["string", "null"]},
                 "confidence": {"type": ["number", "null"]},
                 "completeness": {"type": ["object", "null"]}, "gaps": {"type": ["object", "null"]}}
}$$ AS MEASUREMENT ON COLUMN WHEN role = 'timestamp';

DECLARE FUNCTION temporal FOR GLOBAL AS $$
  WITH bounds AS (
    SELECT count(v) AS present, min(v) AS min_v, max(v) AS max_v, typeof(min(v)) AS ty
    FROM subject_column($subject)),
  w AS (
    SELECT present, ty, (ty = 'DATE' OR ty LIKE 'TIMESTAMP%') AS temporal,
           min_v::VARCHAR AS min_s, max_v::VARCHAR AS max_s,
           epoch(TRY_CAST(max_v AS TIMESTAMP)) - epoch(TRY_CAST(min_v AS TIMESTAMP)) AS span_seconds
    FROM bounds),
  ordered_ts AS (
    SELECT DISTINCT TRY_CAST(v AS TIMESTAMP) AS t FROM subject_column($subject) WHERE v IS NOT NULL),
  gap AS (
    SELECT t AS gap_start, lead(t) OVER (ORDER BY t) AS gap_end,
           (epoch(lead(t) OVER (ORDER BY t)) - epoch(t))::DOUBLE AS gap_seconds
    FROM ordered_ts),
  g AS (SELECT median(gap_seconds) AS med, min(gap_seconds) AS lo, max(gap_seconds) AS hi
        FROM gap WHERE gap_seconds IS NOT NULL),
  grain AS (
    SELECT med, lo, hi,
      CASE WHEN med IS NULL THEN 'unknown'
           WHEN abs(med - 1.0) < 0.5 THEN 'second'
           WHEN abs(med - 60.0) < 5.0 THEN 'minute'
           WHEN abs(med - 3600.0) < 300.0 THEN 'hour'
           WHEN abs(med - 86400.0) < 3600.0 THEN 'day'
           WHEN abs(med - 604800.0) < 86400.0 THEN 'week'
           WHEN abs(med - 2592000.0) < 259200.0 THEN 'month'
           WHEN abs(med - 7776000.0) < 777600.0 THEN 'quarter'
           WHEN abs(med - 31536000.0) < 3153600.0 THEN 'year'
           ELSE 'irregular' END AS granularity
    FROM g),
  conf AS (
    SELECT granularity, med,
      CASE WHEN med IS NULL THEN 0.0
           WHEN granularity = 'irregular' THEN 0.3
           WHEN lo > 0.0 AND hi > 0.0 AND med > 0.0 THEN 1.0 - least((hi - lo) / med / 10.0, 0.5)
           ELSE 0.7 END AS confidence
    FROM grain),
  big AS (
    SELECT gap_start, gap_end, gap_seconds, med FROM gap CROSS JOIN g
    WHERE med IS NOT NULL AND med > 0.0 AND gap_seconds > med * 2.0),
  sig AS (SELECT count(*) AS n, max(gap_seconds) / 86400.0 AS largest_days FROM big),
  largest AS (SELECT gap_start, gap_end, gap_seconds, med FROM big ORDER BY gap_seconds DESC, gap_start LIMIT 20),
  samp AS (
    SELECT list({
      'start': gap_start::VARCHAR, 'end': gap_end::VARCHAR, 'days': gap_seconds / 86400.0,
      'missing_periods': floor(gap_seconds / med)::BIGINT - 1,
      'severity': CASE WHEN gap_seconds > med * 10.0 THEN 'severe'
                       WHEN gap_seconds > med * 5.0 THEN 'moderate' ELSE 'minor' END
    } ORDER BY gap_seconds DESC, gap_start) AS sample
    FROM largest),
  comp AS (
    SELECT min(t) AS lo_t, max(t) AS hi_t,
           count(DISTINCT epoch(t)) AS a_second,
           count(DISTINCT floor(epoch(t) / 60.0)) AS a_minute,
           count(DISTINCT floor(epoch(t) / 3600.0)) AS a_hour,
           count(DISTINCT floor(epoch(t) / 86400.0)) AS a_day,
           count(DISTINCT floor((epoch(t) + 259200) / 604800.0)) AS a_week,
           count(DISTINCT year(t) * 12 + month(t)) AS a_month,
           count(DISTINCT year(t) * 4 + quarter(t)) AS a_quarter,
           count(DISTINCT year(t)) AS a_year
    FROM ordered_ts WHERE t IS NOT NULL),
  expect AS (
    SELECT
      CASE granularity
        WHEN 'year' THEN (year(hi_t) - year(lo_t) + 1)::BIGINT
        WHEN 'quarter' THEN ((year(hi_t) - year(lo_t)) * 4 + quarter(hi_t) - quarter(lo_t) + 1)::BIGINT
        WHEN 'month' THEN ((year(hi_t) - year(lo_t)) * 12 + month(hi_t) - month(lo_t) + 1)::BIGINT
        WHEN 'week' THEN (floor((epoch(hi_t) + 259200) / 604800.0) - floor((epoch(lo_t) + 259200) / 604800.0) + 1)::BIGINT
        WHEN 'day' THEN (floor(epoch(hi_t) / 86400.0) - floor(epoch(lo_t) / 86400.0) + 1)::BIGINT
        WHEN 'hour' THEN (floor(epoch(hi_t) / 3600.0) - floor(epoch(lo_t) / 3600.0) + 1)::BIGINT
        WHEN 'minute' THEN (floor(epoch(hi_t) / 60.0) - floor(epoch(lo_t) / 60.0) + 1)::BIGINT
        WHEN 'second' THEN (epoch(hi_t) - epoch(lo_t) + 1)::BIGINT END AS expected,
      CASE granularity
        WHEN 'year' THEN a_year WHEN 'quarter' THEN a_quarter WHEN 'month' THEN a_month WHEN 'week' THEN a_week
        WHEN 'day' THEN a_day WHEN 'hour' THEN a_hour WHEN 'minute' THEN a_minute WHEN 'second' THEN a_second END AS actual
    FROM comp CROSS JOIN grain)
  SELECT
    (w.temporal AND w.present > 0) AS applicable,
    CASE WHEN NOT w.temporal THEN 'column type is ' || w.ty || ', not DATE/TIMESTAMP — a date landed as text needs typing in the recipe'
         WHEN w.present = 0 THEN 'no non-null values — nothing bounds a window' END AS reason,
    CASE WHEN w.temporal AND w.present > 0 THEN w.min_s END AS min,
    CASE WHEN w.temporal AND w.present > 0 THEN w.max_s END AS max,
    CASE WHEN w.temporal AND w.present > 0 THEN w.span_seconds / 86400.0 END AS span_days,
    CASE WHEN w.temporal AND w.present > 0 THEN c.granularity END AS granularity,
    CASE WHEN w.temporal AND w.present > 0 THEN c.confidence END AS confidence,
    CASE WHEN w.temporal AND w.present > 0 THEN {'count': sig.n, 'largest_days': sig.largest_days, 'sample': samp.sample} END AS gaps,
    CASE WHEN w.temporal AND w.present > 0 AND c.granularity NOT IN ('unknown', 'irregular')
         THEN {'expected': e.expected, 'actual': e.actual, 'ratio': e.actual::DOUBLE / e.expected::DOUBLE} END AS completeness
  FROM w CROSS JOIN conf c CROSS JOIN sig CROSS JOIN samp CROSS JOIN expect e
$$ RETURNS temporal_profile;

--===------------------------------------------------------------------===--
-- rate_tolerance: the validation detector. A validation is a FACT aspect
-- whose schema carries `tolerance` and `breach_rate`: an agent or a human
-- glosses the expectation with the tolerance, and a function returning
-- the same aspect is the check, its `breach_rate` the violation share
-- (0.0 passes). Per subject: no check voice yet is yellow; a rate at or
-- under the tolerance is green, above it red. The human's tolerance
-- outranks the agent's; the witness's THRESHOLD stands in while no
-- expectation carries one, and 0.0 when neither does. One-sided: a
-- source whose expected dirt must not be cleaned away declares its own
-- detector, red on both sides.
--===------------------------------------------------------------------===--

DECLARE FUNCTION rate_tolerance FOR GLOBAL AS $$
  WITH s AS (
    SELECT subject,
           max(try_cast(body->>'breach_rate' AS DOUBLE)) FILTER (WHERE speaker = 'function') AS rate,
           coalesce(max(try_cast(body->>'tolerance' AS DOUBLE)) FILTER (WHERE speaker = 'human'),
                    max(try_cast(body->>'tolerance' AS DOUBLE)) FILTER (WHERE speaker = 'agent')) AS tolerance
    FROM slots GROUP BY subject)
  SELECT subject,
         CASE WHEN rate IS NULL THEN 'yellow'
              WHEN rate <= coalesce(tolerance, $threshold, 0.0) THEN 'green'
              ELSE 'red' END AS band,
         least(greatest(coalesce(rate, 0.0), 0.0), 1.0) AS score
  FROM s
$$;
