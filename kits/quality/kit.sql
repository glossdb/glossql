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
-- stored type. The engine's primitives, dataset_cells,
-- glossql_relationship_checks and glossql_additivity_cells, are what names a
-- column; a screen is SQL over them. A primitive names what its bind read
-- beside the plan, the record's edges and roles and the tables' shapes, so a
-- measurement over a screen is served while those stand and computes when one
-- moves, as when a table it read moves. A screen that pairs a
-- table's columns reads it under a bound of cells (SPEC §6): a larger table
-- is read as a sample of its rows kept by the hash of each row, the same
-- rows every run, and `rows` says how many were read. Each screen's measurement function reads the screen, keeps what
-- crosses its line, and lands the aspect; the thresholds are the function's,
-- the counts the screen's. A key a screen did not reach is null, and the
-- schemas say so.

--===------------------------------------------------------------------===--
-- hierarchy_candidates(table): functional dependencies between a table's
-- columns, one row per screened direction. Measures stay out by type
-- (floating and decimal); a column with a distinct value per filled row is
-- out, since it determines everything; NULL is a category. Per direction:
-- g3 is the share of rows that break from → to, g3_reverse the other way,
-- gk_lambda Goodman–Kruskal's lambda for predicting `to` from `from`,
-- rows_per_value the evidence per from-value. Fewer than two such columns,
-- or no rows, is served as one row that says why. Read under two million
-- cells: on a table of a hundred columns that is eighteen thousand rows,
-- and the pairs a second.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.hierarchy_candidates(tbl) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(tbl, cells := 2000000)),
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
-- a pool column's values as dense integer ids, NULL one of them: the pairs then
-- group on four integers, where grouping on the values' text took seventy
-- times as long on a wide table
ids AS (
  SELECT ci, v, dense_rank() OVER (PARTITION BY ci ORDER BY v) AS vid
  FROM (SELECT DISTINCT ci, coalesce(val, chr(1)) AS v FROM cells WHERE ci IN (SELECT ci FROM pool))),
pc AS (
  SELECT c.ci, c.rn, i.vid FROM cells c JOIN ids i ON i.ci = c.ci AND i.v = coalesce(c.val, chr(1))
  WHERE c.ci IN (SELECT ci FROM pool)),
pairs AS (
  SELECT a.ci AS fi, b.ci AS ti, a.vid AS fv, b.vid AS tv, count(*) AS c
  FROM pc a JOIN pc b ON a.rn = b.rn AND a.ci <> b.ci
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
CREATE OR REPLACE MACRO main.relationship_candidates(ds) AS TABLE
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
-- the column pairs the rescue needs: the anchor's and the scope's leg on each
-- side of every scoped attempt, never every pair of a table (a wide table's
-- pairs are its rows times its width squared)
needed AS (
  SELECT DISTINCT ft AS "table", pf AS c0, sf AS c1 FROM scoped
  UNION
  SELECT DISTINCT kt, pk, sk FROM scoped),
-- a row's two columns side by side, for those pairs
duo AS (
  SELECT n."table", n.c0, n.c1, a.rn, a.val AS v0, b.val AS v1
  FROM needed n
  JOIN cells a ON a."table" = n."table" AND a.ci = n.c0
  JOIN cells b ON b."table" = n."table" AND b.ci = n.c1 AND b.rn = a.rn),
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
         CASE WHEN df = from_filled AND dt = to_filled THEN 'one-to-one' WHEN dt = to_filled THEN 'many-to-one'
              WHEN df = from_filled THEN 'one-to-many' ELSE 'many-to-many' END AS cardinality,
         overlap, matched, df - matched AS orphans, df AS from_distinct, dt AS to_distinct,
         dt = to_filled AS to_unique, CASE WHEN pdk >= sdk THEN k_temporal ELSE s_temporal END AS to_temporal,
         CASE WHEN pdk >= sdk THEN ft || '.' || sfc ELSE ft || '.' || pfc END AS kc_from,
         CASE WHEN pdk >= sdk THEN kt || '.' || skc ELSE kt || '.' || pkc END AS kc_to,
         least(pf, sf) AS o1, greatest(pf, sf) AS o2
  FROM rescues WHERE pick = 1),
found AS (
  SELECT ft || '.' || fc AS from_col, kt || '.' || kc AS to_col,
         CASE WHEN f_uniq AND k_uniq THEN 'one-to-one' WHEN k_uniq THEN 'many-to-one'
              WHEN f_uniq THEN 'one-to-many' ELSE 'many-to-many' END AS cardinality,
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
-- relationship_checks(dataset): whether the declared relationships hold,
-- the engine's glossql_relationship_checks: per edge, over its own two or
-- four columns, typed and linear in the rows, where a screen over the cells
-- pivoted every cell of a table back into rows per edge. Per edge: filled
-- rows, the orphans (child values no parent row carries; inside one table,
-- child values that reach more than one parent), and for an edge across two
-- tables the temporal pairs: the first three date columns of each side,
-- joined on the edge, how often the child's date precedes the parent's. Legs
-- are one column or a pair. No declared relationships is one row that says
-- so.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.relationship_checks(ds) AS TABLE
SELECT * FROM glossql_relationship_checks(ds);

--===------------------------------------------------------------------===--
-- derivation_candidates(table): which numeric column is another two
-- combined, as a product or a sum. Numeric columns in table order, twelve at
-- most; a target and two operands each filled on twenty rows or more; the
-- form admitted when the magnitudes fit within a factor of thirty. A row
-- supports a triple when all three are filled and violates it when the
-- target misses the form by more than 0.011 or a millionth of itself.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.derivation_candidates(tbl) AS TABLE
WITH cells AS (SELECT * FROM dataset_cells(tbl, cells := 2000000)),
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
  SELECT a.ci AS ai, a.nth AS an, a."column" AS target, b.ci AS bi, b.nth AS bn, b."column" AS operand_1,
         c.ci AS ci2, c.nth AS cn, c."column" AS operand_2, form
  FROM capped a, capped b, capped c, (SELECT unnest(['product', 'sum']) AS form)
  WHERE b.nth < c.nth AND a.ci <> b.ci AND a.ci <> c.ci AND a.filled >= 20 AND b.filled >= 20 AND c.filled >= 20
    AND CASE form
          WHEN 'product' THEN coalesce(a.mag, 0) > 0 AND coalesce(b.mag, 0) > 0 AND coalesce(c.mag, 0) > 0
                             AND b.mag * c.mag < 30 * a.mag AND a.mag < 30 * b.mag * c.mag
          ELSE coalesce(b.mag, 0) + coalesce(c.mag, 0) > 0 AND coalesce(a.mag, 0) > 0
               AND b.mag + c.mag < 30 * a.mag AND a.mag < 30 * (b.mag + c.mag) END),
-- a row's capped values once, in the capped columns' order, so a triple reads a
-- row by position: three joins of the cells with themselves planned badly on a
-- wide table, minutes for thousands of rows
rowvals AS (
  SELECT rn, list(TRY_CAST(val AS DOUBLE) ORDER BY ci) AS v
  FROM cells WHERE ci IN (SELECT ci FROM capped) GROUP BY rn),
rows3 AS (
  SELECT t.ai, t.bi, t.ci2, t.form, t.target, t.operand_1, t.operand_2, r.v[t.an] AS av, r.v[t.bn] AS bv, r.v[t.cn] AS cv
  FROM triples t, rowvals r),
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
-- value, the first date column standing in for the period. The additivity
-- glossed on the value column folds the month: a flow summed, a stock the
-- month's last level, a ratio its summed halves re-divided, the columns
-- its numerator and denominator name; none claimed reads as flow. A
-- collision is two groundings of one flavor, the aspect's x-kind: a
-- grounding flavored otherwise stands beside a metric and is never its
-- twin. No groundings is one row with groundings 0.
--===------------------------------------------------------------------===--
CREATE OR REPLACE MACRO main.grounding_collisions(ds) AS TABLE
WITH slots AS (
  SELECT g.aspect, g.subject, g.body, coalesce(json_extract_string(a.schema, '$."x-kind"'), '') AS flavor,
         row_number() OVER (PARTITION BY g.aspect, g.subject ORDER BY CASE g.actor_kind WHEN 'human' THEN 0 ELSE 1 END, g.seq DESC) AS rk
  FROM main.glossary g JOIN main.aspects a ON a.name = g.aspect
  WHERE g.dataset = ds AND a.kind = 'VIEW' AND json_extract_string(g.body, '$.sql') IS NOT NULL),
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
  SELECT split_part(subject, '.', 2) AS aspect,
         substr(subject, length(split_part(subject, '.', 1)) + length(split_part(subject, '.', 2)) + 3) AS col,
         g.aspect AS claim, value
  FROM glossql_glossary(ds, NULL, false) g
  WHERE g.aspect IN ('role', 'additivity') AND state IN ('current', 'stale') AND value IS NOT NULL
    AND len(string_split(subject, '.')) >= 3 AND split_part(subject, '.', 2) IN (SELECT aspect FROM groundings)),
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
         coalesce(b.value->>'value', 'additive') AS verb, b.value->>'numerator' AS num, b.value->>'denominator' AS den
  FROM chosen c LEFT JOIN claims b ON b.claim = 'additivity' AND b.aspect = c.aspect AND b.col = c.value_column
  WHERE c.period_column IS NOT NULL AND c.value_column IS NOT NULL),
series AS (
  SELECT f.aspect, date_trunc('month', TRY_CAST(p.val AS TIMESTAMP)) AS month,
         CASE f.verb
           WHEN 'semi-additive' THEN arg_max(TRY_CAST(v.val AS DOUBLE), TRY_CAST(p.val AS TIMESTAMP)) FILTER (WHERE v."column" = f.value_column)
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
-- additivity_anchors(column): whether a numeric column behaves as a stock, a
-- level correct at a point in time, or a flow, a movement that sums over a
-- period. A column's own trajectory cannot decide it: a trending flow and a
-- mean-reverting stock look alike. The evidence is cross-table: a movement
-- summed per entity and period from an event table the declared
-- relationships align with the column's table, scored against both readings
-- with scale-free residuals, flow where y ≈ m, stock where Δy ≈ m. One row
-- per anchor: an event table, an alignment, a time axis on each side, a
-- grain and a scope. Over glossql_additivity_cells(column), both sides at the
-- day, folded to the anchor's grain, the coarser of the two sides' native
-- grains, a day-native alignment serving a month beside the day, and scoped
-- by year where the grain allows, since a cumulative that resets is a stock
-- only inside its year. Movement terms are the event table's columns the
-- record says are measures, a role claim standing, minus the legs of
-- declared edges, and every ordered pair difference of them on a pool of
-- eight terms or fewer; nothing is guessed from a type, and an event table
-- with no measure glossed abstains and names the gloss to make. The event
-- side is summed for the entities the measure side can align, those with
-- four days or more, so a wide, long event table costs what the alignment
-- needs. Per entity the two
-- residuals are Σ|y − m| / Σ|m| and Σ|Δy − m| / Σ|m| over the periods both
-- sides carry; an entity votes when the winner is under 0.05 and the loser
-- stands off by a third of their sum, and abstains on fewer than four
-- periods, a flat level or a dead movement. A convention decides when two
-- entities or more voted, four in five agree, and the winners are a majority
-- of the entities on both sides; Wilson's lower bound over those entities
-- ranks the conventions, a support tie going to the fewer terms unless the
-- pair's fit is decisive, ΔBIC over 10. The sign partition says how many
-- voters fire only under the negated movement, the ledger-signed reading.
-- One anchor per alignment and scope reads the measure's own shape instead:
-- entities that never decrease over four periods or more at the native
-- grain vote stock under the convention monotone, movers only. The verdict
-- is evidence the judge reads before glossing additivity, never a voice in its
-- slots. A column that is not numeric, an identifier (a leg of a declared
-- relationship, or an integer distinct on nine filled rows in ten, which is
-- what the monotone reading would otherwise read as a cumulative), a table
-- with no time axis or no declared edge to an event table is one row that
-- says why.
--===------------------------------------------------------------------===--

-- Wilson's lower bound on s successes in n at 95%: the rank of a vote rate under small n
CREATE OR REPLACE MACRO main.wilson_lower(s, n) AS
  CASE WHEN n <= 0 THEN 0.0
       ELSE greatest(0.0, (s / n::DOUBLE + 1.9208 / n - 1.96 * sqrt((s / n::DOUBLE) * (1.0 - s / n::DOUBLE) / n + 0.9604 / (n::DOUBLE * n)))
                          / (1.0 + 3.8416 / n)) END;

-- One entity's vote under a convention from its two residuals: stock, flow, or NULL for an abstention
CREATE OR REPLACE MACRO main.additivity_vote(n, flat, moving, nan, rf, rs) AS
  CASE WHEN n < 4 OR flat OR NOT moving OR nan THEN NULL
       WHEN least(rf, rs) > 0.05 THEN NULL
       WHEN (CASE WHEN isinf(least(rf, rs)) THEN 0.0 WHEN isinf(greatest(rf, rs)) THEN 1.0
                  WHEN rf + rs = 0.0 THEN 0.0 ELSE (greatest(rf, rs) - least(rf, rs)) / (rf + rs) END) < 1.0 / 3.0 THEN NULL
       WHEN rs < rf THEN 'semi-additive' ELSE 'additive' END;

CREATE OR REPLACE MACRO main.additivity_anchors(col) AS TABLE
WITH cells AS MATERIALIZED (SELECT * FROM glossql_additivity_cells(col)),
-- the native grain of each side's axis: the coarsest named grain every period is already truncated to
native AS (
  SELECT side, event, align, axis,
         CASE WHEN bool_and(period = date_trunc('year', period)) THEN 'year'
              WHEN bool_and(period = date_trunc('quarter', period)) THEN 'quarter'
              WHEN bool_and(period = date_trunc('month', period)) THEN 'month' ELSE 'day' END AS grain
  FROM cells WHERE side IN ('measure', 'event') GROUP BY ALL),
-- the event tables' identifiers, served beside each anchor: declared legs and key-shaped integers
identifiers AS (
  SELECT event, coalesce(list(term ORDER BY term) FILTER (WHERE identifier OR value >= 0.9), []) AS identifier_columns
  FROM cells WHERE side = 'column' GROUP BY event),
-- the anchors: every measure axis against every event alignment it keys, at the coarser native grain,
-- a month beside a day, each scope the grain allows
coords AS (
  SELECT ev.event, ev.align, ev.measure_align, ma.axis AS measure_time, ev.axis AS event_time, ma.grain AS m_native,
         CASE WHEN 'year' IN (ma.grain, ea.grain) THEN 'year' WHEN 'quarter' IN (ma.grain, ea.grain) THEN 'quarter'
              WHEN 'month' IN (ma.grain, ea.grain) THEN 'month' ELSE 'day' END AS native
  FROM (SELECT event, align, measure_align, axis FROM cells WHERE side = 'alignment') ev
  JOIN native ma ON ma.side = 'measure' AND ma.align = ev.measure_align
  LEFT JOIN native ea ON ea.side = 'event' AND ea.event = ev.event AND ea.align = ev.align AND ea.axis = ev.axis),
grained AS (SELECT c.*, unnest(CASE WHEN c.native = 'day' THEN ['day', 'month'] ELSE [c.native] END) AS grain FROM coords c),
scoped AS (SELECT g.*, unnest(CASE WHEN g.grain = 'year' THEN ['none'] ELSE ['none', 'year'] END) AS scope FROM grained g),
anchors AS (
  SELECT *, row_number() OVER (ORDER BY measure_time, event, align, event_time, grain, scope) AS anchor_id FROM scoped),
-- the two series per anchor: the level and each term's movement, per entity and period, the year scope on the entity
m_series AS (
  SELECT k.anchor_id, m.entity || CASE WHEN k.scope = 'year' THEN '@' || year(m.period)::VARCHAR ELSE '' END AS entity,
         date_trunc(k.grain, m.period) AS b, sum(m.value) AS y
  FROM anchors k JOIN cells m ON m.side = 'measure' AND m.align = k.measure_align AND m.axis = k.measure_time
  GROUP BY ALL),
e_series AS (
  SELECT k.anchor_id, e.entity || CASE WHEN k.scope = 'year' THEN '@' || year(e.period)::VARCHAR ELSE '' END AS entity,
         date_trunc(k.grain, e.period) AS b, e.term, sum(e.value) AS m
  FROM anchors k
  JOIN cells e ON e.side = 'event' AND e.event = k.event AND e.align = k.align AND e.axis = k.event_time
  GROUP BY ALL),
terms AS (SELECT anchor_id, count(DISTINCT term) AS terms FROM e_series GROUP BY anchor_id),
viable AS (
  SELECT anchor_id, count(*) AS viable_entities
  FROM (SELECT anchor_id, entity FROM m_series GROUP BY anchor_id, entity HAVING count(*) >= 4) GROUP BY anchor_id),
common AS (
  SELECT anchor_id, count(*) AS entities
  FROM (SELECT DISTINCT anchor_id, entity FROM m_series INTERSECT SELECT DISTINCT anchor_id, entity FROM e_series)
  GROUP BY anchor_id),
runnable AS (
  SELECT k.anchor_id FROM anchors k JOIN viable v ON v.anchor_id = k.anchor_id JOIN terms t ON t.anchor_id = k.anchor_id
  WHERE v.viable_entities >= 2),
-- the cells both sides carry, under every convention: each term, and every ordered pair difference on a pool of eight or fewer
aligned AS (
  SELECT y.anchor_id, y.entity, y.b, y.y, e.term, e.m
  FROM m_series y JOIN e_series e ON e.anchor_id = y.anchor_id AND e.entity = y.entity AND e.b = y.b
  WHERE y.anchor_id IN (SELECT anchor_id FROM runnable)),
conv AS (
  SELECT anchor_id, entity, b, y, term AS convention, 1 AS terms, m FROM aligned
  UNION ALL
  SELECT a.anchor_id, a.entity, a.b, a.y, a.term || ' - ' || o.term, 2, a.m - o.m
  FROM aligned a JOIN aligned o ON o.anchor_id = a.anchor_id AND o.entity = a.entity AND o.b = a.b AND o.term <> a.term
  JOIN terms t ON t.anchor_id = a.anchor_id AND t.terms <= 8),
stepped AS (
  SELECT *, y - lag(y) OVER w AS dy, row_number() OVER w AS t
  FROM conv WINDOW w AS (PARTITION BY anchor_id, convention, entity ORDER BY b)),
per_entity AS (
  SELECT anchor_id, convention, terms, entity, count(*) AS n,
         min(y) = max(y) AS flat, bool_or(m <> 0.0) AS moving, bool_or(isnan(y) OR isnan(m)) AS nan,
         sum(abs(y - m)) AS nf, sum(abs(m)) AS df,
         sum(abs(dy - m)) FILTER (WHERE t > 1) AS ns, sum(abs(m)) FILTER (WHERE t > 1) AS ds,
         sum(abs(y + m)) AS nf_neg, sum(abs(dy + m)) FILTER (WHERE t > 1) AS ns_neg
  FROM stepped GROUP BY ALL),
voters AS (
  SELECT anchor_id, convention, terms, entity, r_flow, r_stock,
         additivity_vote(n, flat, moving, nan, r_flow, r_stock) AS vote,
         additivity_vote(n, flat, moving, nan, r_flow_neg, r_stock_neg) AS mirror
  FROM (SELECT *,
               CASE WHEN df > 0 THEN nf / df ELSE 'infinity'::DOUBLE END AS r_flow,
               CASE WHEN ds > 0 THEN ns / ds ELSE 'infinity'::DOUBLE END AS r_stock,
               CASE WHEN df > 0 THEN nf_neg / df ELSE 'infinity'::DOUBLE END AS r_flow_neg,
               CASE WHEN ds > 0 THEN ns_neg / ds ELSE 'infinity'::DOUBLE END AS r_stock_neg
        FROM per_entity)),
-- per convention: the votes, the winning label, its diagnostics over the winning voters, the support over the common entities
counts AS (
  SELECT anchor_id, convention, terms, count(vote) AS voted,
         count(*) FILTER (WHERE vote = 'semi-additive') AS stock, count(*) FILTER (WHERE vote = 'additive') AS flow
  FROM voters GROUP BY ALL),
winning AS (SELECT *, CASE WHEN stock > flow THEN 'semi-additive' ELSE 'additive' END AS label, greatest(stock, flow) AS winners FROM counts),
diag AS (
  SELECT w.anchor_id, w.convention,
         median(v.r_flow) FILTER (WHERE v.vote = w.label) AS r_flow, median(v.r_stock) FILTER (WHERE v.vote = w.label) AS r_stock,
         count(*) FILTER (WHERE v.vote = w.label AND v.mirror IS DISTINCT FROM w.label) AS sign_primary,
         count(*) FILTER (WHERE v.vote IS DISTINCT FROM w.label AND v.mirror = w.label) AS sign_mirror,
         count(*) FILTER (WHERE v.vote = w.label AND v.mirror = w.label) AS sign_both,
         sum(least(v.r_flow, v.r_stock) ** 2) FILTER (WHERE v.vote = w.label) AS rss, count(*) FILTER (WHERE v.vote = w.label) AS fired
  FROM winning w JOIN voters v ON v.anchor_id = w.anchor_id AND v.convention = w.convention GROUP BY ALL),
summaries AS (
  SELECT w.anchor_id, w.convention, w.terms, w.voted, w.winners, w.label, c.entities,
         CASE WHEN w.voted > 0 THEN w.winners / w.voted::DOUBLE ELSE 0.0 END AS agreement,
         CASE WHEN w.voted >= 2 AND w.winners / w.voted::DOUBLE >= 0.8 THEN w.label ELSE 'abstain' END AS verdict,
         d.r_flow, d.r_stock, d.sign_primary, d.sign_mirror, d.sign_both,
         CASE WHEN d.fired > 0 THEN d.fired * ln(greatest(d.rss, 1e-12) / d.fired) + w.terms * ln(d.fired) END AS bic,
         wilson_lower(w.winners, c.entities) AS support
  FROM winning w JOIN diag d ON d.anchor_id = w.anchor_id AND d.convention = w.convention
  JOIN common c ON c.anchor_id = w.anchor_id),
-- the anchor's winner: among the conventions that decide, support first, a tie to the fewer terms unless ΔBIC > 10
deciders AS (SELECT * FROM summaries WHERE verdict <> 'abstain' AND winners >= 0.5 * greatest(entities, 1)),
simple AS (SELECT anchor_id, round(support, 8) AS tie, min(bic) AS bic FROM deciders WHERE terms = 1 GROUP BY ALL),
ranked AS (
  SELECT d.*, row_number() OVER (PARTITION BY d.anchor_id ORDER BY round(d.support, 8) DESC,
           CASE WHEN d.terms = 1 THEN 1 WHEN s.bic IS NOT NULL AND d.bic IS NOT NULL AND s.bic - d.bic > 10.0 THEN 0 ELSE 2 END,
           d.convention) AS rank
  FROM deciders d LEFT JOIN simple s ON s.anchor_id = d.anchor_id AND s.tie = round(d.support, 8)),
winner AS (SELECT * FROM ranked WHERE rank = 1),
alts AS (
  SELECT s.anchor_id,
         list_slice(list({'convention': s.convention, 'verdict': CASE WHEN d.convention IS NOT NULL THEN s.verdict ELSE 'abstain' END,
                          'voted': s.voted, 'support': s.support} ORDER BY s.voted DESC, s.support DESC, s.convention), 1, 4) AS alternatives
  FROM summaries s
  LEFT JOIN winner w ON w.anchor_id = s.anchor_id
  LEFT JOIN deciders d ON d.anchor_id = s.anchor_id AND d.convention = s.convention
  WHERE w.convention IS DISTINCT FROM s.convention
  GROUP BY s.anchor_id),
floored AS (
  SELECT anchor_id, verdict, voted, winners, entities,
         row_number() OVER (PARTITION BY anchor_id ORDER BY winners / greatest(entities, 1)::DOUBLE DESC, convention) AS rn
  FROM summaries WHERE verdict <> 'abstain'),
anyvote AS (SELECT anchor_id, bool_or(voted > 0) AS any_votes FROM summaries GROUP BY anchor_id),
recon AS (
  SELECT k.event, k.align, k.measure_time, k.event_time, k.grain, k.scope,
         CASE WHEN r.anchor_id IS NOT NULL THEN coalesce(c.entities, 0) END AS entities,
         coalesce(v.viable_entities, 0) AS viable_entities,
         i.identifier_columns,
         CASE WHEN r.anchor_id IS NULL OR w.anchor_id IS NULL THEN 'abstain' ELSE w.verdict END AS verdict,
         w.convention, coalesce(w.voted, f.voted) AS voted, w.agreement, w.support,
         CASE WHEN w.verdict <> 'abstain' THEN w.r_flow END AS r_flow, CASE WHEN w.verdict <> 'abstain' THEN w.r_stock END AS r_stock,
         w.sign_primary, w.sign_mirror, w.sign_both,
         coalesce(al.alternatives, []::STRUCT(convention VARCHAR, verdict VARCHAR, voted BIGINT, support DOUBLE)[]) AS alternatives,
         CASE WHEN coalesce(v.viable_entities, 0) < 2 THEN 'fewer than two entities carry 4+ periods on this alignment'
              WHEN w.anchor_id IS NULL THEN
                CASE WHEN f.anchor_id IS NOT NULL THEN f.winners || ' of ' || f.entities || ' common entities voted ' || f.verdict || ': under the majority floor'
                     WHEN coalesce(av.any_votes, false) THEN 'votes below the candidacy floor (2 voters, 0.8 agreement)'
                     ELSE 'no entity series reconciled: wrong anchor, short series, or dead values' END
                || CASE WHEN coalesce(t.terms, 0) > 8 THEN '; pairs not scored above 8 terms' ELSE '' END END AS reason,
         0 AS kind
  FROM anchors k
  LEFT JOIN terms t ON t.anchor_id = k.anchor_id
  LEFT JOIN viable v ON v.anchor_id = k.anchor_id
  LEFT JOIN common c ON c.anchor_id = k.anchor_id
  LEFT JOIN runnable r ON r.anchor_id = k.anchor_id
  LEFT JOIN winner w ON w.anchor_id = k.anchor_id
  LEFT JOIN alts al ON al.anchor_id = k.anchor_id
  LEFT JOIN floored f ON f.anchor_id = k.anchor_id AND f.rn = 1
  LEFT JOIN anyvote av ON av.anchor_id = k.anchor_id
  LEFT JOIN identifiers i ON i.event = k.event),
-- the monotone anchor, once per event alignment and scope, at the measure axis's native grain
mono_keys AS (
  SELECT event, measure_align, scope, min(measure_time) AS measure_time, arg_min(m_native, measure_time) AS grain,
         min(align) AS align, min(event_time) AS event_time
  FROM anchors GROUP BY ALL),
mono_series AS (
  SELECT mk.event, mk.measure_align, mk.scope,
         m.entity || CASE WHEN mk.scope = 'year' THEN '@' || year(m.period)::VARCHAR ELSE '' END AS entity,
         date_trunc(mk.grain, m.period) AS b, sum(m.value) AS y
  FROM mono_keys mk JOIN cells m ON m.side = 'measure' AND m.align = mk.measure_align AND m.axis = mk.measure_time
  GROUP BY ALL),
mono_entities AS (
  SELECT event, measure_align, scope, entity, count(*) AS steps,
         count(*) FILTER (WHERE y < prev) AS decreases, count(*) FILTER (WHERE y > prev) AS increases
  FROM (SELECT *, lag(y) OVER (PARTITION BY event, measure_align, scope, entity ORDER BY b) AS prev FROM mono_series)
  WHERE prev IS NOT NULL GROUP BY ALL HAVING count(*) >= 3),
mono_read AS (
  SELECT event, measure_align, scope, count(*) AS considered,
         count(*) FILTER (WHERE decreases = 0 AND increases > 0) AS monotone,
         count(*) FILTER (WHERE increases > 0 OR decreases > 0) AS movers, sum(steps) AS steps
  FROM mono_entities GROUP BY ALL),
mono AS (
  SELECT mk.event, mk.align, mk.measure_time, mk.event_time, mk.grain, mk.scope,
         coalesce(r.considered, 0) AS entities, coalesce(r.considered, 0) AS viable_entities, i.identifier_columns,
         CASE WHEN coalesce(r.considered, 0) < 2 OR coalesce(r.movers, 0) < 2 OR r.monotone * 2 < r.movers THEN 'abstain' ELSE 'semi-additive' END AS verdict,
         'monotone' AS convention,
         CASE WHEN r.considered >= 2 AND r.movers >= 2 THEN r.movers END AS voted,
         CASE WHEN r.considered >= 2 AND r.movers >= 2 THEN r.monotone / r.movers::DOUBLE END AS agreement,
         CASE WHEN r.considered >= 2 AND r.movers >= 2 THEN wilson_lower(r.monotone, r.movers) END AS support,
         NULL::DOUBLE AS r_flow, NULL::DOUBLE AS r_stock, NULL::BIGINT AS sign_primary, NULL::BIGINT AS sign_mirror, NULL::BIGINT AS sign_both,
         []::STRUCT(convention VARCHAR, verdict VARCHAR, voted BIGINT, support DOUBLE)[] AS alternatives,
         CASE WHEN coalesce(r.considered, 0) < 2 THEN 'fewer than two entities carry 4+ periods at the native grain'
              WHEN r.movers < 2 THEN r.movers || ' of ' || r.considered || ' entities move within the scope: no monotone evidence'
              WHEN r.monotone * 2 < r.movers THEN (r.movers - r.monotone) || ' of ' || r.movers || ' moving entities decrease within the scope over ' || r.steps || ' steps: not a cumulative here' END AS reason,
         1 AS kind
  FROM mono_keys mk
  LEFT JOIN mono_read r ON r.event = mk.event AND r.measure_align = mk.measure_align AND r.scope = mk.scope
  LEFT JOIN identifiers i ON i.event = mk.event),
-- an event table with no measure glossed: one abstaining row per alignment and axis pair, naming the gloss
unglossed AS (
  SELECT u.event, u.align, u.term AS measure_time, u.axis AS event_time, NULL::VARCHAR AS grain, NULL::VARCHAR AS scope,
         NULL::BIGINT AS entities, NULL::BIGINT AS viable_entities, i.identifier_columns, 'abstain' AS verdict,
         NULL::VARCHAR AS convention, NULL::BIGINT AS voted, NULL::DOUBLE AS agreement, NULL::DOUBLE AS support,
         NULL::DOUBLE AS r_flow, NULL::DOUBLE AS r_stock, NULL::BIGINT AS sign_primary, NULL::BIGINT AS sign_mirror, NULL::BIGINT AS sign_both,
         []::STRUCT(convention VARCHAR, verdict VARCHAR, voted BIGINT, support DOUBLE)[] AS alternatives, u.reason, 0 AS kind
  FROM cells u LEFT JOIN identifiers i ON i.event = u.event WHERE u.side = 'unglossed'),
numbered AS (
  SELECT *, row_number() OVER (ORDER BY measure_time, event, align, event_time, kind, grain, scope) - 1 AS seq
  FROM (SELECT * FROM recon UNION ALL SELECT * FROM mono UNION ALL SELECT * FROM unglossed)),
-- what refuses the column: the primitive's reason
refused AS (SELECT reason FROM cells WHERE side = 'reason')
SELECT true AS applicable, reason, seq, event, align, measure_time, event_time, grain, scope, entities, viable_entities,
       identifier_columns, verdict, convention, voted, agreement, support, r_flow, r_stock, sign_primary, sign_mirror, sign_both, alternatives
FROM numbered WHERE NOT EXISTS (SELECT 1 FROM refused)
UNION ALL
SELECT false, coalesce((SELECT min(reason) FROM refused), 'no anchor aligned: no cells on the measure side or the event side'),
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM numbered) OR EXISTS (SELECT 1 FROM refused)
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
               CASE cardinality WHEN 'many-to-one' THEN 0 WHEN 'one-to-one' THEN 1 ELSE 2 END,
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

-- The stock/flow evidence on a measure column: the anchors' election. Support
-- first within a billionth, then a reconciliation over the monotone reading
-- (the movement explains the level), then the wider vote, then the event's
-- name; a support tie names the layer that decided. When every anchor
-- abstains the summary carries the reason of the one that came closest: a
-- reconciliation over the monotone reading, one that aligned over one that
-- did not, the most votes. Extraction serves the summary; the anchors read
-- back through GLOSSARY(column::additivity_evidence).
DECLARE ASPECT additivity_evidence WITH $${
  "type": "object", "required": ["applicable"],
  "properties": {"applicable": {"type": "boolean"}, "reason": {"type": ["string", "null"]},
                 "anchors_n": {"type": ["integer", "null"]}, "decided": {"type": ["integer", "null"]},
                 "summary": {"type": ["object", "null"],
                   "properties": {"verdict": {"enum": ["semi-additive", "additive", "abstain"]},
                                  "convention": {"type": ["string", "null"]}, "support": {"type": ["number", "null"]},
                                  "voted": {"type": ["integer", "null"]}, "event": {"type": ["string", "null"]},
                                  "align": {"type": ["string", "null"]}, "grain": {"type": ["string", "null"]},
                                  "scope": {"type": ["string", "null"]}, "r_flow": {"type": ["number", "null"]},
                                  "r_stock": {"type": ["number", "null"]}, "sign": {"type": ["object", "null"]},
                                  "tiebreak": {"type": ["string", "null"]}, "reason": {"type": ["string", "null"]}}},
                 "anchors": {"type": ["array", "null"], "items": {"type": "object"}}}
}$$ AS MEASUREMENT ON COLUMN WHEN role = 'measure';

DECLARE FUNCTION additivity_evidence FOR GLOBAL AS $$
  WITH a AS (SELECT * FROM additivity_anchors($subject)),
  ranked AS (
    SELECT *, convention = 'monotone' AS monotone,
           row_number() OVER (ORDER BY round(support, 8) DESC, convention = 'monotone', voted DESC, event, align) AS rank
    FROM a WHERE applicable AND verdict <> 'abstain'),
  top AS (SELECT * FROM ranked WHERE rank = 1),
  runner AS (SELECT * FROM ranked WHERE rank = 2),
  quiet AS (
    SELECT reason FROM a WHERE applicable
    ORDER BY convention = 'monotone', entities IS NULL, voted DESC NULLS LAST, seq LIMIT 1),
  elected AS (
    SELECT {'verdict': coalesce(t.verdict, 'abstain'), 'event': t.event, 'align': t.align, 'grain': t.grain, 'scope': t.scope,
            'convention': t.convention, 'support': t.support, 'voted': t.voted, 'r_flow': t.r_flow, 'r_stock': t.r_stock,
            'sign': CASE WHEN t.sign_primary IS NOT NULL THEN {'primary': t.sign_primary, 'mirror': t.sign_mirror, 'both': t.sign_both} END,
            'tiebreak': CASE WHEN r.support IS NOT NULL AND abs(t.support - r.support) <= 1e-9
                             THEN CASE WHEN t.monotone <> r.monotone THEN 'reconciliation-over-monotone'
                                       WHEN t.voted <> r.voted THEN 'voted' ELSE 'event-name' END END,
            'reason': CASE WHEN t.verdict IS NULL THEN q.reason END} AS summary
    FROM (SELECT 1) one LEFT JOIN top t ON true LEFT JOIN runner r ON true LEFT JOIN quiet q ON true)
  SELECT bool_and(a.applicable) AS applicable, min(a.reason) FILTER (WHERE NOT a.applicable) AS reason,
         CASE WHEN bool_and(a.applicable) THEN count(*) END AS anchors_n,
         CASE WHEN bool_and(a.applicable) THEN (SELECT count(*) FROM ranked) END AS decided,
         CASE WHEN bool_and(a.applicable) THEN (SELECT summary FROM elected) END AS summary,
         CASE WHEN bool_and(a.applicable) THEN list({
           'seq': a.seq, 'event': a.event, 'align': a.align, 'measure_time': a.measure_time, 'event_time': a.event_time,
           'grain': a.grain, 'scope': a.scope, 'entities': a.entities, 'viable_entities': a.viable_entities,
           'identifier_columns': a.identifier_columns, 'verdict': a.verdict, 'convention': a.convention, 'voted': a.voted,
           'agreement': a.agreement, 'support': a.support, 'r_flow': a.r_flow, 'r_stock': a.r_stock,
           'sign_primary': a.sign_primary, 'sign_mirror': a.sign_mirror, 'sign_both': a.sign_both,
           'alternatives': a.alternatives, 'reason': a.reason} ORDER BY a.seq) END AS anchors
  FROM a
$$ RETURNS additivity_evidence;

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
-- outranks the agent's; the test's THRESHOLD stands in while no
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
         CASE WHEN rate IS NULL OR rate <= coalesce(tolerance, $threshold, 0.0) THEN 0.0
              ELSE least(greatest(rate, 0.0), 1.0) END AS score
  FROM s
$$;
