-- The metrics kit: a metric is a view, its columns carry the vocabulary,
-- and the reads are SQL over the collapsed slots and the view. Loaded after
-- the KPI kit, whose words the columns carry (role, behavior, dimension),
-- and the quality kit, whose temporal profile measures a time column's
-- cadence. What an actor must learn about these words is SKILL.md beside
-- this file.
--
--   FROM gloss_kit('kits/kpi/kit.sql');
--   FROM gloss_kit('kits/quality/kit.sql');
--   FROM gloss_kit('kits/metrics/kit.sql');
--
-- A grounding's view is a table of its dataset: the column that carries
-- role timestamp is the metric's period, the column that carries role
-- measure its value (the one named value where several do), the columns
-- that carry role dimension its slices. The behavior on the value column
-- says how a number combines over periods and members. Nothing is
-- re-marked at read: a claim older than its data is stale by the record's
-- own rule, and the reads serve it as the record does.

--===------------------------------------------------------------------===--
-- The ladder: how a time column's periods are walked. floor is the grain
-- the series is served at, by default the measured cadence; windows is the
-- rung per grain, how many whole buckets a read serves, measured back from
-- the data's edge, by default the kit's. Owed where the role is timestamp.
--===------------------------------------------------------------------===--

DECLARE ASPECT ladder WITH $${
  "type": "object",
  "properties": {
    "floor": {"enum": ["minute", "hour", "day", "week", "month", "quarter", "year"]},
    "windows": {"type": "object", "additionalProperties": false, "properties": {
      "minute": {"type": "integer", "minimum": 1}, "hour": {"type": "integer", "minimum": 1},
      "day": {"type": "integer", "minimum": 1}, "week": {"type": "integer", "minimum": 1},
      "month": {"type": "integer", "minimum": 1}, "quarter": {"type": "integer", "minimum": 1},
      "year": {"type": "integer", "minimum": 1}}},
    "grounds": {"type": "string"}
  }
}$$ AS FACT ON COLUMN WHEN role = 'timestamp';

DECLARE WITNESS ladder_w ON ladder BY (AGENT, HUMAN);

-- The grains in order, a bucket's step, and the kit's rung per grain.
CREATE OR REPLACE MACRO glossql.grain_rank(g) AS
  CASE g WHEN 'minute' THEN 1 WHEN 'hour' THEN 2 WHEN 'day' THEN 3 WHEN 'week' THEN 4
         WHEN 'month' THEN 5 WHEN 'quarter' THEN 6 WHEN 'year' THEN 7 END;
CREATE OR REPLACE MACRO glossql.grain_step(g) AS
  CASE g WHEN 'minute' THEN INTERVAL 1 MINUTE WHEN 'hour' THEN INTERVAL 1 HOUR WHEN 'day' THEN INTERVAL 1 DAY
         WHEN 'week' THEN INTERVAL 7 DAY WHEN 'month' THEN INTERVAL 1 MONTH WHEN 'quarter' THEN INTERVAL 3 MONTH
         WHEN 'year' THEN INTERVAL 1 YEAR END;
CREATE OR REPLACE MACRO glossql.rung_default(g) AS
  CASE g WHEN 'minute' THEN 1440 WHEN 'hour' THEN 720 WHEN 'day' THEN 548 WHEN 'week' THEN 156
         WHEN 'month' THEN 48 WHEN 'quarter' THEN 40 WHEN 'year' THEN 20 END;

--===------------------------------------------------------------------===--
-- metric_axes(): one row per metric of the bound dataset — a QUERY aspect
-- whose grounding stands as a view — what the record says its columns are
-- and whether a series can be served: metric, kind (the aspect's x-kind),
-- stopped (the author's text), applicable, reason (the road out when it is
-- not), period_column, value_column, numerator and denominator (a ratio's
-- halves), cadence (the period column's measured granularity), floor (the
-- ladder's, else the cadence), rung (how many buckets the floor serves), behavior
-- and behavior_state (the collapsed claim on the value column, NULL and
-- 'none' where nobody spoke, 'contested' where the witness withholds it),
-- dims (the slicing columns, primary first), unadmitted (the served
-- columns no role stands on), owed (the gloss each of those wants),
-- wanted (the measurement the row reads and nobody ran), judged_current
-- (the profile stands at the data's own version).
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.metric_axes() AS TABLE
WITH ds AS (SELECT current_schema() AS dataset),
aspects AS (
  SELECT name, schema FROM (
    SELECT name, kind, schema, row_number() OVER (PARTITION BY name ORDER BY seq DESC) AS rn
    FROM glossql.aspects) WHERE rn = 1 AND kind = 'QUERY'),
views AS (
  SELECT t.table_name AS metric FROM information_schema.tables t, ds
  WHERE t.table_catalog = current_database() AND t.table_schema = ds.dataset AND t.table_type = 'VIEW'
    AND t.table_name IN (SELECT name FROM aspects)),
bodies AS (
  SELECT aspect AS metric, body FROM (
    SELECT g.aspect, g.body, row_number() OVER (PARTITION BY g.aspect ORDER BY g.seq DESC) AS rn
    FROM glossql.glossary g, ds
    WHERE g.dataset = ds.dataset AND g.subject = ds.dataset AND g.aspect IN (SELECT name FROM aspects)) WHERE rn = 1),
cols AS (
  SELECT c.table_name AS metric, c.column_name AS col, c.ordinal_position AS pos
  FROM information_schema.columns c, ds
  WHERE c.table_catalog = current_database() AND c.table_schema = ds.dataset
    AND c.table_name IN (SELECT metric FROM views)),
slots AS MATERIALIZED (
  SELECT split_part(subject, '.', 2) AS metric, split_part(subject, '.', 3) AS col, aspect, value, state
  FROM gloss_glossary(current_schema(), NULL, false)
  WHERE aspect IN ('role', 'behavior', 'dimension', 'ladder', 'temporal_profile')
    AND len(string_split(subject, '.')) = 3
    AND split_part(subject, '.', 2) IN (SELECT metric FROM bodies)),
claims AS (SELECT * FROM slots WHERE state IN ('current', 'stale') AND value IS NOT NULL),
roles AS (SELECT metric, col, value->>'value' AS role FROM claims WHERE aspect = 'role'),
periods AS (
  SELECT r.metric, list(r.col ORDER BY CASE WHEN r.col = 'period' THEN 0 ELSE 1 END, c.pos) AS cols
  FROM roles r JOIN cols c USING (metric, col) WHERE r.role = 'timestamp' GROUP BY r.metric),
measures AS (
  SELECT r.metric, list(r.col ORDER BY CASE WHEN r.col = 'value' THEN 0 ELSE 1 END, c.pos) AS cols
  FROM roles r JOIN cols c USING (metric, col) WHERE r.role = 'measure' GROUP BY r.metric),
chosen AS (
  SELECT b.metric,
         CASE WHEN len(p.cols) = 1 OR p.cols[1] = 'period' THEN p.cols[1] END AS period_column,
         CASE WHEN len(m.cols) = 1 OR m.cols[1] = 'value' THEN m.cols[1] END AS value_column,
         p.cols AS period_cols, m.cols AS measure_cols,
         b.metric IN (SELECT metric FROM views) AS served
  FROM bodies b LEFT JOIN periods p USING (metric) LEFT JOIN measures m USING (metric)),
behaviors AS (
  SELECT s.metric, s.col,
         CASE WHEN s.state IN ('current', 'stale') THEN s.value->>'value' END AS behavior,
         CASE WHEN s.state IN ('current', 'stale') THEN s.value->>'numerator' END AS numerator,
         CASE WHEN s.state IN ('current', 'stale') THEN s.value->>'denominator' END AS denominator,
         CASE WHEN s.state IN ('current', 'stale') THEN s.state ELSE 'contested' END AS behavior_state
  FROM slots s WHERE s.aspect = 'behavior' AND s.state <> 'unassessed'),
profiles AS (
  SELECT metric, col,
         CASE WHEN (value->>'applicable') = 'true' AND (value->>'granularity') IN ('minute', 'hour', 'day', 'week', 'month', 'quarter', 'year')
              THEN value->>'granularity' END AS cadence,
         state = 'current' AS judged_current
  FROM claims WHERE aspect = 'temporal_profile'),
ladders AS (SELECT metric, col, value->>'floor' AS floor, value->'windows' AS windows FROM claims WHERE aspect = 'ladder'),
dims AS (
  SELECT r.metric, list(r.col ORDER BY CASE d.rank WHEN 'primary' THEN 0 WHEN 'supporting' THEN 1 ELSE 2 END, c.pos) AS cols
  FROM roles r JOIN cols c USING (metric, col)
  LEFT JOIN (SELECT metric, col, value->>'value' AS rank FROM claims WHERE aspect = 'dimension') d USING (metric, col)
  WHERE r.role = 'dimension' AND coalesce(d.rank, '') <> 'none'
  GROUP BY r.metric),
unadmitted AS (
  SELECT c.metric, list(c.col ORDER BY c.pos) AS cols,
         list('GLOSS role ON ' || ds.dataset || '.' || c.metric || '.' || c.col || ' AS $${"value": "<key|measure|dimension|timestamp|attribute>"}$$;' ORDER BY c.pos) AS acts
  FROM cols c, ds
  WHERE NOT EXISTS (SELECT 1 FROM roles r WHERE r.metric = c.metric AND r.col = c.col)
  GROUP BY c.metric),
rows_ AS (
  SELECT ch.metric,
         json_extract_string(a.schema, '$.x-kind') AS kind,
         json_extract_string(b.body, '$.stopped') AS stopped,
         ch.period_column, ch.value_column, ch.period_cols, ch.measure_cols, ch.served,
         bh.behavior, bh.numerator, bh.denominator, coalesce(bh.behavior_state, 'none') AS behavior_state,
         pr.cadence, coalesce(pr.judged_current, false) AS judged_current,
         coalesce(la.floor, pr.cadence) AS floor, la.windows,
         coalesce(d.cols, []::VARCHAR[]) AS dims,
         coalesce(u.cols, []::VARCHAR[]) AS unadmitted, coalesce(u.acts, []::VARCHAR[]) AS owed,
         pr.metric IS NOT NULL AS profiled
  FROM chosen ch
  JOIN aspects a ON a.name = ch.metric
  LEFT JOIN bodies b USING (metric)
  LEFT JOIN behaviors bh ON bh.metric = ch.metric AND bh.col = ch.value_column
  LEFT JOIN profiles pr ON pr.metric = ch.metric AND pr.col = ch.period_column
  LEFT JOIN ladders la ON la.metric = ch.metric AND la.col = ch.period_column
  LEFT JOIN dims d ON d.metric = ch.metric
  LEFT JOIN unadmitted u ON u.metric = ch.metric),
judged AS (
  SELECT *,
    CASE WHEN stopped IS NOT NULL THEN 'stopped: ' || stopped
         WHEN NOT served THEN 'the grounding serves no view'
         WHEN kind = 'fact' THEN 'a fact serves one number, read through fact_values()'
         WHEN kind = 'relation' THEN 'a relation serves rows, not a series'
         WHEN period_column IS NULL AND len(period_cols) > 1 THEN
              list_aggregate(period_cols, 'string_agg', ' and ') || ' carry role timestamp; the period is the column named period — serve it so, or gloss role attribute on the others'
         WHEN period_column IS NULL THEN
              'no column carries role timestamp: gloss role on the period column, GLOSS role ON ' || ds.dataset || '.' || metric || '.<column> AS $${"value": "timestamp"}$$'
         WHEN value_column IS NULL AND len(measure_cols) > 1 THEN
              list_aggregate(measure_cols, 'string_agg', ' and ') || ' carry role measure; the value is the column named value — serve it so, or gloss role attribute on the others'
         WHEN value_column IS NULL THEN
              'no column carries role measure: gloss role on the value column, GLOSS role ON ' || ds.dataset || '.' || metric || '.<column> AS $${"value": "measure"}$$'
         WHEN floor IS NULL AND NOT profiled THEN
              'no cadence stands on ' || period_column || ': SELECT temporal() FROM ' || ds.dataset || '.' || metric || '.' || period_column || ', or gloss ladder with a floor'
         WHEN floor IS NULL THEN
              'the profile finds no cadence on ' || period_column || ': gloss ladder with a floor, GLOSS ladder ON ' || ds.dataset || '.' || metric || '.' || period_column || ' AS $${"floor": "<grain>"}$$'
    END AS reason,
    CASE WHEN period_column IS NOT NULL AND NOT profiled AND stopped IS NULL AND coalesce(kind, '') NOT IN ('fact', 'relation')
         THEN ['temporal over ' || ds.dataset || '.' || metric || '.' || period_column] ELSE []::VARCHAR[] END AS wanted
  FROM rows_, ds)
SELECT metric, kind, stopped, reason IS NULL AS applicable, reason,
       period_column, value_column, numerator, denominator, cadence, floor,
       CASE WHEN floor IS NOT NULL THEN coalesce(json_extract(windows, '$.' || floor)::INTEGER, glossql.rung_default(floor)) END AS rung,
       behavior, behavior_state, dims, unadmitted, owed, wanted, judged_current
FROM judged
ORDER BY metric;

--===------------------------------------------------------------------===--
-- metric_series(grain := NULL): the cells of every applicable metric of
-- the bound dataset — metric, world ('actual'), dimension ('' the total,
-- else a slicing column), member, period (the bucket's start), value,
-- num and den (a ratio's summed halves), behavior. Without a grain, each
-- metric at its own floor; with one, each metric at or below it at that
-- grain, a metric coarser than the asked grain serving no rows. A rung of
-- n buckets is served, the edge's bucket the last. A flow sums and fills
-- its total's empty periods with zero up to the edge; a stock keeps the
-- bucket's last level, summed over what stands at that instant; a ratio
-- re-divides its summed halves. A dimension wider than 24 members serves
-- its top 23 by weight and 'other'.
--
-- The no-claim rule: where rows have to be combined — a coarser grain than
-- the floor, or several rows in one period — and no behaviour stands on
-- the value column, the read refuses and names the gloss. It defaults
-- nothing.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.metric_series(grain := NULL) AS TABLE
WITH asked AS MATERIALIZED (
  SELECT m.*, coalesce(grain, m.floor) AS at_grain
  FROM glossql.metric_axes() m
  WHERE m.applicable AND (grain IS NULL OR glossql.grain_rank(grain) >= glossql.grain_rank(m.floor))),
cells AS (
  SELECT * FROM dataset_cells(current_schema(), true) WHERE "table" IN (SELECT metric FROM asked)),
rows_ AS MATERIALIZED (
  SELECT c."table" AS metric, c.rn,
         max(CASE WHEN c."column" = a.period_column THEN TRY_CAST(c.val AS TIMESTAMP) END) AS instant,
         max(CASE WHEN c."column" = a.value_column THEN TRY_CAST(c.val AS DOUBLE) END) AS value,
         max(CASE WHEN c."column" = a.numerator THEN TRY_CAST(c.val AS DOUBLE) END) AS num,
         max(CASE WHEN c."column" = a.denominator THEN TRY_CAST(c.val AS DOUBLE) END) AS den,
         list({'dimension': c."column", 'member': coalesce(c.val, 'NULL')}) FILTER (WHERE list_contains(a.dims, c."column")) AS members
  FROM cells c JOIN asked a ON a.metric = c."table"
  GROUP BY c."table", c.rn),
combining AS (
  SELECT a.metric, a.value_column, a.behavior_state,
         glossql.grain_rank(a.at_grain) > glossql.grain_rank(a.floor) AS coarser,
         count(*) > count(DISTINCT r.instant) AS shared
  FROM asked a JOIN rows_ r USING (metric)
  WHERE a.behavior IS NULL
  GROUP BY a.metric, a.value_column, a.behavior_state, a.at_grain, a.floor),
refused AS (
  SELECT CASE WHEN count(*) > 0 THEN error(string_agg(
           CASE WHEN behavior_state = 'contested' THEN 'the behaviour on ' || current_schema() || '.' || metric || '.' || value_column || ' is contested'
                ELSE 'no behaviour stands on ' || current_schema() || '.' || metric || '.' || value_column END
           || CASE WHEN coarser THEN ', and ' || metric || ' is asked at a coarser grain than its floor' ELSE ', and ' || metric || ' serves several rows in one period' END
           || ': gloss it — GLOSS behavior ON ' || current_schema() || '.' || metric || '.' || value_column || ' AS $${"value": "<flow|stock|ratio>"}$$', '; ')) END AS refusal
  FROM combining WHERE coarser OR shared),
bucketed AS (
  SELECT r.metric, r.instant, date_trunc(a.at_grain, r.instant) AS period, r.value, r.num, r.den, r.members,
         coalesce(a.behavior, 'none') AS behavior, a.at_grain, a.rung,
         max(r.instant) OVER (PARTITION BY r.metric, date_trunc(a.at_grain, r.instant)) AS last_at
  FROM rows_ r JOIN asked a USING (metric)
  WHERE r.instant IS NOT NULL),
edges AS (
  SELECT metric, min(period) AS first, max(period) AS edge, any_value(at_grain) AS at_grain, any_value(rung) AS rung,
         any_value(behavior) AS behavior
  FROM bucketed GROUP BY metric),
windowed AS (
  SELECT b.* FROM bucketed b JOIN edges e USING (metric)
  WHERE b.period > e.edge - glossql.grain_step(e.at_grain) * e.rung),
total AS (
  SELECT metric, '' AS dimension, '' AS member, period, behavior,
         CASE behavior WHEN 'stock' THEN sum(value) FILTER (WHERE instant = last_at)
                       WHEN 'ratio' THEN sum(num) / nullif(sum(den), 0)
                       ELSE sum(value) END AS value,
         CASE behavior WHEN 'ratio' THEN sum(num) END AS num,
         CASE behavior WHEN 'ratio' THEN sum(den) END AS den
  FROM windowed GROUP BY metric, period, behavior),
filled AS (
  SELECT e.metric, '' AS dimension, '' AS member, g.period, e.behavior, 0.0 AS value, NULL::DOUBLE AS num, NULL::DOUBLE AS den
  FROM edges e, unnest(generate_series(greatest(e.first, e.edge - glossql.grain_step(e.at_grain) * (e.rung - 1)), e.edge, glossql.grain_step(e.at_grain))) AS g(period)
  WHERE e.behavior = 'flow'
    AND NOT EXISTS (SELECT 1 FROM total t WHERE t.metric = e.metric AND t.period = g.period)),
long AS (
  SELECT w.metric, u.m.dimension AS dimension, u.m.member AS member, w.instant, w.period, w.value, w.num, w.den, w.behavior,
         max(w.instant) OVER (PARTITION BY w.metric, u.m.dimension, u.m.member, w.period) AS member_last_at
  FROM windowed w, unnest(w.members) AS u(m)),
weights AS (
  SELECT metric, dimension, member,
         row_number() OVER (PARTITION BY metric, dimension ORDER BY sum(abs(value)) DESC NULLS LAST, member) AS rank
  FROM long GROUP BY metric, dimension, member),
member_cells AS (
  SELECT l.metric, l.dimension, CASE WHEN wt.rank <= 23 THEN l.member ELSE 'other' END AS member, l.period, l.behavior,
         CASE l.behavior WHEN 'stock' THEN sum(l.value) FILTER (WHERE l.instant = l.member_last_at)
                         WHEN 'ratio' THEN sum(l.num) / nullif(sum(l.den), 0)
                         ELSE sum(l.value) END AS value,
         CASE l.behavior WHEN 'ratio' THEN sum(l.num) END AS num,
         CASE l.behavior WHEN 'ratio' THEN sum(l.den) END AS den
  FROM long l JOIN weights wt USING (metric, dimension, member)
  GROUP BY l.metric, l.dimension, CASE WHEN wt.rank <= 23 THEN l.member ELSE 'other' END, l.period, l.behavior)
SELECT metric, 'actual' AS world, dimension, member, period, value, num, den, behavior
FROM (SELECT * FROM total UNION ALL SELECT * FROM filled UNION ALL SELECT * FROM member_cells), refused
WHERE refused.refusal IS NULL
ORDER BY metric, dimension, member, period;

--===------------------------------------------------------------------===--
-- fact_values(): the declared facts of the bound dataset, served whole —
-- one row per metric whose aspect declares x-kind fact: metric, value where
-- the view is one row with a value column, reason where it is not.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.fact_values() AS TABLE
WITH facts AS (SELECT metric, stopped FROM glossql.metric_axes() WHERE kind = 'fact'),
cells AS (SELECT * FROM dataset_cells(current_schema(), true) WHERE "table" IN (SELECT metric FROM facts)),
shape AS (
  SELECT "table" AS metric, count(DISTINCT rn) AS rows_, bool_or("column" = 'value') AS has_value,
         max(CASE WHEN "column" = 'value' THEN TRY_CAST(val AS DOUBLE) END) AS value
  FROM cells GROUP BY "table")
SELECT f.metric,
       CASE WHEN f.stopped IS NULL AND s.rows_ = 1 AND s.has_value THEN s.value END AS value,
       CASE WHEN f.stopped IS NOT NULL THEN 'stopped: ' || f.stopped
            WHEN s.metric IS NULL THEN 'the view serves no row'
            WHEN NOT s.has_value THEN 'the view serves no value column'
            WHEN s.rows_ <> 1 THEN 'the view serves ' || s.rows_ || ' rows, not one' END AS reason
FROM facts f LEFT JOIN shape s USING (metric)
ORDER BY f.metric;

--===------------------------------------------------------------------===--
-- metric_sources(metric): what feeds a metric — one row per served field
-- and the landed column it descends from, through gloss_lineage over the
-- view: metric, field, source (table.column), table_name; a computed
-- field with no column behind it has no row; beside those, one row per
-- table the view reads, table_name alone.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.metric_sources(metric) AS TABLE
WITH lineage AS (
  SELECT "column" AS field, derivation FROM gloss_lineage('SELECT * FROM ' || current_schema() || '.' || metric)),
fields AS (
  SELECT field, unnest(regexp_extract_all(derivation, '([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*):', 1)) AS path
  FROM lineage),
sources AS (
  SELECT DISTINCT field, split_part(path, '.', 2) || '.' || split_part(path, '.', 3) AS source, split_part(path, '.', 2) AS table_name
  FROM fields)
SELECT metric AS metric, field, source, table_name FROM sources
UNION ALL
SELECT DISTINCT metric AS metric, NULL, NULL, table_name FROM sources
ORDER BY table_name, field NULLS FIRST;
