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
-- plan_of and covers (a plan's metric and the grain its rows are booked
-- against), stopped (the author's text), applicable, reason (the road out
-- when it is not), period_column, value_column, numerator and denominator
-- (a ratio's halves), cadence (the period column's measured granularity),
-- floor (the ladder's, else the cadence), rung (how many buckets the floor serves), behavior
-- and behavior_state (the collapsed claim on the value column, NULL and
-- 'none' where nobody spoke, 'contested' where the witness withholds it),
-- dims (the slicing columns, primary first), unranked (the dimension
-- columns no dimension gloss ranks), unadmitted (the served columns no
-- role stands on), owed (the gloss each of those wants),
-- wanted (the measurement the row reads and nobody ran), judged_current
-- (the profile stands at the data's own version). A plan (x-kind plan) is
-- read through the metric it names: its period and value are the columns
-- named as that metric's, or period and value, its behaviour the metric's,
-- its floor the grain its rows are booked against where the declaration
-- says so (x-covers); nothing is glossed on a plan's own columns. A served
-- plan's row wants the judgement the futures kit measures on the plan
-- (metric_plan over the plan) until one stands, as a metric's row wants
-- the profile over its period column.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.metric_axes() AS TABLE
WITH ds AS (SELECT current_schema() AS dataset),
aspects AS (
  SELECT name, schema FROM (
    SELECT name, kind, schema, row_number() OVER (PARTITION BY name ORDER BY seq DESC) AS rn
    FROM glossql.aspects) WHERE rn = 1 AND kind = 'QUERY'),
views AS (
  SELECT DISTINCT c.table_name AS metric FROM gloss_columns(current_schema()) c
  WHERE c.table_type = 'VIEW' AND c.table_name IN (SELECT name FROM aspects)),
bodies AS (
  SELECT aspect AS metric, body FROM (
    SELECT g.aspect, g.body, row_number() OVER (PARTITION BY g.aspect ORDER BY g.seq DESC) AS rn
    FROM glossql.glossary g, ds
    WHERE g.dataset = ds.dataset AND g.subject = ds.dataset AND g.aspect IN (SELECT name FROM aspects)) WHERE rn = 1),
cols AS (
  SELECT c.table_name AS metric, c.column_name AS col, c.ordinal_position AS pos
  FROM gloss_columns(current_schema()) c
  WHERE c.table_name IN (SELECT metric FROM views)),
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
unranked AS (
  SELECT r.metric, list(r.col ORDER BY c.pos) AS cols
  FROM roles r JOIN cols c USING (metric, col)
  WHERE r.role = 'dimension'
    AND NOT EXISTS (SELECT 1 FROM claims d WHERE d.aspect = 'dimension' AND d.metric = r.metric AND d.col = r.col)
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
         json_extract_string(a.schema, '$.x-plan-of') AS plan_of,
         json_extract_string(a.schema, '$.x-covers') AS covers,
         json_extract_string(b.body, '$.stopped') AS stopped,
         ch.period_column, ch.value_column, ch.period_cols, ch.measure_cols, ch.served,
         bh.behavior, bh.numerator, bh.denominator, coalesce(bh.behavior_state, 'none') AS behavior_state,
         pr.cadence, coalesce(pr.judged_current, false) AS judged_current,
         coalesce(la.floor, pr.cadence) AS floor, la.windows,
         coalesce(d.cols, []::VARCHAR[]) AS dims, coalesce(ur.cols, []::VARCHAR[]) AS unranked,
         coalesce(u.cols, []::VARCHAR[]) AS unadmitted, coalesce(u.acts, []::VARCHAR[]) AS owed,
         pr.metric IS NOT NULL AS profiled
  FROM chosen ch
  JOIN aspects a ON a.name = ch.metric
  LEFT JOIN bodies b USING (metric)
  LEFT JOIN behaviors bh ON bh.metric = ch.metric AND bh.col = ch.value_column
  LEFT JOIN profiles pr ON pr.metric = ch.metric AND pr.col = ch.period_column
  LEFT JOIN ladders la ON la.metric = ch.metric AND la.col = ch.period_column
  LEFT JOIN dims d ON d.metric = ch.metric
  LEFT JOIN unranked ur ON ur.metric = ch.metric
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
  FROM rows_, ds WHERE coalesce(kind, '') <> 'plan'),
-- a plan's row: its columns through the metric it plans
plan_cols AS (SELECT metric, list(col ORDER BY pos) AS cols FROM cols GROUP BY metric),
plans AS (
  SELECT p.metric, p.kind, p.plan_of, p.covers, p.stopped, p.served,
         CASE WHEN list_contains(pc.cols, m.period_column) THEN m.period_column WHEN list_contains(pc.cols, 'period') THEN 'period' END AS period_column,
         CASE WHEN list_contains(pc.cols, m.value_column) THEN m.value_column WHEN list_contains(pc.cols, 'value') THEN 'value' END AS value_column,
         m.metric IS NOT NULL AS of_declared, m.reason IS NULL AS of_applicable, m.reason AS of_reason,
         m.period_column AS of_period, m.value_column AS of_value,
         m.numerator, m.denominator, m.behavior, m.behavior_state
  FROM rows_ p
  LEFT JOIN plan_cols pc ON pc.metric = p.metric
  LEFT JOIN judged m ON m.metric = p.plan_of
  WHERE p.kind = 'plan'),
judged_plans AS (
  SELECT *,
    CASE WHEN stopped IS NOT NULL THEN 'stopped: ' || stopped
         WHEN plan_of IS NULL THEN 'a plan names the metric it plans: declare it with x-plan-of'
         WHEN NOT of_declared THEN 'x-plan-of names ' || plan_of || ', which no QUERY aspect declares on this dataset'
         WHEN NOT served THEN 'the grounding serves no view'
         WHEN covers IS NOT NULL AND covers NOT IN ('quarter', 'year') THEN 'x-covers names the grain a row is booked against, year or quarter — ' || covers || ' is neither'
         WHEN NOT of_applicable THEN 'the metric it plans is not served: ' || of_reason
         WHEN period_column IS NULL THEN 'the plan serves no column named ' || coalesce(of_period, 'period') || ' (its metric''s period) nor period'
         WHEN value_column IS NULL THEN 'the plan serves no column named ' || coalesce(of_value, 'value') || ' (its metric''s value) nor value'
    END AS reason
  FROM plans)
SELECT metric, kind, plan_of, covers, stopped, reason IS NULL AS applicable, reason,
       period_column, value_column, numerator, denominator, cadence, floor,
       CASE WHEN floor IS NOT NULL THEN coalesce(json_extract(windows, '$.' || floor)::INTEGER, glossql.rung_default(floor)) END AS rung,
       behavior, behavior_state, dims, unranked, unadmitted, owed, wanted, judged_current
FROM judged
UNION ALL
SELECT metric, kind, plan_of, covers, stopped, reason IS NULL AS applicable, reason,
       period_column, value_column, numerator, denominator, NULL AS cadence, covers AS floor, NULL AS rung,
       behavior, behavior_state, []::VARCHAR[], []::VARCHAR[], []::VARCHAR[], []::VARCHAR[],
       CASE WHEN reason IS NULL AND NOT EXISTS (SELECT 1 FROM glossql.measurements m
                                                WHERE m.dataset = current_schema() AND m.function = 'metric_plan'
                                                  AND m.subject = current_schema() || '.' || judged_plans.metric)
            THEN ['metric_plan over ' || current_schema() || '.' || metric] ELSE []::VARCHAR[] END,
       true
FROM judged_plans
ORDER BY metric;

--===------------------------------------------------------------------===--
-- metric_series(grain := NULL, world := 'actual', plan := NULL): the cells
-- of every applicable metric of the bound dataset — metric, world,
-- dimension ('' the total, else a slicing column), member, period (the
-- bucket's start), value, num and den (a ratio's summed halves), behavior.
-- Without a grain, each metric at its own floor; with one, each metric at
-- or below it at that grain, a metric coarser than the asked grain serving
-- no rows. A rung of n buckets is served, the edge's bucket the last. A
-- flow sums and fills its total's empty periods with zero up to the edge;
-- a stock keeps the bucket's last level, summed over what stands at that
-- instant; a ratio re-divides its summed halves. A dimension wider than 24
-- members serves its top 23 by weight and 'other'. The actual world is
-- this kit's; a plan (world := 'plan', plan := '<name>'), the forecast
-- (world := 'forecast') and a scenario (world := 'scenario', scenario :=
-- '<name>') are the futures kit's, served under the metric through
-- world_cells(), which stands empty until that kit is loaded.
--
-- The no-claim rule: where rows have to be combined — a coarser grain than
-- the floor, or several rows in one period — and no behaviour stands on
-- the value column, the read refuses and names the gloss. It defaults
-- nothing. A list over every metric asks unclaimed := 'skip' instead:
-- such a metric serves no cell, and its fact row says what it owes.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.world_cells(w, p, s, g) AS TABLE
SELECT NULL::VARCHAR AS metric, NULL::VARCHAR AS world, NULL::VARCHAR AS dimension, NULL::VARCHAR AS member,
       NULL::TIMESTAMP AS period, NULL::DOUBLE AS value, NULL::DOUBLE AS num, NULL::DOUBLE AS den, NULL::VARCHAR AS behavior
WHERE false;

CREATE OR REPLACE MACRO glossql.metric_series(grain := NULL, unclaimed := 'refuse', world := 'actual', plan := NULL, scenario := NULL) AS TABLE
WITH asked AS MATERIALIZED (
  SELECT m.*, coalesce(grain, m.floor) AS at_grain
  FROM glossql.metric_axes() m
  WHERE m.applicable AND coalesce(m.kind, '') <> 'plan' AND world = 'actual'
    AND (grain IS NULL OR glossql.grain_rank(grain) >= glossql.grain_rank(m.floor))),
cells AS (
  SELECT * FROM dataset_cells(current_schema(), 'views') WHERE "table" IN (SELECT metric FROM asked)),
all_rows AS MATERIALIZED (
  SELECT c."table" AS metric, c.rn,
         max(CASE WHEN c."column" = a.period_column THEN TRY_CAST(c.val AS TIMESTAMP) END) AS instant,
         max(CASE WHEN c."column" = a.value_column THEN TRY_CAST(c.val AS DOUBLE) END) AS value,
         max(CASE WHEN c."column" = a.numerator THEN TRY_CAST(c.val AS DOUBLE) END) AS num,
         max(CASE WHEN c."column" = a.denominator THEN TRY_CAST(c.val AS DOUBLE) END) AS den,
         list({'dimension': c."column", 'member': coalesce(c.val, 'NULL')}) FILTER (WHERE list_contains(a.dims, c."column")) AS members
  FROM cells c JOIN asked a ON a.metric = c."table"
  GROUP BY c."table", c.rn),
-- a value column with no behaviour, a contested one, or one said not to
-- combine (none) cannot be combined over rows
combining AS (
  SELECT a.metric, a.value_column, a.behavior, a.behavior_state,
         glossql.grain_rank(a.at_grain) > glossql.grain_rank(a.floor) AS coarser,
         count(*) > count(DISTINCT r.instant) AS shared
  FROM asked a JOIN all_rows r USING (metric)
  WHERE a.behavior IS NULL OR a.behavior = 'none'
  GROUP BY a.metric, a.value_column, a.behavior, a.behavior_state, a.at_grain, a.floor),
refused AS (
  SELECT CASE WHEN count(*) > 0 AND unclaimed = 'refuse' THEN error(string_agg(
           CASE WHEN behavior_state = 'contested' THEN 'the behaviour on ' || current_schema() || '.' || metric || '.' || value_column || ' is contested'
                WHEN behavior = 'none' THEN 'the behaviour on ' || current_schema() || '.' || metric || '.' || value_column || ' is none, so its rows do not combine'
                ELSE 'no behaviour stands on ' || current_schema() || '.' || metric || '.' || value_column END
           || CASE WHEN coarser THEN ', and ' || metric || ' is asked at a coarser grain than its floor' ELSE ', and ' || metric || ' serves several rows in one period' END
           || ': gloss it — GLOSS behavior ON ' || current_schema() || '.' || metric || '.' || value_column || ' AS $${"value": "<flow|stock|ratio>"}$$', '; ')) END AS refusal
  FROM combining WHERE coarser OR shared),
-- the metrics served: every asked one but those that need a claim nobody made; the refusal gates the
-- cells served, below, where the rows are few
rows_ AS (
  SELECT r.* FROM all_rows r
  WHERE r.metric NOT IN (SELECT metric FROM combining WHERE coarser OR shared)),
bucketed AS (
  SELECT r.metric, r.instant, date_trunc(a.at_grain, r.instant) AS period, r.value, r.num, r.den, r.members,
         coalesce(a.behavior, 'none') AS behavior, a.at_grain, a.rung
  FROM rows_ r JOIN asked a USING (metric)
  WHERE r.instant IS NOT NULL),
edges AS (
  SELECT metric, min(period) AS first, max(period) AS edge, any_value(at_grain) AS at_grain, any_value(rung) AS rung,
         any_value(behavior) AS behavior
  FROM bucketed GROUP BY metric),
windowed AS (
  SELECT b.* FROM bucketed b JOIN edges e USING (metric)
  WHERE b.period > e.edge - glossql.grain_step(e.at_grain) * e.rung),
-- a stock's level in a bucket is what stands at the bucket's last instant: that instant, per stock metric and bucket,
-- grouped rather than windowed, since a window over every row of every flow costs the read more than the rest
levels AS (
  SELECT metric, period, max(instant) AS last_at FROM windowed WHERE behavior = 'stock' GROUP BY metric, period),
total AS (
  SELECT w.metric, '' AS dimension, '' AS member, w.period, w.behavior,
         CASE w.behavior WHEN 'stock' THEN sum(w.value) FILTER (WHERE w.instant = l.last_at)
                         WHEN 'ratio' THEN sum(w.num) / nullif(sum(w.den), 0)
                         ELSE sum(w.value) END AS value,
         CASE w.behavior WHEN 'ratio' THEN sum(w.num) END AS num,
         CASE w.behavior WHEN 'ratio' THEN sum(w.den) END AS den
  FROM windowed w LEFT JOIN levels l USING (metric, period) GROUP BY w.metric, w.period, w.behavior),
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
UNION ALL
SELECT metric, world, dimension, member, period, value, num, den, behavior
FROM glossql.world_cells(world, plan, scenario, grain) WHERE world <> 'actual'
ORDER BY metric, dimension, member, period;

--===------------------------------------------------------------------===--
-- fact_values(): the declared facts of the bound dataset, served whole —
-- one row per metric whose aspect declares x-kind fact: metric, value where
-- the view is one row with a value column, reason where it is not.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.fact_values() AS TABLE
WITH facts AS (SELECT metric, stopped FROM glossql.metric_axes() WHERE kind = 'fact'),
cells AS (SELECT * FROM dataset_cells(current_schema(), 'views') WHERE "table" IN (SELECT metric FROM facts)),
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
-- metric_sources(for_metric := NULL): what feeds each metric of the bound
-- dataset, or one — one row per served field and the landed column it
-- descends from, through the views' lineage: metric, field, source
-- (table.column), table_name; a computed field with no column behind it
-- has no row; beside those, one row per table the view reads, table_name
-- alone; a view that does not plan serves one row with reason.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.metric_sources(for_metric := NULL) AS TABLE
WITH metrics AS (
  SELECT m.metric FROM glossql.metric_axes() m WHERE m.stopped IS NULL AND (for_metric IS NULL OR m.metric = for_metric)),
lineage AS (
  SELECT l.view AS metric, l."column" AS field, l.derivation, l.complete
  FROM gloss_views_lineage(current_schema()) l WHERE l.view IN (SELECT metric FROM metrics)),
-- a field descends from the columns its derivation names; an aggregate
-- descends from no column — it is computed over them
fields AS (
  SELECT metric, field, unnest(regexp_extract_all(derivation, '([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*):', 1)) AS path,
         regexp_matches(derivation, '^(sum|count|avg|mean|min|max|median|mode|stddev|stddev_pop|stddev_samp|var_pop|var_samp|variance|string_agg|list|array_agg|arg_min|arg_max|first|last|any_value|bool_and|bool_or|quantile|quantile_cont|quantile_disc|approx_count_distinct|product)\(') AS aggregate
  FROM lineage WHERE field IS NOT NULL),
tables AS (
  SELECT DISTINCT metric, split_part(path, '.', 2) AS table_name FROM fields),
sources AS (
  SELECT DISTINCT metric, field, split_part(path, '.', 2) || '.' || split_part(path, '.', 3) AS source, split_part(path, '.', 2) AS table_name
  FROM fields WHERE NOT aggregate)
SELECT metric, field, source, table_name, NULL::VARCHAR AS reason FROM sources
UNION ALL
SELECT metric, NULL, NULL, table_name, NULL FROM tables
UNION ALL
SELECT metric, NULL, NULL, NULL, 'the grounding does not plan' FROM lineage WHERE field IS NULL
ORDER BY metric, table_name NULLS LAST, field NULLS FIRST;

--===------------------------------------------------------------------===--
-- The two goals this kit answers for a door's next read. A route is its
-- steps in order, one arm per step; an arm serves a row when its
-- condition holds on the record, and the lowest step that serves decides
-- the goal. The door composes one next read from every routes_<kit>()
-- macro: this one serves its CTEs and its arms as SQL text over the
-- relations the door reads once — ds (the bound dataset), axes (the
-- fact rows), bodies (the standing grounding per metric). Columns of an
-- arm: surface, step, state (next, blocked, done), act, say, why,
-- statement (the act as a statement, filled from the record; <...>
-- marks what only the author can fill), "then".
--
-- metrics: every metric the dataset claims is served, or stopped.
-- slices: every applicable metric slices by its dimension columns.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.routes_metrics() AS TABLE
SELECT $ctes$
metrics_aspects AS (
  SELECT name, schema FROM (
    SELECT name, kind, schema, row_number() OVER (PARTITION BY name ORDER BY seq DESC) AS rn
    FROM glossql.aspects) WHERE rn = 1 AND kind = 'QUERY'),
metrics_declared AS (
  SELECT name FROM metrics_aspects WHERE coalesce(json_extract_string(schema, '$.x-kind'), '') <> 'plan'),
metrics_claimed AS (
  SELECT a.name FROM metrics_declared a, ds
  WHERE EXISTS (SELECT 1 FROM glossql.glossary g WHERE g.dataset = ds.dataset AND g.subject = ds.dataset AND g.aspect = a.name)
     OR EXISTS (SELECT 1 FROM glossql.glossary d WHERE d.dataset = ds.dataset AND d.subject = ds.dataset AND d.aspect = 'definitions'
                AND json_extract(d.body, '$.definitions.' || a.name) IS NOT NULL)),
metrics_read_tables AS (
  SELECT DISTINCT split_part(path, '.', 2) AS table_name
  FROM (SELECT unnest(regexp_extract_all(derivation, '([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*):', 1)) AS path
        FROM gloss_views_lineage(current_schema()))),
metrics_unread AS (
  SELECT nullif(string_agg(t.table_name, ', ' ORDER BY t.table_name), '') AS tables
  FROM (SELECT DISTINCT table_name FROM gloss_columns(current_schema(), false)) t
  WHERE t.table_name NOT IN (SELECT table_name FROM metrics_read_tables))
$ctes$ AS ctes, $arms$
SELECT 'metrics' AS surface, 1 AS step, 'next' AS state, 'GLOSS definitions' AS act,
       'claim ' || a.name || ' for ' || ds.dataset AS say,
       a.name || ' is declared and no dataset claims it: its definitions entry or its grounding makes it this dataset''s' AS why,
       'GLOSS definitions ON ' || ds.dataset || ' AS $${"definitions": {"' || a.name || '": {"unit": "<unit>", "meaning": "<what it measures, in the business''s words>"}}}$$;' AS statement,
       'GLOSS ' || a.name || ' ON ' || ds.dataset || ' AS $${"sql": "<the grounding>"}$$;' AS "then"
FROM metrics_declared a, ds
WHERE NOT EXISTS (SELECT 1 FROM metrics_claimed)
UNION ALL
SELECT 'metrics', 2, 'next', 'DECLARE ASPECT query',
       'declare the first concept',
       'no metric is claimed by this dataset: a declared QUERY aspect is this dataset''s once its definitions entry or its grounding stands; the names are the human''s',
       '', ''
FROM ds WHERE NOT EXISTS (SELECT 1 FROM metrics_claimed)
UNION ALL
SELECT 'metrics', 3, 'next', 'GLOSS query',
       'ground ' || c.name || ', or stop it',
       c.name || ' is declared and not grounded: a declaration nobody grounds holds this goal — ground it, or stop it with the reason no number is served',
       'GLOSS ' || c.name || ' ON ' || ds.dataset || ' AS $${' || chr(10)
         || '  "sql": "SELECT <period column> AS period, <measure> AS value, <slicing columns> FROM <table>",' || chr(10)
         || '  "assumptions": [{"dimension": "<scope|definition>", "key": "<stable-key>", "assumption": "<the judgment call, and the reading you rejected>", "basis": "<what it rests on>", "confidence": 0.7}]' || chr(10)
         || '}$$;' || chr(10)
         || '-- or, where no number should be served: GLOSS ' || c.name || ' ON ' || ds.dataset || ' AS $${"stopped": "<what is missing, and why no number is served>"}$$;',
       ''
FROM metrics_claimed c, ds
WHERE NOT EXISTS (SELECT 1 FROM axes a WHERE a.metric = c.name)
UNION ALL
SELECT 'metrics', 4, 'next', 'temporal',
       'profile ' || a.period_column || ' of ' || a.metric,
       a.reason,
       'SELECT temporal() FROM ' || ds.dataset || '.' || a.metric || '.' || a.period_column,
       ''
FROM axes a, ds
WHERE NOT a.applicable AND a.stopped IS NULL AND coalesce(a.kind, '') <> 'plan' AND len(a.wanted) > 0
UNION ALL
SELECT 'metrics', 5, 'next', 'GLOSS role',
       'gloss the columns of ' || a.metric,
       a.reason,
       list_aggregate(a.owed, 'string_agg', chr(10)),
       ''
FROM axes a, ds
WHERE NOT a.applicable AND a.stopped IS NULL AND coalesce(a.kind, '') NOT IN ('fact', 'relation', 'plan')
  AND len(a.wanted) = 0 AND len(a.owed) > 0 AND (a.period_column IS NULL OR a.value_column IS NULL)
UNION ALL
SELECT 'metrics', 6, 'next', 'GLOSS query',
       're-record ' || a.metric || ' — ' || split_part(a.reason, ':', 1),
       a.reason,
       'GLOSS ' || a.metric || ' ON ' || ds.dataset || ' AS $$' || coalesce(b.body, '<the standing body>') || '$$;',
       ''
FROM axes a LEFT JOIN bodies b ON b.metric = a.metric, ds
WHERE NOT a.applicable AND a.stopped IS NULL AND coalesce(a.kind, '') NOT IN ('fact', 'relation', 'plan')
  AND len(a.wanted) = 0 AND NOT (len(a.owed) > 0 AND (a.period_column IS NULL OR a.value_column IS NULL))
UNION ALL
SELECT 'metrics', 7, 'next', 'GLOSS behavior',
       'say how ' || a.metric || ' combines',
       'no behaviour stands on ' || a.value_column || ' of ' || a.metric || ': a coarser grain than ' || a.floor || ' and a total over its members need it — a flow sums, a stock keeps the period''s last level, a ratio re-divides its summed halves',
       'GLOSS behavior ON ' || ds.dataset || '.' || a.metric || '.' || a.value_column || ' AS $${"value": "<flow|stock|ratio>", "grounds": "<why, in the business''s words>"}$$;',
       'SELECT * FROM metric_series(grain := ' || chr(39) || a.floor || chr(39) || ') WHERE metric = ' || chr(39) || a.metric || chr(39)
FROM axes a, ds
WHERE a.applicable AND coalesce(a.kind, '') <> 'plan' AND a.behavior IS NULL AND a.behavior_state = 'none'
UNION ALL
SELECT 'metrics', 8, 'done', '', '',
       'every metric the dataset claims is served or stopped; the next concept is the human''s to name'
         || coalesce(' — unread: ' || u.tables, ''),
       '', ''
FROM ds CROSS JOIN metrics_unread u
UNION ALL
SELECT 'slices', 1, 'blocked', '', '', 'no applicable metric stands', '', ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable)
UNION ALL
SELECT 'slices', 2, 'next', 'GLOSS role',
       'gloss ' || a.unadmitted[1] || ' on ' || a.metric,
       a.unadmitted[1] || ' is served by ' || a.metric || ' and carries no role: a dimension slices the series, an attribute rides along',
       a.owed[1],
       ''
FROM axes a, ds
WHERE a.applicable AND len(a.unadmitted) > 0
UNION ALL
SELECT 'slices', 3, 'next', 'GLOSS dimension',
       'rank ' || a.unranked[1] || ' on ' || a.metric,
       a.unranked[1] || ' slices ' || a.metric || ' and no dimension gloss ranks it: primary leads the slices, supporting follows, none closes it',
       'GLOSS dimension ON ' || ds.dataset || '.' || a.metric || '.' || a.unranked[1] || ' AS $${"value": "<primary|supporting|none>"}$$;',
       ''
FROM axes a, ds
WHERE a.applicable AND len(a.unranked) > 0
UNION ALL
SELECT 'slices', 4, 'done', '', '',
       'every applicable metric slices by its dimension columns, ranked'
         || coalesce(' — no dimension on: ' || (SELECT string_agg(metric, ', ' ORDER BY metric) FROM axes WHERE applicable AND len(dims) = 0), ''),
       '', ''
FROM ds
$arms$ AS arms;
