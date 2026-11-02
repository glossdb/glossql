-- The futures kit: the worlds beside the actual — the walk's bands, the
-- forecast, the plan, the scenario's estimate — each what the model
-- service answers, landed as a measurement with its pin and reads, and
-- read back as SQL. Loaded after the metrics kit, whose series the
-- requests are built from. The service is named once for the instance
-- (gloss_service_url, gloss_service_token); every call goes through
-- glossql.service, one macro over gloss_service, so a suite stands in for
-- the service by re-declaring the macro. The service owns its topic: the
-- bands, the PIT and why it is withheld, the walk's verdict; the kit owns
-- the calendar, and sends complete periods only.
--
--   FROM gloss_kit('kits/kpi/kit.sql');
--   FROM gloss_kit('kits/quality/kit.sql');
--   FROM gloss_kit('kits/metrics/kit.sql');
--   FROM gloss_kit('kits/futures/kit.sql');

CREATE OR REPLACE MACRO glossql.service(route, body) AS gloss_service(route, body);

--===------------------------------------------------------------------===--
-- The walk. band_series(): every applicable metric's total at month grain
-- as the walk sends it — a month the data's edge falls inside is left out
-- where rows land finer than monthly, since a short sum against a corridor
-- fitted on whole months reads as a breach that is only the calendar.
-- band_request(): one row per metric — behavior, the complete months, and
-- whether the walk can take it: seven complete months at least, so that
-- one origin has the six known values the service needs. band_body(): the
-- request as the service's contract spells it, one table and a
-- specification, with the witness's threshold as the walk's red line.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.band_series() AS TABLE
WITH axes AS (SELECT * FROM glossql.metric_axes() WHERE applicable),
edges AS (
  SELECT c."table" AS metric, max(TRY_CAST(c.val AS TIMESTAMP)) AS edge,
         count(DISTINCT date_trunc('day', TRY_CAST(c.val AS TIMESTAMP))) > count(DISTINCT date_trunc('month', TRY_CAST(c.val AS TIMESTAMP))) AS finer
  FROM dataset_cells(current_schema(), true) c JOIN axes a ON a.metric = c."table" AND a.period_column = c."column"
  GROUP BY c."table"),
partials AS (
  SELECT metric, date_trunc('month', edge) AS period FROM edges
  WHERE finer AND edge::DATE < (date_trunc('month', edge) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE)
SELECT s.metric, s.period, s.value, s.behavior
FROM glossql.metric_series(grain := 'month', unclaimed := 'skip') s
WHERE s.dimension = '' AND s.world = 'actual' AND s.value IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM partials p WHERE p.metric = s.metric AND p.period = s.period)
ORDER BY s.metric, s.period;

CREATE OR REPLACE MACRO glossql.band_request() AS TABLE
WITH counted AS (SELECT metric, count(*) AS months FROM glossql.band_series() GROUP BY metric)
SELECT a.metric, a.behavior, coalesce(c.months, 0) AS months,
       a.applicable AND coalesce(c.months, 0) >= 7 AS walkable,
       CASE WHEN NOT a.applicable THEN a.reason
            WHEN c.metric IS NULL THEN 'the month read serves no cell: ' || coalesce(a.reason,
              'no behaviour stands on ' || current_schema() || '.' || a.metric || '.' || a.value_column || ', and the month read combines its rows — gloss it')
            WHEN c.months < 7 THEN 'the walk needs seven complete months, and ' || a.metric || ' has ' || c.months END AS reason
FROM glossql.metric_axes() a LEFT JOIN counted c USING (metric)
WHERE coalesce(a.kind, '') NOT IN ('fact', 'plan', 'scenario')
ORDER BY a.metric;

CREATE OR REPLACE MACRO glossql.band_body() AS TABLE
WITH s AS (SELECT * FROM glossql.band_series() WHERE metric IN (SELECT metric FROM glossql.band_request() WHERE walkable)),
t AS (SELECT coalesce((SELECT threshold FROM glossql.witnesses WHERE aspect = 'metric_bands' ORDER BY seq DESC LIMIT 1), 0.98) AS threshold)
SELECT CASE WHEN count(s.metric) > 0 THEN json_object(
         'spec', json_object('targets', ['value'], 'origins', 6, 'horizon', 1,
                             'levels', [0.05, 0.1, 0.5, 0.9, 0.95], 'grain', 'month', 'threshold', t.threshold),
         'table', json_object('member', list(s.metric ORDER BY s.metric, s.period),
                              'period', list(strftime(s.period, '%Y-%m') ORDER BY s.metric, s.period),
                              'status', list('actual' ORDER BY s.metric, s.period),
                              'value', list(s.value ORDER BY s.metric, s.period))) END AS body,
       t.threshold
FROM t LEFT JOIN s ON true
GROUP BY t.threshold;

--===------------------------------------------------------------------===--
-- metric_bands: the walk as a measurement on the dataset. served_by names
-- the service and its version; score, band and threshold are the
-- service's verdict over every metric walked; metrics holds one row per
-- metric — applicable and reason, behavior, walked, held_80 and held_90
-- (the share of walked points inside the 10-90 and the 5-95 band), newest
-- (the latest walked point: period, pit, displacement, withheld), errors
-- (the origins the service could not walk, in its words) and points, one
-- per walked month: period, actual, p05 to p95, pit, withheld.
--===------------------------------------------------------------------===--

DECLARE ASPECT metric_bands WITH $${
  "type": "object",
  "properties": {
    "served_by": {"type": ["string", "null"]},
    "grain": {"type": "string"},
    "score": {"type": ["number", "null"]},
    "band": {"type": ["string", "null"]},
    "threshold": {"type": ["number", "null"]},
    "metrics": {"type": "array", "items": {"type": "object"}}
  }
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION metric_bands FOR GLOBAL AS $$
  WITH req AS (SELECT * FROM glossql.band_request()),
  b AS (SELECT body, threshold FROM glossql.band_body()),
  a AS (SELECT CASE WHEN body IS NOT NULL THEN glossql.service('/forecast', body) END AS a, threshold FROM b),
  rows_ AS (
    SELECT unnest(json_extract(a, '$.table.member')::VARCHAR[]) AS metric,
           unnest(json_extract(a, '$.table.period')::VARCHAR[]) AS period,
           unnest(json_extract(a, '$.table.level')::DOUBLE[]) AS level,
           unnest(json_extract(a, '$.table.value')::DOUBLE[]) AS value,
           unnest(json_extract(a, '$.table.actual')::DOUBLE[]) AS actual,
           unnest(json_extract(a, '$.table.pit')::DOUBLE[]) AS pit,
           unnest(json_extract(a, '$.table.withheld')::VARCHAR[]) AS withheld
    FROM a WHERE a IS NOT NULL),
  points AS (
    SELECT metric, period, any_value(actual) AS actual,
           max(CASE WHEN level = 0.05 THEN value END) AS p05, max(CASE WHEN level = 0.1 THEN value END) AS p10,
           max(CASE WHEN level = 0.5 THEN value END) AS p50, max(CASE WHEN level = 0.9 THEN value END) AS p90,
           max(CASE WHEN level = 0.95 THEN value END) AS p95,
           any_value(pit) AS pit, any_value(withheld) AS withheld
    FROM rows_ WHERE period IS NOT NULL GROUP BY metric, period),
  verdicts AS (
    SELECT v.member AS metric, v.walked, v.held_80, v.held_90, v.newest
    FROM (SELECT unnest(from_json(json_extract(a, '$.answer.walk.members'),
            '[{"member": "VARCHAR", "column": "VARCHAR", "walked": "BIGINT", "held_80": "DOUBLE", "held_90": "DOUBLE", "newest": {"origin": "VARCHAR", "period": "VARCHAR", "pit": "DOUBLE", "displacement": "DOUBLE", "withheld": "VARCHAR"}}]')) AS v
          FROM a WHERE a IS NOT NULL)),
  failures AS (
    SELECT e.member AS metric, list(e.error ORDER BY e.origin) AS errors
    FROM (SELECT unnest(from_json(json_extract(a, '$.answer.errors'),
            '[{"member": "VARCHAR", "column": "VARCHAR", "origin": "VARCHAR", "error": "VARCHAR"}]')) AS e
          FROM a WHERE a IS NOT NULL)
    GROUP BY e.member),
  per_metric AS (
    SELECT r.metric, r.behavior,
           r.walkable AND EXISTS (SELECT 1 FROM points p WHERE p.metric = r.metric) AS applicable,
           CASE WHEN NOT r.walkable THEN r.reason
                WHEN NOT EXISTS (SELECT 1 FROM points p WHERE p.metric = r.metric) THEN coalesce(f.errors[1], 'the service answered no point') END AS reason,
           v.walked, v.held_80, v.held_90, v.newest, coalesce(f.errors, []) AS errors,
           (SELECT coalesce(list({'period': p.period, 'actual': p.actual, 'p05': p.p05, 'p10': p.p10, 'p50': p.p50,
                                  'p90': p.p90, 'p95': p.p95, 'pit': p.pit, 'withheld': p.withheld} ORDER BY p.period), [])
            FROM points p WHERE p.metric = r.metric) AS points
    FROM req r LEFT JOIN verdicts v USING (metric) LEFT JOIN failures f USING (metric))
  SELECT (SELECT json_extract_string(a, '$.answer.service') || ' ' || json_extract_string(a, '$.answer.version') FROM a) AS served_by,
         'month' AS grain,
         (SELECT json_extract(a, '$.answer.walk.score')::DOUBLE FROM a) AS score,
         (SELECT json_extract_string(a, '$.answer.walk.band') FROM a) AS band,
         (SELECT threshold FROM b) AS threshold,
         (SELECT coalesce(list({'metric': metric, 'applicable': applicable, 'reason': reason, 'behavior': behavior,
                                'walked': walked, 'held_80': held_80, 'held_90': held_90, 'newest': newest,
                                'errors': errors, 'points': points} ORDER BY metric), [])
          FROM per_metric) AS metrics
$$ RETURNS metric_bands;

-- The breach is the service's verdict, repeated by the witness's detector
-- over the measurement's own voice, so that ATTEST() carries it and the
-- checks goal routes on it; the threshold is the red line the walk is
-- asked with.
DECLARE FUNCTION band_breach FOR GLOBAL AS $$
  SELECT subject, body->>'band' AS band, (body->>'score')::DOUBLE AS score
  FROM slots WHERE speaker = 'function' AND body->>'band' IS NOT NULL
$$;

DECLARE WITNESS bands_w ON metric_bands DETECTOR band_breach THRESHOLD 0.98;

--===------------------------------------------------------------------===--
-- band_verdict(): the newest walk's verdict — served_by, grain, score,
-- band, threshold, computed_at — one row, none before a walk.
-- band_points(): the newest walk, one row per metric and walked month —
-- metric, applicable, reason, behavior, walked, held_80, held_90, period,
-- actual, p05 to p95, pit, withheld, displacement (|2·pit − 1|), newest
-- (the latest walked point, the one the verdict scores), point_seq; a
-- metric not walked serves one row with its reason and no numbers.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.band_verdict() AS TABLE
SELECT json_extract_string(value, '$.served_by') AS served_by, json_extract_string(value, '$.grain') AS grain,
       json_extract(value, '$.score')::DOUBLE AS score, json_extract_string(value, '$.band') AS band,
       json_extract(value, '$.threshold')::DOUBLE AS threshold, computed_at
FROM glossql.measurements
WHERE dataset = current_schema() AND function = 'metric_bands'
ORDER BY seq DESC LIMIT 1;

CREATE OR REPLACE MACRO glossql.band_points() AS TABLE
WITH newest_walk AS (
  SELECT value, computed_at FROM glossql.measurements
  WHERE dataset = current_schema() AND function = 'metric_bands' ORDER BY seq DESC LIMIT 1),
metrics AS (
  SELECT unnest(from_json(json_extract(value, '$.metrics'),
    '[{"metric": "VARCHAR", "applicable": "BOOLEAN", "reason": "VARCHAR", "behavior": "VARCHAR", "walked": "BIGINT",
       "held_80": "DOUBLE", "held_90": "DOUBLE",
       "newest": {"origin": "VARCHAR", "period": "VARCHAR", "pit": "DOUBLE", "displacement": "DOUBLE", "withheld": "VARCHAR"},
       "errors": ["VARCHAR"],
       "points": [{"period": "VARCHAR", "actual": "DOUBLE", "p05": "DOUBLE", "p10": "DOUBLE", "p50": "DOUBLE",
                   "p90": "DOUBLE", "p95": "DOUBLE", "pit": "DOUBLE", "withheld": "VARCHAR"}]}]')) AS m,
         computed_at
  FROM newest_walk),
walked AS (
  SELECT m.metric, m.applicable, m.reason, m.behavior, m.walked, m.held_80, m.held_90, m.newest.period AS newest_period,
         unnest(m.points) AS p, computed_at
  FROM metrics WHERE cardinality(m.points) > 0)
SELECT metric, applicable, reason, behavior, walked, held_80, held_90,
       p.period, p.actual, p.p05, p.p10, p.p50, p.p90, p.p95, p.pit, p.withheld,
       CASE WHEN p.pit IS NOT NULL THEN abs(2.0 * p.pit - 1.0) END AS displacement,
       p.period = newest_period AS newest,
       row_number() OVER (PARTITION BY metric ORDER BY p.period) AS point_seq, computed_at
FROM walked
UNION ALL
SELECT m.metric, m.applicable, m.reason, m.behavior, m.walked, m.held_80, m.held_90,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, computed_at
FROM metrics WHERE cardinality(m.points) = 0
ORDER BY metric, point_seq NULLS FIRST;

--===------------------------------------------------------------------===--
-- The bands goal for a door's next read: the walk stands and no band is
-- red. Served as CTEs and arms over the relations the door reads once
-- (ds, axes, owed, reds, bodies); futures_red is the metric whose newest
-- point the red verdict scores.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.routes_futures() AS TABLE
SELECT $ctes$
futures_walk AS (SELECT * FROM glossql.band_verdict()),
futures_red AS (
  SELECT metric, period FROM glossql.band_points()
  WHERE newest AND displacement > 0.9 AND EXISTS (SELECT 1 FROM futures_walk w WHERE w.band = 'red'))
$ctes$ AS ctes, $arms$
SELECT 'bands' AS surface, 1 AS step, 'blocked' AS state, '' AS act, '' AS say, 'no applicable metric stands' AS why, '' AS statement, '' AS "then"
FROM ds WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable)
UNION ALL
SELECT 'bands', 2, 'next', 'metric_bands', 'run the walk', o.why,
       'SELECT metric_bands() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds WHERE o.kind = 'never measured' AND o.subject = 'metric_bands'
UNION ALL
SELECT 'bands', 3, 'next', 'metric_bands', 'run the walk again', o.why,
       'SELECT metric_bands() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds WHERE o.kind = 're-measure' AND o.subject = 'metric_bands'
UNION ALL
SELECT 'bands', 4, 'done', '', '',
       'the walk stands; the red on ' || red.metric || ' at ' || red.period || ' carries the human''s ruling', '', ''
FROM futures_red red CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = red.metric AND r.key = 'band-' || red.period)
UNION ALL
SELECT 'bands', 5, 'blocked', '', '',
       red.metric || ' is red at ' || red.period || ' and the question stands for the human', '', ''
FROM futures_red red CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.open_questions q WHERE q.dataset = ds.dataset AND q.aspect = red.metric AND q.key = 'band-' || red.period)
UNION ALL
SELECT 'bands', 6, 'next', 'band_points',
       'judge ' || red.metric || ', red at ' || red.period,
       'the newest walked point of ' || red.metric || ' sits outside its corridor at ' || red.period
         || '; a shift and a defect breach alike. A defect is the grounding''s to fix; a shift is the human''s to hear — record it as an assumption under the key band-' || red.period || ', and the docket asks',
       'SELECT period, actual, p05, p50, p95, pit, withheld FROM band_points() WHERE metric = ''' || red.metric || ''' ORDER BY point_seq',
       '-- append to "assumptions" of the standing body: {"dimension": "definition", "key": "band-' || red.period || '", "assumption": "<a shift at ' || red.period || ', in the business''s words — or the defect and its fix>", "basis": "<what it rests on>", "confidence": 0.7}' || chr(10)
         || 'GLOSS ' || red.metric || ' ON ' || ds.dataset || ' AS $$' || coalesce(b.body, '<the standing body>') || '$$;'
FROM futures_red red LEFT JOIN bodies b ON b.metric = red.metric CROSS JOIN ds
UNION ALL
SELECT 'bands', 7, 'next', 'band_points',
       'read the walk behind the red verdict on ' || reds.subject,
       reds.witness || ' bands ' || reds.subject || ' red and the walk names no point',
       'SELECT metric, period, displacement FROM band_points() ORDER BY displacement DESC NULLS LAST', ''
FROM reds CROSS JOIN ds WHERE reds.witness = 'bands_w' AND NOT EXISTS (SELECT 1 FROM futures_red)
UNION ALL
SELECT 'bands', 8, 'blocked', '', '',
       'the walk stands and took no metric: ' || string_agg(p.metric || ' — ' || p.reason, '; ' ORDER BY p.metric), '', ''
FROM glossql.band_points() p
WHERE NOT EXISTS (SELECT 1 FROM glossql.band_points() WHERE applicable)
HAVING count(*) > 0
UNION ALL
SELECT 'bands', 9, 'done', '', '', 'the walk stands and no band is red, or the red carries a question or a ruling', '', ''
FROM ds
$arms$ AS arms;
