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
           unnest(coalesce(json_extract(a, '$.table.withheld')::VARCHAR[],
                           list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::VARCHAR))) AS withheld
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
-- The forecast. forecast_request(): one row per metric — grain (its
-- floor), horizon (31 days, 13 weeks, 12 months, 4 quarters; an hour or a
-- minute is finer than the grains the service measured its coverage at, a
-- year has too few periods), origin (the last period with a value),
-- history, the periods ahead, and for a flow the spans it asks totals
-- for: the periods left in the month (a day), the quarter (a week), the
-- quarter and the year (a month), the year (a quarter) that the first
-- period ahead falls in, where the horizon reaches the span's end; two
-- spans of one length are asked once and served under both names.
-- forecast_bodies(): one request per grain and set of totals, the history
-- as actual rows and the periods ahead as forecast rows, so the answer
-- carries their labels.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.forecast_request() AS TABLE
WITH axes AS (
  SELECT * FROM glossql.metric_axes() WHERE coalesce(kind, '') NOT IN ('fact', 'relation', 'plan', 'scenario')),
series AS (
  SELECT metric, period, value FROM glossql.metric_series(unclaimed := 'skip')
  WHERE dimension = '' AND world = 'actual' AND value IS NOT NULL),
hist AS (
  SELECT metric, count(*) AS n, max(period) AS origin, list({'period': period, 'value': value} ORDER BY period) AS history
  FROM series GROUP BY metric),
judged AS (
  SELECT a.metric, a.behavior, a.floor AS grain,
         CASE a.floor WHEN 'day' THEN 31 WHEN 'week' THEN 13 WHEN 'month' THEN 12 WHEN 'quarter' THEN 4 END AS horizon,
         h.n, h.origin, h.history,
         CASE WHEN NOT a.applicable THEN a.reason
              WHEN a.floor IN ('minute', 'hour') THEN 'a ' || a.floor || ' grain is finer than the grains the service measured its coverage at'
              WHEN a.floor = 'year' THEN 'a year grain has too few periods to forecast'
              WHEN h.metric IS NULL THEN 'the series read serves no cell: ' || coalesce(a.reason,
                'no behaviour stands on ' || current_schema() || '.' || a.metric || '.' || a.value_column || ' — gloss it')
              WHEN h.n < 6 THEN h.n || ' ' || a.floor || 's of history: a forecast needs 6' END AS reason
  FROM axes a LEFT JOIN hist h USING (metric)),
ahead AS (
  SELECT j.metric, list(g.period ORDER BY g.period) AS periods
  FROM judged j, unnest(generate_series(j.origin + glossql.grain_step(j.grain),
                                        j.origin + glossql.grain_step(j.grain) * (j.horizon + 1),
                                        glossql.grain_step(j.grain))) AS g(period)
  WHERE j.reason IS NULL GROUP BY j.metric),
spans AS (
  SELECT metric, list({'span': n, 'to_go': to_go} ORDER BY n, to_go) AS spans
  FROM (
    SELECT j.metric, s.to_go, j.horizon,
           count(*) FILTER (WHERE u.p < date_trunc(s.to_go, ah.periods[1]) + glossql.grain_step(s.to_go)) AS reached,
           least(count(*) FILTER (WHERE u.p < date_trunc(s.to_go, ah.periods[1]) + glossql.grain_step(s.to_go)), j.horizon) AS n
    FROM judged j JOIN ahead ah USING (metric),
         unnest(CASE j.grain WHEN 'day' THEN ['month'] WHEN 'week' THEN ['quarter'] WHEN 'month' THEN ['quarter', 'year']
                             WHEN 'quarter' THEN ['year'] ELSE []::VARCHAR[] END) AS s(to_go),
         unnest(ah.periods) AS u(p)
    WHERE j.behavior = 'flow'
    GROUP BY j.metric, s.to_go, j.horizon)
  WHERE n >= 1 AND reached <= horizon
  GROUP BY metric)
SELECT j.metric, j.behavior, j.grain, j.horizon, j.origin, j.reason, j.reason IS NULL AS applicable, j.history,
       coalesce(ah.periods[1:j.horizon], []::TIMESTAMP[]) AS ahead,
       coalesce(s.spans, []::STRUCT(span BIGINT, to_go VARCHAR)[]) AS spans
FROM judged j LEFT JOIN ahead ah USING (metric) LEFT JOIN spans s USING (metric)
ORDER BY j.metric;

CREATE OR REPLACE MACRO glossql.forecast_bodies() AS TABLE
WITH r AS (SELECT * FROM glossql.forecast_request() WHERE applicable),
rows_ AS (
  SELECT r.metric, r.grain, r.horizon, list_sort(list_distinct(list_transform(r.spans, lambda s: s.span))) AS totals,
         cell.period AS period, cell.value AS value, 'actual' AS status
  FROM r, unnest(r.history) AS hs(cell)
  UNION ALL
  SELECT r.metric, r.grain, r.horizon, list_sort(list_distinct(list_transform(r.spans, lambda s: s.span))),
         ap.p, NULL::DOUBLE, 'forecast'
  FROM r, unnest(r.ahead) AS ap(p))
SELECT grain, totals, horizon,
       json_object('spec', json_object('targets', ['value'], 'horizon', horizon, 'levels', [0.05, 0.1, 0.5, 0.9, 0.95],
                                       'grain', grain, 'totals', totals),
                   'table', json_object('member', list(metric ORDER BY metric, period),
                                        'period', list(strftime(period, '%Y-%m-%d') ORDER BY metric, period),
                                        'status', list(status ORDER BY metric, period),
                                        'value', list(value ORDER BY metric, period))) AS body
FROM rows_ GROUP BY grain, totals, horizon;

--===------------------------------------------------------------------===--
-- metric_forecast: the forecast as a measurement on the dataset, at an
-- origin: per metric its grain, origin, applicable and reason, and its
-- points — a step (one period past the origin) or a span (the total of
-- the first so many steps, to_go naming the month, the quarter or the year
-- it runs to) with its period, p05 to p95, and service_80 and service_90,
-- the service's held-out share of actuals inside the 80% and the 90% band
-- at that grain, step and span. No actual and no PIT land here: neither
-- is known at the origin; forecast_points() joins the actual at read.
--===------------------------------------------------------------------===--

DECLARE ASPECT metric_forecast WITH $${
  "type": "object",
  "properties": {
    "served_by": {"type": ["string", "null"]},
    "metrics": {"type": "array", "items": {"type": "object"}}
  }
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION metric_forecast FOR GLOBAL AS $$
  WITH req AS (SELECT * FROM glossql.forecast_request()),
  b AS (SELECT grain, totals, body, glossql.service('/forecast', body) AS a FROM glossql.forecast_bodies()),
  rows_ AS (
    SELECT unnest(json_extract(a, '$.table.member')::VARCHAR[]) AS metric,
           unnest(json_extract(a, '$.table.step')::BIGINT[]) AS step,
           unnest(json_extract(a, '$.table.span')::BIGINT[]) AS span,
           unnest(json_extract(a, '$.table.period')::VARCHAR[]) AS period,
           unnest(json_extract(a, '$.table.level')::DOUBLE[]) AS level,
           unnest(json_extract(a, '$.table.value')::DOUBLE[]) AS value
    FROM b),
  points AS (
    SELECT metric, step, span, any_value(period) AS period,
           max(CASE WHEN level = 0.05 THEN value END) AS p05, max(CASE WHEN level = 0.1 THEN value END) AS p10,
           max(CASE WHEN level = 0.5 THEN value END) AS p50, max(CASE WHEN level = 0.9 THEN value END) AS p90,
           max(CASE WHEN level = 0.95 THEN value END) AS p95
    FROM rows_ GROUP BY metric, step, span),
  coverage AS (
    SELECT b.grain, cv.c.step AS step, cv.c.span AS span, cv.c.low AS low, cv.c.high AS high, cv.c.share AS share
    FROM b, unnest(from_json(json_extract(b.a, '$.answer.coverage'),
                   '[{"step": "BIGINT", "span": "BIGINT", "low": "DOUBLE", "high": "DOUBLE", "share": "DOUBLE"}]')) AS cv(c)),
  failures AS (
    SELECT e.member AS metric, list(e.error ORDER BY e.error) AS errors
    FROM (SELECT unnest(from_json(json_extract(a, '$.answer.errors'),
            '[{"member": "VARCHAR", "column": "VARCHAR", "origin": "VARCHAR", "error": "VARCHAR"}]')) AS e FROM b)
    GROUP BY e.member),
  req_spans AS (SELECT r.metric, sp.s.span AS span, sp.s.to_go AS to_go FROM req r, unnest(r.spans) AS sp(s)),
  named AS (
    SELECT p.metric, p.step, p.span, rs.to_go, p.period, p.p05, p.p10, p.p50, p.p90, p.p95
    FROM points p LEFT JOIN req_spans rs ON rs.metric = p.metric AND rs.span = p.span),
  per_metric AS (
    SELECT r.metric, r.behavior, r.grain, r.origin,
           r.applicable AND EXISTS (SELECT 1 FROM points p WHERE p.metric = r.metric) AS applicable,
           CASE WHEN NOT r.applicable THEN r.reason
                WHEN NOT EXISTS (SELECT 1 FROM points p WHERE p.metric = r.metric) THEN coalesce(f.errors[1], 'the service answered no point for this metric') END AS reason,
           (SELECT coalesce(list({'step': n.step, 'span': n.span, 'to_go': n.to_go, 'period': n.period,
                                  'p05': n.p05, 'p10': n.p10, 'p50': n.p50, 'p90': n.p90, 'p95': n.p95,
                                  'service_80': c80.share, 'service_90': c90.share} ORDER BY n.span NULLS FIRST, n.step, n.to_go), [])
            FROM named n
            LEFT JOIN coverage c80 ON c80.grain = r.grain AND c80.step = n.step AND c80.span IS NOT DISTINCT FROM n.span AND c80.low = 0.1 AND c80.high = 0.9
            LEFT JOIN coverage c90 ON c90.grain = r.grain AND c90.step = n.step AND c90.span IS NOT DISTINCT FROM n.span AND c90.low = 0.05 AND c90.high = 0.95
            WHERE n.metric = r.metric) AS points
    FROM req r LEFT JOIN failures f USING (metric))
  SELECT (SELECT string_agg(DISTINCT json_extract_string(a, '$.answer.service') || ' ' || json_extract_string(a, '$.answer.version'), ', ') FROM b) AS served_by,
         (SELECT coalesce(list({'metric': metric, 'applicable': applicable, 'reason': reason, 'behavior': behavior, 'grain': grain,
                                'origin': strftime(origin, '%Y-%m-%d'), 'points': points} ORDER BY metric), [])
          FROM per_metric) AS metrics
$$ RETURNS metric_forecast;

-- The forecast's witness says a forecast stands: green once one landed,
-- so that the forecast is owed while none has, and re-owed once stale.
DECLARE FUNCTION forecast_stands FOR GLOBAL AS $$
  SELECT subject, 'green' AS band, 0.0 AS score FROM slots WHERE speaker = 'function'
$$;

DECLARE WITNESS forecast_w ON metric_forecast DETECTOR forecast_stands;

--===------------------------------------------------------------------===--
-- forecast_record(): every forecast run's points as rows, from the
-- measurements alone. forecast_runs(): the same with the actual joined
-- from the actual world where it has landed (for a span, the sum over its
-- periods where every one has). forecast_coverage(): the record's tally —
-- per metric and for the dataset ('' the metric), per grain, step and
-- span: points landed, inside_80, inside_90, held_80, held_90.
-- forecast_points(): the newest run, flattened — one row per metric and
-- point: seq, metric, applicable, reason, behavior, grain, origin,
-- served_by, computed_at, current, point_seq, step, span, to_go, period,
-- p05 to p95, service_80, service_90, actual, landed (below, low, inside,
-- high, above against p05, p10, p90 and p95), held_80 and held_90 (how
-- often such a band held: the record's own share where it holds thirty
-- landed points at this grain, step and span for the metric, else for the
-- dataset, else the service's) and held_from (metric, dataset, service). A
-- metric the forecast found inapplicable is one row with its reason.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.forecast_record() AS TABLE
WITH runs AS (
  SELECT seq AS run, value, computed_at FROM glossql.measurements
  WHERE dataset = current_schema() AND function = 'metric_forecast'),
metrics AS (
  SELECT run, computed_at, json_extract_string(value, '$.served_by') AS served_by,
         unnest(from_json(json_extract(value, '$.metrics'),
           '[{"metric": "VARCHAR", "applicable": "BOOLEAN", "reason": "VARCHAR", "behavior": "VARCHAR", "grain": "VARCHAR", "origin": "VARCHAR",
              "points": [{"step": "BIGINT", "span": "BIGINT", "to_go": "VARCHAR", "period": "VARCHAR", "p05": "DOUBLE", "p10": "DOUBLE",
                          "p50": "DOUBLE", "p90": "DOUBLE", "p95": "DOUBLE", "service_80": "DOUBLE", "service_90": "DOUBLE"}]}]')) AS m
  FROM runs),
pts AS (
  SELECT run, computed_at, served_by, m.metric, m.applicable, m.reason, m.behavior, m.grain, m.origin::TIMESTAMP AS origin, unnest(m.points) AS p
  FROM metrics WHERE cardinality(m.points) > 0)
SELECT run, computed_at, served_by, metric, applicable, reason, behavior, grain, origin,
       p.step AS step, p.span AS span, p.to_go AS to_go, p.period::TIMESTAMP AS period,
       p.p05 AS p05, p.p10 AS p10, p.p50 AS p50, p.p90 AS p90, p.p95 AS p95, p.service_80 AS service_80, p.service_90 AS service_90
FROM pts
UNION ALL
SELECT run, computed_at, served_by, m.metric, m.applicable, m.reason, m.behavior, m.grain, m.origin::TIMESTAMP,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM metrics WHERE cardinality(m.points) = 0;

CREATE OR REPLACE MACRO glossql.forecast_runs() AS TABLE
WITH actuals AS (
  SELECT metric, period, value FROM glossql.metric_series(unclaimed := 'skip') WHERE dimension = '' AND world = 'actual')
SELECT r.*,
       CASE WHEN r.step IS NULL THEN NULL
            WHEN r.span IS NULL THEN a.value
            ELSE (SELECT CASE WHEN count(*) = r.span THEN sum(a2.value) END FROM actuals a2
                  WHERE a2.metric = r.metric AND a2.period > r.origin AND a2.period <= r.period) END AS actual
FROM glossql.forecast_record() r
LEFT JOIN actuals a ON a.metric = r.metric AND a.period = r.period AND r.span IS NULL;

CREATE OR REPLACE MACRO glossql.forecast_coverage() AS TABLE
WITH tally AS (
  SELECT metric, grain, step, span, count(actual) AS points,
         count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80,
         count(*) FILTER (WHERE actual BETWEEN p05 AND p95) AS inside_90
  FROM glossql.forecast_runs() WHERE applicable AND step IS NOT NULL
  GROUP BY metric, grain, step, span),
whole AS (
  SELECT '' AS metric, grain, step, span, sum(points) AS points, sum(inside_80) AS inside_80, sum(inside_90) AS inside_90
  FROM tally GROUP BY grain, step, span)
SELECT metric, grain, step, span, points, inside_80, inside_90,
       CASE WHEN points > 0 THEN inside_80 / points END AS held_80,
       CASE WHEN points > 0 THEN inside_90 / points END AS held_90
FROM (SELECT * FROM tally UNION ALL SELECT * FROM whole)
ORDER BY metric, grain, step, span NULLS FIRST;

CREATE OR REPLACE MACRO glossql.forecast_points() AS TABLE
WITH runs AS MATERIALIZED (SELECT * FROM glossql.forecast_runs()),
newest AS (SELECT max(run) AS run FROM runs),
current_ AS (
  SELECT coalesce(bool_and(current), true) AS current
  FROM gloss_glossary(current_schema(), NULL, true) WHERE actor = 'function' AND aspect = 'metric_forecast'),
-- the record's tally behind held, over every run: per metric, and for the dataset
tally AS (
  SELECT metric, grain, step, span, count(actual) AS points,
         count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80,
         count(*) FILTER (WHERE actual BETWEEN p05 AND p95) AS inside_90
  FROM runs WHERE applicable AND step IS NOT NULL
  GROUP BY metric, grain, step, span),
whole AS (
  SELECT grain, step, span, sum(points) AS points, sum(inside_80) AS inside_80, sum(inside_90) AS inside_90
  FROM tally GROUP BY grain, step, span),
rows_ AS (
  SELECT f.*, dense_rank() OVER (ORDER BY f.metric) - 1 AS seq,
         CASE WHEN f.step IS NULL THEN NULL
              ELSE row_number() OVER (PARTITION BY f.metric ORDER BY f.span NULLS FIRST, f.step, f.to_go) - 1 END AS point_seq
  FROM runs f, newest WHERE f.run = newest.run)
SELECT r.seq, r.metric, r.applicable, r.reason, r.behavior, r.grain, strftime(r.origin, '%Y-%m-%d') AS origin, r.served_by, r.computed_at,
       (SELECT current FROM current_) AS current, r.point_seq,
       r.step, r.span, r.to_go, strftime(r.period, '%Y-%m-%d') AS period, r.p05, r.p10, r.p50, r.p90, r.p95, r.service_80, r.service_90,
       r.actual,
       CASE WHEN r.actual IS NULL THEN NULL WHEN r.actual < r.p05 THEN 'below' WHEN r.actual < r.p10 THEN 'low'
            WHEN r.actual <= r.p90 THEN 'inside' WHEN r.actual <= r.p95 THEN 'high' ELSE 'above' END AS landed,
       CASE WHEN tm.points >= 30 THEN tm.inside_80 / tm.points WHEN td.points >= 30 THEN td.inside_80 / td.points ELSE r.service_80 END AS held_80,
       CASE WHEN tm.points >= 30 THEN tm.inside_90 / tm.points WHEN td.points >= 30 THEN td.inside_90 / td.points ELSE r.service_90 END AS held_90,
       CASE WHEN tm.points >= 30 THEN 'metric' WHEN td.points >= 30 THEN 'dataset' WHEN r.service_80 IS NOT NULL THEN 'service' END AS held_from
FROM rows_ r
LEFT JOIN tally tm ON tm.metric = r.metric AND tm.grain = r.grain AND tm.step = r.step AND tm.span IS NOT DISTINCT FROM r.span
LEFT JOIN whole td ON td.grain = r.grain AND td.step = r.step AND td.span IS NOT DISTINCT FROM r.span
ORDER BY r.seq, r.point_seq NULLS FIRST;

--===------------------------------------------------------------------===--
-- The plan. plan_rows(): every applicable plan's rows through the metric
-- it plans — plan, metric, behavior, covers, instant, value, and grain,
-- the plan's own: the grain its rows are booked against where the
-- declaration says so, else the rows' cadence. plan_grains(): one row per
-- plan with it. plan_cells(at_grain): the
-- plan world's cells per plan at a grain, or at its own where NULL — a
-- flow sums, a stock keeps the last level, anything else averages; a plan
-- coarser than the asked grain serves nothing. world_cells(): the plan
-- world (named plan, refused by name where it is no plan or nothing
-- serves it) and the forecast world (the newest forecast's medians at its
-- grain) under the metric, for metric_series(world := …).
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.plan_rows() AS TABLE
WITH p AS (
  SELECT a.metric AS plan, a.plan_of AS metric, a.covers, a.period_column, a.value_column, a.behavior
  FROM glossql.metric_axes() a WHERE a.kind = 'plan' AND a.applicable),
rows_ AS (
  SELECT p.plan, p.metric, p.behavior, p.covers,
         max(CASE WHEN c."column" = p.period_column THEN TRY_CAST(c.val AS TIMESTAMP) END) AS instant,
         max(CASE WHEN c."column" = p.value_column THEN TRY_CAST(c.val AS DOUBLE) END) AS value
  FROM dataset_cells(current_schema(), true) c JOIN p ON p.plan = c."table"
  GROUP BY p.plan, p.metric, p.behavior, p.covers, c.rn),
gaps AS (
  SELECT plan, min(gap) AS gap
  FROM (SELECT plan, date_diff('day', lag(instant) OVER (PARTITION BY plan ORDER BY instant), instant) AS gap
        FROM (SELECT DISTINCT plan, instant FROM rows_ WHERE instant IS NOT NULL))
  GROUP BY plan)
SELECT r.plan, r.metric, r.behavior, r.covers, r.instant, r.value,
       coalesce(r.covers, CASE WHEN g.gap IS NULL THEN 'month' WHEN g.gap >= 360 THEN 'year' WHEN g.gap >= 89 THEN 'quarter'
                               WHEN g.gap >= 28 THEN 'month' WHEN g.gap >= 7 THEN 'week' ELSE 'day' END) AS grain
FROM rows_ r LEFT JOIN gaps g USING (plan);

CREATE OR REPLACE MACRO glossql.plan_grains() AS TABLE
SELECT DISTINCT plan, metric, behavior, grain FROM glossql.plan_rows();

CREATE OR REPLACE MACRO glossql.plan_cells(at_grain) AS TABLE
SELECT r.plan, r.metric, coalesce(at_grain, r.grain) AS grain, date_trunc(coalesce(at_grain, r.grain), r.instant) AS period,
       CASE r.behavior WHEN 'stock' THEN arg_max(r.value, r.instant) WHEN 'flow' THEN sum(r.value) ELSE avg(r.value) END AS value,
       coalesce(r.behavior, 'none') AS behavior
FROM glossql.plan_rows() r
WHERE r.instant IS NOT NULL AND r.value IS NOT NULL
  AND (at_grain IS NULL OR glossql.grain_rank(at_grain) >= glossql.grain_rank(r.grain))
GROUP BY r.plan, r.metric, coalesce(at_grain, r.grain), date_trunc(coalesce(at_grain, r.grain), r.instant), r.behavior;

CREATE OR REPLACE MACRO glossql.world_cells(w, p, g) AS TABLE
WITH named AS (
  SELECT a.metric AS plan, a.applicable, a.reason FROM glossql.metric_axes() a WHERE a.kind = 'plan' AND a.metric = p),
refusal AS (
  SELECT CASE WHEN w NOT IN ('actual', 'plan', 'forecast') THEN error('metric_series(world := ''' || w || '''): the worlds are actual, plan and forecast')
              WHEN w = 'plan' AND p IS NULL THEN error('metric_series(world := ''plan'') names the plan it serves, plan := ''<name>'' — a plan is a QUERY aspect declared with x-kind plan and x-plan-of <metric>')
              WHEN w = 'plan' AND NOT EXISTS (SELECT 1 FROM named) THEN error(
                CASE WHEN NOT EXISTS (SELECT 1 FROM glossql.aspects WHERE name = p)
                     THEN 'metric_series(plan := ''' || p || '''): no aspect of that name is declared — a plan is a QUERY aspect declared with x-kind plan and x-plan-of <metric>'
                     ELSE 'metric_series(plan := ''' || p || '''): ' || p || ' is no plan — a plan is a QUERY aspect declared with x-kind plan and x-plan-of <metric>' END)
              WHEN w = 'plan' AND EXISTS (SELECT 1 FROM named WHERE NOT applicable) THEN error(
                'metric_series(plan := ''' || p || '''): ' || (SELECT reason FROM named))
         END AS r),
plan_world AS (
  SELECT c.metric, 'plan' AS world, '' AS dimension, '' AS member, c.period, c.value, NULL::DOUBLE AS num, NULL::DOUBLE AS den, c.behavior
  FROM glossql.plan_cells(g) c WHERE w = 'plan' AND c.plan = p),
newest AS (SELECT max(run) AS run FROM glossql.forecast_record()),
forecast_world AS (
  SELECT f.metric, 'forecast' AS world, '' AS dimension, '' AS member, f.period, f.p50 AS value,
         NULL::DOUBLE AS num, NULL::DOUBLE AS den, coalesce(f.behavior, 'none') AS behavior
  FROM glossql.forecast_record() f JOIN newest n ON n.run = f.run
  WHERE w = 'forecast' AND f.applicable AND f.span IS NULL AND f.period IS NOT NULL AND (g IS NULL OR g = f.grain))
SELECT * FROM plan_world
UNION ALL SELECT * FROM forecast_world
UNION ALL SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM refusal WHERE r IS NOT NULL;

--===------------------------------------------------------------------===--
-- plan_points(): every served plan against the actual and the forecast,
-- one row per plan and period at the plan's own grain and, where that is
-- finer, at the year — plan, metric, grain, period, label, value (the
-- plan's cell), actual (the actual world's cell at that grain where the
-- period has begun; for the current period, to date), variance (actual
-- minus plan, on a complete period), low, expected, high (the newest
-- forecast's 80% band and median: the step's at the metric's own grain;
-- at a coarser grain, for a flow the actual to date plus the total that
-- runs to the period's end, for a stock the band of the last period of
-- the metric's grain inside it), stands (landed: complete; inside, below,
-- above: where the plan lies against the band; partial: begun and the
-- rows cover part of the period, no band; open: begun, no band covers the
-- rest; ahead: not begun), covers (on the year row of a plan whose rows
-- cover part of the year: how much), held_80, held_from, origin (the
-- forecast's). Computed at read, nothing recorded.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.plan_points() AS TABLE
WITH prow AS MATERIALIZED (SELECT * FROM glossql.plan_rows()),
g AS (
  SELECT pg.plan, pg.metric, pg.behavior, pg.grain, m.floor AS metric_floor
  FROM (SELECT DISTINCT plan, metric, behavior, grain FROM prow) pg
  JOIN glossql.metric_axes() m ON m.metric = pg.metric WHERE m.applicable),
floor_cells AS MATERIALIZED (
  SELECT metric, period, value, num, den, behavior FROM glossql.metric_series(unclaimed := 'skip')
  WHERE dimension = '' AND world = 'actual'),
origins AS (SELECT metric, max(period) AS origin FROM floor_cells WHERE value IS NOT NULL GROUP BY metric),
cells AS (
  -- the plan's cells at its own grain and at the year
  SELECT r.plan, r.metric, gr.step_grain AS grain, date_trunc(gr.step_grain, r.instant) AS period,
         CASE r.behavior WHEN 'stock' THEN arg_max(r.value, r.instant) WHEN 'flow' THEN sum(r.value) ELSE avg(r.value) END AS value
  FROM prow r, unnest(CASE WHEN r.grain = 'year' THEN ['year'] ELSE [r.grain, 'year'] END) AS gr(step_grain)
  WHERE r.instant IS NOT NULL AND r.value IS NOT NULL
  GROUP BY r.plan, r.metric, gr.step_grain, date_trunc(gr.step_grain, r.instant), r.behavior),
own AS (SELECT c.* FROM cells c JOIN g USING (plan) WHERE c.grain = g.grain),
rows_ AS (SELECT * FROM cells),
cover AS (
  SELECT o.plan, date_trunc('year', o.period) AS year, g.grain AS own_grain, count(*) AS covered,
         min(o.period) AS first, max(o.period + glossql.grain_step(g.grain)) AS last_end,
         CASE g.grain WHEN 'month' THEN 12 WHEN 'quarter' THEN 4
              WHEN 'day' THEN date_diff('day', date_trunc('year', o.period), date_trunc('year', o.period) + INTERVAL 1 YEAR)
              WHEN 'week' THEN cardinality(generate_series(date_trunc('week', date_trunc('year', o.period) + INTERVAL 6 DAY),
                                                           date_trunc('year', o.period) + INTERVAL 1 YEAR - INTERVAL 1 MICROSECOND, INTERVAL 7 DAY)) END AS of_
  FROM own o JOIN g USING (plan) WHERE g.grain <> 'year'
  GROUP BY o.plan, date_trunc('year', o.period), g.grain),
actuals AS (
  SELECT f.metric, r.grain, date_trunc(r.grain, f.period) AS period,
         CASE f.behavior WHEN 'stock' THEN arg_max(f.value, f.period) WHEN 'ratio' THEN sum(f.num) / nullif(sum(f.den), 0) ELSE sum(f.value) END AS value
  FROM floor_cells f JOIN (SELECT DISTINCT metric, grain FROM rows_) r USING (metric)
  GROUP BY f.metric, r.grain, date_trunc(r.grain, f.period), f.behavior),
judged AS (
  SELECT r.plan, r.metric, r.grain, r.period, r.value, g.behavior, g.metric_floor, g.grain AS own_grain, o.origin,
         o.origin + glossql.grain_step(g.metric_floor) AS origin_end,
         r.period + glossql.grain_step(r.grain) AS period_end,
         date_trunc(g.metric_floor, r.period + glossql.grain_step(r.grain) - INTERVAL 1 MICROSECOND) AS last_inside,
         r.period <= o.origin AS begun,
         a.value AS actual_cell,
         c.covered, c.of_, c.first, c.last_end,
         coalesce(r.grain = 'year' AND c.plan IS NOT NULL AND c.covered < c.of_, false) AS part
  FROM rows_ r JOIN g USING (plan)
  LEFT JOIN origins o ON o.metric = r.metric
  LEFT JOIN actuals a ON a.metric = r.metric AND a.grain = r.grain AND a.period = r.period
  LEFT JOIN cover c ON c.plan = r.plan AND r.grain = 'year' AND c.year = r.period),
part_actual AS (
  SELECT j.plan, j.period AS year,
         CASE j.behavior WHEN 'flow' THEN sum(a.value) WHEN 'stock' THEN arg_max(a.value, a.period) END AS actual
  FROM judged j JOIN own o ON o.plan = j.plan AND date_trunc('year', o.period) = j.period
  JOIN actuals a ON a.metric = j.metric AND a.grain = o.grain AND a.period = o.period AND o.period <= j.origin
  WHERE j.part GROUP BY j.plan, j.period, j.behavior),
verdict AS (
  SELECT j.*,
    CASE WHEN j.part THEN pa.actual WHEN j.begun THEN j.actual_cell END AS actual,
    CASE WHEN j.part THEN j.last_end <= j.origin_end ELSE j.period_end <= j.origin_end END AS complete,
    CASE WHEN j.part THEN j.first <= j.origin ELSE j.begun END AS has_begun
  FROM judged j LEFT JOIN part_actual pa ON pa.plan = j.plan AND pa.year = j.period),
-- the newest forecast's bands, with how often such a band held: the record's own share where it holds thirty points, else the service's
runs AS MATERIALIZED (SELECT * FROM glossql.forecast_runs() WHERE applicable AND step IS NOT NULL),
tally AS (
  SELECT metric, grain, step, span, count(actual) AS points, count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80
  FROM runs GROUP BY metric, grain, step, span),
whole AS (SELECT grain, step, span, sum(points) AS points, sum(inside_80) AS inside_80 FROM tally GROUP BY grain, step, span),
newest_run AS (SELECT max(run) AS run FROM runs),
fp AS MATERIALIZED (
  SELECT r.metric, r.grain, r.origin, r.step, r.span, r.to_go, r.period, r.p10, r.p50, r.p90,
         CASE WHEN tm.points >= 30 THEN tm.inside_80 / tm.points WHEN td.points >= 30 THEN td.inside_80 / td.points ELSE r.service_80 END AS held_80,
         CASE WHEN tm.points >= 30 THEN 'metric' WHEN td.points >= 30 THEN 'dataset' WHEN r.service_80 IS NOT NULL THEN 'service' END AS held_from
  FROM runs r JOIN newest_run n ON n.run = r.run
  LEFT JOIN tally tm ON tm.metric = r.metric AND tm.grain = r.grain AND tm.step = r.step AND tm.span IS NOT DISTINCT FROM r.span
  LEFT JOIN whole td ON td.grain = r.grain AND td.step = r.step AND td.span IS NOT DISTINCT FROM r.span),
-- the band a row is held against: the step's at the metric's own grain; at a coarser grain, for a stock the band of the
-- last period of the metric's grain inside it, for a flow that has begun the total that runs to the period's end
candidates AS (
  SELECT v.plan, v.grain, v.period, f.p10, f.p50, f.p90, f.held_80, f.held_from, f.origin,
         row_number() OVER (PARTITION BY v.plan, v.grain, v.period ORDER BY f.span NULLS FIRST, f.step) AS rn
  FROM verdict v JOIN fp f ON f.metric = v.metric
  WHERE NOT v.complete AND NOT v.part
    AND ((v.grain = v.metric_floor AND f.span IS NULL AND f.period = v.period)
      OR (v.grain <> v.metric_floor AND v.behavior = 'stock' AND f.span IS NULL AND f.period = v.last_inside)
      OR (v.grain <> v.metric_floor AND v.behavior = 'flow' AND v.has_begun AND v.actual IS NOT NULL
          AND f.span IS NOT NULL AND f.to_go = v.grain AND f.period >= v.period AND f.period < v.period_end))),
banded AS (
  SELECT v.*, c.p10, c.p50, c.p90, c.held_80, c.held_from, c.origin AS forecast_origin,
         CASE WHEN v.grain = v.metric_floor OR v.behavior = 'stock' THEN 0.0 ELSE coalesce(v.actual, 0.0) END AS base
  FROM verdict v LEFT JOIN candidates c ON c.plan = v.plan AND c.grain = v.grain AND c.period = v.period AND c.rn = 1)
SELECT plan, metric, grain, period,
       CASE grain WHEN 'year' THEN strftime(period, '%Y')
                  WHEN 'quarter' THEN strftime(period, '%Y') || '-Q' || ((month(period) - 1) // 3 + 1)
                  WHEN 'month' THEN strftime(period, '%Y-%m')
                  ELSE strftime(period, '%Y-%m-%d') END AS label,
       value, actual,
       CASE WHEN complete THEN actual - value END AS variance,
       CASE WHEN complete OR part OR p10 IS NULL THEN NULL ELSE base + p10 END AS low,
       CASE WHEN complete OR part OR p50 IS NULL THEN NULL ELSE base + p50 END AS expected,
       CASE WHEN complete OR part OR p90 IS NULL THEN NULL ELSE base + p90 END AS high,
       CASE WHEN complete THEN 'landed'
            WHEN part THEN CASE WHEN has_begun THEN 'partial' ELSE 'ahead' END
            WHEN p10 IS NULL THEN CASE WHEN has_begun THEN 'open' ELSE 'ahead' END
            WHEN value < base + p10 THEN 'below' WHEN value > base + p90 THEN 'above' ELSE 'inside' END AS stands,
       CASE WHEN part THEN covered || ' of ' || of_ || ' ' || own_grain || 's' END AS covers,
       CASE WHEN complete OR part THEN NULL ELSE held_80 END AS held_80,
       CASE WHEN complete OR part THEN NULL ELSE held_from END AS held_from,
       CASE WHEN complete OR part THEN NULL ELSE strftime(forecast_origin, '%Y-%m-%d') END AS origin
FROM banded
ORDER BY plan, CASE grain WHEN 'year' THEN 0 ELSE 1 END, period;

--===------------------------------------------------------------------===--
-- The two goals this kit answers for a door's next read. bands: the walk
-- stands and no band is red. plan: the plan served, the forecast at the
-- current origin, every gap judged. Served as CTEs and arms over the
-- relations the door reads once (ds, axes, owed, reds, bodies,
-- applicable); futures_red is the metric whose newest point the red
-- verdict scores, futures_gaps the plan years the forecast's band does
-- not hold.
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.routes_futures() AS TABLE
SELECT $ctes$
futures_walk AS (SELECT * FROM glossql.band_verdict()),
futures_red AS (
  SELECT metric, period FROM glossql.band_points()
  WHERE newest AND displacement > 0.9 AND EXISTS (SELECT 1 FROM futures_walk w WHERE w.band = 'red')),
futures_plans AS (
  SELECT a.name AS plan, json_extract_string(a.schema, '$.x-plan-of') AS metric, x.applicable, x.reason, x.stopped
  FROM (SELECT name, kind, schema FROM glossql.aspects QUALIFY row_number() OVER (PARTITION BY name ORDER BY seq DESC) = 1) a
  LEFT JOIN axes x ON x.metric = a.name
  WHERE a.kind = 'QUERY' AND json_extract_string(a.schema, '$.x-kind') = 'plan'),
futures_gaps AS (
  SELECT plan, metric, label, value, low, high, stands FROM glossql.plan_points()
  WHERE grain = 'year' AND stands IN ('below', 'above')),
futures_scenarios AS (
  SELECT a.name FROM (SELECT name, kind, schema FROM glossql.aspects QUALIFY row_number() OVER (PARTITION BY name ORDER BY seq DESC) = 1) a
  WHERE a.kind = 'FACT' AND json_extract_string(a.schema, '$.x-kind') = 'scenario'
    AND EXISTS (SELECT 1 FROM gloss_glossary(current_schema(), NULL, false) g WHERE g.aspect = a.name AND g.state = 'current')),
futures_levers AS (
  SELECT string_agg(subject, ', ' ORDER BY subject) AS columns
  FROM gloss_glossary(current_schema(), NULL, false) WHERE aspect = 'lever' AND state = 'current' AND (value->>'value') = 'true')
$ctes$ AS ctes, $arms$
SELECT 'bands' AS surface, 1 AS step, 'blocked' AS state, '' AS act, '' AS say, 'no applicable metric stands' AS why, '' AS statement, '' AS "then"
FROM ds WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable AND coalesce(kind, '') <> 'plan')
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
UNION ALL
SELECT 'plan', 1, 'blocked', '', '', 'no applicable metric stands', '', ''
FROM ds WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable AND coalesce(kind, '') <> 'plan')
UNION ALL
SELECT 'plan', 2, 'next', 'DECLARE ASPECT plan',
       'declare the first plan',
       'no plan is declared on ' || m.metrics || ': a plan is a QUERY aspect with x-kind plan and x-plan-of naming the metric it plans, grounded by SQL over the landed budget table, one aspect per edition, x-covers year where a row is booked against the year; which metrics the budget covers, and its name, are the human''s',
       'DECLARE ASPECT <name> WITH $${"title": "<Budget>", "x-kind": "plan", "x-plan-of": "<metric>"}$$ AS QUERY;',
       'GLOSS <name> ON ' || ds.dataset || ' AS $${"sql": "SELECT <date column> AS <the metric''s period column>, <planned value> AS <the metric''s value column> FROM <budget table>"}$$;'
FROM applicable m CROSS JOIN ds
WHERE m.n > 0 AND NOT EXISTS (SELECT 1 FROM futures_plans)
UNION ALL
SELECT 'plan', 3, 'next', 'GLOSS plan',
       'ground ' || p.plan || ', or stop it',
       p.plan || ' plans ' || p.metric || ' and is not grounded: its SQL over the landed budget table serves the plan world, or a stop says why no number is served',
       'GLOSS ' || p.plan || ' ON ' || ds.dataset || ' AS $${"sql": "SELECT <date column> AS <the metric''s period column>, <planned value> AS <the metric''s value column> FROM <budget table>"}$$;' || chr(10)
         || '-- or, where no number should be served: GLOSS ' || p.plan || ' ON ' || ds.dataset || ' AS $${"stopped": "<what is missing, and why no number is served>"}$$;',
       ''
FROM futures_plans p LEFT JOIN glossql.metric_surfaces s ON s.name = p.plan CROSS JOIN ds
WHERE NOT coalesce(s.grounded, false) AND coalesce(s.stopped, '') = ''
UNION ALL
SELECT 'plan', 4, 'next', 'GLOSS plan',
       're-record ' || p.plan || ' — ' || split_part(split_part(p.reason, '. ', 1), ': ', 1),
       p.plan || ' is grounded and the plan read refuses it — ' || p.reason || '. Re-record it, or stop it with the reason no number is served',
       'GLOSS ' || p.plan || ' ON ' || ds.dataset || ' AS $$' || coalesce(b.body, '<the standing body>') || '$$;' || chr(10)
         || '-- or, where no number should be served: GLOSS ' || p.plan || ' ON ' || ds.dataset || ' AS $${"stopped": "<what is missing, and why no number is served>"}$$;',
       ''
FROM futures_plans p JOIN glossql.metric_surfaces s ON s.name = p.plan LEFT JOIN bodies b ON b.metric = p.plan CROSS JOIN ds
WHERE NOT p.applicable AND p.stopped IS NULL AND s.grounded AND coalesce(p.reason, '') <> ''
UNION ALL
SELECT 'plan', 5, 'next', 'metric_forecast', 'run the forecast', o.why,
       'SELECT metric_forecast() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'never measured' AND o.subject = 'metric_forecast' AND EXISTS (SELECT 1 FROM futures_plans WHERE applicable)
UNION ALL
SELECT 'plan', 6, 'next', 'metric_forecast', 'run the forecast again', o.why,
       'SELECT metric_forecast() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 're-measure' AND o.subject = 'metric_forecast' AND EXISTS (SELECT 1 FROM futures_plans WHERE applicable)
UNION ALL
SELECT 'plan', 7, 'next', 'GLOSS scenario',
       'judge ' || g.plan || ' at ' || g.label || ': declare a scenario, or record the gap',
       g.plan || ' at ' || g.label || ' is ' || CAST(round(g.value, 2) AS VARCHAR)
         || ' and the forecast''s 80% band for ' || g.metric || ' runs ' || CAST(round(g.low, 2) AS VARCHAR) || ' to ' || CAST(round(g.high, 2) AS VARCHAR)
         || ': the plan lies ' || g.stands || ' it' || coalesce(' (levers glossed: ' || l.columns || ')', '')
         || '. A scenario on a lever says what closes the gap; without one, record the gap as the plan''s assumption under plan-' || g.label || ', and the docket asks',
       'GLOSS lever ON <table>.<column> AS $${"value": true, "grounds": "<who sets it>"}$$;' || chr(10)
         || 'DECLARE ASPECT <scenario> WITH $${"title": "<what moves>", "x-kind": "scenario", "type": "object", "required": ["levers"], "properties": {"levers": {"type": "array"}}}$$ AS FACT ON DATASET;' || chr(10)
         || 'GLOSS <scenario> ON ' || ds.dataset || ' AS $${"levers": [{"column": "<table>.<column>", "factor": {"<YYYY-MM>": <factor>}, "basis": "<what it rests on>"}]}$$;',
       '-- append to "assumptions" of the plan''s standing body: {"dimension": "definition", "key": "plan-' || g.label || '", "assumption": "<the gap at ' || g.label || ' and what closes it, in the business''s words — a lever moved, or the plan held>", "basis": "<what it rests on>", "confidence": 0.7}' || chr(10)
         || 'GLOSS ' || g.plan || ' ON ' || ds.dataset || ' AS $$' || coalesce(b.body, '<the standing body>') || '$$;'
FROM futures_gaps g LEFT JOIN bodies b ON b.metric = g.plan CROSS JOIN futures_levers l CROSS JOIN ds
WHERE NOT EXISTS (SELECT 1 FROM futures_scenarios)
  AND NOT EXISTS (SELECT 1 FROM glossql.ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = g.plan AND r.key = 'plan-' || g.label)
  AND NOT EXISTS (SELECT 1 FROM glossql.open_questions q WHERE q.dataset = ds.dataset AND q.aspect = g.plan AND q.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 8, 'blocked', '', '',
       g.plan || ' at ' || g.label || ' lies ' || g.stands || ' the forecast''s band and the question stands for the human', '', ''
FROM futures_gaps g CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.open_questions q WHERE q.dataset = ds.dataset AND q.aspect = g.plan AND q.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 9, 'done', '', '',
       'the plan stands; the gap on ' || g.plan || ' at ' || g.label || ' carries the human''s ruling', '', ''
FROM futures_gaps g CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = g.plan AND r.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 10, 'done', '', '', 'every plan is served and no plan period lies outside the forecast''s band, or the gap carries a question or a ruling', '', ''
FROM ds
$arms$ AS arms;
