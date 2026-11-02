-- The futures kit: the worlds beside the actual — the walk's bands and the
-- forecast, each what the model service answers; the plan, judged against
-- them — each landed as a measurement with its pin and reads, and read
-- back as SQL, so that a door's next read judges from the record and
-- never computes over the data. Loaded after the metrics kit, whose
-- series the requests are built from. The service is named once for the instance
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
-- the soft form: a refusal or an absent service comes back as {"error": "…"}, for a call the kit can stand without
CREATE OR REPLACE MACRO glossql.service_soft(route, body) AS gloss_service(route, body, true);

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
  FROM dataset_cells(current_schema(), 'views') c JOIN axes a ON a.metric = c."table" AND a.period_column = c."column"
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
  -- the service's held-out share per grain, step and span: every body at a grain carries it, once is enough
  coverage AS (
    SELECT b.grain, cv.c.step AS step, cv.c.span AS span, cv.c.low AS low, cv.c.high AS high, any_value(cv.c.share) AS share
    FROM b, unnest(from_json(json_extract(b.a, '$.answer.coverage'),
                   '[{"step": "BIGINT", "span": "BIGINT", "low": "DOUBLE", "high": "DOUBLE", "share": "DOUBLE"}]')) AS cv(c)
    GROUP BY ALL),
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
-- per metric and for the dataset ('' the metric), per grain, step, span
-- and to_go (a span of one length under two names tallied under each)
-- where a point has landed: points landed, inside_80, inside_90, held_80,
-- held_90.
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
  SELECT metric, grain, step, span, to_go, count(actual) AS points,
         count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80,
         count(*) FILTER (WHERE actual BETWEEN p05 AND p95) AS inside_90
  FROM glossql.forecast_runs() WHERE applicable AND step IS NOT NULL
  GROUP BY metric, grain, step, span, to_go
  HAVING count(actual) > 0),
whole AS (
  SELECT '' AS metric, grain, step, span, to_go, sum(points) AS points, sum(inside_80) AS inside_80, sum(inside_90) AS inside_90
  FROM tally GROUP BY grain, step, span, to_go)
SELECT metric, grain, step, span, to_go, points, inside_80, inside_90,
       CASE WHEN points > 0 THEN inside_80 / points END AS held_80,
       CASE WHEN points > 0 THEN inside_90 / points END AS held_90
FROM (SELECT * FROM tally UNION ALL SELECT * FROM whole)
ORDER BY metric, grain, step, span NULLS FIRST, to_go NULLS FIRST;

CREATE OR REPLACE MACRO glossql.forecast_points() AS TABLE
WITH runs AS MATERIALIZED (SELECT * FROM glossql.forecast_runs()),
newest AS (SELECT max(run) AS run FROM runs),
current_ AS (
  SELECT coalesce(bool_and(current), true) AS current
  FROM gloss_glossary(current_schema(), NULL, true) WHERE actor = 'function' AND aspect = 'metric_forecast'),
-- the record's tally behind held, over every run: per metric, and for the dataset
tally AS (
  SELECT metric, grain, step, span, to_go, count(actual) AS points,
         count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80,
         count(*) FILTER (WHERE actual BETWEEN p05 AND p95) AS inside_90
  FROM runs WHERE applicable AND step IS NOT NULL
  GROUP BY metric, grain, step, span, to_go),
whole AS (
  SELECT grain, step, span, to_go, sum(points) AS points, sum(inside_80) AS inside_80, sum(inside_90) AS inside_90
  FROM tally GROUP BY grain, step, span, to_go),
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
LEFT JOIN tally tm ON tm.metric = r.metric AND tm.grain = r.grain AND tm.step = r.step AND tm.span IS NOT DISTINCT FROM r.span AND tm.to_go IS NOT DISTINCT FROM r.to_go
LEFT JOIN whole td ON td.grain = r.grain AND td.step = r.step AND td.span IS NOT DISTINCT FROM r.span AND td.to_go IS NOT DISTINCT FROM r.to_go
ORDER BY r.seq, r.point_seq NULLS FIRST;

--===------------------------------------------------------------------===--
-- The scenario. A lever is a FACT on a column of a metric's view — a list
-- price, a discount rate — glossed true where someone sets it, false where
-- it was looked at and nobody does; a column with no lever gloss is no
-- lever. A scenario is the actor's own FACT aspect on the dataset, x-kind
-- scenario, its body a factor per period on declared levers: {"levers":
-- [{"column": "<metric>.<column>", "factor": {"YYYY-MM": 1.15, …},
-- "basis": "…"}]}; a period is a month or a day, one not named keeps a
-- factor of one. scenarios(): every scenario declared, judged — glossed,
-- each lever a glossed lever on a served metric's column, the factor a
-- map. scenario_request(): the scenario's reach, one row per scenario
-- and metric whose view carries one of its levers: grain (the metric's
-- floor), origin (the last actual), history, the periods ahead up to the
-- end of the last period the scenario names, and per lever its series
-- (the mean of the column per period) and the value held as planned
-- (the last at or before the origin); applicable needs 26 periods of
-- history, a period ahead, and an input the scenario moves — a lever
-- held at nothing, or factors of one, move nothing, and the service
-- refuses a request where no input differs. lever_bodies(): one
-- forecast request per scenario, metric and lever, the lever's own
-- series as the member and the periods ahead as the horizon, at the
-- forecast's reach per grain. scenario_bodies(paths := NULL): one
-- what-if request per scenario and metric as the service's contract
-- spells it — the history as actual rows, the periods ahead twice, as
-- planned and under the scenario (planned times the period's factor),
-- every lever an input column named as the scenario names it. The
-- planned input is the lever's own forecast where paths carries one
-- ({"<scenario>|<metric>|<lever>": [{period, value}]}, its last median
-- held past its reach), else the lever's last actual value held. The
-- estimate's measurement forecasts each lever first and passes the
-- paths; a plain read of scenario_bodies() holds. No grounding is
-- replayed over the data: the scenario world is the estimate ahead.
--===------------------------------------------------------------------===--

DECLARE ASPECT lever WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"type": "boolean"}, "grounds": {"type": "string"}}
}$$ AS FACT ON COLUMN;

CREATE OR REPLACE MACRO glossql.scenarios() AS TABLE
WITH declared AS (
  SELECT name FROM (SELECT name, kind, schema, row_number() OVER (PARTITION BY name ORDER BY seq DESC) AS rn FROM glossql.aspects)
  WHERE rn = 1 AND kind = 'FACT' AND json_extract_string(schema, '$.x-kind') = 'scenario'),
bodies AS (
  SELECT d.name AS scenario, g.value AS body
  FROM declared d LEFT JOIN gloss_glossary(current_schema(), NULL, false) g
    ON g.aspect = d.name AND g.subject = current_schema() AND g.state = 'current'),
levers AS (
  SELECT b.scenario, u.l."column" AS lever, split_part(u.l."column", '.', 1) AS metric, split_part(u.l."column", '.', 2) AS col,
         u.l.factor AS factor, u.l.basis AS basis
  FROM bodies b, unnest(from_json(json_extract(b.body, '$.levers'), '[{"column": "VARCHAR", "factor": "JSON", "basis": "VARCHAR"}]')) AS u(l)
  WHERE b.body IS NOT NULL),
judged AS (
  SELECT l.*,
         CASE WHEN l.lever IS NULL OR l.col = '' THEN 'a lever names a served metric''s column, <metric>.<column>'
              WHEN NOT EXISTS (SELECT 1 FROM gloss_glossary(current_schema(), NULL, false) g
                               WHERE g.subject = current_schema() || '.' || l.lever AND g.aspect = 'lever' AND g.state = 'current'
                                 AND (g.value->>'value') = 'true')
                THEN l.lever || ' carries no lever gloss: a lever is a column someone sets, glossed lever true on the metric''s view'
              WHEN NOT EXISTS (SELECT 1 FROM glossql.metric_axes() a WHERE a.metric = l.metric AND a.applicable AND coalesce(a.kind, '') NOT IN ('plan', 'fact', 'relation'))
                THEN l.metric || ' is no served metric of this dataset'
              WHEN NOT EXISTS (SELECT 1 FROM gloss_columns(current_schema()) c WHERE c.table_name = l.metric AND c.column_name = l.col)
                THEN l.metric || ' serves no column named ' || l.col
              WHEN json_type(l.factor) <> 'OBJECT' THEN 'the factor on ' || l.lever || ' is a factor per period, {"YYYY-MM": 1.15}'
         END AS reason
  FROM levers l),
folded AS (
  SELECT b.scenario, b.body IS NOT NULL AS glossed,
         coalesce(list({'lever': j.lever, 'metric': j.metric, 'column': j.col, 'factor': j.factor, 'basis': j.basis, 'reason': j.reason}
                       ORDER BY j.lever) FILTER (WHERE j.lever IS NOT NULL), []) AS levers,
         CASE WHEN b.body IS NULL THEN 'declared and not glossed: a scenario is a factor per period on declared levers'
              WHEN count(j.lever) = 0 THEN 'the scenario names no lever'
              ELSE string_agg(j.reason, '; ' ORDER BY j.lever) END AS reason
  FROM bodies b LEFT JOIN judged j USING (scenario)
  GROUP BY b.scenario, b.body)
SELECT scenario, glossed, reason IS NULL AS applicable, reason, levers FROM folded ORDER BY scenario;

CREATE OR REPLACE MACRO glossql.scenario_request() AS TABLE
WITH lv AS (
  SELECT s.scenario, u.l.lever AS lever, u.l.metric AS metric, u.l."column" AS col, u.l.factor AS factor
  FROM glossql.scenarios() s, unnest(s.levers) AS u(l) WHERE s.applicable),
axes AS (SELECT metric, floor, period_column, behavior FROM glossql.metric_axes() WHERE applicable),
series AS (
  SELECT metric, period, value FROM glossql.metric_series(unclaimed := 'skip')
  WHERE dimension = '' AND world = 'actual' AND value IS NOT NULL),
origins AS (SELECT metric, max(period) AS origin, count(*) AS history FROM series GROUP BY metric),
-- the lever's series: the mean of the column per period at the metric's floor
lever_rows AS (
  SELECT l.scenario, l.lever, c.rn,
         max(CASE WHEN c."column" = a.period_column THEN TRY_CAST(c.val AS TIMESTAMP) END) AS instant,
         max(CASE WHEN c."column" = l.col THEN TRY_CAST(c.val AS DOUBLE) END) AS at_row
  FROM (SELECT DISTINCT scenario, lever, metric, col FROM lv) l
  JOIN axes a ON a.metric = l.metric
  JOIN dataset_cells(current_schema(), 'views') c ON c."table" = l.metric AND c."column" IN (a.period_column, l.col)
  GROUP BY l.scenario, l.lever, c.rn),
lever_series AS (
  SELECT r.scenario, r.lever, date_trunc(a.floor, r.instant) AS period, avg(r.at_row) AS value
  FROM lever_rows r JOIN (SELECT DISTINCT scenario, lever, metric FROM lv) l USING (scenario, lever) JOIN axes a ON a.metric = l.metric
  WHERE r.instant IS NOT NULL AND r.at_row IS NOT NULL
  GROUP BY r.scenario, r.lever, date_trunc(a.floor, r.instant)),
-- the end of the last period a lever's factor names
named AS (
  SELECT l.scenario, l.lever, l.metric,
         max(CASE WHEN len(k) = 7 THEN (k || '-01')::TIMESTAMP + INTERVAL 1 MONTH ELSE k::TIMESTAMP + INTERVAL 1 DAY END) AS until
  FROM lv l, unnest(json_keys(l.factor)) AS u(k)
  GROUP BY l.scenario, l.lever, l.metric),
reach AS (
  SELECT n.scenario, n.metric, a.floor AS grain, a.behavior, o.origin, o.history, max(n.until) AS until
  FROM named n JOIN axes a USING (metric) LEFT JOIN origins o USING (metric)
  GROUP BY n.scenario, n.metric, a.floor, a.behavior, o.origin, o.history),
judged AS (
  SELECT r.*,
         CASE WHEN r.origin IS NULL THEN 'the series read serves no cell for ' || r.metric
              WHEN r.until <= r.origin + glossql.grain_step(r.grain)
                THEN 'the scenario names no period after ' || strftime(r.origin, '%Y-%m-%d') || ', the last actual of ' || r.metric
              WHEN r.history < 26 THEN r.history || ' ' || r.grain || 's of history: the estimate needs 26' END AS reason
  FROM reach r),
ahead AS (
  SELECT j.scenario, j.metric, list(g.p ORDER BY g.p) AS periods
  FROM judged j, unnest(generate_series(j.origin + glossql.grain_step(j.grain), j.until - glossql.grain_step(j.grain), glossql.grain_step(j.grain))) AS g(p)
  WHERE j.reason IS NULL GROUP BY j.scenario, j.metric),
held AS (
  SELECT l.scenario, l.metric, l.lever, l.factor,
         (SELECT arg_max(ls.value, ls.period) FROM lever_series ls, judged j
          WHERE ls.scenario = l.scenario AND ls.lever = l.lever AND j.scenario = l.scenario AND j.metric = l.metric AND ls.period <= j.origin) AS planned,
         (SELECT coalesce(list({'period': ls.period, 'value': ls.value} ORDER BY ls.period), [])
          FROM lever_series ls WHERE ls.scenario = l.scenario AND ls.lever = l.lever) AS series
  FROM lv l),
-- a lever the scenario does not move: every factor ahead is one (a planned value of nothing is judged once the
-- planned path is known, in scenario_bodies)
moved AS (
  SELECT h.scenario, h.metric, h.lever, h.planned,
         EXISTS (
           SELECT 1 FROM ahead a, unnest(a.periods) AS u(p)
           WHERE a.scenario = h.scenario AND a.metric = h.metric
             AND coalesce(json_extract(h.factor, '$."' || strftime(u.p, '%Y-%m-%d') || '"')::DOUBLE,
                          json_extract(h.factor, '$."' || strftime(u.p, '%Y-%m') || '"')::DOUBLE, 1.0) <> 1.0) AS moves
  FROM held h),
inputs AS (
  SELECT h.scenario, h.metric,
         list({'lever': h.lever, 'factor': h.factor, 'planned': h.planned, 'series': h.series} ORDER BY h.lever) AS levers,
         string_agg(CASE WHEN NOT m.moves THEN 'every factor on ' || h.lever || ' over the periods ahead is one' END, '; ' ORDER BY h.lever) AS unmoved
  FROM held h JOIN moved m USING (scenario, metric, lever)
  GROUP BY h.scenario, h.metric)
SELECT j.scenario, j.metric, j.grain, j.behavior, j.origin, j.history,
       coalesce(j.reason, CASE WHEN i.unmoved IS NOT NULL AND NOT EXISTS (SELECT 1 FROM moved m WHERE m.scenario = j.scenario AND m.metric = j.metric AND m.moves)
                               THEN 'the scenario moves no input of ' || j.metric || ': ' || i.unmoved END) AS reason,
       coalesce(j.reason, CASE WHEN i.unmoved IS NOT NULL AND NOT EXISTS (SELECT 1 FROM moved m WHERE m.scenario = j.scenario AND m.metric = j.metric AND m.moves) THEN 'x' END) IS NULL AS applicable,
       coalesce(a.periods, []::TIMESTAMP[]) AS ahead, i.levers
FROM judged j LEFT JOIN ahead a USING (scenario, metric) JOIN inputs i USING (scenario, metric)
ORDER BY j.scenario, j.metric;

CREATE OR REPLACE MACRO glossql.lever_bodies() AS TABLE
WITH r AS (SELECT * FROM glossql.scenario_request() WHERE applicable),
lv AS (
  SELECT r.scenario, r.metric, r.grain, r.ahead, u.l.lever AS lever, u.l.series AS series,
         CASE r.grain WHEN 'day' THEN 31 WHEN 'week' THEN 13 WHEN 'month' THEN 12 WHEN 'quarter' THEN 4 ELSE 0 END AS reach
  FROM r, unnest(r.levers) AS u(l)),
rows_ AS (
  SELECT l.scenario, l.metric, l.lever, sp.s.period AS period, sp.s.value AS value, 'actual' AS status
  FROM lv l, unnest(l.series) AS sp(s)
  UNION ALL
  SELECT l.scenario, l.metric, l.lever, ap.p, NULL::DOUBLE, 'forecast'
  FROM lv l, unnest(l.ahead[1:least(len(l.ahead), l.reach)]) AS ap(p))
SELECT l.scenario, l.metric, l.lever, l.grain, least(len(l.ahead), l.reach) AS horizon,
       json_object('spec', json_object('targets', ['value'], 'horizon', least(len(l.ahead), l.reach), 'levels', [0.5], 'grain', l.grain),
                   'table', json_object('member', list(l.lever ORDER BY w.period, w.status), 'period', list(strftime(w.period, '%Y-%m-%d') ORDER BY w.period, w.status),
                                        'status', list(w.status ORDER BY w.period, w.status), 'value', list(w.value ORDER BY w.period, w.status))) AS body
FROM lv l JOIN rows_ w USING (scenario, metric, lever)
WHERE l.reach > 0 AND len(l.series) >= 6
GROUP BY l.scenario, l.metric, l.lever, l.grain, l.ahead, l.reach
ORDER BY l.scenario, l.metric, l.lever;

CREATE OR REPLACE MACRO glossql.scenario_bodies(paths := NULL) AS TABLE
WITH r AS (SELECT * FROM glossql.scenario_request() WHERE applicable),
hist AS (
  SELECT r.scenario, r.metric, s.period, 'actual' AS status, s.value, 0 AS ord
  FROM r JOIN (SELECT metric, period, value FROM glossql.metric_series(unclaimed := 'skip')
               WHERE dimension = '' AND world = 'actual' AND value IS NOT NULL) s USING (metric)),
ahead AS (
  SELECT r.scenario, r.metric, u.p AS period, st.status, NULL::DOUBLE AS value, st.ord
  FROM r, unnest(r.ahead) AS u(p), (VALUES ('plan', 1), ('scenario', 2)) st(status, ord)),
rows_ AS (SELECT * FROM hist UNION ALL SELECT * FROM ahead),
inputs AS (
  SELECT r.scenario, r.metric, u.l.lever AS lever, u.l.factor AS factor, u.l.planned AS held, u.l.series AS series,
         from_json(coalesce(json_extract(paths, '$."' || r.scenario || '|' || r.metric || '|' || u.l.lever || '"'), '[]'::JSON),
                   '[{"period": "VARCHAR", "value": "DOUBLE"}]') AS path
  FROM r, unnest(r.levers) AS u(l)),
-- the planned input at a period: the lever's forecast median there, its last median past the forecast's reach, else the held value
planned AS (
  SELECT w.scenario, w.metric, i.lever, w.period, w.ord, w.status, i.factor, i.series,
         CASE WHEN i.path IS NOT NULL AND len(i.path) > 0
              THEN coalesce(list_extract(list_filter(i.path, lambda e: e.period::TIMESTAMP = w.period), 1).value, i.path[-1].value)
              ELSE i.held END AS planned
  FROM rows_ w JOIN inputs i USING (scenario, metric)),
input_cells AS (
  SELECT p.scenario, p.metric, p.lever, p.period, p.ord,
         CASE p.status WHEN 'actual' THEN list_extract(list_filter(p.series, lambda e: e.period = p.period), 1).value
                       WHEN 'plan' THEN p.planned
                       ELSE p.planned * coalesce(json_extract(p.factor, '$."' || strftime(p.period, '%Y-%m-%d') || '"')::DOUBLE,
                                                 json_extract(p.factor, '$."' || strftime(p.period, '%Y-%m') || '"')::DOUBLE, 1.0) END AS value
  FROM planned p),
-- an input the scenario moves: some period ahead where its value under the scenario differs from the planned one
moves AS (
  SELECT scenario, metric, lever,
         bool_or(scenario_value IS DISTINCT FROM plan_value AND scenario_value IS NOT NULL) AS moves,
         any_value(plan_value) AS planned_at_first
  FROM (SELECT scenario, metric, lever, period,
               max(CASE WHEN ord = 1 THEN value END) AS plan_value, max(CASE WHEN ord = 2 THEN value END) AS scenario_value
        FROM input_cells WHERE ord > 0 GROUP BY scenario, metric, lever, period)
  GROUP BY scenario, metric, lever),
unmoved AS (
  SELECT scenario, metric,
         string_agg(lever || ' stands at ' || coalesce(planned_at_first::VARCHAR, 'nothing') || ' as planned over the periods ahead, and a factor on nothing is nothing', '; ' ORDER BY lever) AS reason
  FROM moves WHERE NOT moves GROUP BY scenario, metric
  HAVING NOT bool_or(moves)),
base AS (
  SELECT w.scenario, w.metric,
         json_object('member', list(w.metric ORDER BY w.period, w.ord), 'period', list(strftime(w.period, '%Y-%m-%d') ORDER BY w.period, w.ord),
                     'status', list(w.status ORDER BY w.period, w.ord), 'value', list(w.value ORDER BY w.period, w.ord)) AS cols
  FROM rows_ w GROUP BY w.scenario, w.metric),
levered AS (
  SELECT scenario, metric, json_group_object(lever, vals) AS cols
  FROM (SELECT scenario, metric, lever, list(value ORDER BY period, ord) AS vals FROM input_cells GROUP BY scenario, metric, lever)
  GROUP BY scenario, metric)
SELECT r.scenario, r.metric, r.grain,
       CASE WHEN u.reason IS NULL THEN json_object('spec', json_object('targets', ['value'], 'levels', [0.1, 0.5, 0.9]),
                                                   'table', json_merge_patch(b.cols, l.cols)) END AS body,
       u.reason AS unmoved
FROM r JOIN base b USING (scenario, metric) JOIN levered l USING (scenario, metric)
LEFT JOIN (SELECT scenario, metric, reason FROM unmoved
           WHERE NOT EXISTS (SELECT 1 FROM moves m WHERE m.scenario = unmoved.scenario AND m.metric = unmoved.metric AND m.moves)) u USING (scenario, metric)
ORDER BY r.scenario, r.metric;

--===------------------------------------------------------------------===--
-- metric_scenario: the estimate as a measurement on the dataset, over
-- every scenario glossed: per scenario its judgement and levers, per
-- metric reached its grain, origin, the service's sign, confidence and
-- basis, explains and error, the points ahead (period, low, value, high
-- at the 10, 50 and 90 levels; the history's own reading with its 90%
-- interval) and the parts (per lever, what it alone does by the
-- history). Stale once the data or the record it read moves, which a new
-- scenario gloss does. scenario_points(s): the newest estimate, one row
-- per scenario (or every one, NULL), metric and period ahead, with how
-- often such a band held, the forecast service's share at that grain and
-- step; a metric not estimated is one row with its reason.
-- scenario_levers(s): the parts.
--===------------------------------------------------------------------===--

DECLARE ASPECT metric_scenario WITH $${
  "type": "object",
  "properties": {
    "served_by": {"type": ["string", "null"]},
    "scenarios": {"type": "array", "items": {"type": "object"}}
  }
}$$ AS MEASUREMENT ON DATASET;

DECLARE FUNCTION metric_scenario FOR GLOBAL AS $$
  WITH sc AS (SELECT * FROM glossql.scenarios()),
  req AS (SELECT * FROM glossql.scenario_request()),
  -- each lever's own forecast, soft: a refusal holds the lever instead
  lf AS (SELECT lb.scenario, lb.metric, lb.lever, glossql.service_soft('/forecast', lb.body) AS fa FROM glossql.lever_bodies() lb),
  medians AS (
    SELECT scenario, metric, lever,
           unnest(json_extract(fa, '$.table.period')::VARCHAR[]) AS period,
           unnest(json_extract(fa, '$.table.level')::DOUBLE[]) AS level,
           unnest(json_extract(fa, '$.table.value')::DOUBLE[]) AS value
    FROM lf WHERE json_extract_string(fa, '$.error') IS NULL),
  paths AS (
    SELECT scenario, metric, lever, list({'period': period, 'value': value} ORDER BY period) AS path
    FROM medians WHERE level = 0.5 AND period IS NOT NULL AND value IS NOT NULL GROUP BY scenario, metric, lever),
  planned_json AS (SELECT json_group_object(scenario || '|' || metric || '|' || lever, path) AS j FROM paths),
  planned_from AS (
    SELECT r.scenario, r.metric, u.l.lever AS lever,
           CASE WHEN p.lever IS NOT NULL THEN 'forecast'
                WHEN lf.lever IS NULL THEN 'held: the lever is not forecast at this grain, or has too short a history'
                ELSE 'held: ' || coalesce(json_extract_string(lf.fa, '$.error'), 'the forecast answered no median') END AS planned_from
    FROM req r, unnest(r.levers) AS u(l)
    LEFT JOIN lf ON lf.scenario = r.scenario AND lf.metric = r.metric AND lf.lever = u.l.lever
    LEFT JOIN paths p ON p.scenario = r.scenario AND p.metric = r.metric AND p.lever = u.l.lever),
  -- a body stands only where an input moves; a NULL body makes no call
  b AS (SELECT scenario, metric, grain, unmoved, CASE WHEN body IS NOT NULL THEN glossql.service('/whatif', body) END AS a
        FROM glossql.scenario_bodies(paths := (SELECT j FROM planned_json))),
  rows_ AS (
    SELECT b.scenario, b.metric,
           unnest(json_extract(a, '$.table.member')::VARCHAR[]) AS member,
           unnest(json_extract(a, '$.table.period')::VARCHAR[]) AS period,
           unnest(json_extract(a, '$.table.status')::VARCHAR[]) AS status,
           unnest(coalesce(json_extract(a, '$.table.column')::VARCHAR[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: 'value'))) AS target,
           unnest(json_extract(a, '$.table.level')::DOUBLE[]) AS level,
           unnest(json_extract(a, '$.table.value')::DOUBLE[]) AS value,
           unnest(coalesce(json_extract(a, '$.table.history_value')::DOUBLE[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::DOUBLE))) AS history_value,
           unnest(coalesce(json_extract(a, '$.table.history_low')::DOUBLE[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::DOUBLE))) AS history_low,
           unnest(coalesce(json_extract(a, '$.table.history_high')::DOUBLE[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::DOUBLE))) AS history_high,
           unnest(coalesce(json_extract(a, '$.table.sign')::VARCHAR[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::VARCHAR))) AS sign,
           unnest(coalesce(json_extract(a, '$.table.confidence')::VARCHAR[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::VARCHAR))) AS confidence,
           unnest(coalesce(json_extract(a, '$.table.basis')::VARCHAR[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::VARCHAR))) AS basis,
           unnest(coalesce(json_extract(a, '$.table.explains')::DOUBLE[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::DOUBLE))) AS explains,
           unnest(coalesce(json_extract(a, '$.table.error')::DOUBLE[], list_transform(json_extract(a, '$.table.member')::VARCHAR[], lambda x: NULL::DOUBLE))) AS error
    FROM b WHERE a IS NOT NULL),
  points AS (
    SELECT scenario, metric, period,
           max(CASE WHEN level = 0.1 THEN value END) AS low, max(CASE WHEN level = 0.5 THEN value END) AS value,
           max(CASE WHEN level = 0.9 THEN value END) AS high,
           any_value(history_value) AS history_value, any_value(history_low) AS history_low, any_value(history_high) AS history_high
    FROM rows_ WHERE member = metric AND status = 'scenario' AND target = 'value' AND period IS NOT NULL
    GROUP BY scenario, metric, period),
  said AS (
    SELECT scenario, metric, any_value(sign) AS sign, any_value(confidence) AS confidence, any_value(basis) AS basis,
           any_value(explains) AS explains, any_value(error) AS error
    FROM rows_ WHERE member = metric AND target = 'value' GROUP BY scenario, metric),
  parts AS (
    SELECT b.scenario, b.metric, u.p.input AS lever, u.p.history_shift AS history_shift, u.p.history_low AS history_low, u.p.history_high AS history_high
    FROM b, unnest(from_json(json_extract(a, '$.answer.parts'),
           '[{"member": "VARCHAR", "column": "VARCHAR", "input": "VARCHAR", "history_shift": "DOUBLE", "history_low": "DOUBLE", "history_high": "DOUBLE"}]')) AS u(p)
    WHERE u.p.member = b.metric AND coalesce(u.p."column", 'value') = 'value'),
  failures AS (
    SELECT b.scenario, b.metric, list(u.e.error ORDER BY u.e.error) AS errors
    FROM b, unnest(from_json(json_extract(a, '$.answer.errors'), '[{"member": "VARCHAR", "column": "VARCHAR", "error": "VARCHAR"}]')) AS u(e)
    WHERE u.e.member = b.metric GROUP BY b.scenario, b.metric),
  per_metric AS (
    SELECT r.scenario, r.metric, r.grain, r.origin,
           r.applicable AND bd.unmoved IS NULL AND EXISTS (SELECT 1 FROM points p WHERE p.scenario = r.scenario AND p.metric = r.metric) AS applicable,
           CASE WHEN NOT r.applicable THEN r.reason
                WHEN bd.unmoved IS NOT NULL THEN 'the scenario moves no input of ' || r.metric || ': ' || bd.unmoved
                WHEN NOT EXISTS (SELECT 1 FROM points p WHERE p.scenario = r.scenario AND p.metric = r.metric)
                  THEN coalesce(f.errors[1], 'the service answered no point') END AS reason,
           (SELECT coalesce(list({'lever': pf.lever, 'planned_from': pf.planned_from} ORDER BY pf.lever), [])
            FROM planned_from pf WHERE pf.scenario = r.scenario AND pf.metric = r.metric) AS levers,
           s.sign, s.confidence, s.basis, s.explains, s.error,
           (SELECT coalesce(list({'period': p.period, 'low': p.low, 'value': p.value, 'high': p.high, 'history_value': p.history_value,
                                  'history_low': p.history_low, 'history_high': p.history_high} ORDER BY p.period), [])
            FROM points p WHERE p.scenario = r.scenario AND p.metric = r.metric) AS points,
           (SELECT coalesce(list({'lever': pt.lever, 'history_shift': pt.history_shift, 'history_low': pt.history_low, 'history_high': pt.history_high} ORDER BY pt.lever), [])
            FROM parts pt WHERE pt.scenario = r.scenario AND pt.metric = r.metric) AS parts
    FROM req r LEFT JOIN said s ON s.scenario = r.scenario AND s.metric = r.metric
    LEFT JOIN failures f ON f.scenario = r.scenario AND f.metric = r.metric
    LEFT JOIN b bd ON bd.scenario = r.scenario AND bd.metric = r.metric),
  per_scenario AS (
    SELECT sc.scenario, sc.applicable, sc.reason,
           list_transform(sc.levers, lambda l: {'lever': l.lever, 'basis': l.basis, 'reason': l.reason}) AS levers,
           (SELECT coalesce(list({'metric': m.metric, 'grain': m.grain, 'origin': strftime(m.origin, '%Y-%m-%d'), 'applicable': m.applicable, 'reason': m.reason,
                                  'levers': m.levers, 'sign': m.sign, 'confidence': m.confidence, 'basis': m.basis, 'explains': m.explains, 'error': m.error,
                                  'points': m.points, 'parts': m.parts} ORDER BY m.metric), [])
            FROM per_metric m WHERE m.scenario = sc.scenario) AS metrics
    FROM sc)
  SELECT (SELECT string_agg(DISTINCT json_extract_string(a, '$.answer.service') || ' ' || json_extract_string(a, '$.answer.version'), ', ') FROM b) AS served_by,
         (SELECT coalesce(list({'scenario': scenario, 'applicable': applicable, 'reason': reason, 'levers': levers, 'metrics': metrics} ORDER BY scenario), [])
          FROM per_scenario) AS scenarios
$$ RETURNS metric_scenario;

-- The estimate's witness says an estimate stands: green once one landed,
-- so that it is owed while none has, and re-owed once stale.
DECLARE FUNCTION scenario_stands FOR GLOBAL AS $$
  SELECT subject, 'green' AS band, 0.0 AS score FROM slots WHERE speaker = 'function'
$$;

DECLARE WITNESS scenario_w ON metric_scenario DETECTOR scenario_stands;

CREATE OR REPLACE MACRO glossql.scenario_points(s) AS TABLE
WITH newest AS (
  SELECT value, computed_at FROM glossql.measurements
  WHERE dataset = current_schema() AND function = 'metric_scenario' ORDER BY seq DESC LIMIT 1),
scn AS (
  SELECT computed_at, json_extract_string(value, '$.served_by') AS served_by,
         unnest(from_json(json_extract(value, '$.scenarios'),
           '[{"scenario": "VARCHAR", "applicable": "BOOLEAN", "reason": "VARCHAR",
              "metrics": [{"metric": "VARCHAR", "grain": "VARCHAR", "origin": "VARCHAR", "applicable": "BOOLEAN", "reason": "VARCHAR",
                           "levers": [{"lever": "VARCHAR", "planned_from": "VARCHAR"}], "sign": "VARCHAR", "confidence": "VARCHAR", "basis": "VARCHAR", "explains": "DOUBLE", "error": "DOUBLE",
                           "points": [{"period": "VARCHAR", "low": "DOUBLE", "value": "DOUBLE", "high": "DOUBLE",
                                       "history_value": "DOUBLE", "history_low": "DOUBLE", "history_high": "DOUBLE"}]}]}]')) AS sc
  FROM newest),
mets AS (
  SELECT computed_at, served_by, sc.scenario, sc.applicable AS scenario_applicable, sc.reason AS scenario_reason, unnest(sc.metrics) AS m
  FROM scn WHERE s IS NULL OR sc.scenario = s),
stepped AS (
  SELECT computed_at, served_by, scenario, m.metric, m.grain, m.origin, m.applicable, m.reason, m.levers, m.sign, m.confidence, m.basis, m.explains, m.error,
         unnest(m.points) AS p, unnest(generate_series(1, cardinality(m.points)::BIGINT)) AS step
  FROM mets WHERE cardinality(m.points) > 0),
-- how often such a band held: the service's share at that grain and step, from the newest forecast run (the record's own
-- tally reads the series, which reads the worlds, which read this)
held AS (
  SELECT r.metric, r.grain, r.step, r.service_80 AS held_80, CASE WHEN r.service_80 IS NOT NULL THEN 'service' END AS held_from
  FROM glossql.forecast_record() r JOIN (SELECT max(run) AS run FROM glossql.forecast_record()) n ON n.run = r.run
  WHERE r.applicable AND r.span IS NULL AND r.step IS NOT NULL)
SELECT s.scenario, s.metric, s.grain, s.origin, s.applicable, s.reason, list_transform(s.levers, lambda l: l.lever) AS levers, 'estimated' AS pass, s.step::BIGINT AS step,
       s.p.period::TIMESTAMP AS period, s.p.low AS low, s.p.value AS value, s.p.high AS high,
       s.p.history_value AS history_value, s.p.history_low AS history_low, s.p.history_high AS history_high,
       h.held_80, h.held_from, s.sign, s.confidence, s.basis, s.explains, s.error, s.served_by, s.computed_at
FROM stepped s LEFT JOIN held h ON h.metric = s.metric AND h.grain = s.grain AND h.step = s.step
UNION ALL
SELECT scenario, m.metric, m.grain, m.origin, m.applicable, m.reason, list_transform(m.levers, lambda l: l.lever), 'estimated', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, m.sign, m.confidence, m.basis, m.explains, m.error, served_by, computed_at
FROM mets WHERE coalesce(cardinality(m.points), 0) = 0
ORDER BY scenario, metric, step NULLS FIRST;

CREATE OR REPLACE MACRO glossql.scenario_levers(s) AS TABLE
WITH newest AS (
  SELECT value, computed_at FROM glossql.measurements
  WHERE dataset = current_schema() AND function = 'metric_scenario' ORDER BY seq DESC LIMIT 1),
scn AS (
  SELECT computed_at, unnest(from_json(json_extract(value, '$.scenarios'),
           '[{"scenario": "VARCHAR", "metrics": [{"metric": "VARCHAR", "applicable": "BOOLEAN", "reason": "VARCHAR", "sign": "VARCHAR", "basis": "VARCHAR",
              "levers": [{"lever": "VARCHAR", "planned_from": "VARCHAR"}],
              "parts": [{"lever": "VARCHAR", "history_shift": "DOUBLE", "history_low": "DOUBLE", "history_high": "DOUBLE"}]}]}]')) AS sc
  FROM newest),
mets AS (SELECT computed_at, sc.scenario, unnest(sc.metrics) AS m FROM scn WHERE s IS NULL OR sc.scenario = s),
levers AS (SELECT computed_at, scenario, m, unnest(m.levers) AS l FROM mets WHERE cardinality(m.levers) > 0),
parts AS (SELECT scenario, m.metric AS metric, unnest(m.parts) AS p FROM mets WHERE cardinality(m.parts) > 0)
SELECT l.scenario, l.m.metric AS metric, l.m.applicable AS applicable, l.m.reason AS reason, l.m.sign AS sign, l.m.basis AS basis,
       l.l.lever AS lever, l.l.planned_from AS planned_from, p.p.history_shift AS history_shift, p.p.history_low AS history_low, p.p.history_high AS history_high, l.computed_at
FROM levers l LEFT JOIN parts p ON p.scenario = l.scenario AND p.metric = l.m.metric AND p.p.lever = l.l.lever
ORDER BY l.scenario, metric, lever;

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
-- serves it), the forecast world (the newest forecast's medians at its
-- grain) and the scenario world (named scenario: the newest estimate's
-- medians ahead) under the metric, for metric_series(world := …).
--===------------------------------------------------------------------===--

CREATE OR REPLACE MACRO glossql.plan_rows() AS TABLE
WITH p AS (
  SELECT a.metric AS plan, a.plan_of AS metric, a.covers, a.period_column, a.value_column, a.behavior
  FROM glossql.metric_axes() a WHERE a.kind = 'plan' AND a.applicable),
rows_ AS (
  SELECT p.plan, p.metric, p.behavior, p.covers,
         max(CASE WHEN c."column" = p.period_column THEN TRY_CAST(c.val AS TIMESTAMP) END) AS instant,
         max(CASE WHEN c."column" = p.value_column THEN TRY_CAST(c.val AS DOUBLE) END) AS value
  FROM dataset_cells(current_schema(), 'views') c JOIN p ON p.plan = c."table"
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

CREATE OR REPLACE MACRO glossql.world_cells(w, p, s, g) AS TABLE
WITH named AS (
  SELECT a.metric AS plan, a.applicable, a.reason FROM glossql.metric_axes() a WHERE a.kind = 'plan' AND a.metric = p),
refusal AS (
  SELECT CASE WHEN w NOT IN ('actual', 'plan', 'forecast', 'scenario') THEN error('metric_series(world := ''' || w || '''): the worlds are actual, plan, forecast and scenario')
              WHEN w = 'scenario' AND s IS NULL THEN error('metric_series(world := ''scenario'') names the scenario it serves, scenario := ''<name>'' — a scenario is a FACT aspect declared with x-kind scenario, glossed with a factor per period on declared levers')
              WHEN w = 'scenario' AND NOT EXISTS (SELECT 1 FROM glossql.scenarios() WHERE scenario = s) THEN error(
                'metric_series(scenario := ''' || s || '''): no scenario of that name is declared — a scenario is a FACT aspect declared with x-kind scenario')
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
  WHERE w = 'forecast' AND f.applicable AND f.span IS NULL AND f.period IS NOT NULL AND (g IS NULL OR g = f.grain)),
scenario_world AS (
  SELECT sp.metric, 'scenario' AS world, '' AS dimension, '' AS member, sp.period, sp.value,
         NULL::DOUBLE AS num, NULL::DOUBLE AS den, coalesce(a.behavior, 'none') AS behavior
  FROM glossql.scenario_points(s) sp LEFT JOIN glossql.metric_axes() a ON a.metric = sp.metric
  WHERE w = 'scenario' AND sp.applicable AND sp.value IS NOT NULL AND (g IS NULL OR g = sp.grain))
SELECT * FROM plan_world
UNION ALL SELECT * FROM forecast_world
UNION ALL SELECT * FROM scenario_world
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
-- forecast's). Computed at read; metric_plan lands it per plan.
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
-- the newest forecast's bands, with how often such a band held: the record's own share where it holds thirty points, else the
-- service's; the actual behind each run as forecast_runs() joins it, over this read's own cells
runs AS MATERIALIZED (
  SELECT r.*,
         CASE WHEN r.span IS NULL THEN a.value
              ELSE (SELECT CASE WHEN count(*) = r.span THEN sum(a2.value) END FROM floor_cells a2
                    WHERE a2.metric = r.metric AND a2.period > r.origin AND a2.period <= r.period) END AS actual
  FROM glossql.forecast_record() r
  LEFT JOIN floor_cells a ON a.metric = r.metric AND a.period = r.period AND r.span IS NULL
  WHERE r.applicable AND r.step IS NOT NULL),
tally AS (
  SELECT metric, grain, step, span, to_go, count(actual) AS points, count(*) FILTER (WHERE actual BETWEEN p10 AND p90) AS inside_80
  FROM runs GROUP BY metric, grain, step, span, to_go),
whole AS (SELECT grain, step, span, to_go, sum(points) AS points, sum(inside_80) AS inside_80 FROM tally GROUP BY grain, step, span, to_go),
newest_run AS (SELECT max(run) AS run FROM runs),
fp AS MATERIALIZED (
  SELECT r.metric, r.grain, r.origin, r.step, r.span, r.to_go, r.period, r.p10, r.p50, r.p90,
         CASE WHEN tm.points >= 30 THEN tm.inside_80 / tm.points WHEN td.points >= 30 THEN td.inside_80 / td.points ELSE r.service_80 END AS held_80,
         CASE WHEN tm.points >= 30 THEN 'metric' WHEN td.points >= 30 THEN 'dataset' WHEN r.service_80 IS NOT NULL THEN 'service' END AS held_from
  FROM runs r JOIN newest_run n ON n.run = r.run
  LEFT JOIN tally tm ON tm.metric = r.metric AND tm.grain = r.grain AND tm.step = r.step AND tm.span IS NOT DISTINCT FROM r.span AND tm.to_go IS NOT DISTINCT FROM r.to_go
  LEFT JOIN whole td ON td.grain = r.grain AND td.step = r.step AND td.span IS NOT DISTINCT FROM r.span AND td.to_go IS NOT DISTINCT FROM r.to_go),
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
-- metric_plan: the plan judged, as a measurement on the plan — the rows
-- plan_points() serves for it as of the data, with the forecast run they
-- were judged against (forecast_run, the newest metric_forecast
-- measurement's seq, and its origin). Wanted on the plan's row until one
-- stands; stale by the pin once the data or the record it read moves; the
-- plan goal asks for it again once a newer forecast stands, which the pin
-- does not see, measurements being no leg. plan_record(): the newest
-- judgement per plan from the measurements alone, one row per point, the
-- read a door's next arm takes; plan_points() stays the live read.
--===------------------------------------------------------------------===--

DECLARE ASPECT metric_plan WITH $${
  "type": "object",
  "properties": {
    "plan": {"type": "string"},
    "metric": {"type": ["string", "null"]},
    "grain": {"type": ["string", "null"]},
    "forecast_run": {"type": ["integer", "null"]},
    "forecast_origin": {"type": ["string", "null"]},
    "points": {"type": "array", "items": {"type": "object"}}
  }
}$$ AS MEASUREMENT ON TABLE;

DECLARE FUNCTION metric_plan FOR GLOBAL AS $$
  WITH pts AS (SELECT * FROM glossql.plan_points() WHERE plan = split_part($subject, '.', 2)),
  own AS (SELECT metric, grain FROM glossql.plan_grains() WHERE plan = split_part($subject, '.', 2)),
  run AS (SELECT max(seq) AS seq FROM glossql.measurements WHERE dataset = current_schema() AND function = 'metric_forecast')
  SELECT split_part($subject, '.', 2) AS plan,
         (SELECT any_value(metric) FROM own) AS metric,
         (SELECT any_value(grain) FROM own) AS grain,
         (SELECT seq FROM run) AS forecast_run,
         (SELECT any_value(origin) FROM pts WHERE origin IS NOT NULL) AS forecast_origin,
         (SELECT coalesce(list({'grain': grain, 'period': strftime(period, '%Y-%m-%d'), 'label': label, 'value': value,
                                'actual': actual, 'variance': variance, 'low': low, 'expected': expected, 'high': high,
                                'stands': stands, 'covers': covers, 'held_80': held_80, 'held_from': held_from}
                               ORDER BY CASE grain WHEN 'year' THEN 0 ELSE 1 END, period), [])
          FROM pts) AS points
$$ RETURNS metric_plan;

-- The gap is read by the witness's detector over the judgement's own
-- voice: red where a banded period lies below or above the forecast's
-- band, the score the share of banded periods that do, so that ATTEST()
-- carries it and the brief counts it.
DECLARE FUNCTION plan_gap FOR GLOBAL AS $$
  SELECT subject,
         CASE WHEN outside > 0 THEN 'red' ELSE 'green' END AS band,
         CASE WHEN banded > 0 THEN outside / banded ELSE 0.0 END AS score
  FROM (SELECT subject,
               coalesce(len(list_filter(stands, lambda s: s IN ('below', 'above'))), 0)::DOUBLE AS outside,
               coalesce(len(list_filter(stands, lambda s: s IN ('below', 'above', 'inside'))), 0)::DOUBLE AS banded
        FROM (SELECT subject,
                     list_transform(from_json(json_extract(body, '$.points'), '[{"stands": "VARCHAR"}]'), lambda p: p.stands) AS stands
              FROM slots WHERE speaker = 'function'))
$$;

DECLARE WITNESS plan_w ON metric_plan DETECTOR plan_gap;

CREATE OR REPLACE MACRO glossql.plan_record() AS TABLE
WITH newest AS (
  SELECT subject, value, computed_at FROM glossql.measurements
  WHERE dataset = current_schema() AND function = 'metric_plan'
  QUALIFY row_number() OVER (PARTITION BY subject ORDER BY seq DESC) = 1),
judged AS (
  SELECT json_extract_string(value, '$.plan') AS plan, json_extract_string(value, '$.metric') AS metric,
         json_extract_string(value, '$.grain') AS plan_grain, json_extract(value, '$.forecast_run')::BIGINT AS forecast_run,
         json_extract_string(value, '$.forecast_origin') AS forecast_origin, computed_at,
         from_json(json_extract(value, '$.points'),
           '[{"grain": "VARCHAR", "period": "VARCHAR", "label": "VARCHAR", "value": "DOUBLE", "actual": "DOUBLE", "variance": "DOUBLE",
              "low": "DOUBLE", "expected": "DOUBLE", "high": "DOUBLE", "stands": "VARCHAR", "covers": "VARCHAR",
              "held_80": "DOUBLE", "held_from": "VARCHAR"}]') AS points
  FROM newest),
pts AS (
  SELECT plan, metric, plan_grain, forecast_run, forecast_origin, computed_at, unnest(points) AS p
  FROM judged WHERE cardinality(points) > 0)
SELECT plan, metric, plan_grain, forecast_run, forecast_origin, computed_at,
       p.grain AS grain, p.period::TIMESTAMP AS period, p.label, p.value, p.actual, p.variance, p.low, p.expected, p.high,
       p.stands, p.covers, p.held_80, p.held_from
FROM pts
UNION ALL
SELECT plan, metric, plan_grain, forecast_run, forecast_origin, computed_at,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM judged WHERE coalesce(cardinality(points), 0) = 0
ORDER BY plan, CASE grain WHEN 'year' THEN 0 ELSE 1 END, period;

--===------------------------------------------------------------------===--
-- The two goals this kit answers for a door's next read. bands: the walk
-- stands and no band is red. plan: the plan served, the forecast at the
-- current origin, the plan judged against it, every gap judged by the
-- human — with a scenario's estimate where one is glossed. Served as
-- CTEs and arms over the relations the door reads once (ds, axes, owed,
-- reds, bodies, applicable); every CTE reads the record, none the data:
-- futures_red is the metric whose newest point the red verdict scores,
-- futures_judged the plans' judgements as they stand, futures_gaps the
-- plan years the forecast's band does not hold, futures_estimate what
-- the newest estimate says per scenario and metric.
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
futures_forecast AS (
  SELECT max(seq) AS run FROM glossql.measurements WHERE dataset = current_schema() AND function = 'metric_forecast'),
futures_judged AS (
  SELECT json_extract_string(m.value, '$.plan') AS plan, json_extract(m.value, '$.forecast_run')::BIGINT AS forecast_run,
         coalesce(g.current, true) AS current
  FROM (SELECT subject, value FROM glossql.measurements WHERE dataset = current_schema() AND function = 'metric_plan'
        QUALIFY row_number() OVER (PARTITION BY subject ORDER BY seq DESC) = 1) m
  LEFT JOIN (SELECT subject, current FROM gloss_glossary(current_schema(), NULL, true)
             WHERE actor = 'function' AND aspect = 'metric_plan') g ON g.subject = m.subject),
futures_gaps AS (
  SELECT plan, metric, label, value, low, high, stands FROM glossql.plan_record()
  WHERE grain = 'year' AND stands IN ('below', 'above')),
futures_scenarios AS (
  SELECT a.name FROM (SELECT name, kind, schema FROM glossql.aspects QUALIFY row_number() OVER (PARTITION BY name ORDER BY seq DESC) = 1) a
  WHERE a.kind = 'FACT' AND json_extract_string(a.schema, '$.x-kind') = 'scenario'
    AND EXISTS (SELECT 1 FROM gloss_glossary(current_schema(), NULL, false) g WHERE g.aspect = a.name AND g.state = 'current')),
futures_levers AS (
  SELECT string_agg(subject, ', ' ORDER BY subject) AS columns
  FROM gloss_glossary(current_schema(), NULL, false) WHERE aspect = 'lever' AND state = 'current' AND (value->>'value') = 'true'),
futures_estimate AS (
  SELECT DISTINCT scenario, metric, applicable, reason, sign, confidence, basis FROM glossql.scenario_points(NULL))
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
SELECT 'plan', 7, 'next', 'metric_plan',
       'judge ' || p.plan,
       p.plan || ' is served and nobody judged it against the actual and the forecast''s band: run the function over it, or the docket''s re-measure — the plan goal reads the judgement from the record',
       'SELECT metric_plan() FROM ' || ds.dataset || '.' || p.plan, ''
FROM futures_plans p CROSS JOIN ds
WHERE p.applicable AND NOT EXISTS (SELECT 1 FROM futures_judged j WHERE j.plan = p.plan)
UNION ALL
SELECT 'plan', 8, 'next', 'metric_plan',
       'judge ' || p.plan || ' again',
       'the judgement of ' || p.plan || CASE WHEN NOT j.current THEN ' predates a change to what it read: run the function over it again, or the docket''s re-measure'
                                             ELSE ' predates the newest forecast: run the function over it again' END,
       'SELECT metric_plan() FROM ' || ds.dataset || '.' || p.plan, ''
FROM futures_plans p JOIN futures_judged j USING (plan) CROSS JOIN ds CROSS JOIN futures_forecast f
WHERE p.applicable AND (NOT j.current OR j.forecast_run IS DISTINCT FROM f.run)
UNION ALL
SELECT 'plan', 9, 'next', 'GLOSS scenario',
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
SELECT 'plan', 10, 'next', 'metric_scenario', 'run the estimate', o.why,
       'SELECT metric_scenario() FROM ' || ds.dataset, ''
FROM owed o, ds
WHERE o.kind = 'never measured' AND o.subject = 'metric_scenario' AND EXISTS (SELECT 1 FROM futures_scenarios)
UNION ALL
SELECT 'plan', 11, 'next', 'metric_scenario', 'run the estimate again', o.why,
       'SELECT metric_scenario() FROM ' || ds.dataset, ''
FROM owed o, ds
WHERE o.kind = 're-measure' AND o.subject = 'metric_scenario' AND EXISTS (SELECT 1 FROM futures_scenarios)
UNION ALL
SELECT 'plan', 12, 'next', 'GLOSS plan',
       'judge ' || g.plan || ' at ' || g.label || ' under ' || s.name,
       g.plan || ' at ' || g.label || ' lies ' || g.stands || ' the forecast''s band, and '
         || CASE WHEN e.metric IS NULL THEN s.name || ' does not reach ' || g.metric || ': its levers move other metrics — record the gap as the plan''s assumption, or declare a scenario on a lever of ' || g.metric
                 WHEN NOT e.applicable THEN s.name || ' gives no estimate for ' || g.metric || ' (' || coalesce(e.reason, 'no reason') || ') — record the gap as the plan''s assumption'
                 ELSE s.name || ' moves ' || g.metric || ' ' || coalesce(e.confidence, '?') || ' (' || coalesce(e.sign, '?') || ': ' || coalesce(e.basis, '') || ') — read scenario_points(''' || s.name || ''') and record the gap with the scenario''s reading as the plan''s assumption' END,
       '-- append to "assumptions" of the plan''s standing body: {"dimension": "definition", "key": "plan-' || g.label || '", "assumption": "<the gap at ' || g.label || ' and what ' || s.name || ' says closes it, or does not>", "basis": "' || s.name || '", "confidence": 0.7}' || chr(10)
         || 'GLOSS ' || g.plan || ' ON ' || ds.dataset || ' AS $$' || coalesce(b.body, '<the standing body>') || '$$;',
       'SELECT metric, period, value, low, high, sign, basis FROM scenario_points(''' || s.name || ''') WHERE metric = ''' || g.metric || ''''
FROM futures_gaps g CROSS JOIN futures_scenarios s LEFT JOIN futures_estimate e ON e.scenario = s.name AND e.metric = g.metric
LEFT JOIN bodies b ON b.metric = g.plan CROSS JOIN ds
WHERE NOT EXISTS (SELECT 1 FROM owed o WHERE o.subject = 'metric_scenario')
  AND NOT EXISTS (SELECT 1 FROM glossql.ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = g.plan AND r.key = 'plan-' || g.label)
  AND NOT EXISTS (SELECT 1 FROM glossql.open_questions q WHERE q.dataset = ds.dataset AND q.aspect = g.plan AND q.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 13, 'blocked', '', '',
       g.plan || ' at ' || g.label || ' lies ' || g.stands || ' the forecast''s band and the question stands for the human', '', ''
FROM futures_gaps g CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.open_questions q WHERE q.dataset = ds.dataset AND q.aspect = g.plan AND q.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 14, 'done', '', '',
       'the plan stands; the gap on ' || g.plan || ' at ' || g.label || ' carries the human''s ruling', '', ''
FROM futures_gaps g CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM glossql.ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = g.plan AND r.key = 'plan-' || g.label)
UNION ALL
SELECT 'plan', 15, 'done', '', '', 'every plan is served and judged, no plan period lies outside the forecast''s band, or the gap carries a question or a ruling', '', ''
FROM ds
$arms$ AS arms;
