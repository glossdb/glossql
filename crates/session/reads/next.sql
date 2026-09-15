-- next — where the record stands toward each goal on the bound
-- dataset, and the one act that moves it, as a statement to send.
--
-- One row per goal — structure, metrics, slices, bands, checks, app,
-- rulings, `goal` their place in that order, since a read's own
-- ORDER BY is not a caller's: `surface`, `step`, `state` (`next`,
-- `blocked`, `done`),
-- `act` (the statement kind, function or read), `say` (the act in a
-- clause), `why` (what on the record decided), `statement` (the act
-- as a statement, filled from the record; `<…>` marks what only the
-- author can fill) and `then` (what follows it).
--
-- A goal's route is its steps in order, each one arm of `steps`
-- below. An arm serves a row when its condition holds on the record —
-- one row per thing the step may act on — and the lowest step that
-- serves decides the goal, the first of its rows by `say`. The order
-- of a route is the goal's preconditions and nothing more; an act
-- that is not a precondition of the goal is not a step. Every route
-- ends on an arm that always holds, so a goal with nothing left is
-- done. Nothing is an order: the goal is the caller's.
--
-- The forms are dollar-quoted, `$f$…$f$`, so a statement carries its
-- `$$` bodies, quotes and semicolons as written. Needs a `USE`: every
-- arm reads `current_dataset`, and an unbound channel serves no row.
--
-- The whole read plans as one query; a heavy read named in several
-- arms plans once per arm (`workspace_next` says what that cost when
-- it was nine arms of scalar subqueries). The reads shared by many
-- arms are the cube's own fact rows and the record's relations.
WITH
ds AS (SELECT dataset FROM current_dataset),
axes AS (SELECT * FROM metric_axes()),

-- the standing grounding of each metric, as written
bodies AS (
  SELECT g.aspect AS metric, g.value AS body
  FROM GLOSSARY() g CROSS JOIN ds
  WHERE g.subject = ds.dataset AND g.state = 'current'
),

-- the table a metric's value comes from: its `value` field's source,
-- else the first source it names
tables AS (
  SELECT metric, t FROM (
    SELECT metric, split_part(source, '.', 1) AS t,
           row_number() OVER (PARTITION BY metric ORDER BY CASE WHEN field = 'value' THEN 0 ELSE 1 END) AS rn
    FROM metric_sources() WHERE source IS NOT NULL
  ) WHERE rn = 1
),

-- the first unadmitted column of an applicable metric per road back
-- in: a verdict to run, a column to serve, a gloss to write
roads AS (
  SELECT a.metric, r.act,
         a.unadmitted[CAST(array_position(a.unadmitted_act, r.act) AS BIGINT)] AS column_name,
         a.unadmitted_why[CAST(array_position(a.unadmitted_act, r.act) AS BIGINT)] AS why
  FROM axes a CROSS JOIN (VALUES ('verdict'), ('unserved'), ('abstained')) AS r(act)
  WHERE a.applicable AND array_has(a.unadmitted_act, r.act)
),

-- the applicable metrics whose frame serves no axis and whose
-- grounding lists none: the unserved columns of each one's table, what
-- a `dimension` gloss or a relevance verdict says on them
axisless AS (
  SELECT metric FROM axes WHERE applicable AND cardinality(dims) = 0 AND axes_basis <> 'authored'
),
served AS (
  SELECT DISTINCT s.metric, split_part(s.source, '.', 2) AS column_name
  FROM metric_sources() s JOIN axisless ON axisless.metric = s.metric
  WHERE s.source IS NOT NULL
),
cols AS (
  SELECT t.metric, t.t, c.column_name, c.ordinal_position
  FROM tables t
  JOIN axisless ON axisless.metric = t.metric
  CROSS JOIN ds
  JOIN information_schema.columns c ON c.table_schema = ds.dataset AND c.table_name = t.t
  LEFT JOIN served ON served.metric = t.metric AND served.column_name = c.column_name
  WHERE served.column_name IS NULL
),
gloss AS (
  SELECT subject, json_get_str(value, 'value') AS stance
  FROM GLOSSARY() WHERE aspect = 'dimension' AND state = 'current'
),
roles AS (
  SELECT subject, json_get_str(value, 'value') AS role_word
  FROM GLOSSARY() WHERE aspect = 'role' AND state = 'current'
),
verdict AS (
  SELECT subject, json_get_bool(value, 'applicable') AS applicable, json_get_float(value, 'relevance') AS relevance
  FROM (
    SELECT m.subject, m.value, row_number() OVER (PARTITION BY m.subject ORDER BY m.computed_at DESC) AS rn
    FROM measurements m JOIN ds ON m.dataset = ds.dataset
    WHERE m.aspect = 'dimension_relevance'
  ) WHERE rn = 1
),
pool AS (
  SELECT cols.metric, cols.t, cols.column_name, cols.ordinal_position,
         gloss.stance, roles.role_word, verdict.applicable, verdict.relevance,
         (gloss.stance IS NOT NULL OR verdict.applicable IS NOT NULL) AS judged
  FROM cols
  LEFT JOIN gloss ON gloss.subject = cols.t || '.' || cols.column_name
  LEFT JOIN roles ON roles.subject = cols.t || '.' || cols.column_name
  LEFT JOIN verdict ON verdict.subject = cols.t || '.' || cols.column_name
),
-- the columns a gloss or an applicable verdict admits: gloss first,
-- then by relevance
admitted AS (
  SELECT metric, column_name,
         CASE stance WHEN 'primary' THEN 0 WHEN 'supporting' THEN 1 ELSE 2 END AS stance_rank,
         coalesce(relevance, 0.0) AS relevance, ordinal_position
  FROM pool
  WHERE stance IN ('primary', 'supporting') OR (stance IS NULL AND applicable)
),
columns AS (
  SELECT metric, array_to_string(array_agg(column_name ORDER BY stance_rank, relevance DESC, ordinal_position), ', ') AS columns
  FROM admitted GROUP BY metric
),
other_axes AS (
  SELECT a.metric, ' — also ' || array_to_string(array_agg(b.metric || ': ' || b.columns ORDER BY b.metric), '; ') AS other_axes
  FROM columns a JOIN columns b ON b.metric <> a.metric
  GROUP BY a.metric
),
-- the unserved columns glossed a dimension that nobody judged
unjudged AS (
  SELECT metric, array_to_string(array_agg(column_name ORDER BY ordinal_position), ', ') AS unjudged
  FROM pool WHERE role_word = 'dimension' AND NOT judged
  GROUP BY metric
),
-- the metric's table when none of its unserved columns is judged
unjudged_table AS (
  SELECT metric, min(t) AS t FROM pool GROUP BY metric HAVING NOT bool_or(judged)
),
-- the detector over each column glossed a dimension and not judged;
-- over every unserved column where nobody judged one
targets AS (
  SELECT metric, t, column_name, ordinal_position, CASE WHEN role_word = 'dimension' THEN 0 ELSE 1 END AS tier
  FROM pool WHERE NOT judged
),
pick AS (SELECT metric, min(tier) AS tier FROM targets GROUP BY metric),
relevance_form AS (
  SELECT targets.metric,
         array_to_string(array_agg('SELECT dimension_relevance() FROM ' || ds.dataset || '.' || targets.t || '.' || targets.column_name ORDER BY targets.ordinal_position), ';' || chr(10)) AS form
  FROM targets JOIN pick ON pick.metric = targets.metric AND pick.tier = targets.tier
  CROSS JOIN ds
  GROUP BY targets.metric
),

-- the applicable metrics, counted and listed
applicable AS (
  SELECT count(*) AS n,
         array_to_string(array_agg(metric ORDER BY metric), ', ') AS metrics,
         array_to_string(array_agg('''' || metric || '''' ORDER BY metric), ', ') AS metric_list
  FROM axes WHERE applicable
),

-- the red band: the metric whose newest complete walked point sits
-- furthest from its corridor's median, and that point's period — what
-- the bands detector scored, read from the recorded walk
walked AS (
  SELECT metric, period, coalesce(partial, false) AS partial, displacement,
         row_number() OVER (PARTITION BY metric ORDER BY point_seq DESC) AS rn
  FROM band_points() WHERE point_seq IS NOT NULL
),
newest AS (
  SELECT a.metric,
         CASE WHEN a.partial THEN b.period ELSE a.period END AS period,
         CASE WHEN a.partial THEN b.displacement ELSE a.displacement END AS displacement
  FROM walked a LEFT JOIN walked b ON b.metric = a.metric AND b.rn = 2
  WHERE a.rn = 1
),
red AS (
  SELECT metric, period FROM newest WHERE displacement IS NOT NULL
  ORDER BY displacement DESC, metric LIMIT 1
),
reds AS (SELECT subject, witness FROM ATTEST() WHERE witness = 'bands_w' AND band = 'red'),

steps AS (

-- ---- structure: the tables landed, the join structure declared and checked

SELECT 'structure' AS surface, 1 AS step, 'next' AS state, 'DECLARE SOURCE' AS act,
       'declare the source the files come from' AS say,
       'no source is declared and no table is landed' AS why,
       $f$DECLARE SOURCE <name> SET (type: parquet, location: '<the root the files sit under>');$f$ AS statement,
       $f$SELECT path, size FROM source_files('<name>')$f$ AS then
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM imports WHERE dataset = ds.dataset) AND NOT EXISTS (SELECT 1 FROM sources)

UNION ALL
SELECT 'structure', 2, 'next', 'DECLARE RECIPE',
       'land the tables from ' || s.name,
       s.name || ' is declared and no table is landed; a recipe lands one table, typed, and nothing more',
       $f$SELECT path, size FROM source_files('$f$ || s.name || $f$')$f$,
       $f$DECLARE RECIPE <table> ON $f$ || ds.dataset || $f$ FROM $f$ || s.name || $f$ AS $$SELECT * FROM read_parquet('<path under the root>')$$;$f$
FROM sources s CROSS JOIN ds
WHERE NOT EXISTS (SELECT 1 FROM imports WHERE dataset = ds.dataset)

UNION ALL
SELECT 'structure', 3, 'next', 'detect_relationships',
       'run the relationship detector',
       'the candidates come from the detector; you judge them',
       'SELECT detect_relationships() FROM ' || ds.dataset,
       $f$SELECT * FROM relationship_candidates('$f$ || ds.dataset || $f$')$f$
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM measurements WHERE dataset = ds.dataset AND function = 'detect_relationships')

UNION ALL
SELECT 'structure', 4, 'next', 'DECLARE RELATIONSHIP',
       'declare the edges you judge from the candidates',
       'no edge is declared; the detector''s candidates stand',
       $f$SELECT * FROM relationship_candidates('$f$ || ds.dataset || $f$')$f$,
       'DECLARE RELATIONSHIP <child>.<column> -> <parent>.<column>;'
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM relationships WHERE dataset = ds.dataset)

UNION ALL
SELECT 'structure', 5, 'next', 'relationship_coherence',
       'run the coherence check over the declared edges',
       'edges declared and never checked: orphan rate, child before parent',
       'SELECT relationship_coherence() FROM ' || ds.dataset,
       ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM measurements WHERE dataset = ds.dataset AND function = 'relationship_coherence')

UNION ALL
SELECT 'structure', 6, 'done', '', '', 'tables landed, edges declared and checked', '', ''
FROM ds

-- ---- metrics: every metric the dataset claims served, or stopped

UNION ALL
SELECT 'metrics', 1, 'next', 'GLOSS definitions',
       'claim ' || a.name || ' for ' || ds.dataset,
       a.name || ' is declared and no dataset claims it: its definitions entry or its grounding makes it this dataset''s',
       $f$GLOSS definitions ON $f$ || ds.dataset || $f$ AS $${"definitions": {"$f$ || a.name || $f$": {"unit": "<unit>", "meaning": "<what it measures, in the business's words>"}}}$$;$f$,
       $f$GLOSS $f$ || a.name || $f$ ON $f$ || ds.dataset || $f$ AS $${"sql": "<the grounding>"}$$;$f$
FROM aspects a CROSS JOIN ds
WHERE a.kind = 'query' AND NOT EXISTS (SELECT 1 FROM metric_surfaces)

UNION ALL
SELECT 'metrics', 2, 'next', 'DECLARE ASPECT query',
       'declare the first concept',
       'no metric is claimed by this dataset: a declared QUERY aspect is this dataset''s once its definitions entry or its grounding stands; the names are the human''s',
       '', ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM metric_surfaces)

UNION ALL
SELECT 'metrics', 3, 'next', 'GLOSS query',
       'ground ' || s.name || ', or stop it',
       s.name || ' is declared and not grounded: a declaration nobody grounds holds this goal — ground it, or stop it with the reason no number is served',
       $f$GLOSS $f$ || s.name || $f$ ON $f$ || ds.dataset || $f$ AS $${
  "sql": "SELECT <date column> AS date, <measure> AS value FROM <table>",
  "assumptions": [{"dimension": "<scope|behavior|definition>", "key": "<stable-key>", "assumption": "<the judgment call, and the reading you rejected>", "basis": "<what it rests on>", "confidence": 0.7}]
}$$;
-- or, where no number should be served: GLOSS $f$ || s.name || $f$ ON $f$ || ds.dataset || $f$ AS $${"stopped": "<what is missing, and why no number is served>"}$$;$f$,
       ''
FROM metric_surfaces s CROSS JOIN ds
WHERE NOT s.grounded AND s.stopped = ''

-- a measure the cube refuses holds the goal with the cube's reason; a
-- relation and a fact serve no number by design, and a stop is the
-- author's word
UNION ALL
SELECT 'metrics', 4, 'next', 'GLOSS query',
       're-record ' || a.metric || ' — ' || split_part(split_part(a.reason, '. ', 1), ': ', 1),
       a.metric || ' is grounded and the cube refuses the grounding — ' || a.reason || '. A grounding the cube cannot serve holds this goal: re-record it, or stop it with the reason no number is served',
       $f$GLOSS $f$ || a.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || b.body || $f$$$;
-- or, where no number should be served: GLOSS $f$ || a.metric || $f$ ON $f$ || ds.dataset || $f$ AS $${"stopped": "<what is missing, and why no number is served>"}$$;$f$,
       ''
FROM axes a
JOIN metric_surfaces s ON s.name = a.metric
JOIN bodies b ON b.metric = a.metric
CROSS JOIN ds
WHERE NOT a.applicable AND cardinality(a.wanted) = 0
  AND s.stopped = '' AND s.kind NOT IN ('relation', 'fact')
  AND coalesce(a.reason, '') <> ''

UNION ALL
SELECT 'metrics', 5, 'done', '', '', 'every metric the dataset claims is served or stopped; the next concept is the human''s to name', '', ''
FROM ds

-- ---- slices: every applicable metric admits an axis

UNION ALL
SELECT 'slices', 1, 'blocked', '', '', 'no applicable metric stands', '', ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable)

UNION ALL
SELECT 'slices', 2, 'next', 'EXTRACT',
       'run ' || a.wanted[1] || ' over ' || a.wanted_over[1],
       a.metric || ' reads ' || a.wanted_over[1] || ' and no verdict stands on it',
       'SELECT ' || a.wanted[1] || '() FROM ' || ds.dataset || '.' || a.wanted_over[1],
       ''
FROM axes a CROSS JOIN ds
WHERE a.applicable AND cardinality(a.wanted) > 0

UNION ALL
SELECT 'slices', 3, 'next', 'temporal',
       're-judge the served columns of ' || a.metric,
       'the verdicts ' || a.metric || ' stands on predate the last change',
       'SELECT temporal() FROM ' || ds.dataset || '.' || coalesce(t.t, '<table>') || '.<date column>;' || chr(10)
         || 'SELECT dimension_relevance() FROM ' || ds.dataset || '.' || coalesce(t.t, '<table>') || '.<served column>',
       ''
FROM axes a LEFT JOIN tables t ON t.metric = a.metric CROSS JOIN ds
WHERE a.applicable AND NOT a.judged_current

UNION ALL
SELECT 'slices', 4, 'next', 'dimension_relevance',
       'judge ' || r.column_name || ' on ' || r.metric,
       r.why,
       'SELECT dimension_relevance() FROM ' || ds.dataset || '.' || coalesce(t.t, '<table>') || '.' || r.column_name,
       $f$GLOSS dimension ON $f$ || ds.dataset || '.' || coalesce(t.t, '<table>') || '.' || r.column_name || $f$ AS $${"value": "supporting"}$$;$f$
FROM roads r LEFT JOIN tables t ON t.metric = r.metric CROSS JOIN ds
WHERE r.act = 'verdict'

UNION ALL
SELECT 'slices', 5, 'next', 'GLOSS query',
       'serve ' || r.column_name || ' in ' || r.metric || ', or drop it from its axes',
       r.why,
       $f$GLOSS $f$ || r.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$,
       ''
FROM roads r LEFT JOIN bodies b ON b.metric = r.metric CROSS JOIN ds
WHERE r.act = 'unserved'

UNION ALL
SELECT 'slices', 6, 'next', 'GLOSS dimension',
       'admit ' || r.column_name || ' on ' || r.metric || ' by gloss, or declare the edge',
       r.why,
       $f$GLOSS dimension ON $f$ || ds.dataset || '.' || coalesce(t.t, '<table>') || '.' || r.column_name || $f$ AS $${"value": "supporting"}$$;$f$,
       ''
FROM roads r LEFT JOIN tables t ON t.metric = r.metric CROSS JOIN ds
WHERE r.act = 'abstained'

-- the frame serves only a date and a value, and a verdict or a gloss
-- admits a column of its table
UNION ALL
SELECT 'slices', 7, 'next', 'GLOSS query',
       're-record ' || c.metric || ' serving one of ' || c.columns || coalesce(o.other_axes, ''),
       'the frame serves only a date and a value; a verdict or a gloss admits ' || c.columns || ' of ' || coalesce(t.t, '<table>')
         || ' as an axis, and the cube slices only on served columns. Serve the axis as the frame''s own column, or a label reached through a declared edge by a LEFT JOIN: the total must hold, and the write''s row says what changed against the standing frame',
       '-- admitted and not served: ' || c.columns || '; the frame''s own column, or a LEFT JOIN to a label — the total must hold. A distinct count or a ratio, which no column slices whole, says "axes": [] instead; a flow serves the axis' || chr(10)
         || $f$GLOSS $f$ || c.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$,
       ''
FROM columns c
LEFT JOIN other_axes o ON o.metric = c.metric
LEFT JOIN tables t ON t.metric = c.metric
LEFT JOIN bodies b ON b.metric = c.metric
CROSS JOIN ds

-- nothing admitted yet: the columns glossed a dimension and never
-- judged, else every unserved column where nobody judged one
UNION ALL
SELECT 'slices', 8, 'next', 'dimension_relevance',
       'judge ' || u.unjudged || ' of ' || coalesce(t.t, '<table>'),
       'the frame serves only a date and a value; ' || u.unjudged || ' carries the dimension role and no verdict',
       f.form,
       $f$GLOSS $f$ || u.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$
FROM unjudged u
JOIN relevance_form f ON f.metric = u.metric
LEFT JOIN tables t ON t.metric = u.metric
LEFT JOIN bodies b ON b.metric = u.metric
CROSS JOIN ds

UNION ALL
SELECT 'slices', 9, 'next', 'dimension_relevance',
       'judge the unserved columns of ' || u.t || ', none is judged',
       'the frame serves only a date and a value, and no unserved column of ' || u.t || ' carries a verdict; the detector admits or abstains, one column per statement, and the line names the admitted',
       f.form,
       $f$GLOSS $f$ || u.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$
FROM unjudged_table u
JOIN relevance_form f ON f.metric = u.metric
LEFT JOIN bodies b ON b.metric = u.metric
CROSS JOIN ds

UNION ALL
SELECT 'slices', 10, 'done', '', '', 'every applicable metric admits an axis, its grounding lists its own, or no judged column of its table can be one', '', ''
FROM ds

-- ---- bands: the bands walk stands and no band is red

UNION ALL
SELECT 'bands', 1, 'blocked', '', '', 'no applicable metric stands', '', ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable)

UNION ALL
SELECT 'bands', 2, 'next', 'metric_bands', 'run the walk', o.why,
       'SELECT metric_bands() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'never measured' AND o.subject = 'metric_bands'

UNION ALL
SELECT 'bands', 3, 'next', 'metric_bands', 'run the walk again', o.why,
       'SELECT metric_bands() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 're-measure' AND o.subject = 'metric_bands'

UNION ALL
SELECT 'bands', 4, 'done', '', '',
       'the walk stands; the red on ' || red.metric || ' at ' || red.period || ' carries the human''s ruling',
       '', ''
FROM red CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM reds)
  AND EXISTS (SELECT 1 FROM ruling_entries r WHERE r.dataset = ds.dataset AND r.aspect = red.metric AND r.key = 'band-' || red.period)

UNION ALL
SELECT 'bands', 5, 'blocked', '', '',
       red.metric || ' is red at ' || red.period || ' and the question stands for the human',
       '', ''
FROM red CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM reds)
  AND EXISTS (SELECT 1 FROM open_questions q WHERE q.dataset = ds.dataset AND q.aspect = red.metric AND q.key = 'band-' || red.period)

-- a shift and a defect breach alike: the judgment is recorded as an
-- assumption under the band's key, appended to the standing body, and
-- the docket asks the human
UNION ALL
SELECT 'bands', 6, 'next', 'band_points',
       'judge ' || red.metric || ', red at ' || red.period,
       'the newest complete point of ' || red.metric || ' sits outside its corridor at ' || red.period
         || '; a shift and a defect breach alike. A defect is the grounding''s to fix; a shift is the human''s to hear — record it as an assumption under the key band-' || red.period || ', and the docket asks',
       $f$SELECT period, actual, p05, p50, p95, pit, partial FROM band_points() WHERE metric = '$f$ || red.metric || $f$' ORDER BY point_seq$f$,
       $f$-- append to "assumptions" of the standing body: {"dimension": "definition", "key": "band-$f$ || red.period || $f$", "assumption": "<a shift at $f$ || red.period || $f$, in the business's words — or the defect and its fix>", "basis": "<what it rests on>", "confidence": 0.7}
GLOSS $f$ || red.metric || $f$ ON $f$ || ds.dataset || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$
FROM red LEFT JOIN bodies b ON b.metric = red.metric CROSS JOIN ds
WHERE EXISTS (SELECT 1 FROM reds)

UNION ALL
SELECT 'bands', 7, 'next', 'band_points',
       'read the walk behind the red verdict on ' || reds.subject,
       reds.witness || ' bands ' || reds.subject || ' red and the walk names no complete point',
       'SELECT metric, period, displacement FROM band_points() ORDER BY displacement DESC',
       ''
FROM reds CROSS JOIN ds

UNION ALL
SELECT 'bands', 8, 'done', '', '', 'the walk stands and no band is red, or the red carries a question or a ruling', '', ''
FROM ds

-- ---- checks: a check of your own stands, every check ran, none is red

UNION ALL
SELECT 'checks', 1, 'next', 'EXTRACT', 're-run ' || o.subject, o.why,
       'SELECT ' || o.subject || '() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 're-measure'

UNION ALL
SELECT 'checks', 2, 'next', 'EXTRACT', 'run ' || o.subject, o.why,
       'SELECT ' || o.subject || '() FROM ' || ds.dataset, ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'never measured'

UNION ALL
SELECT 'checks', 3, 'next', 'ATTEST', 'read the red verdicts',
       v.witness || ' bands ' || v.subject || ' red; the walk''s own witness is the bands goal''s',
       'SELECT subject, aspect, witness, band, score FROM ATTEST(' || ds.dataset || ') WHERE band = ''red'' ORDER BY witness, subject',
       ''
FROM (SELECT subject, witness FROM ATTEST() WHERE band = 'red' AND witness IS DISTINCT FROM 'bands_w') v
CROSS JOIN ds

-- a tie-out you ran by hand is a check that re-runs at every pin move
UNION ALL
SELECT 'checks', 4, 'next', 'DECLARE WITNESS',
       'promote a reconciliation to a standing check',
       'no check is owed and none is yours: a tie-out you ran by hand is a check that re-runs at every pin move',
       $f$DECLARE ASPECT <name>_check WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"}, "breach_rate": {"type": "number"}}
}$$ AS FACT ON DATASET;
DECLARE WITNESS <name>_w ON <name>_check BY (AGENT, HUMAN) DETECTOR rate_tolerance THRESHOLD 0.0;
DECLARE FUNCTION <name>_check_fn FOR $f$ || ds.dataset || $f$ AS $$
  SELECT '<what was compared>' AS outcome, <the violation share, as DOUBLE> AS breach_rate
  FROM <the second route to the number>
$$ RETURNS <name>_check;
GLOSS <name>_check ON $f$ || ds.dataset || $f$ AS $${"outcome": "<what must hold>", "tolerance": <the rate you measured>}$$;$f$,
       'SELECT <name>_check_fn() FROM ' || ds.dataset
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM functions WHERE scope = ds.dataset)

UNION ALL
SELECT 'checks', 5, 'done', '', '', 'your own checks stand, every check ran, none is red', '', ''
FROM ds

-- ---- app: a page over the grounded metrics

UNION ALL
SELECT 'app', 1, 'blocked', '', '', 'no applicable metric stands; an app over nothing is a picture of nothing', '', ''
FROM ds
WHERE NOT EXISTS (SELECT 1 FROM axes WHERE applicable)

-- the page is the proposal made concrete; shape it with the human
-- from there
UNION ALL
SELECT 'app', 2, 'next', 'GLOSS app',
       'write the first page over ' || m.n || ' metrics',
       m.metrics || ' stand and no app does; the page is the proposal made concrete, shape it with the human from there',
       $f$GLOSS app ON review AS $${"title": "$f$ || ds.dataset || $f$ review"}$$;
GLOSS app_frame ON review.series AS $${"sql": "SELECT metric, period, value FROM metric_series(grain => 'month') WHERE dimension = '' AND metric IN ($f$ || m.metric_list || $f$) ORDER BY metric, period"}$$;
GLOSS app_spec ON review.trend AS $${"spec": "{\"$schema\":\"https://vega.github.io/schema/vega-lite/v6.json\",\"data\":{\"name\":\"frame\"},\"mark\":\"line\",\"encoding\":{\"x\":{\"field\":\"period\",\"type\":\"temporal\"},\"y\":{\"field\":\"value\",\"type\":\"quantitative\"},\"color\":{\"field\":\"metric\",\"type\":\"nominal\"}}}"}$$;
GLOSS app_page ON review.index AS $${"html": "{% extends \"shell.html\" %}\n{% import \"modules/tiles.html\" as tiles %}\n{% block main %}\n<div class=\"tiles\">\n  {{ tiles::chart(frame=\"frames/series\", spec=\"specs/trend.vl.json\", title=\"By month\", chip=\"metric_series(grain => 'month')\") }}\n</div>\n{% endblock %}\n"}$$;$f$,
       '-- serves at /' || ds.dataset || '/app/review'
FROM applicable m CROSS JOIN ds
WHERE m.n > 0 AND NOT EXISTS (SELECT 1 FROM app_parts WHERE dataset = ds.dataset)

-- the door serves index.html: a manifest without its page serves
-- nothing yet
UNION ALL
SELECT 'app', 3, 'next', 'GLOSS app_page',
       'write the page of ' || p.app,
       p.app || ' stands without its page: the door serves index.html, so nothing is served yet; the frames and the specs are parts like it',
       $f$GLOSS app_page ON $f$ || p.app || $f$.index AS $${"html": "{% extends \"shell.html\" %}\n{% import \"modules/tiles.html\" as tiles %}\n{% block main %}\n<the page over the app's frames and specs — the glossql-apps skill>\n{% endblock %}\n"}$$;$f$,
       '-- serves at /' || ds.dataset || '/app/' || p.app
FROM (SELECT DISTINCT p.app FROM app_parts p CROSS JOIN ds
      WHERE p.dataset = ds.dataset
        AND NOT EXISTS (SELECT 1 FROM app_parts i WHERE i.dataset = p.dataset AND i.app = p.app AND i.path = 'index.html')) p
CROSS JOIN ds

UNION ALL
SELECT 'app', 4, 'done', '', '', 'an app stands: /' || ds.dataset || '/app/' || p.app, '', ''
FROM (SELECT DISTINCT app FROM app_parts p CROSS JOIN ds WHERE p.dataset = ds.dataset AND p.path = 'index.html') p
CROSS JOIN ds

UNION ALL
SELECT 'app', 5, 'done', '', '', 'an app stands', '', ''
FROM ds

-- ---- rulings: every ruling folded in, nothing owed

UNION ALL
SELECT 'rulings', 1, 'next', 'GLOSS query',
       'fold in the ruling on ' || r.aspect || ' (' || r.key || ': ' || r.stance || ')',
       coalesce(r.note, ''),
       '-- the assumption under key "' || r.key || '" at confidence 1.0 with basis "human-ruled"; an unclear stance is a reformulation under a new key' || chr(10)
         || $f$GLOSS $f$ || r.aspect || $f$ ON $f$ || r.subject || $f$ AS $$$f$ || coalesce(b.body, '<the standing body>') || $f$$$;$f$,
       'SELECT temporal() FROM ' || ds.dataset || '.<table>.<date column>; SELECT metric_bands() FROM ' || ds.dataset
FROM ruling_entries r LEFT JOIN bodies b ON b.metric = r.aspect CROSS JOIN ds
WHERE r.dataset = ds.dataset AND NOT r.folded_in

UNION ALL
SELECT 'rulings', 2, 'next', 'DECLARE RECIPE', 're-declare ' || o.subject || ' as approved', o.why,
       $f$DECLARE RECIPE $f$ || o.subject || $f$ ON $f$ || ds.dataset || $f$ FROM <source> AS $$<the approved sql>$$;$f$,
       ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'recipe'

UNION ALL
SELECT 'rulings', 3, 'next', 'GLOSS formulas', 're-record the formula of ' || o.subject, o.why,
       $f$GLOSS formulas ON $f$ || ds.dataset || $f$ AS $$<the standing map, $f$ || o.subject || $f$ as the human answered>$$;$f$,
       ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'formula'

UNION ALL
SELECT 'rulings', 4, 'next', 'GLOSSARY', 'read the contested slot on ' || o.subject, o.why,
       'SELECT subject, aspect, actor_kind, value, state FROM GLOSSARY(all => true) WHERE subject = ''' || o.subject || '''',
       ''
FROM owed o CROSS JOIN ds
WHERE o.kind = 'contest'

UNION ALL
SELECT 'rulings', 5, 'done', '', '', 'nothing owed', '', ''
FROM ds
),

-- the lowest step that serves decides each goal
first AS (
  SELECT *, row_number() OVER (PARTITION BY surface ORDER BY step, say, why) AS rn FROM steps
)
SELECT CASE surface
         WHEN 'structure' THEN 1 WHEN 'metrics' THEN 2 WHEN 'slices' THEN 3 WHEN 'bands' THEN 4
         WHEN 'checks' THEN 5 WHEN 'app' THEN 6 ELSE 7 END AS goal,
       surface, step, state, act, say, why, statement, then
FROM first
WHERE rn = 1
