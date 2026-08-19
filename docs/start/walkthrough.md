# Walkthrough — from exports to the docket

One continuous session over a service company's exports: work orders
and the sites they serve, as parquet files. Everything below runs
through any door; an agent on the MCP door is the usual driver, with a
person answering the questions only a person can. Statements are
glossql; plain SQL runs on the same surface.

## Enter the workspace

A fresh workspace is not empty — the measurement library and the
semantic vocabulary (the KPI kit) are declared at boot. Read before
declaring:

```glossql
SELECT * FROM datasets;
SELECT name, kind FROM aspects;
```

## Agree the topic, declare the dataset

A dataset has a topic, and the topic is what makes later choices
decidable — which tables to land, which metrics to propose. That
agreement is conversation; the declaration records it:

```glossql
DECLARE DATASET ops SET (purpose: 'service delivery — what gets done, how fast, and where it stalls');
USE ops;
```

## Register the source, bank its conventions

A source names a root directory; globs belong to recipe SQL. What is
learned about a source system — placeholder dates, format warts —
lands at source grain, where every dataset in the workspace reads it:

```glossql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
GLOSS conventions ON erp_export AS $${
  "placeholder_date": "1900-01-01 stands for unset",
  "timestamp_format": "%b %e %Y %I:%M%p, month names mixed-language"
}$$;
```

## Rehearse, then land

A `LIMIT 0` probe still carries every `(name, type)` — the schema a
recipe would stamp, before anything lands. Typing is authored: the
recipe carries the casts and the named column list, and the landed
table is the typed table. A failed cast lands NULL, and the
declaration's outcome carries the cast account — how many cells each
`try_*` nulled and the top such values, to judge, amend
(`NULLIF` before the cast), and re-declare.

```glossql
PROBE erp_export AS $$SELECT order_id,
       try_cast(duration_min AS DOUBLE) AS duration_min,
       try_to_timestamp(completed_at, '%Y-%m-%d %H:%M:%S') AS completed_at
FROM read_parquet('work_orders/*.parquet') LIMIT 0$$;

DECLARE RECIPE work_orders ON ops FROM erp_export AS $$
  SELECT order_id, site_id, status,
         try_cast(duration_min AS DOUBLE) AS duration_min,
         try_to_timestamp(completed_at, '%Y-%m-%d %H:%M:%S') AS completed_at
  FROM read_parquet('work_orders/*.parquet')$$;

DECLARE RECIPE sites ON ops FROM erp_export AS $$
  SELECT id, region, service_line
  FROM read_parquet('sites/*.parquet')$$;

DESCRIBE work_orders;
```

A dataset is a curated working set for its topic, never a mirror of
the export — land the tables the metrics need, take the columns the
SELECT list earns.

## Say what each table is

Before the columns: what one row is, whether the table is fact or
dimension, the row-identifying grain (verified — `COUNT(*)` against
`COUNT(DISTINCT …)` must agree), the one time axis.

```glossql
GLOSS entity ON work_orders AS $${"value": "one site visit on a work order", "role": "fact",
  "grain": ["order_id"], "time_axis": "completed_at"}$$;
```

## Measure before judging

The shipped functions answer what statistics can answer. An extraction
computes at the read's pin, lands a `measurements` row, and serves the
same row back until an input moves:

```glossql
SELECT profile(), outliers() FROM ops.work_orders.duration_min;
SELECT temporal() FROM ops.work_orders.completed_at;
```

Detection functions over-produce by design — candidates with evidence,
never conclusions. Reading a measurement is a judging job.

## Judge the join structure, declare what survives

`detect_relationships` proposes; anti-joins in both directions decide.
Orphans that are exactly a business population confirm an edge; random
misses argue against it. The verdict is recorded on the edge:

```glossql
DECLARE RELATIONSHIP work_orders.site_id -> sites.id;
GLOSS meaning ON work_orders.site_id -> sites.id AS
  $${"value": "each order serves one site; the orphans are the cancelled orders, never dispatched"}$$;
```

Then the grain check, before trusting any join — counts before and
after must be equal, exactly, or the join multiplies every downstream
aggregate:

```sql
SELECT count(*) FROM work_orders;
SELECT count(*) FROM work_orders w JOIN sites s ON w.site_id = s.id;
```

## Gloss the vocabulary

Role first — the rest of a column's obligations derive from it. A
stock/flow verdict is never asserted from a name: `behavior_evidence`
reconciles the column against period movements over the declared
edges, and the gloss cites it.

```glossql
GLOSS meaning ON work_orders.duration_min AS $${"value": "on-site working time per visit, in minutes"}$$;
GLOSS role ON work_orders.duration_min AS $${"value": "measure"}$$;
GLOSS behavior ON work_orders.duration_min AS $${"value": "flow"}$$;
GLOSS unit ON work_orders.duration_min AS $${"value": "minutes"}$$;
```

## Ground the metric

One QUERY aspect per concept. The handbook content — meaning, unit,
owner — lives in the `definitions` gloss, where a correction
supersedes with actor and timestamp; the grounding is the semantic
core, row-grain, no GROUP BY, mechanics as comments in the SQL and
judgment in the assumptions. Every disclosed assumption carries a
`key` — the claim's identity, what rulings join on.

```glossql
DECLARE ASPECT throughput WITH $${"title": "Throughput", "x-kind": "measure"}$$ AS QUERY ON DATASET;

GLOSS definitions ON ops AS $${"definitions": {
  "throughput": {"meaning": "work completed and accepted; counted at completion date",
                 "unit": "hours", "owner": "Operations", "source": "KPI handbook v3 §2"}
}}$$;

GLOSS throughput ON ops AS $${
  "sql": "-- completed work: hours per closed order, at completion date, with its judged axes\nSELECT w.completed_at AS date, w.duration_min / 60.0 AS value, s.region, s.service_line FROM work_orders w JOIN sites s ON w.site_id = s.id WHERE w.status = 'closed'",
  "assumptions": [
    {"dimension": "scope", "key": "closed-only", "assumption": "closed orders only; cancelled and reopened excluded", "basis": "status values + judgment", "confidence": 0.7},
    {"dimension": "behavior", "key": "throughput-is-a-flow", "assumption": "a flow: sums valid over any partition", "basis": "behavior_evidence on work_orders.duration_min", "confidence": 1.0}
  ]
}$$;
```

The grounding serves at any reader's grain through `read.<aspect>()`,
human slot outranking agent — a human answer is what runs:

```sql
SELECT date_trunc('month', date) AS month, sum(value) AS hours
FROM read.throughput() GROUP BY date_trunc('month', date)
ORDER BY month
```

## Stand up a validation

The expectation is authored, never assumed zero — a source with known
dirt expects its own breach rate. The check is a function voice on the
same aspect; a detector bands across both; `ATTEST` is the verdict
surface. The shipped `rate_tolerance` detector is one-sided (green at
or under the tolerance); a known-dirt source that must also catch
overcleaning declares its own detector that goes red on both sides.

```glossql
DECLARE ASPECT duration_present WITH $${
  "type": "object", "required": ["outcome"],
  "properties": {"outcome": {"type": "string"}, "tolerance": {"type": "number"},
                 "breach_rate": {"type": "number"}}
}$$ AS FACT ON TABLE WHEN entity = 'one site visit on a work order';
GLOSS duration_present ON work_orders AS $${
  "outcome": "Closed orders carry a positive on-site duration.", "tolerance": 0.01}$$;
DECLARE WITNESS duration_w ON duration_present BY (AGENT, HUMAN)
  DETECTOR rate_tolerance THRESHOLD 0.01;

DECLARE FUNCTION duration_present_check FOR ops AS $$
  SELECT 'measured: closed orders against positive durations' AS outcome,
         CASE WHEN count(*) = 0 THEN 0.0
              ELSE CAST(count(*) FILTER (WHERE duration_min IS NULL OR duration_min <= 0) AS DOUBLE) / count(*)
         END AS breach_rate
  FROM work_orders WHERE status = 'closed'
$$ RETURNS duration_present;

SELECT subject, band, score FROM ATTEST(ops::duration_present);
```

## The surfaces

The cube computes each metric's monthly series and slices; the docket
and any app chart it through `metric_series()`:

```sql
SELECT metric, title, unit, meaning, period, value, axes, formula
FROM metric_surfaces ORDER BY metric
```

```sql
SELECT metric, period, value FROM metric_series()
WHERE dimension = '' ORDER BY metric, period
```

## The question round

Everything above left a trail of assumptions below full confidence.
Those are the questions — derived from the record, not kept in a
queue:

```sql
SELECT aspect, dimension, key, assumption, conf
FROM open_questions ORDER BY conf ASC;
```

On the MCP door they arrive as forms on the calls that read the
record; on the docket they are the open page. An answer lands as a **ruling** — the judgment
alone, in the human's slot — and the question retires by derivation.
The agent folds each ruling back into its grounding at full
confidence, and the standing record reads back:

```sql
SELECT aspect, key, stance, folded_in FROM ruling_entries ORDER BY written_at DESC
```

## The docket

Open `http://127.0.0.1:8080/app/docket`: what stands open to judge,
what has been settled, what waits on an act — with the metric surfaces
and the record behind them. The ruling form there is the same write
the question round lands; a person who stepped away has a way back
into the record.

## Where the workspace stands

```sql
SELECT surface, how, stands, open FROM workspace_next ORDER BY open DESC;
```

Every surface the workspace can be extended through, what stands and
what is open on each. It reports state, never an order — what to do
next is judgment.
