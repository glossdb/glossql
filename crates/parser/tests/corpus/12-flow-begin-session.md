# 12 · Flow: begin session — modelled as a statement sequence

Source: the running system's session pipeline (verified 2026-08-03). Engine
spine: `packages/engine/src/dataraum/worker/workflows.py:516`
(`BeginSessionWorkflow`), cascade at `:1275`. Ordered steps:

| step | kind | produces (today) |
|---|---|---|
| relationships | deterministic | candidate `relationships` rows (overlap, cardinality) |
| semantic_per_table | LLM | `table_entities`, confirmed relationships, `surrogate_key_intents` |
| materialize overlays | deterministic | human relationship teaches folded in |
| surrogate_mint | deterministic | hash column cure for composite keys |
| enriched_views | LLM + grain check | grain-preserving star views |
| slicing / catalogue / hierarchies | hybrid | `slice_definitions`, `column_concepts`, `dimension_hierarchies`, `bus_matrix` |
| aggregation_lineage / correlations | deterministic | cross-fact reconciliation, `derived_columns` |
| session_detect | deterministic | entropy objects on relationship/table/column grain |
| driver_rankings | deterministic | per-measure ranked dimensions |
| promote | deterministic | catalog head flip |

## Transcription

The candidate → verified → declared arc is the flow's spine. Detection is a
MEASUREMENT aspect; verification is an agent reading it; declaration is the
relationship statement:

```glossql
USE fin;

DECLARE ASPECT relationship_candidates WITH $${
  "type": "object",
  "properties": {"candidates": {"type": "array",
    "items": {"type": "object",
      "properties": {"from": {"type": "string"}, "to": {"type": "string"},
                     "cardinality": {"type": "string"},
                     "overlap": {"type": "number"}}}}}
}$$ AS MEASUREMENT ON DATASET;
DECLARE FUNCTION detect_relationships FOR GLOBAL FROM 'functions/relationships.rhai'
  RETURNS relationship_candidates;

SELECT detect_relationships() FROM fin;
SELECT * FROM GLOSSARY(fin, all => true);

DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
```

A rejected candidate is not declared — it stays visible in the measurement
(fixture 07); a human's earlier declarations are already in the log, so
"materialize overlays" is nothing: both actor kinds write the same statement.

Composite keys are tuple endpoints (ruled 2026-08-05, fixture 14 — the
derived-column cure was retired when a live run showed it required the
view surface §3 closes):

```glossql
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
```

Enrichment and the catalog are views plus agent glosses:

```glossql
CREATE VIEW orders_enriched AS
  SELECT o.order_id, o.line_no, o.amount, c.region, c.segment
  FROM orders o JOIN customers c ON o.customer_id = c.id;

GLOSS entity ON orders AS $${
  "value": "sales order", "role": "fact",
  "grain": ["order_id", "line_no"], "time_axis": "order_date"
}$$;
GLOSS dimension ON orders.channel AS $${"priority": 0.8, "context": "primary go-to-market split"}$$;
GLOSS hierarchy ON customers AS $${"levels": ["country", "region", "city"], "kind": "drilldown"}$$;
GLOSS meaning ON orders_enriched AS $${"value": "orders with customer region and segment"}$$;
```

Cross-fact reconciliation and drivers are measurements with witnesses; the
grain check that today drops a bad join is a detector returning a red band on
the view's subject:

```glossql
DECLARE ASPECT reconciliation WITH $${
  "type": "object",
  "properties": {"pairs": {"type": "array"}, "max_delta": {"type": "number"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION reconcile_aggregates FOR fin FROM 'functions/reconcile.rhai'
  RETURNS reconciliation;
DECLARE FUNCTION reconcile_bands FOR fin FROM 'functions/reconcile_bands.rhai';
DECLARE WITNESS reconciliation_w ON reconciliation
  DETECTOR reconcile_bands THRESHOLD 0.5;

SELECT reconcile_aggregates() FROM fin;
SELECT subject, band FROM ATTEST(fin::reconciliation) WHERE band = 'red';

DECLARE ASPECT driver_rankings WITH $${
  "type": "object", "properties": {"rankings": {"type": "array"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION drivers FOR fin FROM 'functions/drivers.rhai'
  RETURNS driver_rankings;
SELECT drivers() FROM fin.orders;
```

## Findings

- **The flow transcribes as measure → read → declare/gloss → attest**, four
  general-purpose moves repeated per plane. No session construct, no phase
  list, no promote statement.
- **The respell surfaced a hidden hole** (2026-08-04): `drivers` had a
  RETURNS schema but no aspect — its output was cache-only, unreachable
  through `GLOSSARY()`. With `RETURNS` an aspect reference the aspect had
  to exist, so the rankings joined the vocabulary. The grain-check witness
  became the model's cleanest form: a measurement aspect with only a
  detector — `DECLARE WITNESS … ON reconciliation DETECTOR reconcile_bands`
  — no `BY`, because nobody glosses a measurement.
- The LLM/deterministic split in the real pipeline maps exactly onto actor
  kinds: every LLM phase becomes an agent connection writing glosses; every
  deterministic phase becomes a function; every teach a human connection.
  Nothing in the flow needed a fourth kind.
- What stays app-side: the cascade trigger, retries and rollback, the
  silent-accept keeper lift, snapshot heads. All versioning surfaces in the
  grammar collapse to cache + cache-row deletion + supersession keys.
- The downstream context assembly for answer agents (ten-block served
  context) is fixture 09's dropped territory — the experiment: skills over
  `GLOSSARY()` / `ATTEST()` reads instead of a serving layer.
