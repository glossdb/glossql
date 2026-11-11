# Flow: relationships, dimensions and reconciliation

After the tables are loaded, the model of the dataset is built: how the
tables join, what each table is, which columns are dimensions, and
whether aggregates reconcile with their detail. The example walks that
sequence.

## Detect candidates, then declare relationships

A function measures candidate relationships. The agent reads them and
declares the ones that hold.

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
DECLARE FUNCTION detect_relationships FOR GLOBAL AS $$/* candidate edges by containment and cardinality */$$
  RETURNS relationship_candidates;

SELECT detect_relationships() FROM fin;
SELECT * FROM GLOSSARY(fin, all => true);

DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
```

A composite key is a tuple on each side:

```glossql
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
```

## Views, entities, dimensions, hierarchies

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

## Reconciliation and drivers

Both are measurements. The reconciliation has a test whose scoring
function reports a failing state when an aggregate does not match its
detail.

```glossql
DECLARE ASPECT reconciliation WITH $${
  "type": "object",
  "properties": {"pairs": {"type": "array"}, "max_delta": {"type": "number"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION reconcile_aggregates FOR fin AS $$/* aggregate against its detail, residual per key */$$
  RETURNS reconciliation;
DECLARE FUNCTION reconcile_states FOR fin AS $$/* detector: gives the reconciliation values a state */$$;
DECLARE TEST reconciliation_test ON reconciliation
  DETECTOR reconcile_states THRESHOLD 0.5;

SELECT reconcile_aggregates() FROM fin;
SELECT subject, state FROM TESTS(fin::reconciliation) WHERE state = 'red';

DECLARE ASPECT driver_rankings WITH $${
  "type": "object", "properties": {"rankings": {"type": "array"}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION drivers FOR fin AS $$/* which dimensions move the measure */$$
  RETURNS driver_rankings;
SELECT drivers() FROM fin.orders;
```
