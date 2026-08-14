# 10 · Remaining engine artifacts — TRANSCRIBE (coverage completion)

The engine artifacts not covered by fixtures 01–09, each against its real
table shape (engine `schema.sql`).

## Sources, recipes (`sources`, `recipe_hash`)

```glossql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
DECLARE RECIPE segments ON fin FROM crm AS $$SELECT id, segment FROM customer_segments$$;
```

`via:` references engine-held credentials — secrets never appear in
statements. `recipe_hash` needs no clause: statement identity is content hash
(implementation).

## Column + table annotations (`semantic_annotations`, `column_concepts`, `table_entities`)

```glossql
GLOSS meaning ON orders.amount AS $${"value": "gross invoiced amount per order line"}$$;
GLOSS behavior ON orders.amount AS $${"value": "flow"}$$;
GLOSS unit ON orders.amount AS $${"value": "EUR", "source_column": "currency_code"}$$;
GLOSS stored_sign ON journal_lines.amount AS $${"value": "ledger_signed"}$$;
GLOSS type ON orders.amount AS $${"value": "DECIMAL(12,2)"}$$;

GLOSS entity ON orders AS $${
  "value": "sales order", "role": "fact",
  "grain": ["order_id", "line_no"],
  "time_axis": "order_date",
  "identity": ["order_id"]
}$$;
```

(Each aspect above was declared once with `DECLARE ASPECT … AS FACT`; agent
glosses land on agent connections, human corrections supersede on the human
slot.)

## Relationships + composite keys (`relationships`, `surrogate_key_intents`)

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
```

Composite keys are tuple endpoints (ruled 2026-08-05, fixture 14; the
derived-column cure retired with it):

```glossql
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
```

## Dimensions, hierarchies, calendar (`slice_definitions`, `dimension_hierarchies`, `workspace_calendar`)

```glossql
GLOSS dimension ON orders.channel AS $${"priority": 0.8, "context": "primary go-to-market split"}$$;
GLOSS hierarchy ON customers AS $${"levels": ["country", "region", "city"], "kind": "drilldown"}$$;
GLOSS calendar ON fin AS $${"fiscal_year_starts": "april"}$$;
```

## Statistics / profiling (`column_statistics`, `relationship_candidates`)

Measurements are never glossed — a function `RETURNS` the aspect
(respelled 2026-08-04: the reference is the binding, no witness ceremony)
and fills it from its cache:

```glossql
DECLARE ASPECT min_max WITH $${
  "type": "object", "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION profile_min_max FOR GLOBAL FROM 'functions/profile.rhai'
  RETURNS min_max;

SELECT profile_min_max() FROM fin.orders.amount;
SELECT * FROM GLOSSARY(fin.orders.amount);
```

## Enrichment (`enriched_views`)

```glossql
CREATE VIEW orders_enriched AS
  SELECT o.order_id, o.line_no, o.amount, c.region, c.segment
  FROM orders o JOIN customers c ON o.customer_id = c.id;
GLOSS meaning ON orders_enriched AS $${"value": "orders with customer region and segment"}$$;
```

Views are glossable like tables; exposed columns are the select list.

## Findings

- **TRANSCRIBES.** The whole catalog/annotation plane is the aspect/gloss
  sweet spot: one declaration per vocabulary entry, one gloss per application,
  supersession free.
- The vertical binding (`workspace_settings.active_vertical`) has no
  construct: importing a vertical is running its declarations; a vertical is a
  folder (fixture 01).
- Coverage: with fixtures 01–09, every artifact family in the engine schema is
  transcribed, relocated to script, or dropped by a named decision.
