# Data catalog: sources, annotations, relationships, dimensions, statistics

A data catalog holds where the data comes from, what each column and
table means, how tables join, which columns are dimensions and what the
data's statistics are. The examples write each of these.

## Sources and load queries

A source names its credentials with `via:`; the secret is not part of
the statement.

```glossql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
DECLARE RECIPE segments ON fin FROM crm AS $$SELECT id, segment FROM customer_segments$$;
```

## Column and table annotations

```glossql
GLOSS meaning ON orders.amount AS $${"value": "gross invoiced amount per order line"}$$;
GLOSS additivity ON orders.amount AS $${"value": "additive"}$$;
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

## Relationships

```glossql
DECLARE RELATIONSHIP orders.customer_id -> customers.id;
DECLARE RELATIONSHIP invoices.order_id <-> orders.id;
```

A composite key is a tuple on each side:

```glossql
DECLARE RELATIONSHIP txn.(business_id, account) -> coa.(business_id, account_name);
```

## Dimensions, hierarchies, calendar

```glossql
GLOSS dimension ON orders.channel AS $${"priority": 0.8, "context": "primary go-to-market split"}$$;
GLOSS hierarchy ON customers AS $${"levels": ["country", "region", "city"], "kind": "drilldown"}$$;
GLOSS calendar ON fin AS $${"fiscal_year_starts": "april"}$$;
```

## Statistics

A statistic is measured by a function, not annotated. The function
returns the aspect it fills (`RETURNS`).

```glossql
DECLARE ASPECT min_max WITH $${
  "type": "object", "properties": {"min": {}, "max": {}}
}$$ AS MEASUREMENT;
DECLARE FUNCTION profile_min_max FOR GLOBAL AS $$/* min/max/nulls/distincts per column, from kernels */$$
  RETURNS min_max;

SELECT profile_min_max() FROM fin.orders.amount;
SELECT * FROM GLOSSARY(fin.orders.amount);
```

## Views

A view is annotated like a table.

```glossql
CREATE VIEW orders_enriched AS
  SELECT o.order_id, o.line_no, o.amount, c.region, c.segment
  FROM orders o JOIN customers c ON o.customer_id = c.id;
GLOSS meaning ON orders_enriched AS $${"value": "orders with customer region and segment"}$$;
```
