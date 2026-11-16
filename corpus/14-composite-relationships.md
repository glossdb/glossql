# Relationships on composite keys

In a table that holds several businesses, a reference is unique only
together with the business id. The example declares three relationships
whose endpoints are column tuples, then annotates each relationship with
the measured referential integrity: orphans and fan-out.

```glossql
USE fin;

DECLARE RELATIONSHIP master_txn.(business_id, payment_method)
  -> payment_methods.(business_id, payment_method);
DECLARE RELATIONSHIP master_txn.(business_id, account)
  -> chart_of_accounts.(business_id, account_name);
DECLARE RELATIONSHIP master_txn.(business_id, product_service)
  -> products_services.(business_id, product_name);
```

```glossql
GLOSS meaning ON master_txn.(business_id, payment_method)
  -> payment_methods.(business_id, payment_method) AS
  $${"value": "0 orphans, 0 fan-out across all 810,059 rows"}$$;
GLOSS meaning ON master_txn.(business_id, account)
  -> chart_of_accounts.(business_id, account_name) AS
  $${"value": "0 orphans; 138,055 rows (17%) hit a duplicated account registration in the master and fan out — the reference is real, the master is dirty"}$$;
GLOSS meaning ON master_txn.(business_id, product_service)
  -> products_services.(business_id, product_name) AS
  $${"value": "41,110 orphan lines, all three service names: Installation and Design absent from the master, Services registered to 2 businesses but used by 25"}$$;
```
