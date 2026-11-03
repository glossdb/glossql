# 14 · Composite relationships — the finance_2 verdict

Source: our own test run (2026-08-05) on the BookSQL export (`finance_2`:
`master_txn` 810,059 rows, seven tables, 27 trading businesses). The first
fixture transcribed from a glossql run rather than a v0.3 artifact. The
detector proposed, the agent judged in full — and could not declare: every
surviving edge is composite on `business_id`, and the grammar's cure
(materialize a key column in a view) required exactly the surface §3
closes. Ruled 2026-08-05: **the tuple is the key**; the derived-column
cure is retired.

## The declarations the run could not make

```glossql
USE fin;

DECLARE RELATIONSHIP master_txn.(business_id, payment_method)
  -> payment_methods.(business_id, payment_method);
DECLARE RELATIONSHIP master_txn.(business_id, account)
  -> chart_of_accounts.(business_id, account_name);
DECLARE RELATIONSHIP master_txn.(business_id, product_service)
  -> products_services.(business_id, product_name);
```

## Grounds ride the pair path

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

## Findings

- **The unscoped anchor legs were not declared.**
  `master_txn.account -> chart_of_accounts.account_name` alone licenses a
  27× fan-out join — the exact over-count the composite exists to
  collapse. A tuple endpoint says the scope; a single-column endpoint on
  multi-tenant data lies.
- **Two candidates judged and rejected** stay visible in the measurement,
  undeclared (fixture 07's rule): customers — 804,290 of 806,495 sale
  lines fail under the right business; vendors — every ledger name exists
  *somewhere* in the master, but only 12 of 2,214 resolve in-business,
  below the ~3.7% chance rate for 27 businesses (a shared name-generator
  artifact, not a reference).
- **Reads:** composite pairs surface in the table sweep
  (`GLOSSARY(master_txn)`) and by `WHERE subject = '…'` on the subject
  text. Substrate SQL spells no tuples inside `GLOSSARY()` — the pair
  path's home is the statement language, its disclosure the sweep.
