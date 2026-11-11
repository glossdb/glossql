# A view's definition

A view's definition is the SQL that computes a measure or a metric,
with the assumptions its author made. The example defines
`accounts_receivable` over the journal lines: the query, and for each
assumption what it concerns, what it rests on and how confident the
author is.

```glossql
GLOSS accounts_receivable ON fin.journal_lines AS $${
  "sql": "SELECT debit_amount - credit_amount FROM journal_lines JOIN chart_of_accounts USING (account_id) WHERE account_type = 'asset'",
  "assumptions": [
    {"dimension": "sign", "assumption": "ledger stores debits positive",
     "basis": "column_stats", "confidence": 0.9},
    {"dimension": "scope", "assumption": "asset accounts only",
     "basis": "chart_of_accounts", "confidence": 0.95}
  ]
}$$;
```
