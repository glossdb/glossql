# Glossary entry: revenue

A business glossary entry names a measure, describes it and lists the
column names that point to it. The example declares `revenue` as a view
property (`AS VIEW`), so a dataset can later define it in SQL; the entry's
other fields are annotations in the schema.

```glossql
DECLARE PROPERTY revenue WITH $${
  "title": "revenue",
  "description": "Income from sales or services",
  "x-kind": "measure",
  "x-indicators": ["revenue", "sales", "income", "turnover", "receipts"],
  "x-exclude": ["cost", "expense"],
  "x-unit-from": "currency"
}$$ AS VIEW;
```

A measure made of other measures names its parts in the same schema:

```glossql
DECLARE PROPERTY current_assets WITH $${
  "title": "current_assets",
  "x-kind": "measure",
  "x-parts": ["cash", "accounts_receivable", "inventory"]
}$$ AS VIEW;
```
