# Convention: natural-balance sign

An accounting convention is a rule every query over the ledger follows:
here, that each monetary measure is read in its natural-balance
direction. The example declares one aspect that holds a dataset's
conventions as a list and annotates the dataset `fin` with the rule and
the two account groups it refers to.

```glossql
DECLARE ASPECT conventions WITH $${
  "type": "object",
  "properties": {
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "statement"],
        "properties": {
          "id": {"type": "string"},
          "statement": {"type": "string"},
          "targets": {"type": "array", "items": {"type": "string"}},
          "groups": {"type": "object"}
        }
      }
    }
  }
}$$ AS FACT ON DATASET;

GLOSS conventions ON fin AS $${
  "items": [{
    "id": "sign_natural_balance",
    "statement": "Express every monetary measure in its natural-balance direction: credit_normal concepts as credits, debit_normal as debits - never normalize only one side of a comparison.",
    "targets": ["extraction", "qa"],
    "groups": {
      "credit_normal": ["revenue", "accounts_payable", "current_liabilities", "equity"],
      "debit_normal": ["cost_of_goods_sold", "operating_expense", "depreciation",
                       "tax", "accounts_receivable", "inventory", "current_assets", "cash"]
    }
  }]
}$$;
```
