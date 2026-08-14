# 02 · Convention `sign_natural_balance` — TRANSCRIBES (in-blob)

Source: `dataraum-context/packages/dataraum-config/verticals/finance/ontology.yaml`

```yaml
- id: sign_natural_balance
  targets: [extraction, qa]
  statement: >
    Express every monetary measure in its natural-balance direction …
    never normalize only one side of a comparison.
  concept_groups:
    credit_normal: [revenue, accounts_payable, current_liabilities, equity]
    debit_normal: [cost_of_goods_sold, operating_expense, depreciation, tax,
                   accounts_receivable, inventory, current_assets, cash]
```

## Transcription

One FACT aspect on the dataset; conventions are rows in an array-typed blob.
Authored prose stays opaque; the groups the prose refers to are data beside it.

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

## Findings

- **TRANSCRIBES, multiplicity-in-blob.** The `targets` consumer routing that
  the old spec deferred for two sprints lands trivially as a blob field —
  consumers filter it themselves.
- Lost vs. today: declaration-time membership checking of group members
  (engine lint `concept_edge_store.py:78-90`); a dangling group member is
  silent. If it matters, it is a witness's job, not admission's.
- Supersession is per (subject, aspect, actor kind): editing one convention
  re-glosses the whole blob. Accepted — the blob is the unit of authorship.
